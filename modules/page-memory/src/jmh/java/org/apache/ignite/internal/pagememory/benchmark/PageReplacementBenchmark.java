/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.pagememory.benchmark;

import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_DATA;

import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.TestPageIoModule.TestSimpleValuePageIo;
import org.apache.ignite.internal.pagememory.configuration.ReplacementMode;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState;
import org.apache.ignite.internal.util.Constants;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.infra.ThreadParams;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmark for page cache replacement policies (CLOCK, SEGMENTED_LRU, RANDOM_LRU).
 *
 * <p>Tests how well each policy handles page evictions when the working set is larger than
 * available memory. Uses a realistic access pattern where some pages are hot (frequently accessed)
 * and others are cold.
 *
 * <p>Limitations: read-only workload, checkpoint lock held during measurements, cache is pre-warmed.
 */
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
@State(Scope.Benchmark)
public class PageReplacementBenchmark extends PersistentPageMemoryBenchmarkBase {
    private static final long SMALL_REGION_SIZE = 20L * Constants.MiB;
    private static final int PAGE_SIZE = Config.DEFAULT_PAGE_SIZE;
    private static final long REGION_CAPACITY_PAGES = SMALL_REGION_SIZE / PAGE_SIZE;
    private static final long MAX_WORKING_SET_SIZE = 1_000_000;

    /** 0.99 = very skewed, most accesses hit few pages. */
    private static final double ZIPFIAN_SKEW = 0.99;

    private static final long BASE_SEED = 42L;

    /** Different seed for warmup to avoid biasing results. */
    private static final long WARMUP_SEED = BASE_SEED + 999999L;

    private static final int PARTITION_COUNT = 16;

    private static final double WARMUP_MULTIPLIER = 1.1;

    private static final double MIN_WORKING_SET_RATIO = 0.1;

    private static final int CHECKPOINT_TIMEOUT_SECONDS = 30;

    @Param({"CLOCK", "SEGMENTED_LRU", "RANDOM_LRU"})
    public ReplacementMode replacementModeParam;

    @Param({"LOW", "MEDIUM", "HIGH"})
    public CachePressure cachePressure;

    private long[] pageIds;

    private int workingSetSize;

    private volatile MetricsSnapshot beforeMetrics;

    // Use same seed across runs so all policies get the same partition distribution.
    private final Random partitionRandom = new Random(BASE_SEED);

    /** How much bigger the working set is compared to cache size. */
    public enum CachePressure {
        LOW(1.2),
        MEDIUM(2.0),
        HIGH(4.0);

        private final double multiplier;

        CachePressure(double multiplier) {
            this.multiplier = multiplier;
        }

        int computeWorkingSetSize(long capacity) {
            long result = Math.round(capacity * multiplier);
            validateWorkingSetSize(result);
            return (int) result;
        }

        private static void validateWorkingSetSize(long workingSetSize) {
            assert workingSetSize <= Integer.MAX_VALUE : "Working set too large: " + workingSetSize;

            if (workingSetSize > MAX_WORKING_SET_SIZE) {
                throw new IllegalStateException(String.format(
                        "Working set too large: %,d pages (max: %,d). "
                                + "This would require %,d GB of actual page memory and %,d MB for pageId array. "
                                + "Reduce cache pressure or region size.",
                        workingSetSize,
                        MAX_WORKING_SET_SIZE,
                        (workingSetSize * PAGE_SIZE) / Constants.GiB,
                        (workingSetSize * 8L) / Constants.MiB
                ));
            }

            long minWorkingSetSize = Math.round(REGION_CAPACITY_PAGES * MIN_WORKING_SET_RATIO);
            if (workingSetSize < minWorkingSetSize) {
                throw new IllegalStateException(String.format(
                        "Benchmark not properly initialized: workingSetSize=%,d < minimum %,d (%.0f%% of capacity)",
                        workingSetSize, minWorkingSetSize, MIN_WORKING_SET_RATIO * 100
                ));
            }
        }
    }

    /** Each thread has its own access pattern. */
    @State(Scope.Thread)
    public static class ThreadState {
        private ZipfianDistribution zipfianDistribution;
        private boolean checkpointLockAcquired;
        private int threadIndex;
        private PageReplacementBenchmark benchmark;

        /** Setup trial. */
        @Setup(Level.Trial)
        public void setupTrial(ThreadParams threadParams, PageReplacementBenchmark benchmark) {
            this.benchmark = benchmark;
            this.threadIndex = threadParams.getThreadIndex();

            // Give each thread a different seed so they don't all access the same pages.
            long threadSeed = BASE_SEED + (threadIndex * 1000003L);

            this.zipfianDistribution = new ZipfianDistribution(
                    benchmark.workingSetSize,
                    ZIPFIAN_SKEW,
                    threadSeed
            );
        }

        /**
         * Acquire checkpoint lock before iteration to keep it out of measurements.
         */
        @Setup(Level.Iteration)
        public void setupIteration() {
            benchmark.checkpointManager().checkpointTimeoutLock().checkpointReadLock();
            checkpointLockAcquired = true;
        }

        /** Release checkpoint lock. */
        @TearDown(Level.Iteration)
        public void tearDownIteration() {
            if (checkpointLockAcquired) {
                benchmark.checkpointManager().checkpointTimeoutLock().checkpointReadUnlock();
                checkpointLockAcquired = false;
            }
        }

        int nextZipfianIndex() {
            return zipfianDistribution.next();
        }

        int threadIndex() {
            return threadIndex;
        }
    }

    private static final class MetricsSnapshot {
        final long hits;
        final long misses;
        final long replacements;

        MetricsSnapshot(long hits, long misses, long replacements) {
            this.hits = hits;
            this.misses = misses;
            this.replacements = replacements;
        }
    }

    /** Capture metrics before iteration. */
    @Setup(Level.Iteration)
    public void setupIteration() {
        beforeMetrics = captureMetrics();
    }

    /** Print metrics delta after iteration. */
    @TearDown(Level.Iteration)
    public void tearDownIteration() {
        MetricsSnapshot afterMetrics = captureMetrics();
        printMetricsDelta(beforeMetrics, afterMetrics);
    }

    private void printMetricsDelta(MetricsSnapshot before, MetricsSnapshot after) {
        long hits = after.hits - before.hits;
        long misses = after.misses - before.misses;
        long replacements = after.replacements - before.replacements;

        long total = hits + misses;
        double hitRate = total > 0 ? (hits * 100.0 / total) : 0.0;

        System.out.printf("[%s/%s] Hits: %,d | Misses: %,d | Hit Rate: %.1f%% | Replacements: %,d%n",
                replacementModeParam, cachePressure, hits, misses, hitRate, replacements);
    }

    private MetricsSnapshot captureMetrics() {
        return new MetricsSnapshot(
                persistentPageMemory().metrics().cacheHits(),
                persistentPageMemory().metrics().cacheMisses(),
                persistentPageMemory().metrics().replacements()
        );
    }

    /**
     * Prepare benchmark infrastructure and data.
     */
    @Setup
    public void setup(Blackhole blackhole) throws Exception {
        workingSetSize = cachePressure.computeWorkingSetSize(REGION_CAPACITY_PAGES);
        Config infrastructureConfig = Config.builder()
                .regionSize(SMALL_REGION_SIZE)
                .pageSize(PAGE_SIZE)
                .replacementMode(replacementModeParam)
                .partitionsCount(PARTITION_COUNT)
                .build();
        setup(infrastructureConfig);
        prepareWorkingSet();

        warmupCache(blackhole);
        validateCacheWarmed();
    }

    /**
     * Allocates pages in batches to avoid OOM. Checkpoints between batches to flush dirty pages.
     */
    private void prepareWorkingSet() throws Exception {
        pageIds = new long[workingSetSize];

        // Allocate 80% of capacity at a time to avoid hitting dirty pages limit.
        int batchSize = (int) Math.round(REGION_CAPACITY_PAGES * 0.8);

        TestSimpleValuePageIo pageIo = new TestSimpleValuePageIo();

        for (int batchStart = 0; batchStart < workingSetSize; batchStart += batchSize) {
            int batchEnd = Math.min(batchStart + batchSize, workingSetSize);

            checkpointManager().checkpointTimeoutLock().checkpointReadLock();
            try {
                // Distribute pages randomly across partitions.
                for (int i = batchStart; i < batchEnd; i++) {
                    int partitionId = partitionRandom.nextInt(PARTITION_COUNT);
                    pageIds[i] = persistentPageMemory().allocatePage(null, GROUP_ID, partitionId, FLAG_DATA);
                }

                for (int i = batchStart; i < batchEnd; i++) {
                    long pageId = pageIds[i];
                    writePage(pageId, pageIo);
                }
            } finally {
                checkpointManager().checkpointTimeoutLock().checkpointReadUnlock();
            }

            if (batchEnd < workingSetSize) {
                flushDirtyPages();
            }
        }

        flushDirtyPages();
    }

    private void flushDirtyPages() throws Exception {
        CheckpointProgress progress = checkpointManager().forceCheckpoint("Flush dirty pages after allocation");

        try {
            progress.futureFor(CheckpointState.FINISHED).get(CHECKPOINT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException | ExecutionException e) {
            long dirtyPages = persistentPageMemory().dirtyPagesCount();
            int usedCpBuf = persistentPageMemory().usedCheckpointBufferPages();
            int maxCpBuf = persistentPageMemory().maxCheckpointBufferPages();

            throw new IllegalStateException(String.format(
                    "Checkpoint failed [dirtyPages=%,d, checkpointBuffer=%d/%d]. "
                            + "Check disk health and system logs.",
                    CHECKPOINT_TIMEOUT_SECONDS, dirtyPages, usedCpBuf, maxCpBuf
            ), e);
        }
    }

    private void warmupCache(Blackhole blackhole) throws IgniteInternalCheckedException {
        int warmupPages = (int) Math.round(REGION_CAPACITY_PAGES * WARMUP_MULTIPLIER);
        ZipfianDistribution warmupDistribution = new ZipfianDistribution(
                workingSetSize,
                ZIPFIAN_SKEW,
                WARMUP_SEED
        );

        checkpointManager().checkpointTimeoutLock().checkpointReadLock();
        try {
            for (int i = 0; i < warmupPages; i++) {
                int index = warmupDistribution.next();
                long pageId = pageIds[index];

                accessPageReadOnly(pageId, 0, blackhole);
            }
        } finally {
            checkpointManager().checkpointTimeoutLock().checkpointReadUnlock();
        }
    }

    private void validateCacheWarmed() {
        long loadedPages = persistentPageMemory().loadedPages();
        double capacityUtilization = (loadedPages * 100.0) / REGION_CAPACITY_PAGES;

        System.out.printf("Loaded pages after warmup: %d (%.1f%% full)", loadedPages, capacityUtilization);

        if (capacityUtilization < 95.0) {
            throw new IllegalStateException(String.format(
                    "Cache not sufficiently warmed: loaded %,d pages but capacity is %,d (%.1f%% full). "
                            + "Benchmark requires >=95%% capacity to measure replacement behavior.",
                    loadedPages, REGION_CAPACITY_PAGES, capacityUtilization
            ));
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Threads(1)
    public void zipfianThroughputSingleThread(PageReplacementBenchmark.ThreadState state, Blackhole blackhole)
            throws IgniteInternalCheckedException {
        benchmarkIteration(state, blackhole);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Threads(4)
    public void zipfianThroughputFourThreads(ThreadState state, Blackhole blackhole)
            throws IgniteInternalCheckedException {
        benchmarkIteration(state, blackhole);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @Threads(1)
    public void zipfianLatencySingleThread(ThreadState state, Blackhole blackhole)
            throws IgniteInternalCheckedException {
        benchmarkIteration(state, blackhole);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @Threads(4)
    public void zipfianLatencyFourThreads(ThreadState state, Blackhole blackhole)
            throws IgniteInternalCheckedException {
        benchmarkIteration(state, blackhole);
    }

    private void benchmarkIteration(ThreadState state, Blackhole blackhole) throws IgniteInternalCheckedException {
        int index = state.nextZipfianIndex();
        long pageId = pageIds[index];
        accessPageReadOnly(pageId, state.threadIndex(), blackhole);
    }

    private void accessPageReadOnly(long pageId, int threadIndex, Blackhole blackhole)
            throws IgniteInternalCheckedException {
        long page = persistentPageMemory().acquirePage(GROUP_ID, pageId);

        if (page == 0) {
            throw new IllegalStateException(String.format(
                    "Failed to acquire page [pageId=0x%x, policy=%s, pressure=%s, thread=%s (index=%d), "
                            + "loadedPages=%,d/%,d]",
                    pageId, replacementModeParam, cachePressure,
                    Thread.currentThread().getName(), threadIndex,
                    persistentPageMemory().loadedPages(), REGION_CAPACITY_PAGES
            ));
        }

        try {
            blackhole.consume(page);
        } finally {
            persistentPageMemory().releasePage(GROUP_ID, pageId, page);
        }
    }

    private void writePage(long pageId, PageIo pageIo) throws IgniteInternalCheckedException {
        long page = persistentPageMemory().acquirePage(GROUP_ID, pageId);
        try {
            long pageAddr = persistentPageMemory().writeLock(GROUP_ID, pageId, page);
            try {
                pageIo.initNewPage(pageAddr, pageId, PAGE_SIZE);
                TestSimpleValuePageIo.setLongValue(pageAddr, pageId);
            } finally {
                persistentPageMemory().writeUnlock(GROUP_ID, pageId, page, true);
            }
        } finally {
            persistentPageMemory().releasePage(GROUP_ID, pageId, page);
        }
    }

    /** Run benchmark from IDE or command line. */
    public static void main(String[] args) throws Exception {
        Options opts = new OptionsBuilder()
                .include(PageReplacementBenchmark.class.getSimpleName())
                .build();

        new Runner(opts).run();
    }
}
