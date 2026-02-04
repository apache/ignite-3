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
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState;
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
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 10, time = 2)
@Fork(1)
@State(Scope.Benchmark)
public class PageCacheReplacementBenchmark extends PersistentPageMemoryBenchmarkBase {

    private static final long SMALL_REGION_SIZE = 20L * 1024 * 1024;

    private static final int EXPECTED_PAGE_SIZE = 4 * 1024;

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

    private long regionCapacityPages;

    private int workingSetSize;

    private volatile MetricsSnapshot beforeMetrics;

    /** Prevents JIT from removing warmup reads. */
    @SuppressWarnings("unused")
    private volatile long warmupAccumulator;

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
            assert result <= Integer.MAX_VALUE : "Working set too large: " + result;
            return (int) result;
        }
    }

    /** Each thread has its own access pattern. */
    @State(Scope.Thread)
    public static class ThreadState {
        private ZipfianDistribution zipfianDistribution;
        private boolean checkpointLockAcquired;
        private int threadIndex;
        private PageCacheReplacementBenchmark benchmark;

        /** Setup trial. */
        @Setup(Level.Trial)
        public void setupTrial(ThreadParams threadParams, PageCacheReplacementBenchmark benchmark) {
            if (benchmark == null) {
                throw new IllegalStateException("Benchmark instance is null - JMH contract violated");
            }

            if (benchmark.checkpointManager == null) {
                throw new IllegalStateException(
                        "CheckpointManager is null - benchmark.setup() did not run or failed"
                );
            }

            this.benchmark = benchmark;
            this.threadIndex = threadParams.getThreadIndex();

            long minWorkingSetSize = Math.round(benchmark.regionCapacityPages * MIN_WORKING_SET_RATIO);
            if (benchmark.workingSetSize < minWorkingSetSize) {
                throw new IllegalStateException(String.format(
                        "Benchmark not properly initialized: workingSetSize=%,d < minimum %,d (%.0f%% of capacity)",
                        benchmark.workingSetSize, minWorkingSetSize, MIN_WORKING_SET_RATIO * 100
                ));
            }

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
            if (benchmark == null || benchmark.checkpointManager == null) {
                throw new IllegalStateException(
                        "ThreadState not properly initialized - setupTrial() did not run"
                );
            }

            benchmark.checkpointManager.checkpointTimeoutLock().checkpointReadLock();
            checkpointLockAcquired = true;
        }

        /** Release checkpoint lock. */
        @TearDown(Level.Iteration)
        public void tearDownIteration() {
            if (checkpointLockAcquired) {
                benchmark.checkpointManager.checkpointTimeoutLock().checkpointReadUnlock();
                checkpointLockAcquired = false;
            }
        }

        int nextZipfianIndex() {
            return zipfianDistribution.next();
        }

        int getThreadIndex() {
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
        if (beforeMetrics == null) {
            throw new IllegalStateException(
                    "setupIteration() did not run - JMH contract violated"
            );
        }

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
                persistentPageMemory.metrics().cacheHits(),
                persistentPageMemory.metrics().cacheMisses(),
                persistentPageMemory.metrics().replacements()
        );
    }

    @Setup
    @Override
    public void setup() throws Exception {
        validateConfiguration();

        this.regionSizeOverride = SMALL_REGION_SIZE;
        this.replacementMode = replacementModeParam;

        super.setup();

        for (int i = 1; i < PARTITION_COUNT; i++) {
            createPartitionFilePageStore(i);
        }

        allocateWorkingSet();

        // Checkpoint to flush pages to disk so they can be evicted later.
        flushDirtyPages();

        warmupCache();

        validateCacheWarmed();
    }

    private void validateConfiguration() {
        regionCapacityPages = SMALL_REGION_SIZE / PAGE_SIZE;
        workingSetSize = cachePressure.computeWorkingSetSize(regionCapacityPages);

        if (workingSetSize > 10_000_000) {
            throw new IllegalStateException(String.format(
                    "Working set too large: %,d pages (max: 10,000,000). "
                            + "This would require %,d GB of actual page memory and %,d MB for pageId array. "
                            + "Reduce cache pressure or region size.",
                    workingSetSize,
                    ((long) workingSetSize * PAGE_SIZE) / (1024L * 1024 * 1024),
                    (workingSetSize * 8L) / (1024 * 1024)
            ));
        }
    }

    /**
     * Allocates pages in batches to avoid OOM. Checkpoints between batches to flush dirty pages.
     */
    private void allocateWorkingSet() throws Exception {
        pageIds = new long[workingSetSize];

        // Allocate 80% of capacity at a time to avoid hitting dirty pages limit.
        int batchSize = (int) Math.round(regionCapacityPages * 0.8);

        TestSimpleValuePageIo pageIo = new TestSimpleValuePageIo();

        // Use same seed across runs so all policies get the same partition distribution.
        Random partitionRandom = new Random(BASE_SEED);

        for (int batchStart = 0; batchStart < workingSetSize; batchStart += batchSize) {
            int batchEnd = Math.min(batchStart + batchSize, workingSetSize);

            checkpointManager.checkpointTimeoutLock().checkpointReadLock();
            try {
                // Distribute pages randomly across partitions.
                for (int i = batchStart; i < batchEnd; i++) {
                    int partitionId = partitionRandom.nextInt(PARTITION_COUNT);
                    pageIds[i] = persistentPageMemory.allocatePage(null, GROUP_ID, partitionId, FLAG_DATA);
                }

                for (int i = batchStart; i < batchEnd; i++) {
                    long pageId = pageIds[i];
                    long page = persistentPageMemory.acquirePage(GROUP_ID, pageId);
                    try {
                        long pageAddr = persistentPageMemory.writeLock(GROUP_ID, pageId, page);
                        try {
                            pageIo.initNewPage(pageAddr, pageId, PAGE_SIZE);
                            TestSimpleValuePageIo.setLongValue(pageAddr, pageId);
                        } finally {
                            persistentPageMemory.writeUnlock(GROUP_ID, pageId, page, true);
                        }
                    } finally {
                        persistentPageMemory.releasePage(GROUP_ID, pageId, page);
                    }
                }
            } finally {
                checkpointManager.checkpointTimeoutLock().checkpointReadUnlock();
            }

            if (batchEnd < workingSetSize) {
                flushDirtyPages();
            }
        }
    }

    private void flushDirtyPages() throws Exception {
        CheckpointProgress progress = checkpointManager.forceCheckpoint("Flush dirty pages after allocation");

        try {
            progress.futureFor(CheckpointState.FINISHED).get(CHECKPOINT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            long dirtyPages = persistentPageMemory.dirtyPagesCount();
            int usedCpBuf = persistentPageMemory.usedCheckpointBufferPages();
            int maxCpBuf = persistentPageMemory.maxCheckpointBufferPages();

            throw new IllegalStateException(String.format(
                    "Checkpoint timed out after %d seconds [reason='Flush dirty pages after allocation', "
                            + "dirtyPages=%,d, checkpointBuffer=%d/%d]. "
                            + "Check disk health and system logs.",
                    CHECKPOINT_TIMEOUT_SECONDS, dirtyPages, usedCpBuf, maxCpBuf
            ), e);
        } catch (ExecutionException e) {
            throw new IllegalStateException(String.format(
                    "Checkpoint failed [reason='Flush dirty pages after allocation', "
                            + "dirtyPages=%,d, cause=%s]",
                    persistentPageMemory.dirtyPagesCount(), e.getCause()
            ), e);
        }
    }

    private void warmupCache() throws IgniteInternalCheckedException {
        int warmupPages = (int) Math.round(regionCapacityPages * WARMUP_MULTIPLIER);
        ZipfianDistribution warmupDistribution = new ZipfianDistribution(
                workingSetSize,
                ZIPFIAN_SKEW,
                WARMUP_SEED
        );

        long accumulator = 0;

        checkpointManager.checkpointTimeoutLock().checkpointReadLock();
        try {
            for (int i = 0; i < warmupPages; i++) {
                int index = warmupDistribution.next();
                long pageId = pageIds[index];

                long page = persistentPageMemory.acquirePage(GROUP_ID, pageId);
                try {
                    long pageAddr = persistentPageMemory.readLock(GROUP_ID, pageId, page);
                    try {
                        accumulator += TestSimpleValuePageIo.getLongValue(pageAddr);
                    } finally {
                        persistentPageMemory.readUnlock(GROUP_ID, pageId, page);
                    }
                } finally {
                    persistentPageMemory.releasePage(GROUP_ID, pageId, page);
                }
            }
        } finally {
            checkpointManager.checkpointTimeoutLock().checkpointReadUnlock();
        }

        warmupAccumulator = accumulator;
    }

    private void validateCacheWarmed() {
        long loadedPages = persistentPageMemory.loadedPages();
        double capacityUtilization = (loadedPages * 100.0) / regionCapacityPages;

        if (capacityUtilization < 95.0) {
            throw new IllegalStateException(String.format(
                    "Cache not sufficiently warmed: loaded %,d pages but capacity is %,d (%.1f%% full). "
                            + "Benchmark requires >=95%% capacity to measure replacement behavior.",
                    loadedPages, regionCapacityPages, capacityUtilization
            ));
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Threads(1)
    public void zipfianThroughputSingleThread(ThreadState state, Blackhole blackhole)
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
        accessPageReadOnly(pageId, state, blackhole);
    }

    private void accessPageReadOnly(long pageId, ThreadState state, Blackhole blackhole)
            throws IgniteInternalCheckedException {
        long page = persistentPageMemory.acquirePage(GROUP_ID, pageId);

        if (page == 0) {
            throw new IllegalStateException(String.format(
                    "Failed to acquire page [pageId=0x%x, policy=%s, pressure=%s, thread=%s (index=%d), "
                            + "loadedPages=%,d/%,d]",
                    pageId, replacementModeParam, cachePressure,
                    Thread.currentThread().getName(), state.getThreadIndex(),
                    persistentPageMemory.loadedPages(), regionCapacityPages
            ));
        }

        try {
            readPageData(pageId, page, blackhole);
        } finally {
            persistentPageMemory.releasePage(GROUP_ID, pageId, page);
        }
    }

    private void readPageData(long pageId, long page, Blackhole blackhole) throws IgniteInternalCheckedException {
        long pageAddr = persistentPageMemory.readLock(GROUP_ID, pageId, page);
        try {
            long value = TestSimpleValuePageIo.getLongValue(pageAddr);
            blackhole.consume(value);
        } finally {
            persistentPageMemory.readUnlock(GROUP_ID, pageId, page);
        }
    }

    /** Run benchmark from IDE or command line. */
    public static void main(String[] args) throws Exception {
        Options opts = new OptionsBuilder()
                .include(PageCacheReplacementBenchmark.class.getSimpleName())
                .build();

        new Runner(opts).run();
    }
}
