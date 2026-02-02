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
 * JMH benchmark measuring page cache replacement policy performance in PersistentPageMemory.
 *
 * <p>This benchmark measures page cache miss handling, replacement overhead, and access latency
 * under memory pressure using realistic access patterns (Zipfian distribution modeling hot/cold data).
 *
 * <p><b>What This Benchmark Measures:</b>
 * <ul>
 *     <li>Page cache miss and replacement overhead for different policies</li>
 *     <li>Throughput and latency under single-threaded and multi-threaded contention</li>
 *     <li>Policy effectiveness at retaining hot pages under memory pressure</li>
 * </ul>
 *
 * <p><b>Configuration:</b>
 * <ul>
 *     <li>Region: 20 MiB (5,120 pages at 4 KiB/page) - sized to force replacements without excessive setup time</li>
 *     <li>Partitions: 16 (power-of-2 for efficient modulo, representative of small-to-medium tables)</li>
 *     <li>Cache Pressure: Working set exceeds capacity by 1.2x, 2x, or 4x</li>
 *     <li>Access Pattern: Zipfian with skew=0.99 (commonly used in YCSB)</li>
 * </ul>
 *
 * <p><b>Important Limitations:</b>
 * <ul>
 *     <li><b>Read-only workload:</b> Does not test dirty page eviction, write amplification,
 *         or checkpoint blocking. Real workloads have 10-30% writes which significantly
 *         impact replacement behavior.</li>
 *     <li><b>Checkpoint lock held during measurement:</b> Each benchmark iteration acquires
 *         checkpoint read lock before measurement and releases after (see ThreadState.setupIteration).
 *         This eliminates checkpoint contention from measurements but means we don't measure
 *         checkpoint lock acquisition overhead (100ms+ blocking every 30-60s in production).
 *         Note: Lock is NOT held during setup - checkpoints occur between allocation batches.</li>
 *     <li><b>Single access pattern:</b> Tests pure Zipfian only. Real workloads mix
 *         hot key access, range scans, and bulk operations.</li>
 *     <li><b>Pre-warmed cache:</b> Does not measure cold start or cache warming behavior.</li>
 * </ul>
 *
 * <p><b>Results Interpretation:</b> This benchmark measures page cache replacement efficiency
 * in isolation. Production performance will be lower due to checkpoint contention, dirty page
 * writes, and mixed workload patterns not represented here.
 *
 * <p><b>Benchmark Matrix:</b> 3 policies × 3 pressures × 4 methods = 36 configurations
 * (approximately 18-20 minutes depending on hardware)
 */
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 10, time = 2)
@Fork(1)
@State(Scope.Benchmark)
public class PageCacheReplacementBenchmark extends PersistentPageMemoryBenchmarkBase {

    /**
     * Small region size to force page replacements quickly.
     * Using literal instead of Constants.MiB for invariance.
     */
    private static final long SMALL_REGION_SIZE = 20L * 1024 * 1024;  // 20 MiB

    /** Expected page size for this benchmark. Must match PAGE_SIZE from base class. */
    private static final int EXPECTED_PAGE_SIZE = 4 * 1024;  // 4 KiB

    /**
     * Zipfian skew parameter: 0.99 creates strong hot/cold separation.
     * This value is commonly used in YCSB benchmarks.
     */
    private static final double ZIPFIAN_SKEW = 0.99;

    /** Base random seed for reproducibility. */
    private static final long BASE_SEED = 42L;

    /**
     * Warmup seed (distinct from thread seeds to avoid access pattern overlap).
     * Using 999999 keeps it far away from thread seeds but below the thread spacing prime (1000003).
     */
    private static final long WARMUP_SEED = BASE_SEED + 999999L;

    /**
     * Number of partitions: 16 is representative of small-to-medium tables.
     * (Small tables have 10-100 partitions, medium 100-1000, large 1000+)
     */
    private static final int PARTITION_COUNT = 16;

    /**
     * Warmup multiplier: Access 110% of capacity to ensure cache fills completely
     * and initial replacements begin before measurement starts.
     */
    private static final double WARMUP_MULTIPLIER = 1.1;

    /**
     * Minimum working set size as fraction of region capacity.
     * Just a sanity check - even LOW pressure is 120%, so anything below 10% is a setup failure.
     */
    private static final double MIN_WORKING_SET_RATIO = 0.1;

    /** Page replacement policy to test. */
    @Param({"CLOCK", "SEGMENTED_LRU", "RANDOM_LRU"})
    public ReplacementMode replacementModeParam;

    /**
     * Cache pressure level: working set size as multiple of capacity.
     * Higher pressure = more cache misses and replacements = better policy differentiation.
     */
    @Param({"LOW", "MEDIUM", "HIGH"})
    public CachePressure cachePressure;

    /**
     * Pre-allocated page IDs for the working set.
     * Shared across all threads (populated in setup, read-only during benchmark).
     */
    private long[] pageIds;

    /** Computed region capacity in pages. */
    private long regionCapacityPages;

    /** Working set size. */
    private int workingSetSize;

    /**
     * Metrics snapshot before iteration (captured once per iteration at benchmark level).
     * Volatile because it's written in setupIteration() and read in tearDownIteration().
     */
    private volatile MetricsSnapshot beforeMetrics;

    /**
     * Volatile accumulator to prevent warmup reads from being optimized away.
     * This field is write-only - never read after warmupCache() completes.
     * Its purpose is to force the JIT compiler to keep all warmup read operations.
     */
    @SuppressWarnings("unused")
    private volatile long warmupAccumulator;

    /**
     * Cache pressure levels determining working set size relative to capacity.
     */
    public enum CachePressure {
        /** Low pressure: 1.2× capacity. Minimal replacement activity. */
        LOW(1.2),
        /** Medium pressure: 2× capacity. Moderate replacement activity. */
        MEDIUM(2.0),
        /** High pressure: 4× capacity. Heavy replacement activity. */
        HIGH(4.0);

        private final double multiplier;

        CachePressure(double multiplier) {
            this.multiplier = multiplier;
        }

        /**
         * Computes working set size from capacity.
         *
         * @param capacity Region capacity in pages.
         * @return Working set size.
         * @throws IllegalArgumentException If capacity is too large or result exceeds Integer.MAX_VALUE.
         */
        int computeWorkingSetSize(long capacity) {
            // Check for overflow BEFORE multiplication
            // Use double division to preserve precision (e.g., multiplier=1.2 shouldn't truncate to 1)
            if (capacity > Long.MAX_VALUE / multiplier) {
                throw new IllegalArgumentException(String.format(
                        "Capacity too large: %,d pages with multiplier %.1f would overflow. "
                                + "Maximum safe capacity: %,d pages",
                        capacity, multiplier, (long) (Long.MAX_VALUE / multiplier)
                ));
            }

            // Round to nearest integer for accurate sizing
            long result = Math.round(capacity * multiplier);

            // Check if result fits in an int
            if (result > Integer.MAX_VALUE) {
                throw new IllegalArgumentException(String.format(
                        "Working set too large: %,d pages (capacity=%,d, multiplier=%.1f). "
                                + "Maximum: %,d pages (Integer.MAX_VALUE)",
                        result, capacity, multiplier, Integer.MAX_VALUE
                ));
            }

            return (int) result;
        }
    }

    /**
     * Per-thread state for multi-threaded benchmarks.
     * Each thread maintains independent access pattern and checkpoint lock.
     */
    @State(Scope.Thread)
    public static class ThreadState {
        private ZipfianDistribution zipfianDistribution;
        private boolean checkpointLockAcquired;
        private int threadIndex;
        private PageCacheReplacementBenchmark benchmark;

        /**
         * Initializes per-thread state with unique seed for Zipfian distribution.
         * Each thread gets a deterministic seed based on thread index for reproducibility.
         *
         * @param threadParams JMH thread parameters (provides deterministic thread index).
         * @param benchmark Benchmark instance.
         */
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

            // Validate that benchmark is initialized with reasonable working set
            long minWorkingSetSize = Math.round(benchmark.regionCapacityPages * MIN_WORKING_SET_RATIO);
            if (benchmark.workingSetSize < minWorkingSetSize) {
                throw new IllegalStateException(String.format(
                        "Benchmark not properly initialized: workingSetSize=%,d < minimum %,d (%.0f%% of capacity)",
                        benchmark.workingSetSize, minWorkingSetSize, MIN_WORKING_SET_RATIO * 100
                ));
            }

            // Give each thread a deterministic seed for reproducibility.
            // Multiply by large prime (1000003) to spread seeds apart - adjacent seeds like 42, 43, 44
            // would produce overlapping random sequences.
            long threadSeed = BASE_SEED + (threadIndex * 1000003L);

            this.zipfianDistribution = new ZipfianDistribution(
                    benchmark.workingSetSize,
                    ZIPFIAN_SKEW,
                    threadSeed
            );
        }

        /**
         * Acquires checkpoint read lock before each iteration (per-thread).
         * Lock is held for entire iteration to eliminate checkpoint contention from measurements
         * and isolate page cache performance.
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

        /**
         * Releases checkpoint read lock after iteration (per-thread).
         */
        @TearDown(Level.Iteration)
        public void tearDownIteration() {
            if (checkpointLockAcquired) {
                benchmark.checkpointManager.checkpointTimeoutLock().checkpointReadUnlock();
                checkpointLockAcquired = false;
            }
        }

        /**
         * Returns the next Zipfian-distributed index.
         *
         * @return Index in range [0, workingSetSize).
         */
        int nextZipfianIndex() {
            return zipfianDistribution.next();
        }

        /**
         * Returns the thread index for error reporting.
         *
         * @return Thread index (0, 1, 2, 3 for 4-thread benchmark).
         */
        int getThreadIndex() {
            return threadIndex;
        }
    }

    /**
     * Metrics snapshot at a point in time.
     */
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

    /**
     * Captures metrics before iteration (runs once per iteration, before all threads).
     */
    @Setup(Level.Iteration)
    public void setupIteration() {
        beforeMetrics = captureMetrics();
    }

    /**
     * Captures metrics after iteration and prints delta (runs once per iteration, after all threads).
     */
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

    /**
     * Prints metrics delta between iterations.
     */
    private void printMetricsDelta(MetricsSnapshot before, MetricsSnapshot after) {
        long hits = after.hits - before.hits;
        long misses = after.misses - before.misses;
        long replacements = after.replacements - before.replacements;

        long total = hits + misses;
        double hitRate = total > 0 ? (hits * 100.0 / total) : 0.0;

        System.out.printf("[%s/%s] Hits: %,d | Misses: %,d | Hit Rate: %.1f%% | Replacements: %,d%n",
                replacementModeParam, cachePressure, hits, misses, hitRate, replacements);
    }

    /**
     * Captures current metrics snapshot from PersistentPageMemory.
     *
     * @return Metrics snapshot with current values.
     */
    private MetricsSnapshot captureMetrics() {
        return new MetricsSnapshot(
                persistentPageMemory.metrics().cacheHits(),
                persistentPageMemory.metrics().cacheMisses(),
                persistentPageMemory.metrics().replacements()
        );
    }

    /**
     * Initializes the benchmark by setting up PersistentPageMemory with a small region
     * and pre-allocating a working set across multiple partitions.
     *
     * @throws IllegalStateException If configuration validation fails, cache doesn't warm properly,
     *                               or Zipfian distribution doesn't produce expected pattern.
     * @throws IgniteInternalCheckedException If page memory operations fail.
     * @throws InterruptedException If checkpoint wait is interrupted.
     * @throws ExecutionException If checkpoint execution fails.
     * @throws TimeoutException If checkpoint takes longer than 300 seconds.
     * @throws Exception From super.setup() for other infrastructure initialization failures.
     */
    @Setup
    @Override
    public void setup() throws Exception {
        // Validate configuration BEFORE expensive setup (computes regionCapacityPages and workingSetSize)
        validateConfiguration();

        // Override region size to force replacements
        this.regionSizeOverride = SMALL_REGION_SIZE;
        this.replacementMode = replacementModeParam;

        super.setup();

        // Create additional partitions
        for (int i = 1; i < PARTITION_COUNT; i++) {
            createPartitionFilePageStore(i);
        }

        // Allocate and initialize working set
        allocateWorkingSet();

        // Force checkpoint to flush dirty pages to disk and mark them as clean
        // This is critical: without this checkpoint, all allocated pages remain dirty
        // and cannot be evicted, causing OOM when working set exceeds dirty pages limit
        flushDirtyPages();

        // Warm up cache with hot pages
        warmupCache();

        // Verify cache is ready for benchmark
        validateCacheWarmed();
    }

    /**
     * Validates benchmark configuration before expensive setup operations.
     *
     * @throws IllegalStateException If configuration is invalid.
     */
    private void validateConfiguration() {
        // Validate page size matches expected value
        if (PAGE_SIZE != EXPECTED_PAGE_SIZE) {
            throw new IllegalStateException(String.format(
                    "Benchmark requires PAGE_SIZE=%,d bytes, got: %,d bytes",
                    EXPECTED_PAGE_SIZE, PAGE_SIZE
            ));
        }

        // Validate region size is evenly divisible by page size
        if (SMALL_REGION_SIZE % PAGE_SIZE != 0) {
            throw new IllegalStateException(String.format(
                    "Region size (%,d bytes) must be evenly divisible by page size (%,d bytes)",
                    SMALL_REGION_SIZE, PAGE_SIZE
            ));
        }

        // Compute capacity once and validate it's positive
        regionCapacityPages = SMALL_REGION_SIZE / PAGE_SIZE;

        if (regionCapacityPages <= 0) {
            throw new IllegalStateException(String.format(
                    "Invalid region capacity: %,d pages (regionSize=%,d bytes, pageSize=%,d bytes). "
                            + "Both must be positive and regionSize > pageSize.",
                    regionCapacityPages, SMALL_REGION_SIZE, PAGE_SIZE
            ));
        }

        // Compute working set size and validate it exceeds capacity
        workingSetSize = cachePressure.computeWorkingSetSize(regionCapacityPages);

        if (workingSetSize <= regionCapacityPages) {
            throw new IllegalStateException(String.format(
                    "Invalid configuration: working set (%,d pages) must exceed capacity (%,d pages)",
                    workingSetSize, regionCapacityPages
            ));
        }

        // Validate working set size has reasonable upper limit
        // Limit to 10M pages (40GB at 4KB/page) to prevent excessive setup time and memory usage
        // The value 10_000_000 is chosen as: allocation takes ~30-60s, array needs ~80MB
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
     * Allocates and initializes pages for the working set across all partitions.
     *
     * <p>To avoid OOM when working set exceeds dirty pages limit, this method allocates
     * and initializes pages in batches, forcing a checkpoint after each batch to flush
     * dirty pages to disk and allow them to be evicted.
     *
     * <p><b>Precondition:</b> workingSetSize must be validated by validateConfiguration()
     * before calling this method. This method assumes workingSetSize is valid.
     *
     * @throws Exception If page allocation or checkpoint fails.
     */
    private void allocateWorkingSet() throws Exception {
        pageIds = new long[workingSetSize];

        // Batch size conservative enough to avoid hitting dirty pages limit.
        // Using 80% of capacity provides sufficient headroom below the ~87% dirty pages threshold
        // to account for background operations and avoid OOM during allocation.
        // The value 0.8 represents 80% of capacity - conservative enough to avoid hitting the
        // ~87% dirty pages limit while maximizing batch efficiency.
        long batchSizeLong = Math.round(regionCapacityPages * 0.8);
        if (batchSizeLong > Integer.MAX_VALUE) {
            throw new IllegalStateException(String.format(
                    "Batch size too large: %,d pages exceeds Integer.MAX_VALUE. "
                            + "Reduce region size or increase batch count.",
                    batchSizeLong
            ));
        }
        int batchSize = (int) batchSizeLong;

        // Create PageIo instance once and reuse for all page initializations
        TestSimpleValuePageIo pageIo = new TestSimpleValuePageIo();

        // Use fixed seed so partition assignment is identical across all policies.
        // This controls for confounding variables like memory alignment, I/O patterns, etc.
        // We want to measure replacement policy differences, not partition assignment luck.
        Random partitionRandom = new Random(BASE_SEED);

        for (int batchStart = 0; batchStart < workingSetSize; batchStart += batchSize) {
            int batchEnd = Math.min(batchStart + batchSize, workingSetSize);

            // Allocate and initialize one batch
            checkpointManager.checkpointTimeoutLock().checkpointReadLock();
            try {
                // Allocate pages randomly across partitions to avoid skew
                // (Sequential assignment would cause partition 0 to get most Zipfian-hot pages)
                for (int i = batchStart; i < batchEnd; i++) {
                    int partitionId = partitionRandom.nextInt(PARTITION_COUNT);
                    pageIds[i] = persistentPageMemory.allocatePage(null, GROUP_ID, partitionId, FLAG_DATA);
                }

                // Initialize pages with test data (marks them dirty)
                for (int i = batchStart; i < batchEnd; i++) {
                    long pageId = pageIds[i];
                    long page = persistentPageMemory.acquirePage(GROUP_ID, pageId);
                    try {
                        long pageAddr = persistentPageMemory.writeLock(GROUP_ID, pageId, page);
                        try {
                            // Initialize page with proper header
                            pageIo.initNewPage(pageAddr, pageId, PAGE_SIZE);
                            // Write test value that benchmark will read
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

            // Checkpoint after each batch to flush dirty pages and avoid OOM
            // Only checkpoint if there are more batches to process
            if (batchEnd < workingSetSize) {
                flushDirtyPages();
            }
        }
    }

    /**
     * Forces a checkpoint to flush dirty pages to disk and mark them as clean in memory.
     *
     * <p>This is critical for benchmarks with large working sets: without this checkpoint,
     * all allocated pages remain dirty and cannot be evicted. When the working set exceeds
     * the dirty pages limit (~87% of capacity), page replacement fails with OOM.
     *
     * <p>After this checkpoint completes, pages are written to disk and marked clean,
     * allowing them to be evicted during warmup and benchmark execution.
     *
     * @throws Exception If checkpoint fails.
     */
    private void flushDirtyPages() throws Exception {
        CheckpointProgress progress = checkpointManager.forceCheckpoint("Flush dirty pages after allocation");

        try {
            // 30 second timeout: we're checkpointing ~4000 pages (16MB).
            // SSD does this in ~100-500ms, HDD in ~1-2s, slow storage in ~2-10s.
            // If it takes 30+ seconds, something is seriously broken (deadlock, disk failure, etc).
            progress.futureFor(CheckpointState.FINISHED).get(30, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            long dirtyPages = persistentPageMemory.dirtyPagesCount();
            int usedCpBuf = persistentPageMemory.usedCheckpointBufferPages();
            int maxCpBuf = persistentPageMemory.maxCheckpointBufferPages();

            throw new IllegalStateException(String.format(
                    "Checkpoint timed out after 30 seconds [reason='Flush dirty pages after allocation', "
                            + "dirtyPages=%,d, checkpointBuffer=%d/%d]. "
                            + "This indicates a serious problem: deadlock, I/O device failure, or system hang. "
                            + "Check disk health and system logs.",
                    dirtyPages, usedCpBuf, maxCpBuf
            ), e);
        } catch (ExecutionException e) {
            throw new IllegalStateException(String.format(
                    "Checkpoint failed [reason='Flush dirty pages after allocation', "
                            + "dirtyPages=%,d, cause=%s]",
                    persistentPageMemory.dirtyPagesCount(), e.getCause()
            ), e);
        }
    }

    /**
     * Warms up the cache by accessing hot pages with Zipfian distribution.
     * Uses a different seed than benchmark threads to avoid biasing results.
     *
     * @throws IgniteInternalCheckedException If page access fails.
     */
    private void warmupCache() throws IgniteInternalCheckedException {
        // Make sure warmup count fits in an int
        long warmupPagesLong = Math.round(regionCapacityPages * WARMUP_MULTIPLIER);
        if (warmupPagesLong > Integer.MAX_VALUE) {
            throw new IllegalStateException(String.format(
                    "Warmup page count too large: %,d pages exceeds Integer.MAX_VALUE. "
                            + "Reduce region size or warmup multiplier.",
                    warmupPagesLong
            ));
        }

        int warmupPages = (int) warmupPagesLong;
        ZipfianDistribution warmupDistribution = new ZipfianDistribution(
                workingSetSize,
                ZIPFIAN_SKEW,
                WARMUP_SEED
        );

        long accumulator = 0;  // Prevent dead code elimination of reads

        checkpointManager.checkpointTimeoutLock().checkpointReadLock();
        try {
            for (int i = 0; i < warmupPages; i++) {
                int index = warmupDistribution.next();
                long pageId = pageIds[index];

                long page = persistentPageMemory.acquirePage(GROUP_ID, pageId);
                try {
                    long pageAddr = persistentPageMemory.readLock(GROUP_ID, pageId, page);
                    try {
                        // Accumulate read value to prevent JIT from eliminating the load
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

        // Write to volatile field to ensure all reads are observed
        warmupAccumulator = accumulator;
    }

    /**
     * Validates that cache is nearly full (>=95% capacity).
     * We need the cache full to force page replacements. The 110% warmup multiplier
     * with 95% threshold gives us enough wiggle room for policies like RANDOM_LRU
     * that evict pages during warmup.
     *
     * @throws IllegalStateException If cache is not sufficiently warmed.
     */
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

    // ========== Core Benchmark Methods ==========

    /**
     * Primary test: Single-threaded Zipfian throughput.
     * Measures raw page replacement performance without contention.
     */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Threads(1)
    public void zipfianThroughputSingleThread(ThreadState state, Blackhole blackhole)
            throws IgniteInternalCheckedException {
        benchmarkIteration(state, blackhole);
    }

    /**
     * Contention test: 4-threaded Zipfian throughput.
     * Measures page replacement performance under moderate contention.
     */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Threads(4)
    public void zipfianThroughputFourThreads(ThreadState state, Blackhole blackhole)
            throws IgniteInternalCheckedException {
        benchmarkIteration(state, blackhole);
    }

    /**
     * Latency test: Single-threaded average latency.
     * Measures mean page access latency including cache misses and replacements.
     */
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @Threads(1)
    public void zipfianLatencySingleThread(ThreadState state, Blackhole blackhole)
            throws IgniteInternalCheckedException {
        benchmarkIteration(state, blackhole);
    }

    /**
     * Latency under contention: 4-threaded average latency.
     * Measures page access latency including lock contention and cache misses.
     * Critical for understanding tail latencies in production.
     */
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @Threads(4)
    public void zipfianLatencyFourThreads(ThreadState state, Blackhole blackhole)
            throws IgniteInternalCheckedException {
        benchmarkIteration(state, blackhole);
    }

    /**
     * Core benchmark iteration logic shared by all benchmark methods.
     * Selects a page according to Zipfian distribution and accesses it.
     *
     * @param state Thread state with Zipfian distribution.
     * @param blackhole JMH blackhole to prevent dead code elimination.
     * @throws IgniteInternalCheckedException If page access fails.
     */
    private void benchmarkIteration(ThreadState state, Blackhole blackhole) throws IgniteInternalCheckedException {
        int index = state.nextZipfianIndex();
        long pageId = pageIds[index];
        accessPageReadOnly(pageId, state, blackhole);
    }

    // ========== Helper Methods ==========

    /**
     * Acquires a page with read lock and reads data (no writes to avoid dirty page tracking).
     *
     * @param pageId Page ID to access.
     * @param state Thread state for error reporting.
     * @param blackhole JMH blackhole to prevent dead code elimination.
     * @throws IgniteInternalCheckedException If page acquisition fails.
     * @throws IllegalStateException If page cannot be acquired (with diagnostic context).
     */
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

    /**
     * Reads page data under read lock.
     *
     * @param pageId Page ID.
     * @param page Page handle.
     * @param blackhole JMH blackhole.
     * @throws IgniteInternalCheckedException If read lock acquisition fails.
     */
    private void readPageData(long pageId, long page, Blackhole blackhole) throws IgniteInternalCheckedException {
        long pageAddr = persistentPageMemory.readLock(GROUP_ID, pageId, page);
        try {
            // Read page value using proper PageIo method
            long value = TestSimpleValuePageIo.getLongValue(pageAddr);
            blackhole.consume(value);
        } finally {
            persistentPageMemory.readUnlock(GROUP_ID, pageId, page);
        }
    }

    /**
     * Runs the benchmark standalone via JMH Runner (alternative to Gradle jmh task).
     * Useful for IDE-based execution or custom JMH options.
     *
     * @param args Command-line arguments passed to JMH.
     * @throws Exception If benchmark execution fails.
     */
    public static void main(String[] args) throws Exception {
        Options opts = new OptionsBuilder()
                .include(PageCacheReplacementBenchmark.class.getSimpleName())
                .build();

        new Runner(opts).run();
    }
}
