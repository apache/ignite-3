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

package org.apache.ignite.internal.pagememory.metrics;

/**
 * Defines histogram bucket boundaries for various page memory metrics.
 *
 * <p>This class contains predefined bucket arrays for {@link org.apache.ignite.internal.metrics.DistributionMetric}
 * used throughout the page memory subsystem. Buckets are carefully chosen to capture performance characteristics
 * across different storage tiers (NVMe, SSD, HDD) and operational patterns.
 */
public final class MetricBounds {
    // ==================== Time bounds (nanoseconds) ====================

    /**
     * Bounds for checkpoint read lock acquisition time in nanoseconds.
     *
     * <p>Buckets: [0-1µs], [1µs-10µs], [10µs-100µs], [100µs-1ms], [1ms-10ms], [10ms-100ms], [100ms-1s], [1s-∞]
     *
     * <p>Expected distribution:
     * <ul>
     *     <li>&lt;1µs: Uncontended lock, fast path</li>
     *     <li>1-10µs: Minor contention</li>
     *     <li>10-100µs: Moderate contention</li>
     *     <li>100µs-1ms: High contention</li>
     *     <li>&gt;1ms: Checkpoint in progress, blocking operations</li>
     * </ul>
     */
    public static final long[] LOCK_ACQUISITION_NANOS = {
            1_000,         // 1µs   - uncontended, fast path
            10_000,        // 10µs  - minor contention
            100_000,       // 100µs - moderate contention
            1_000_000,     // 1ms   - high contention
            10_000_000,    // 10ms  - checkpoint in progress
            100_000_000,   // 100ms - severe contention
            1_000_000_000  // 1s    - pathological case
    };

    /**
     * Bounds for checkpoint read lock hold time in nanoseconds.
     *
     * <p>Buckets: [0-1µs], [1µs-10µs], [10µs-100µs], [100µs-1ms], [1ms-10ms], [10ms-100ms], [100ms-1s], [1s-∞]
     *
     * <p>Helps identify operations holding locks too long, which may need to use runConsistently.
     */
    public static final long[] LOCK_HOLD_NANOS = {
            1_000,         // 1µs   - very fast operation
            10_000,        // 10µs  - fast single-page operation
            100_000,       // 100µs - multi-page operation
            1_000_000,     // 1ms   - complex operation
            10_000_000,    // 10ms  - batch operation
            100_000_000,   // 100ms - large batch or slow I/O
            1_000_000_000  // 1s    - very large operation
    };

    /**
     * Bounds for page acquire time in nanoseconds.
     *
     * <p>Buckets: [0-500ns], [500ns-1µs], [1µs-10µs], [10µs-100µs], [100µs-1ms], [1ms-10ms], [10ms-100ms], [100ms-∞]
     *
     * <p>Expected distribution by storage tier:
     * <ul>
     *     <li>&lt;1µs: Hot page in cache (L3 cache hit)</li>
     *     <li>1-100µs: Cache miss, fast SSD/NVMe read</li>
     *     <li>&gt;1ms: Slow disk or I/O saturation</li>
     * </ul>
     */
    public static final long[] PAGE_ACQUIRE_NANOS = {
            500,           // 500ns - L3 cache hit
            1_000,         // 1µs   - memory access, cache hit
            10_000,        // 10µs  - cache hit with minor contention
            100_000,       // 100µs - page cache miss, fast SSD
            1_000_000,     // 1ms   - slow SSD or NVMe
            10_000_000,    // 10ms  - HDD or slow I/O
            100_000_000    // 100ms - very slow I/O or high load
    };

    /**
     * Bounds for physical disk I/O operations (read/write) in nanoseconds.
     *
     * <p>Buckets: [0-10µs], [10µs-50µs], [50µs-100µs], [100µs-500µs], [500µs-1ms], [1ms-10ms], [10ms-100ms], [100ms-∞]
     *
     * <p>Storage tier characteristics:
     * <ul>
     *     <li>10-50µs: NVMe sequential/random read</li>
     *     <li>100-500µs: SATA SSD</li>
     *     <li>1-10ms: HDD seek + read</li>
     *     <li>&gt;10ms: Very slow HDD or high queue depth</li>
     * </ul>
     */
    public static final long[] DISK_IO_NANOS = {
            10_000,        // 10µs  - NVMe, sequential
            50_000,        // 50µs  - NVMe, random
            100_000,       // 100µs - Fast SSD
            500_000,       // 500µs - SATA SSD
            1_000_000,     // 1ms   - Slow SSD or small HDD read
            10_000_000,    // 10ms  - HDD seek + read
            100_000_000    // 100ms - Very slow HDD or high queue depth
    };

    /**
     * Bounds for file open/create operations in nanoseconds.
     *
     * <p>Buckets: [0-100µs], [100µs-1ms], [1ms-10ms], [10ms-100ms], [100ms-1s], [1s-∞]
     *
     * <p>File operations are typically slower than page I/O due to metadata updates.
     */
    public static final long[] FILE_OPEN_NANOS = {
            100_000,       // 100µs - cached metadata
            1_000_000,     // 1ms   - normal file open
            10_000_000,    // 10ms  - cold file, needs disk seek
            100_000_000,   // 100ms - slow disk or heavy load
            1_000_000_000  // 1s    - pathological (network filesystem?)
    };

    /**
     * Bounds for fsync/fdatasync operations in nanoseconds.
     *
     * <p>Buckets: [0-100µs], [100µs-1ms], [1ms-10ms], [10ms-100ms], [100ms-1s], [1s-∞]
     *
     * <p>fsync is often the slowest operation as it forces data to physical media:
     * <ul>
     *     <li>100µs-1ms: SSD with battery-backed cache</li>
     *     <li>1-10ms: Fast SSD without write cache</li>
     *     <li>&gt;100ms: HDD or high load</li>
     * </ul>
     */
    public static final long[] FSYNC_NANOS = {
            100_000,       // 100µs - SSD with battery-backed cache
            1_000_000,     // 1ms   - Fast SSD
            10_000_000,    // 10ms  - SATA SSD or many files
            100_000_000,   // 100ms - HDD or high load
            1_000_000_000  // 1s    - Very slow disk
    };

    /**
     * Bounds for page replacement operations in nanoseconds.
     *
     * <p>Buckets: [0-10µs], [10µs-100µs], [100µs-1ms], [1ms-10ms], [10ms-100ms], [100ms-∞]
     *
     * <p>Page replacement may need to write dirty pages first:
     * <ul>
     *     <li>&lt;10µs: Fast, clean page eviction</li>
     *     <li>100µs-1ms: Need to write dirty page first</li>
     *     <li>&gt;10ms: Memory pressure, many dirty pages</li>
     * </ul>
     */
    public static final long[] PAGE_REPLACEMENT_NANOS = {
            10_000,        // 10µs  - fast, clean page eviction
            100_000,       // 100µs - need to write dirty page first
            1_000_000,     // 1ms   - dirty page write to slow disk
            10_000_000,    // 10ms  - multiple dirty pages or slow I/O
            100_000_000    // 100ms - memory pressure, many dirty pages
    };

    /**
     * Bounds for runConsistently closure execution time in nanoseconds.
     *
     * <p>Buckets: [0-1µs], [1µs-10µs], [10µs-100µs], [100µs-1ms], [1ms-10ms], [10ms-100ms], [100ms-1s], [1s-10s], [10s-∞]
     *
     * <p>Long-running operations that need to span checkpoints:
     * <ul>
     *     <li>&lt;1ms: Fast operations, few pages</li>
     *     <li>10-100ms: Large batch operations</li>
     *     <li>&gt;1s: Very large operations (may block checkpoints)</li>
     * </ul>
     */
    public static final long[] RUN_CONSISTENTLY_NANOS = {
            1_000,           // 1µs   - very fast, likely no I/O
            10_000,          // 10µs  - fast with few pages
            100_000,         // 100µs - moderate work
            1_000_000,       // 1ms   - multiple pages
            10_000_000,      // 10ms  - batch operation
            100_000_000,     // 100ms - large batch
            1_000_000_000,   // 1s    - very large operation
            10_000_000_000L  // 10s   - bulk operation
    };

    // ==================== Size bounds (bytes) ====================

    /**
     * Bounds for I/O operation sizes in bytes.
     *
     * <p>Buckets: [0-4KB], [4KB-8KB], [8KB-16KB], [16KB-64KB], [64KB-256KB], [256KB-1MB], [1MB-4MB], [4MB-∞]
     *
     * <p>Identifies read/write patterns:
     * <ul>
     *     <li>4KB: Single page operation</li>
     *     <li>4-16KB: Small batch (2-4 pages)</li>
     *     <li>64KB+: Large batch or sequential scan</li>
     * </ul>
     */
    public static final long[] IO_SIZE_BYTES = {
            4_096,      // 4KB   - single page
            8_192,      // 8KB   - two pages
            16_384,     // 16KB  - small batch
            65_536,     // 64KB  - medium batch
            262_144,    // 256KB - large batch
            1_048_576,  // 1MB   - very large batch
            4_194_304   // 4MB   - bulk read
    };

    // ==================== Count bounds ====================

    /**
     * Bounds for number of I/O operations per runConsistently closure.
     *
     * <p>Buckets: [0-1], [1-5], [5-10], [10-50], [50-100], [100-500], [500-1000], [1000-10000], [10000-∞]
     *
     * <p>Helps identify operations that do too many I/O calls:
     * <ul>
     *     <li>1-10: Typical transaction</li>
     *     <li>50-100: Large batch</li>
     *     <li>&gt;1000: Bulk operation (consider optimization)</li>
     * </ul>
     */
    public static final long[] IO_OPS_PER_CLOSURE = {
            1,      // 1 op    - single page operation
            5,      // 5 ops   - small transaction
            10,     // 10 ops  - typical transaction
            50,     // 50 ops  - medium batch
            100,    // 100 ops - large batch
            500,    // 500 ops - very large batch
            1000,   // 1000 ops - bulk operation
            10000   // 10000 ops - pathological
    };

    /**
     * Bounds for dirty pages count at checkpoint start.
     *
     * <p>Buckets: [0-100], [100-500], [500-1K], [1K-5K], [5K-10K], [10K-50K], [50K-100K], [100K-500K], [500K-∞]
     *
     * <p>Correlates dirty page count with checkpoint duration:
     * <ul>
     *     <li>&lt;1000: Small checkpoint</li>
     *     <li>1K-10K: Normal checkpoint</li>
     *     <li>&gt;50K: Large checkpoint (may take long time)</li>
     * </ul>
     */
    public static final long[] DIRTY_PAGES_COUNT = {
            100,
            500,
            1000,
            5000,
            10000,
            50000,
            100000,
            500000
    };

    // ==================== Duration bounds (milliseconds) ====================

    /**
     * Bounds for full checkpoint duration in milliseconds.
     *
     * <p>Buckets: [0-100ms], [100ms-500ms], [500ms-1s], [1s-5s], [5s-10s], [10s-30s], [30s-60s], [60s-∞]
     *
     * <p>Expected distribution:
     * <ul>
     *     <li>&lt;1s: Fast checkpoint (normal)</li>
     *     <li>1-5s: Normal checkpoint</li>
     *     <li>&gt;10s: Slow checkpoint (investigate)</li>
     * </ul>
     */
    public static final long[] CHECKPOINT_DURATION_MILLIS = {
            100,    // 100ms - very fast checkpoint
            500,    // 500ms - fast checkpoint
            1_000,  // 1s    - normal checkpoint
            5_000,  // 5s    - slow checkpoint
            10_000, // 10s   - very slow checkpoint
            30_000, // 30s   - pathological
            60_000  // 60s   - extreme
    };

    /**
     * Bounds for time between consecutive checkpoints in milliseconds.
     *
     * <p>Buckets: [0-1s], [1s-5s], [5s-10s], [10s-30s], [30s-60s], [60s-5min], [5min-10min], [10min-∞]
     *
     * <p>Helps understand checkpoint patterns and optimize frequency.
     */
    public static final long[] CHECKPOINT_INTERVAL_MILLIS = {
            1_000,   // 1s    - very frequent
            5_000,   // 5s    - frequent
            10_000,  // 10s   - normal
            30_000,  // 30s   - infrequent
            60_000,  // 60s   - very infrequent
            300_000, // 5min  - rare
            600_000  // 10min - very rare
    };

    // ==================== Percentage bounds ====================

    /**
     * Bounds for segment load factor (utilization) in percentage.
     *
     * <p>Buckets: [0-10%], [10%-20%], ..., [90%-95%], [95%-98%], [98%-100%], [100%-∞]
     *
     * <p>Identifies imbalanced segments:
     * <ul>
     *     <li>&lt;50%: Underutilized</li>
     *     <li>50-90%: Normal</li>
     *     <li>&gt;90%: Near full (may trigger evictions)</li>
     * </ul>
     */
    public static final long[] LOAD_FACTOR_PERCENT = {
            10, 20, 30, 40, 50, 60, 70, 80, 90, 95, 98, 100
    };

    /**
     * Private constructor to prevent instantiation.
     */
    private MetricBounds() {
    }
}
