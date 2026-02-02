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

import java.lang.reflect.Method;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;

/**
 * Collects and reports page memory metrics for benchmarks.
 *
 * <p>Captures snapshots of page memory metrics before and after benchmark iterations
 * to compute deltas and report performance characteristics like hit rates, replacement
 * counts, and I/O operations.
 *
 * <p>Uses reflection to access package-private metrics methods.
 */
public class BenchmarkMetricsCollector {
    /** The page memory instance to collect metrics from. */
    private final PersistentPageMemory pageMemory;

    /** Metrics object retrieved via reflection. */
    private final Object metrics;

    /** Reflection methods for accessing metrics. */
    private final Method cacheHitsMethod;
    private final Method cacheMissesMethod;
    private final Method replacementsMethod;
    private final Method pageReadsMethod;
    private final Method pageWritesMethod;

    /** Snapshot taken at the start of measurement. */
    private Snapshot beforeSnapshot;

    /**
     * Creates a new metrics collector.
     *
     * @param pageMemory Page memory instance to monitor.
     */
    public BenchmarkMetricsCollector(PersistentPageMemory pageMemory) {
        this.pageMemory = pageMemory;

        try {
            // Access package-private metrics() method via reflection
            Method metricsMethod = PersistentPageMemory.class.getDeclaredMethod("metrics");
            metricsMethod.setAccessible(true);
            this.metrics = metricsMethod.invoke(pageMemory);

            // Get metrics accessor methods
            Class<?> metricsClass = metrics.getClass();
            this.cacheHitsMethod = metricsClass.getDeclaredMethod("cacheHits");
            this.cacheMissesMethod = metricsClass.getDeclaredMethod("cacheMisses");
            this.replacementsMethod = metricsClass.getDeclaredMethod("replacements");
            this.pageReadsMethod = metricsClass.getDeclaredMethod("pageReads");
            this.pageWritesMethod = metricsClass.getDeclaredMethod("pageWrites");
        } catch (Exception e) {
            throw new IllegalStateException("Failed to initialize metrics collector", e);
        }
    }

    /**
     * Captures a snapshot of current metrics.
     */
    public void captureBeforeSnapshot() {
        this.beforeSnapshot = captureSnapshot();
    }

    /**
     * Computes and returns the delta between current metrics and the before snapshot.
     *
     * @return Metrics delta.
     */
    public MetricsDelta computeDelta() {
        if (beforeSnapshot == null) {
            throw new IllegalStateException("Before snapshot not captured");
        }

        Snapshot afterSnapshot = captureSnapshot();

        return new MetricsDelta(
                afterSnapshot.hits - beforeSnapshot.hits,
                afterSnapshot.misses - beforeSnapshot.misses,
                afterSnapshot.replacements - beforeSnapshot.replacements,
                afterSnapshot.reads - beforeSnapshot.reads,
                afterSnapshot.writes - beforeSnapshot.writes,
                afterSnapshot.loadedPages
        );
    }

    /**
     * Captures a snapshot of current metrics.
     *
     * @return Metrics snapshot.
     */
    private Snapshot captureSnapshot() {
        try {
            return new Snapshot(
                    (Long) cacheHitsMethod.invoke(metrics),
                    (Long) cacheMissesMethod.invoke(metrics),
                    (Long) replacementsMethod.invoke(metrics),
                    (Long) pageReadsMethod.invoke(metrics),
                    (Long) pageWritesMethod.invoke(metrics),
                    pageMemory.loadedPages()
            );
        } catch (Exception e) {
            throw new IllegalStateException("Failed to capture metrics snapshot", e);
        }
    }

    /**
     * Immutable snapshot of metrics at a point in time.
     */
    private static class Snapshot {
        final long hits;
        final long misses;
        final long replacements;
        final long reads;
        final long writes;
        final long loadedPages;

        Snapshot(long hits, long misses, long replacements, long reads, long writes, long loadedPages) {
            this.hits = hits;
            this.misses = misses;
            this.replacements = replacements;
            this.reads = reads;
            this.writes = writes;
            this.loadedPages = loadedPages;
        }
    }

    /**
     * Delta between two metric snapshots.
     */
    public static class MetricsDelta {
        private final long hits;
        private final long misses;
        private final long replacements;
        private final long reads;
        private final long writes;
        private final long loadedPages;

        MetricsDelta(long hits, long misses, long replacements, long reads, long writes, long loadedPages) {
            this.hits = hits;
            this.misses = misses;
            this.replacements = replacements;
            this.reads = reads;
            this.writes = writes;
            this.loadedPages = loadedPages;
        }

        /**
         * Returns cache hit count.
         *
         * @return Hit count.
         */
        public long getHits() {
            return hits;
        }

        /**
         * Returns cache miss count.
         *
         * @return Miss count.
         */
        public long getMisses() {
            return misses;
        }

        /**
         * Returns page replacement count.
         *
         * @return Replacement count.
         */
        public long getReplacements() {
            return replacements;
        }

        /**
         * Returns page read count.
         *
         * @return Read count.
         */
        public long getReads() {
            return reads;
        }

        /**
         * Returns page write count.
         *
         * @return Write count.
         */
        public long getWrites() {
            return writes;
        }

        /**
         * Returns hit rate as a percentage (0.0 to 100.0).
         *
         * @return Hit rate percentage.
         */
        public double getHitRatePercent() {
            long total = hits + misses;
            return total > 0 ? (hits * 100.0 / total) : 0.0;
        }

        /**
         * Formats metrics as a human-readable string.
         *
         * @return Formatted metrics.
         */
        public String format() {
            return String.format(
                    "Hits: %,d | Misses: %,d | Hit Rate: %.2f%% | "
                            + "Replacements: %,d | Reads: %,d | Writes: %,d | Loaded: %,d",
                    hits, misses, getHitRatePercent(), replacements, reads, writes, loadedPages
            );
        }

        @Override
        public String toString() {
            return format();
        }
    }
}
