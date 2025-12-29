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

package org.apache.ignite.internal.pagememory.persistence;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.metrics.DistributionMetric;
import org.apache.ignite.internal.metrics.HitRateMetric;
import org.apache.ignite.internal.metrics.IntGauge;
import org.apache.ignite.internal.metrics.LongAdderMetric;
import org.apache.ignite.internal.metrics.LongGauge;
import org.apache.ignite.internal.pagememory.configuration.PersistentDataRegionConfiguration;

/** Persistent page memory metrics. */
class PersistentPageMemoryMetrics implements PageCacheMetrics {
    private static final long[] PAGE_ACQUISITIONS_BOUNDS_NANOS = {
            1_000,         // 1µs   - cache hit
            100_000,       // 100µs - page cache miss, fast SSD
            10_000_000,    // 10ms  - HDD or slow I/O
            100_000_000    // 100ms - very slow I/O or high load
    };

    private final LongAdderMetric readPagesFromDisk;

    private final LongAdderMetric writePagesToDisk;

    private final LongAdderMetric pageCacheHitsTotal;

    private final LongAdderMetric pageCacheMisses;

    private final DistributionMetric pageAcquireTime;

    private final HitRateMetric pageCacheHitRate;

    private final LongAdderMetric pageReplacements;

    PersistentPageMemoryMetrics(
            PersistentPageMemoryMetricSource source,
            PersistentPageMemory pageMemory,
            PersistentDataRegionConfiguration dataRegionConfiguration
    ) {
        source.addMetric(new IntGauge(
                "UsedCheckpointBufferPages",
                "Number of currently used pages in checkpoint buffer.",
                pageMemory::usedCheckpointBufferPages
        ));

        source.addMetric(new IntGauge(
                "MaxCheckpointBufferPages",
                "The capacity of checkpoint buffer in pages.",
                pageMemory::maxCheckpointBufferPages
        ));

        // TODO: IGNITE-25702 Fix the concept of "region"
        source.addMetric(new LongGauge(
                "MaxSize",
                "Maximum in-memory region size in bytes.",
                dataRegionConfiguration::sizeBytes
        ));

        readPagesFromDisk = source.addMetric(new LongAdderMetric(
                "PagesRead",
                "Number of pages read from disk since the last restart."
        ));

        writePagesToDisk = source.addMetric(new LongAdderMetric(
                "PagesWritten",
                "Number of pages written to disk since the last restart."
        ));

        pageCacheHitsTotal = source.addMetric(new LongAdderMetric(
                "PageCacheHits",
                "Number of times a page was found in the page cache."
        ));

        pageCacheMisses = source.addMetric(new LongAdderMetric(
                "PageCacheMisses",
                "Number of times a page was not found in the page cache and had to be loaded from disk."
        ));

        pageAcquireTime = source.addMetric(new DistributionMetric(
                "PageAcquireTime",
                "Distribution of page acquisition time in nanoseconds.",
                PAGE_ACQUISITIONS_BOUNDS_NANOS
        ));

        pageCacheHitRate = source.addMetric(new HitRateMetric(
                "PageCacheHitRate",
                "Page cache hit rate over the last 5 minutes.",
                TimeUnit.MINUTES.toMillis(5)
        ));

        pageReplacements = source.addMetric(new LongAdderMetric(
                "PageReplacements",
                "Number of times a page was replaced (evicted) from the page cache."
        ));

        source.addMetric(new LongGauge(
                "LoadedPages",
                "Current number of pages loaded in memory.",
                pageMemory::loadedPages
        ));

        source.addMetric(new LongGauge(
                "DirtyPages",
                "Current number of dirty pages in memory.",
                pageMemory::dirtyPagesCount
        ));
    }

    /** Increases the disk page read metric by one. */
    public void incrementReadFromDiskMetric() {
        readPagesFromDisk.increment();
    }

    /** Increases the page writes to disk metric by one. */
    public void incrementWriteToDiskMetric() {
        writePagesToDisk.increment();
    }

    /** Increases the page cache hit metric by one. */
    @Override
    public void incrementPageCacheHit() {
        pageCacheHitsTotal.increment();
        pageCacheHitRate.increment();
    }

    /** Increases the page cache miss metric by one. */
    @Override
    public void incrementPageCacheMiss() {
        pageCacheMisses.increment();
    }

    /** Records a page acquisition time in nanoseconds. */
    public void recordPageAcquireTime(long nanos) {
        pageAcquireTime.add(nanos);
    }

    /** Increases the page replacement metric by one. */
    @Override
    public void incrementPageReplacement() {
        pageReplacements.increment();
    }
}
