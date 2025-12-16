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

import org.apache.ignite.internal.metrics.DistributionMetric;
import org.apache.ignite.internal.metrics.HitRateMetric;
import org.apache.ignite.internal.metrics.IntGauge;
import org.apache.ignite.internal.metrics.LongAdderMetric;
import org.apache.ignite.internal.metrics.LongGauge;
import org.apache.ignite.internal.pagememory.configuration.PersistentDataRegionConfiguration;
import org.apache.ignite.internal.pagememory.metrics.MetricBounds;

/** Persistent page memory metrics. */
class PersistentPageMemoryMetrics {
    private final LongAdderMetric readPagesFromDisk;

    private final LongAdderMetric writePagesToDisk;

    private final LongAdderMetric pageCacheHits;

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

        pageCacheHits = source.addMetric(new LongAdderMetric(
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
                MetricBounds.PAGE_ACQUIRE_NANOS
        ));

        pageCacheHitRate = source.addMetric(new HitRateMetric(
                "PageCacheHitRate",
                "Page cache hit rate over the last 60 seconds.",
                60_000 // 60 seconds time window
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

        source.addMetric(new IntGauge(
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
    public void incrementPageCacheHit() {
        pageCacheHits.increment();
        pageCacheHitRate.increment();
    }

    /** Increases the page cache miss metric by one. */
    public void incrementPageCacheMiss() {
        pageCacheMisses.increment();
    }

    /** Records a page acquisition time in nanoseconds. */
    public void recordPageAcquireTime(long nanos) {
        pageAcquireTime.add(nanos);
    }

    /** Increases the page replacement metric by one. */
    public void incrementPageReplacement() {
        pageReplacements.increment();
    }
}
