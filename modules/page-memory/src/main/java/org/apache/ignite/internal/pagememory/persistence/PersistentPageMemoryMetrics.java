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

import org.apache.ignite.internal.metrics.IntGauge;
import org.apache.ignite.internal.metrics.LongAdderMetric;
import org.apache.ignite.internal.metrics.LongGauge;
import org.apache.ignite.internal.pagememory.configuration.PersistentDataRegionConfiguration;

/** Persistent page memory metrics. */
class PersistentPageMemoryMetrics {
    private final LongAdderMetric readPagesFromDisk;

    private final LongAdderMetric writePagesToDisk;

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
    }

    /** Increases the disk page read metric by one. */
    public void incrementReadFromDiskMetric() {
        readPagesFromDisk.increment();
    }

    /** Increases the page writes to disk metric by one. */
    public void incrementWriteToDiskMetric() {
        writePagesToDisk.increment();
    }
}
