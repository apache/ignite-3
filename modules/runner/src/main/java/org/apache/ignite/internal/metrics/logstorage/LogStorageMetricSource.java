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

package org.apache.ignite.internal.metrics.logstorage;

import java.util.List;
import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.LongGauge;
import org.apache.ignite.internal.metrics.LongMetric;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.MetricSource;
import org.apache.ignite.internal.metrics.logstorage.LogStorageMetricSource.Holder;

/**
 * {@link MetricSource} for log storage metrics.
 */
class LogStorageMetricSource extends AbstractMetricSource<Holder> {
    static final String NAME = "log.storage";

    private volatile long cmgLogStorageSizeBytes;
    private volatile long metastorageLogStorageSizeBytes;
    private volatile long partitionsLogStorageSizeBytes;

    LogStorageMetricSource() {
        super(NAME, "Log storage metrics.");
    }

    void cmgLogStorageSize(long newSize) {
        this.cmgLogStorageSizeBytes = newSize;
    }

    void metastorageLogStorageSize(long newSize) {
        this.metastorageLogStorageSizeBytes = newSize;
    }

    void partitionsLogStorageSize(long newSize) {
        this.partitionsLogStorageSizeBytes = newSize;
    }

    @Override
    protected Holder createHolder() {
        return new Holder();
    }

    protected class Holder implements AbstractMetricSource.Holder<Holder> {
        private final LongMetric cmgLogStorageSize = new LongGauge(
                "CmgLogStorageSize",
                "Number of bytes occupied on disk by the CMG log.",
                () -> cmgLogStorageSizeBytes
        );

        private final LongMetric metastorageLogStorageSize = new LongGauge(
                "MetastorageLogStorageSize",
                "Number of bytes occupied on disk by the Metastorage group log.",
                () -> metastorageLogStorageSizeBytes
        );

        private final LongMetric partitionsLogStorageSize = new LongGauge(
                "PartitionsLogStorageSize",
                "Number of bytes occupied on disk by the partitions groups logs.",
                () -> partitionsLogStorageSizeBytes
        );

        private final LongMetric totalLogStorageSize = new LongGauge(
                "TotalLogStorageSize",
                "Number of bytes occupied on disk by logs of all replication groups.",
                () -> cmgLogStorageSizeBytes + metastorageLogStorageSizeBytes + partitionsLogStorageSizeBytes
        );

        private final List<Metric> metrics = List.of(
                cmgLogStorageSize,
                metastorageLogStorageSize,
                partitionsLogStorageSize,
                totalLogStorageSize
        );

        @Override
        public Iterable<Metric> metrics() {
            return metrics;
        }
    }
}
