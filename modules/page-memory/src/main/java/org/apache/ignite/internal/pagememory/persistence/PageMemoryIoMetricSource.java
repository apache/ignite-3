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

import java.util.List;
import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.DistributionMetric;
import org.apache.ignite.internal.metrics.LongAdderMetric;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.pagememory.metrics.MetricBounds;

/**
 * Metric source for page memory byte-level I/O operations.
 *
 * <p>This metric source tracks physical I/O performance including bytes transferred,
 * operation sizes, and latencies at the file I/O level.
 */
public class PageMemoryIoMetricSource extends AbstractMetricSource<PageMemoryIoMetricSource.Holder> {
    /**
     * Constructor.
     */
    public PageMemoryIoMetricSource() {
        super("pagememory.io", "Page memory byte-level I/O metrics", "pagememory");
    }

    @Override
    protected Holder createHolder() {
        return new Holder();
    }

    /** Metrics holder. */
    protected static class Holder implements AbstractMetricSource.Holder<Holder> {
        private final LongAdderMetric totalBytesRead = new LongAdderMetric(
                "TotalBytesRead",
                "Cumulative bytes read from disk since startup."
        );

        private final LongAdderMetric totalBytesWritten = new LongAdderMetric(
                "TotalBytesWritten",
                "Cumulative bytes written to disk since startup."
        );

        private final DistributionMetric bytesPerRead = new DistributionMetric(
                "BytesPerRead",
                "Distribution of read operation sizes in bytes.",
                MetricBounds.IO_SIZE_BYTES
        );

        private final DistributionMetric bytesPerWrite = new DistributionMetric(
                "BytesPerWrite",
                "Distribution of write operation sizes in bytes.",
                MetricBounds.IO_SIZE_BYTES
        );

        private final DistributionMetric physicalReadsTime = new DistributionMetric(
                "PhysicalReadsTime",
                "Time spent in physical disk read operations (FileChannel.read) in nanoseconds.",
                MetricBounds.DISK_IO_NANOS
        );

        private final DistributionMetric physicalWritesTime = new DistributionMetric(
                "PhysicalWritesTime",
                "Time spent in physical disk write operations (FileChannel.write) in nanoseconds.",
                MetricBounds.DISK_IO_NANOS
        );

        private final LongAdderMetric pageReadErrors = new LongAdderMetric(
                "PageReadErrors",
                "Failed page read operations (I/O errors)."
        );

        private final LongAdderMetric pageWriteErrors = new LongAdderMetric(
                "PageWriteErrors",
                "Failed page write operations (I/O errors)."
        );

        @Override
        public Iterable<Metric> metrics() {
            return List.of(
                    totalBytesRead,
                    totalBytesWritten,
                    bytesPerRead,
                    bytesPerWrite,
                    physicalReadsTime,
                    physicalWritesTime,
                    pageReadErrors,
                    pageWriteErrors
            );
        }

        /** Returns total bytes read metric. */
        public LongAdderMetric totalBytesRead() {
            return totalBytesRead;
        }

        /** Returns total bytes written metric. */
        public LongAdderMetric totalBytesWritten() {
            return totalBytesWritten;
        }

        /** Returns bytes per read metric. */
        public DistributionMetric bytesPerRead() {
            return bytesPerRead;
        }

        /** Returns bytes per write metric. */
        public DistributionMetric bytesPerWrite() {
            return bytesPerWrite;
        }

        /** Returns physical reads time metric. */
        public DistributionMetric physicalReadsTime() {
            return physicalReadsTime;
        }

        /** Returns physical writes time metric. */
        public DistributionMetric physicalWritesTime() {
            return physicalWritesTime;
        }

        /** Returns page read errors metric. */
        public LongAdderMetric pageReadErrors() {
            return pageReadErrors;
        }

        /** Returns page write errors metric. */
        public LongAdderMetric pageWriteErrors() {
            return pageWriteErrors;
        }
    }
}
