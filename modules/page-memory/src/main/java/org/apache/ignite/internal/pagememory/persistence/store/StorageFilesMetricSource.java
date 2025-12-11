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

package org.apache.ignite.internal.pagememory.persistence.store;

import java.util.List;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.DistributionMetric;
import org.apache.ignite.internal.metrics.IntGauge;
import org.apache.ignite.internal.metrics.LongAdderMetric;
import org.apache.ignite.internal.metrics.LongGauge;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.pagememory.metrics.MetricBounds;

/**
 * Metric source for storage file operations.
 *
 * <p>This metric source tracks file lifecycle operations (open, create, close),
 * file counts, sizes, and operation latencies for data and delta files.
 */
public class StorageFilesMetricSource extends AbstractMetricSource<StorageFilesMetricSource.Holder> {
    private final IntSupplier openFilesCountSupplier;
    private final LongSupplier totalFileSizeSupplier;
    private final IntSupplier deltaFilesCountSupplier;
    private final LongSupplier deltaFilesTotalSizeSupplier;

    /**
     * Constructor.
     *
     * @param openFilesCountSupplier Supplier for the number of open files.
     * @param totalFileSizeSupplier Supplier for the total file size.
     * @param deltaFilesCountSupplier Supplier for the number of delta files.
     * @param deltaFilesTotalSizeSupplier Supplier for the total delta files size.
     */
    public StorageFilesMetricSource(
            IntSupplier openFilesCountSupplier,
            LongSupplier totalFileSizeSupplier,
            IntSupplier deltaFilesCountSupplier,
            LongSupplier deltaFilesTotalSizeSupplier
    ) {
        super("storage.files", "Storage file operation metrics", "storage");
        this.openFilesCountSupplier = openFilesCountSupplier;
        this.totalFileSizeSupplier = totalFileSizeSupplier;
        this.deltaFilesCountSupplier = deltaFilesCountSupplier;
        this.deltaFilesTotalSizeSupplier = deltaFilesTotalSizeSupplier;
    }

    @Override
    protected Holder createHolder() {
        return new Holder(openFilesCountSupplier, totalFileSizeSupplier, deltaFilesCountSupplier, deltaFilesTotalSizeSupplier);
    }

    /** Metrics holder. */
    protected static class Holder implements AbstractMetricSource.Holder<Holder> {
        private final IntGauge openFilesCount;
        private final LongAdderMetric fileOpenTotal = new LongAdderMetric("FileOpenTotal", "Total file open operations since startup.");
        private final LongAdderMetric fileCreateTotal = new LongAdderMetric(
                "FileCreateTotal",
                "Total file create operations since startup."
        );
        private final LongAdderMetric deltaFileCreateTotal = new LongAdderMetric(
                "DeltaFileCreateTotal",
                "Total delta file create operations since startup."
        );
        private final DistributionMetric fileOpenTime = new DistributionMetric(
                "FileOpenTime",
                "Time to open existing file in nanoseconds.",
                MetricBounds.FILE_OPEN_NANOS
        );
        private final DistributionMetric fileCreateTime = new DistributionMetric(
                "FileCreateTime",
                "Time to create new file (includes allocation) in nanoseconds.",
                MetricBounds.FILE_OPEN_NANOS
        );
        private final DistributionMetric fileSyncTime = new DistributionMetric(
                "FileSyncTime",
                "Time spent in fsync/fdatasync operations in nanoseconds.",
                MetricBounds.FSYNC_NANOS
        );
        private final LongGauge totalFileSize;
        private final IntGauge deltaFilesCount;
        private final LongGauge deltaFilesTotalSize;
        private final LongAdderMetric fileOpenErrors = new LongAdderMetric("FileOpenErrors", "Failed file open attempts.");

        /**
         * Constructor.
         *
         * @param openFilesCountSupplier Supplier for the number of open files.
         * @param totalFileSizeSupplier Supplier for the total file size.
         * @param deltaFilesCountSupplier Supplier for the number of delta files.
         * @param deltaFilesTotalSizeSupplier Supplier for the total delta files size.
         */
        Holder(
                IntSupplier openFilesCountSupplier,
                LongSupplier totalFileSizeSupplier,
                IntSupplier deltaFilesCountSupplier,
                LongSupplier deltaFilesTotalSizeSupplier
        ) {
            this.openFilesCount = new IntGauge("OpenFilesCount", "Current number of open file handles.", openFilesCountSupplier);
            this.totalFileSize = new LongGauge(
                    "TotalFileSize",
                    "Total size of all data files (main + delta) in bytes.",
                    totalFileSizeSupplier
            );
            this.deltaFilesCount = new IntGauge(
                    "DeltaFilesCount",
                    "Current number of delta files across all partitions.",
                    deltaFilesCountSupplier
            );
            this.deltaFilesTotalSize = new LongGauge(
                    "DeltaFilesTotalSize",
                    "Total size of all delta files in bytes.",
                    deltaFilesTotalSizeSupplier
            );
        }

        @Override
        public Iterable<Metric> metrics() {
            return List.of(
                    openFilesCount,
                    fileOpenTotal,
                    fileCreateTotal,
                    deltaFileCreateTotal,
                    fileOpenTime,
                    fileCreateTime,
                    fileSyncTime,
                    totalFileSize,
                    deltaFilesCount,
                    deltaFilesTotalSize,
                    fileOpenErrors
            );
        }

        /** Returns open files count metric. */
        public IntGauge openFilesCount() {
            return openFilesCount;
        }

        /** Returns file open total metric. */
        public LongAdderMetric fileOpenTotal() {
            return fileOpenTotal;
        }

        /** Returns file create total metric. */
        public LongAdderMetric fileCreateTotal() {
            return fileCreateTotal;
        }

        /** Returns delta file create total metric. */
        public LongAdderMetric deltaFileCreateTotal() {
            return deltaFileCreateTotal;
        }

        /** Returns file open time metric. */
        public DistributionMetric fileOpenTime() {
            return fileOpenTime;
        }

        /** Returns file create time metric. */
        public DistributionMetric fileCreateTime() {
            return fileCreateTime;
        }

        /** Returns file sync time metric. */
        public DistributionMetric fileSyncTime() {
            return fileSyncTime;
        }

        /** Returns total file size metric. */
        public LongGauge totalFileSize() {
            return totalFileSize;
        }

        /** Returns delta files count metric. */
        public IntGauge deltaFilesCount() {
            return deltaFilesCount;
        }

        /** Returns delta files total size metric. */
        public LongGauge deltaFilesTotalSize() {
            return deltaFilesTotalSize;
        }

        /** Returns file open errors metric. */
        public LongAdderMetric fileOpenErrors() {
            return fileOpenErrors;
        }
    }
}
