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

import org.apache.ignite.internal.metrics.DistributionMetric;
import org.apache.ignite.internal.metrics.IntGauge;
import org.apache.ignite.internal.metrics.LongAdderMetric;
import org.apache.ignite.internal.metrics.LongGauge;
import org.apache.ignite.internal.pagememory.metrics.MetricBounds;

/**
 * Storage file operation metrics.
 *
 * <p>Tracks file lifecycle (open/create/close), file counts, sizes, and operation latencies.
 */
class StorageFilesMetrics {
    private final IntGauge openFilesCount;

    private final LongAdderMetric fileOpenTotal;

    private final LongAdderMetric fileCreateTotal;

    private final LongAdderMetric deltaFileCreateTotal;

    private final DistributionMetric fileOpenTime;

    private final DistributionMetric fileCreateTime;

    private final DistributionMetric fileSyncTime;

    private final LongGauge totalFileSize;

    private final IntGauge deltaFilesCount;

    private final LongGauge deltaFilesTotalSize;

    private final LongAdderMetric fileOpenErrors;

    /**
     * Constructor.
     *
     * @param source Metric source to register metrics with.
     * @param openFilesCountSupplier Supplier for the number of open files.
     * @param totalFileSizeSupplier Supplier for the total file size.
     * @param deltaFilesCountSupplier Supplier for the number of delta files.
     * @param deltaFilesTotalSizeSupplier Supplier for the total delta files size.
     */
    StorageFilesMetrics(
            StorageFilesMetricSource source,
            java.util.function.IntSupplier openFilesCountSupplier,
            java.util.function.LongSupplier totalFileSizeSupplier,
            java.util.function.IntSupplier deltaFilesCountSupplier,
            java.util.function.LongSupplier deltaFilesTotalSizeSupplier
    ) {
        openFilesCount = source.addMetric(new IntGauge(
                "OpenFilesCount",
                "Current number of open file handles.",
                openFilesCountSupplier
        ));

        fileOpenTotal = source.addMetric(new LongAdderMetric(
                "FileOpenTotal",
                "Total file open operations since startup."
        ));

        fileCreateTotal = source.addMetric(new LongAdderMetric(
                "FileCreateTotal",
                "Total file create operations since startup."
        ));

        deltaFileCreateTotal = source.addMetric(new LongAdderMetric(
                "DeltaFileCreateTotal",
                "Total delta file create operations since startup."
        ));

        fileOpenTime = source.addMetric(new DistributionMetric(
                "FileOpenTime",
                "Time to open existing file in nanoseconds.",
                MetricBounds.FILE_OPEN_NANOS
        ));

        fileCreateTime = source.addMetric(new DistributionMetric(
                "FileCreateTime",
                "Time to create new file (includes allocation) in nanoseconds.",
                MetricBounds.FILE_OPEN_NANOS
        ));

        fileSyncTime = source.addMetric(new DistributionMetric(
                "FileSyncTime",
                "Time spent in fsync/fdatasync operations in nanoseconds.",
                MetricBounds.FSYNC_NANOS
        ));

        totalFileSize = source.addMetric(new LongGauge(
                "TotalFileSize",
                "Total size of all data files (main + delta) in bytes.",
                totalFileSizeSupplier
        ));

        deltaFilesCount = source.addMetric(new IntGauge(
                "DeltaFilesCount",
                "Current number of delta files across all partitions.",
                deltaFilesCountSupplier
        ));

        deltaFilesTotalSize = source.addMetric(new LongGauge(
                "DeltaFilesTotalSize",
                "Total size of all delta files in bytes.",
                deltaFilesTotalSizeSupplier
        ));

        fileOpenErrors = source.addMetric(new LongAdderMetric(
                "FileOpenErrors",
                "Failed file open attempts."
        ));
    }

    /**
     * Returns the open files count gauge metric.
     *
     * @return Open files count metric.
     */
    public IntGauge openFilesCount() {
        return openFilesCount;
    }

    /**
     * Returns the file open total counter metric.
     *
     * @return File open total metric.
     */
    public LongAdderMetric fileOpenTotal() {
        return fileOpenTotal;
    }

    /**
     * Returns the file create total counter metric.
     *
     * @return File create total metric.
     */
    public LongAdderMetric fileCreateTotal() {
        return fileCreateTotal;
    }

    /**
     * Returns the delta file create total counter metric.
     *
     * @return Delta file create total metric.
     */
    public LongAdderMetric deltaFileCreateTotal() {
        return deltaFileCreateTotal;
    }

    /**
     * Returns the file open time distribution metric.
     *
     * @return File open time metric.
     */
    public DistributionMetric fileOpenTime() {
        return fileOpenTime;
    }

    /**
     * Returns the file create time distribution metric.
     *
     * @return File create time metric.
     */
    public DistributionMetric fileCreateTime() {
        return fileCreateTime;
    }

    /**
     * Returns the file sync time distribution metric.
     *
     * @return File sync time metric.
     */
    public DistributionMetric fileSyncTime() {
        return fileSyncTime;
    }

    /**
     * Returns the total file size gauge metric.
     *
     * @return Total file size metric.
     */
    public LongGauge totalFileSize() {
        return totalFileSize;
    }

    /**
     * Returns the delta files count gauge metric.
     *
     * @return Delta files count metric.
     */
    public IntGauge deltaFilesCount() {
        return deltaFilesCount;
    }

    /**
     * Returns the delta files total size gauge metric.
     *
     * @return Delta files total size metric.
     */
    public LongGauge deltaFilesTotalSize() {
        return deltaFilesTotalSize;
    }

    /**
     * Returns the file open errors counter metric.
     *
     * @return File open errors metric.
     */
    public LongAdderMetric fileOpenErrors() {
        return fileOpenErrors;
    }
}
