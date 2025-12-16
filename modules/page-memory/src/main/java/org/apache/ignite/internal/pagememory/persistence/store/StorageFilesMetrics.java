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

/**
 * Storage file operation metrics.
 *
 * <p>Tracks file lifecycle (open/create/close), file counts, sizes, and operation latencies.
 */
public class StorageFilesMetrics {
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
     * @param source Metric source to get metrics from.
     */
    public StorageFilesMetrics(StorageFilesMetricSource source) {
        // Enable the source immediately to create the holder
        source.enable();

        // Get the holder with metric instances
        StorageFilesMetricSource.Holder holder = source.holder();

        assert holder != null : "Holder must be non-null after enable()";

        this.openFilesCount = holder.openFilesCount();
        this.fileOpenTotal = holder.fileOpenTotal();
        this.fileCreateTotal = holder.fileCreateTotal();
        this.deltaFileCreateTotal = holder.deltaFileCreateTotal();
        this.fileOpenTime = holder.fileOpenTime();
        this.fileCreateTime = holder.fileCreateTime();
        this.fileSyncTime = holder.fileSyncTime();
        this.totalFileSize = holder.totalFileSize();
        this.deltaFilesCount = holder.deltaFilesCount();
        this.deltaFilesTotalSize = holder.deltaFilesTotalSize();
        this.fileOpenErrors = holder.fileOpenErrors();
    }

    public IntGauge openFilesCount() {
        return openFilesCount;
    }

    public LongAdderMetric fileOpenTotal() {
        return fileOpenTotal;
    }

    public LongAdderMetric fileCreateTotal() {
        return fileCreateTotal;
    }

    public LongAdderMetric deltaFileCreateTotal() {
        return deltaFileCreateTotal;
    }

    public DistributionMetric fileOpenTime() {
        return fileOpenTime;
    }

    public DistributionMetric fileCreateTime() {
        return fileCreateTime;
    }

    public DistributionMetric fileSyncTime() {
        return fileSyncTime;
    }

    public LongGauge totalFileSize() {
        return totalFileSize;
    }

    public IntGauge deltaFilesCount() {
        return deltaFilesCount;
    }

    public LongGauge deltaFilesTotalSize() {
        return deltaFilesTotalSize;
    }

    public LongAdderMetric fileOpenErrors() {
        return fileOpenErrors;
    }
}
