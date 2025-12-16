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
import org.apache.ignite.internal.metrics.LongAdderMetric;

/**
 * Page memory byte-level I/O metrics.
 *
 * <p>Tracks actual bytes transferred and latencies of physical I/O operations at the file level.
 * Complements existing page count metrics with byte-level granularity.
 */
public class PageMemoryIoMetrics {
    private final LongAdderMetric totalBytesRead;

    private final LongAdderMetric totalBytesWritten;

    private final DistributionMetric bytesPerRead;

    private final DistributionMetric bytesPerWrite;

    private final DistributionMetric physicalReadsTime;

    private final DistributionMetric physicalWritesTime;

    private final LongAdderMetric pageReadErrors;

    private final LongAdderMetric pageWriteErrors;

    /**
     * Constructor.
     *
     * @param source Metric source to get metrics from.
     */
    public PageMemoryIoMetrics(PageMemoryIoMetricSource source) {
        // Enable the source immediately to create the holder
        source.enable();

        // Get the holder with metric instances
        PageMemoryIoMetricSource.Holder holder = source.holder();

        assert holder != null : "Holder must be non-null after enable()";

        this.totalBytesRead = holder.totalBytesRead();
        this.totalBytesWritten = holder.totalBytesWritten();
        this.bytesPerRead = holder.bytesPerRead();
        this.bytesPerWrite = holder.bytesPerWrite();
        this.physicalReadsTime = holder.physicalReadsTime();
        this.physicalWritesTime = holder.physicalWritesTime();
        this.pageReadErrors = holder.pageReadErrors();
        this.pageWriteErrors = holder.pageWriteErrors();
    }

    /**
     * Returns the total bytes read metric.
     *
     * @return Total bytes read metric.
     */
    public LongAdderMetric totalBytesRead() {
        return totalBytesRead;
    }

    /**
     * Returns the total bytes written metric.
     *
     * @return Total bytes written metric.
     */
    public LongAdderMetric totalBytesWritten() {
        return totalBytesWritten;
    }

    /**
     * Returns the bytes per read distribution metric.
     *
     * @return Bytes per read metric.
     */
    public DistributionMetric bytesPerRead() {
        return bytesPerRead;
    }

    /**
     * Returns the bytes per write distribution metric.
     *
     * @return Bytes per write metric.
     */
    public DistributionMetric bytesPerWrite() {
        return bytesPerWrite;
    }

    /**
     * Returns the physical reads time distribution metric.
     *
     * @return Physical reads time metric.
     */
    public DistributionMetric physicalReadsTime() {
        return physicalReadsTime;
    }

    /**
     * Returns the physical writes time distribution metric.
     *
     * @return Physical writes time metric.
     */
    public DistributionMetric physicalWritesTime() {
        return physicalWritesTime;
    }

    /**
     * Returns the page read errors counter metric.
     *
     * @return Page read errors metric.
     */
    public LongAdderMetric pageReadErrors() {
        return pageReadErrors;
    }

    /**
     * Returns the page write errors counter metric.
     *
     * @return Page write errors metric.
     */
    public LongAdderMetric pageWriteErrors() {
        return pageWriteErrors;
    }
}
