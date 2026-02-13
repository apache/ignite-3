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
import org.apache.ignite.internal.fileio.FileIoMetrics;
import org.apache.ignite.internal.metrics.DistributionMetric;
import org.apache.ignite.internal.metrics.LongAdderMetric;

/**
 * Byte-level I/O metrics for page memory operations.
 */
public class PageMemoryIoMetrics implements FileIoMetrics {
    public static final String TOTAL_BYTES_READ = "TotalBytesRead";
    public static final String TOTAL_BYTES_WRITTEN = "TotalBytesWritten";
    public static final String READS_TIME = "ReadsTime";
    public static final String WRITES_TIME = "WritesTime";

    /**
     * Histogram buckets for I/O latency in microseconds.
     *
     * <p>All cached access operations hit the first bucket. Their estimate is within 1µs.
     *
     * <p>Values are estimated on possible storage types:
     * <ul>
     *   <li>NVMe SSD: Most operations &lt;10µs, outliers 10-100µs</li>
     *   <li>SATA SSD: Most operations 10-100µs, outliers 100-1000µs</li>
     *   <li>HDD: Most operations 1-10ms, outliers &gt;10ms</li>
     * </ul>
     */
    private static final long[] DISK_IO_MICROSECONDS = {10, 100, 1_000, 10_000};

    private final LongAdderMetric totalBytesRead = new LongAdderMetric(
            TOTAL_BYTES_READ,
            "Cumulative bytes read from disk since startup."
    );

    private final LongAdderMetric totalBytesWritten = new LongAdderMetric(
            TOTAL_BYTES_WRITTEN,
            "Cumulative bytes written to disk since startup."
    );

    private final DistributionMetric readsTime = new DistributionMetric(
            READS_TIME,
            "Time spent in disk read operation in microseconds.",
            DISK_IO_MICROSECONDS
    );

    private final DistributionMetric writesTime = new DistributionMetric(
            WRITES_TIME,
            "Time spent in disk write operation in microseconds.",
            DISK_IO_MICROSECONDS
    );

    /** Constructor. */
    public PageMemoryIoMetrics(PageMemoryIoMetricSource source) {
        source.addMetric(totalBytesRead);
        source.addMetric(totalBytesWritten);
        source.addMetric(readsTime);
        source.addMetric(writesTime);
    }

    @Override
    public void recordRead(int bytesRead, long durationNanos) {
        if (bytesRead > 0) {
            totalBytesRead.add(bytesRead);
        }
        readsTime.add(TimeUnit.NANOSECONDS.toMicros(durationNanos));
    }

    @Override
    public void recordWrite(int bytesWritten, long durationNanos) {
        if (bytesWritten > 0) {
            totalBytesWritten.add(bytesWritten);
        }
        writesTime.add(TimeUnit.NANOSECONDS.toMicros(durationNanos));
    }
}
