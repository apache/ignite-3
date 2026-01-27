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
 * Page memory byte-level I/O metrics.
 *
 * <p>Tracks actual bytes transferred and latencies of physical I/O operations at the file level.
 * Complements existing page count metrics with byte-level granularity.
 */
public class PageMemoryIoMetrics implements FileIoMetrics {
    public static final String TOTAL_BYTES_READ = "TotalBytesRead";
    public static final String TOTAL_BYTES_WRITTEN = "TotalBytesWritten";
    public static final String READS_TIME = "ReadsTime";
    public static final String WRITES_TIME = "WritesTime";

    private static final long[] DISK_IO_MICROSECONDS = {
            100,       // 100µs - Fast IO
            1_000,     // 1ms   - Slow IO
            100_000    // 100ms - Very slow IO
    };

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

    /**
     * Constructor.
     *
     * @param source Metric source to get metrics from.
     */
    public PageMemoryIoMetrics(PageMemoryIoMetricSource source) {
        source.addMetric(totalBytesRead);
        source.addMetric(totalBytesWritten);
        source.addMetric(readsTime);
        source.addMetric(writesTime);
    }

    @Override
    public void recordRead(int bytesRead, long durationNanos) {
        totalBytesRead.add(bytesRead);
        readsTime.add(TimeUnit.NANOSECONDS.toMicros(durationNanos));
    }

    @Override
    public void recordWrite(int bytesWritten, long durationNanos) {
        totalBytesWritten.add(bytesWritten);
        writesTime.add(TimeUnit.NANOSECONDS.toMicros(durationNanos));
    }
}
