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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link PageMemoryIoMetrics}.
 */
class PageMemoryIoMetricsTest {
    @Test
    void testMetricsCreation() {
        PageMemoryIoMetricSource source = new PageMemoryIoMetricSource();
        PageMemoryIoMetrics metrics = new PageMemoryIoMetrics(source);

        assertThat(metrics.totalBytesRead(), notNullValue());
        assertThat(metrics.totalBytesWritten(), notNullValue());
        assertThat(metrics.bytesPerRead(), notNullValue());
        assertThat(metrics.bytesPerWrite(), notNullValue());
        assertThat(metrics.physicalReadsTime(), notNullValue());
        assertThat(metrics.physicalWritesTime(), notNullValue());
        assertThat(metrics.pageReadErrors(), notNullValue());
        assertThat(metrics.pageWriteErrors(), notNullValue());
    }

    @Test
    void testTotalBytesReadTracking() {
        PageMemoryIoMetricSource source = new PageMemoryIoMetricSource();
        PageMemoryIoMetrics metrics = new PageMemoryIoMetrics(source);

        metrics.totalBytesRead().add(4096); // 1 page
        metrics.totalBytesRead().add(8192); // 2 pages

        assertThat(metrics.totalBytesRead().value(), equalTo(12288L));
    }

    @Test
    void testTotalBytesWrittenTracking() {
        PageMemoryIoMetricSource source = new PageMemoryIoMetricSource();
        PageMemoryIoMetrics metrics = new PageMemoryIoMetrics(source);

        metrics.totalBytesWritten().add(4096); // 1 page
        metrics.totalBytesWritten().add(16384); // 4 pages

        assertThat(metrics.totalBytesWritten().value(), equalTo(20480L));
    }

    @Test
    void testBytesPerReadDistribution() {
        PageMemoryIoMetricSource source = new PageMemoryIoMetricSource();
        PageMemoryIoMetrics metrics = new PageMemoryIoMetrics(source);

        // Simulate different read sizes
        metrics.bytesPerRead().add(4096);  // Single page
        metrics.bytesPerRead().add(8192);  // Two pages
        metrics.bytesPerRead().add(16384); // Four pages

        long[] values = metrics.bytesPerRead().value();
        long totalCount = 0;
        for (long value : values) {
            totalCount += value;
        }

        assertThat(totalCount, equalTo(3L));
    }

    @Test
    void testPhysicalReadsTimeDistribution() {
        PageMemoryIoMetricSource source = new PageMemoryIoMetricSource();
        PageMemoryIoMetrics metrics = new PageMemoryIoMetrics(source);

        // Simulate fast SSD read (100µs)
        metrics.physicalReadsTime().add(100_000);

        long[] values = metrics.physicalReadsTime().value();
        long totalCount = 0;
        for (long value : values) {
            totalCount += value;
        }

        assertThat(totalCount, greaterThan(0L));
    }

    @Test
    void testPhysicalWritesTimeDistribution() {
        PageMemoryIoMetricSource source = new PageMemoryIoMetricSource();
        PageMemoryIoMetrics metrics = new PageMemoryIoMetrics(source);

        // Simulate SSD write (500µs)
        metrics.physicalWritesTime().add(500_000);

        long[] values = metrics.physicalWritesTime().value();
        long totalCount = 0;
        for (long value : values) {
            totalCount += value;
        }

        assertThat(totalCount, greaterThan(0L));
    }

    @Test
    void testPageReadErrorsCounter() {
        PageMemoryIoMetricSource source = new PageMemoryIoMetricSource();
        PageMemoryIoMetrics metrics = new PageMemoryIoMetrics(source);

        metrics.pageReadErrors().increment();
        metrics.pageReadErrors().increment();

        assertThat(metrics.pageReadErrors().value(), equalTo(2L));
    }

    @Test
    void testPageWriteErrorsCounter() {
        PageMemoryIoMetricSource source = new PageMemoryIoMetricSource();
        PageMemoryIoMetrics metrics = new PageMemoryIoMetrics(source);

        metrics.pageWriteErrors().increment();

        assertThat(metrics.pageWriteErrors().value(), equalTo(1L));
    }

    @Test
    void testMetricSourceName() {
        PageMemoryIoMetricSource source = new PageMemoryIoMetricSource();

        assertThat(source.name(), equalTo("pagememory.io"));
    }

    @Test
    void testMetricSourceEnabled() {
        PageMemoryIoMetricSource source = new PageMemoryIoMetricSource();

        source.enable();
        assertThat(source.enabled(), equalTo(true));

        source.disable();
        assertThat(source.enabled(), equalTo(false));
    }
}
