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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link StorageFilesMetrics}.
 */
class StorageFilesMetricsTest {
    @Test
    void testMetricsCreation() {
        AtomicInteger openFilesCount = new AtomicInteger(0);
        AtomicLong totalFileSize = new AtomicLong(0);
        AtomicInteger deltaFilesCount = new AtomicInteger(0);
        AtomicLong deltaFilesTotalSize = new AtomicLong(0);

        StorageFilesMetricSource source = new StorageFilesMetricSource(
                openFilesCount::get,
                totalFileSize::get,
                deltaFilesCount::get,
                deltaFilesTotalSize::get
        );

        StorageFilesMetrics metrics = new StorageFilesMetrics(source);

        assertThat(metrics.openFilesCount(), notNullValue());
        assertThat(metrics.fileOpenTotal(), notNullValue());
        assertThat(metrics.fileCreateTotal(), notNullValue());
        assertThat(metrics.deltaFileCreateTotal(), notNullValue());
        assertThat(metrics.fileOpenTime(), notNullValue());
        assertThat(metrics.fileCreateTime(), notNullValue());
        assertThat(metrics.fileSyncTime(), notNullValue());
        assertThat(metrics.totalFileSize(), notNullValue());
        assertThat(metrics.deltaFilesCount(), notNullValue());
        assertThat(metrics.deltaFilesTotalSize(), notNullValue());
        assertThat(metrics.fileOpenErrors(), notNullValue());
    }

    @Test
    void testOpenFilesCountGauge() {
        AtomicInteger openFilesCount = new AtomicInteger(5);
        AtomicLong totalFileSize = new AtomicLong(0);
        AtomicInteger deltaFilesCount = new AtomicInteger(0);
        AtomicLong deltaFilesTotalSize = new AtomicLong(0);

        StorageFilesMetricSource source = new StorageFilesMetricSource(
                openFilesCount::get,
                totalFileSize::get,
                deltaFilesCount::get,
                deltaFilesTotalSize::get
        );

        StorageFilesMetrics metrics = new StorageFilesMetrics(source);

        assertThat(metrics.openFilesCount().value(), equalTo(5));

        openFilesCount.set(10);
        assertThat(metrics.openFilesCount().value(), equalTo(10));
    }

    @Test
    void testFileOpenTotalCounter() {
        AtomicInteger openFilesCount = new AtomicInteger(0);
        AtomicLong totalFileSize = new AtomicLong(0);
        AtomicInteger deltaFilesCount = new AtomicInteger(0);
        AtomicLong deltaFilesTotalSize = new AtomicLong(0);

        StorageFilesMetricSource source = new StorageFilesMetricSource(
                openFilesCount::get,
                totalFileSize::get,
                deltaFilesCount::get,
                deltaFilesTotalSize::get
        );

        StorageFilesMetrics metrics = new StorageFilesMetrics(source);

        metrics.fileOpenTotal().increment();
        metrics.fileOpenTotal().increment();
        metrics.fileOpenTotal().increment();

        assertThat(metrics.fileOpenTotal().value(), equalTo(3L));
    }

    @Test
    void testFileCreateTotalCounter() {
        AtomicInteger openFilesCount = new AtomicInteger(0);
        AtomicLong totalFileSize = new AtomicLong(0);
        AtomicInteger deltaFilesCount = new AtomicInteger(0);
        AtomicLong deltaFilesTotalSize = new AtomicLong(0);

        StorageFilesMetricSource source = new StorageFilesMetricSource(
                openFilesCount::get,
                totalFileSize::get,
                deltaFilesCount::get,
                deltaFilesTotalSize::get
        );

        StorageFilesMetrics metrics = new StorageFilesMetrics(source);

        metrics.fileCreateTotal().increment();
        metrics.fileCreateTotal().increment();

        assertThat(metrics.fileCreateTotal().value(), equalTo(2L));
    }

    @Test
    void testDeltaFileCreateTotalCounter() {
        AtomicInteger openFilesCount = new AtomicInteger(0);
        AtomicLong totalFileSize = new AtomicLong(0);
        AtomicInteger deltaFilesCount = new AtomicInteger(0);
        AtomicLong deltaFilesTotalSize = new AtomicLong(0);

        StorageFilesMetricSource source = new StorageFilesMetricSource(
                openFilesCount::get,
                totalFileSize::get,
                deltaFilesCount::get,
                deltaFilesTotalSize::get
        );

        StorageFilesMetrics metrics = new StorageFilesMetrics(source);

        metrics.deltaFileCreateTotal().increment();

        assertThat(metrics.deltaFileCreateTotal().value(), equalTo(1L));
    }

    @Test
    void testFileOpenTimeDistribution() {
        AtomicInteger openFilesCount = new AtomicInteger(0);
        AtomicLong totalFileSize = new AtomicLong(0);
        AtomicInteger deltaFilesCount = new AtomicInteger(0);
        AtomicLong deltaFilesTotalSize = new AtomicLong(0);

        StorageFilesMetricSource source = new StorageFilesMetricSource(
                openFilesCount::get,
                totalFileSize::get,
                deltaFilesCount::get,
                deltaFilesTotalSize::get
        );

        StorageFilesMetrics metrics = new StorageFilesMetrics(source);

        // Simulate file open (1ms)
        metrics.fileOpenTime().add(1_000_000);

        long[] values = metrics.fileOpenTime().value();
        long totalCount = 0;
        for (long value : values) {
            totalCount += value;
        }

        assertThat(totalCount, greaterThan(0L));
    }

    @Test
    void testFileSyncTimeDistribution() {
        AtomicInteger openFilesCount = new AtomicInteger(0);
        AtomicLong totalFileSize = new AtomicLong(0);
        AtomicInteger deltaFilesCount = new AtomicInteger(0);
        AtomicLong deltaFilesTotalSize = new AtomicLong(0);

        StorageFilesMetricSource source = new StorageFilesMetricSource(
                openFilesCount::get,
                totalFileSize::get,
                deltaFilesCount::get,
                deltaFilesTotalSize::get
        );

        StorageFilesMetrics metrics = new StorageFilesMetrics(source);

        // Simulate fsync (10ms)
        metrics.fileSyncTime().add(10_000_000);

        long[] values = metrics.fileSyncTime().value();
        long totalCount = 0;
        for (long value : values) {
            totalCount += value;
        }

        assertThat(totalCount, greaterThan(0L));
    }

    @Test
    void testTotalFileSizeGauge() {
        AtomicInteger openFilesCount = new AtomicInteger(0);
        AtomicLong totalFileSize = new AtomicLong(1048576); // 1MB
        AtomicInteger deltaFilesCount = new AtomicInteger(0);
        AtomicLong deltaFilesTotalSize = new AtomicLong(0);

        StorageFilesMetricSource source = new StorageFilesMetricSource(
                openFilesCount::get,
                totalFileSize::get,
                deltaFilesCount::get,
                deltaFilesTotalSize::get
        );

        StorageFilesMetrics metrics = new StorageFilesMetrics(source);

        assertThat(metrics.totalFileSize().value(), equalTo(1048576L));

        totalFileSize.set(2097152); // 2MB
        assertThat(metrics.totalFileSize().value(), equalTo(2097152L));
    }

    @Test
    void testDeltaFilesCountGauge() {
        AtomicInteger openFilesCount = new AtomicInteger(0);
        AtomicLong totalFileSize = new AtomicLong(0);
        AtomicInteger deltaFilesCount = new AtomicInteger(3);
        AtomicLong deltaFilesTotalSize = new AtomicLong(0);

        StorageFilesMetricSource source = new StorageFilesMetricSource(
                openFilesCount::get,
                totalFileSize::get,
                deltaFilesCount::get,
                deltaFilesTotalSize::get
        );

        StorageFilesMetrics metrics = new StorageFilesMetrics(source);

        assertThat(metrics.deltaFilesCount().value(), equalTo(3));
    }

    @Test
    void testDeltaFilesTotalSizeGauge() {
        AtomicInteger openFilesCount = new AtomicInteger(0);
        AtomicLong totalFileSize = new AtomicLong(0);
        AtomicInteger deltaFilesCount = new AtomicInteger(0);
        AtomicLong deltaFilesTotalSize = new AtomicLong(524288); // 512KB

        StorageFilesMetricSource source = new StorageFilesMetricSource(
                openFilesCount::get,
                totalFileSize::get,
                deltaFilesCount::get,
                deltaFilesTotalSize::get
        );

        StorageFilesMetrics metrics = new StorageFilesMetrics(source);

        assertThat(metrics.deltaFilesTotalSize().value(), equalTo(524288L));
    }

    @Test
    void testFileOpenErrorsCounter() {
        AtomicInteger openFilesCount = new AtomicInteger(0);
        AtomicLong totalFileSize = new AtomicLong(0);
        AtomicInteger deltaFilesCount = new AtomicInteger(0);
        AtomicLong deltaFilesTotalSize = new AtomicLong(0);

        StorageFilesMetricSource source = new StorageFilesMetricSource(
                openFilesCount::get,
                totalFileSize::get,
                deltaFilesCount::get,
                deltaFilesTotalSize::get
        );

        StorageFilesMetrics metrics = new StorageFilesMetrics(source);

        metrics.fileOpenErrors().increment();

        assertThat(metrics.fileOpenErrors().value(), equalTo(1L));
    }

    @Test
    void testMetricSourceName() {
        StorageFilesMetricSource source = new StorageFilesMetricSource(() -> 0, () -> 0L, () -> 0, () -> 0L);

        assertThat(source.name(), equalTo("storage.files"));
    }
}
