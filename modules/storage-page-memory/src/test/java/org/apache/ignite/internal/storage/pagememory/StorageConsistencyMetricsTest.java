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

package org.apache.ignite.internal.storage.pagememory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link StorageConsistencyMetrics}.
 */
class StorageConsistencyMetricsTest {
    @Test
    void testMetricsCreation() {
        AtomicInteger activeCount = new AtomicInteger(0);
        StorageConsistencyMetricSource source = new StorageConsistencyMetricSource(activeCount::get);

        StorageConsistencyMetrics metrics = new StorageConsistencyMetrics(source);

        assertThat(metrics.runConsistentlyExecutions(), notNullValue());
        assertThat(metrics.runConsistentlyDuration(), notNullValue());
        assertThat(metrics.runConsistentlyIoOperations(), notNullValue());
        assertThat(metrics.runConsistentlyActiveCount(), notNullValue());
        assertThat(metrics.runConsistentlyCheckpointWaitTime(), notNullValue());
    }

    @Test
    void testRunConsistentlyExecutionsCounter() {
        AtomicInteger activeCount = new AtomicInteger(0);
        StorageConsistencyMetricSource source = new StorageConsistencyMetricSource(activeCount::get);

        StorageConsistencyMetrics metrics = new StorageConsistencyMetrics(source);

        metrics.runConsistentlyExecutions().increment();
        metrics.runConsistentlyExecutions().increment();
        metrics.runConsistentlyExecutions().increment();

        assertThat(metrics.runConsistentlyExecutions().value(), equalTo(3L));
    }

    @Test
    void testRunConsistentlyDurationDistribution() {
        AtomicInteger activeCount = new AtomicInteger(0);
        StorageConsistencyMetricSource source = new StorageConsistencyMetricSource(activeCount::get);

        StorageConsistencyMetrics metrics = new StorageConsistencyMetrics(source);

        // Simulate 1ms operation
        metrics.runConsistentlyDuration().add(1_000_000);
        // Simulate 10ms operation
        metrics.runConsistentlyDuration().add(10_000_000);

        long[] values = metrics.runConsistentlyDuration().value();
        long totalCount = 0;
        for (long value : values) {
            totalCount += value;
        }

        assertThat(totalCount, equalTo(2L));
    }

    @Test
    void testRunConsistentlyIoOperationsDistribution() {
        AtomicInteger activeCount = new AtomicInteger(0);
        StorageConsistencyMetricSource source = new StorageConsistencyMetricSource(activeCount::get);

        StorageConsistencyMetrics metrics = new StorageConsistencyMetrics(source);

        // Simulate operations with different I/O counts
        metrics.runConsistentlyIoOperations().add(5);   // 5 I/O ops
        metrics.runConsistentlyIoOperations().add(50);  // 50 I/O ops
        metrics.runConsistentlyIoOperations().add(100); // 100 I/O ops

        long[] values = metrics.runConsistentlyIoOperations().value();
        long totalCount = 0;
        for (long value : values) {
            totalCount += value;
        }

        assertThat(totalCount, equalTo(3L));
    }

    @Test
    void testRunConsistentlyActiveCountGauge() {
        AtomicInteger activeCount = new AtomicInteger(2);
        StorageConsistencyMetricSource source = new StorageConsistencyMetricSource(activeCount::get);

        StorageConsistencyMetrics metrics = new StorageConsistencyMetrics(source);

        assertThat(metrics.runConsistentlyActiveCount().value(), equalTo(2));

        activeCount.set(5);
        assertThat(metrics.runConsistentlyActiveCount().value(), equalTo(5));

        activeCount.set(0);
        assertThat(metrics.runConsistentlyActiveCount().value(), equalTo(0));
    }

    @Test
    void testRunConsistentlyCheckpointWaitTimeDistribution() {
        AtomicInteger activeCount = new AtomicInteger(0);
        StorageConsistencyMetricSource source = new StorageConsistencyMetricSource(activeCount::get);

        StorageConsistencyMetrics metrics = new StorageConsistencyMetrics(source);

        // Simulate checkpoint wait (5ms)
        metrics.runConsistentlyCheckpointWaitTime().add(5_000_000);

        long[] values = metrics.runConsistentlyCheckpointWaitTime().value();
        long totalCount = 0;
        for (long value : values) {
            totalCount += value;
        }

        assertThat(totalCount, greaterThan(0L));
    }

    @Test
    void testMetricSourceName() {
        StorageConsistencyMetricSource source = new StorageConsistencyMetricSource(() -> 0);

        assertThat(source.name(), equalTo("storage.consistency"));
    }

    @Test
    void testMetricSourceEnabled() {
        StorageConsistencyMetricSource source = new StorageConsistencyMetricSource(() -> 0);

        source.enable();
        assertThat(source.enabled(), equalTo(true));

        source.disable();
        assertThat(source.enabled(), equalTo(false));
    }

    @Test
    void testMultipleExecutionsTracking() {
        AtomicInteger activeCount = new AtomicInteger(0);
        StorageConsistencyMetricSource source = new StorageConsistencyMetricSource(activeCount::get);

        StorageConsistencyMetrics metrics = new StorageConsistencyMetrics(source);

        // Simulate multiple runConsistently executions
        for (int i = 0; i < 10; i++) {
            metrics.runConsistentlyExecutions().increment();
            metrics.runConsistentlyDuration().add(i * 1_000_000); // Variable durations
            metrics.runConsistentlyIoOperations().add(i * 10);     // Variable I/O counts
        }

        assertThat(metrics.runConsistentlyExecutions().value(), equalTo(10L));
    }
}
