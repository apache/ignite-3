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

package org.apache.ignite.internal.pagememory.persistence.checkpoint;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link CheckpointReadLockMetrics}.
 */
class CheckpointReadLockMetricsTest {
    @Test
    void testMetricsCreation() {
        AtomicInteger waitingThreads = new AtomicInteger(0);
        CheckpointReadLockMetricSource source = new CheckpointReadLockMetricSource(waitingThreads::get);

        CheckpointReadLockMetrics metrics = new CheckpointReadLockMetrics(source);

        assertThat(metrics.acquisitionTime(), notNullValue());
        assertThat(metrics.holdTime(), notNullValue());
        assertThat(metrics.acquisitions(), notNullValue());
        assertThat(metrics.contentionCount(), notNullValue());
        assertThat(metrics.waitingThreads(), notNullValue());
    }

    @Test
    void testAcquisitionTimeTracking() {
        AtomicInteger waitingThreads = new AtomicInteger(0);
        CheckpointReadLockMetricSource source = new CheckpointReadLockMetricSource(waitingThreads::get);

        CheckpointReadLockMetrics metrics = new CheckpointReadLockMetrics(source);

        // Simulate lock acquisition with 1ms delay
        metrics.acquisitionTime().add(1_000_000); // 1ms in nanoseconds

        long[] values = metrics.acquisitionTime().value();
        long totalCount = 0;
        for (long value : values) {
            totalCount += value;
        }

        assertThat(totalCount, equalTo(1L));
    }

    @Test
    void testAcquisitionsCounter() {
        AtomicInteger waitingThreads = new AtomicInteger(0);
        CheckpointReadLockMetricSource source = new CheckpointReadLockMetricSource(waitingThreads::get);

        CheckpointReadLockMetrics metrics = new CheckpointReadLockMetrics(source);

        metrics.acquisitions().increment();
        metrics.acquisitions().increment();
        metrics.acquisitions().increment();

        assertThat(metrics.acquisitions().value(), equalTo(3L));
    }

    @Test
    void testContentionCounter() {
        AtomicInteger waitingThreads = new AtomicInteger(0);
        CheckpointReadLockMetricSource source = new CheckpointReadLockMetricSource(waitingThreads::get);

        CheckpointReadLockMetrics metrics = new CheckpointReadLockMetrics(source);

        metrics.contentionCount().increment();
        metrics.contentionCount().increment();

        assertThat(metrics.contentionCount().value(), equalTo(2L));
    }

    @Test
    void testWaitingThreadsGauge() {
        AtomicInteger waitingThreads = new AtomicInteger(5);
        CheckpointReadLockMetricSource source = new CheckpointReadLockMetricSource(waitingThreads::get);

        CheckpointReadLockMetrics metrics = new CheckpointReadLockMetrics(source);

        assertThat(metrics.waitingThreads().value(), equalTo(5));

        waitingThreads.set(10);
        assertThat(metrics.waitingThreads().value(), equalTo(10));
    }

    @Test
    void testMetricSourceName() {
        CheckpointReadLockMetricSource source = new CheckpointReadLockMetricSource(() -> 0);

        assertThat(source.name(), equalTo("checkpoint.readlock"));
    }

    @Test
    void testMetricSourceEnabled() {
        CheckpointReadLockMetricSource source = new CheckpointReadLockMetricSource(() -> 0);

        source.enable();
        assertThat(source.enabled(), equalTo(true));

        source.disable();
        assertThat(source.enabled(), equalTo(false));
    }

    @Test
    void testHoldTimeTracking() {
        AtomicInteger waitingThreads = new AtomicInteger(0);
        CheckpointReadLockMetricSource source = new CheckpointReadLockMetricSource(waitingThreads::get);

        CheckpointReadLockMetrics metrics = new CheckpointReadLockMetrics(source);

        // Simulate lock hold with 500µs duration
        metrics.holdTime().add(500_000); // 500µs in nanoseconds

        long[] values = metrics.holdTime().value();
        long totalCount = 0;
        for (long value : values) {
            totalCount += value;
        }

        assertThat(totalCount, greaterThan(0L));
    }
}
