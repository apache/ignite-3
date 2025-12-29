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

import org.apache.ignite.internal.metrics.DistributionMetric;
import org.apache.ignite.internal.metrics.LongAdderMetric;

/**
 * Metrics for checkpoint read/write lock operations.
 *
 * <p>This metric source tracks performance and contention characteristics of checkpoint read lock
 * acquired by normal operations during database operation.
 */
public class CheckpointReadWriteLockMetrics {
    private static final long[] LOCK_ACQUISITION_BOUNDS_NANOS = {
            1_000,           // 1µs   - uncontended, fast path
            10_000,          // 10µs  - minor contention
            100_000,         // 100µs - moderate contention
            1_000_000,       // 1ms   - high contention
            10_000_000,      // 10ms  - checkpoint in progress?
            100_000_000,     // 100ms - severe contention, reported as warning in logs
            1_000_000_000    // 1s    - pathological case, shall be treated as an emergency error
    };

    private static final long[] LOCK_HOLD_BOUNDS_NANOS = {
            1_000,           // 1µs    - very fast operation (single field update)
            10_000,          // 10µs   - fast single-page operation
            100_000,         // 100µs  - multi-page operation
            1_000_000,       // 1ms    - complex operation
            10_000_000,      // 10ms   - batch operation
            100_000_000,     // 100ms  - large batch or slow I/O
            1_000_000_000    // 1s     - pathologically long operation
    };

    private final DistributionMetric readLockAcquisitionTime = new DistributionMetric(
            "ReadLockAcquisitionTime",
            "Time from requesting checkpoint read lock until acquisition in nanoseconds.",
            LOCK_ACQUISITION_BOUNDS_NANOS
    );

    private final DistributionMetric readLockHoldTime = new DistributionMetric(
            "ReadLockHoldTime",
            "Duration between checkpoint read lock acquisition and release in nanoseconds.",
            LOCK_HOLD_BOUNDS_NANOS
    );

    private final LongAdderMetric readLockWaitingThreads = new LongAdderMetric(
            "ReadLockWaitingThreads",
            "Current number of threads waiting for checkpoint read lock."
    );

    /**
     * Constructor.
     *
     * @param metricSource Metric source to register metrics with.
     */
    public CheckpointReadWriteLockMetrics(CheckpointMetricSource metricSource) {
        metricSource.addMetric(readLockAcquisitionTime);
        metricSource.addMetric(readLockHoldTime);
        metricSource.addMetric(readLockWaitingThreads);
    }

    /**
     * Records the duration of a lock acquisition in nanoseconds.
     */
    public void recordReadLockAcquisitionTime(long acquisitionDurationNanos) {
        readLockAcquisitionTime.add(acquisitionDurationNanos);
    }

    /**
     * Records the duration of a lock hold in nanoseconds.
     */
    public void recordReadLockHoldDuration(long lockHoldDurationNanos) {
        readLockHoldTime.add(lockHoldDurationNanos);
    }

    /**
     * Increments the count of threads waiting for the read lock.
     */
    public void incrementReadLockWaitingThreads() {
        readLockWaitingThreads.increment();
    }

    /**
     * Decrements the count of threads waiting for the read lock.
     */
    public void decrementReadLockWaitingThreads() {
        readLockWaitingThreads.decrement();
    }

    /** Returns the read lock acquisition time metric. */
    DistributionMetric readLockAcquisitionTime() {
        return readLockAcquisitionTime;
    }

    /** Returns the read lock hold time metric. */
    DistributionMetric readLockHoldTime() {
        return readLockHoldTime;
    }

    /** Returns the read lock waiting threads metric. */
    LongAdderMetric readLockWaitingThreads() {
        return readLockWaitingThreads;
    }
}
