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

import java.util.List;
import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.DistributionMetric;
import org.apache.ignite.internal.metrics.LongAdderMetric;
import org.apache.ignite.internal.metrics.Metric;

/**
 * Metric source for checkpoint read lock operations.
 *
 * <p>This metric source tracks performance and contention characteristics of checkpoint read locks
 * acquired by normal operations during database operation. These locks coordinate with the checkpoint
 * write lock to ensure consistency.
 */
public class CheckpointReadWriteLockMetricSource extends AbstractMetricSource<CheckpointReadWriteLockMetricSource.Holder> {
    private static final long[] LOCK_ACQUISITION_MILLIS = {
            1,      // 1ms   - uncontended, fast path
            10,     // 10ms  - minor contention
            50,     // 100ms - moderate contention
            100,    // 100ms - high contention
            500,    // 500ms - checkpoint in progress?
            1_000,  // 1s    - severe contention, reported as warning in logs
            10_000  // 10s   - pathological case, shall be treated as an emergency error
    };

    private static final long[] LOCK_HOLD_MILLIS = {
            1,      // 1ms   - very fast operation
            10,     // 10ms  - fast single-page operation
            50,     // 50ms  - multi-page operation
            100,    // 100ms - complex operation
            500,    // 500ms - batch operation
            1_000,  // 1s    - too large batch or slow I/O
            10_000  // 10s  - pathologically large operation
    };

    /** Counter for threads currently waiting for checkpoint read lock. */
    private final LongAdderMetric readLockWaitingThreads = new LongAdderMetric(
            "ReadLockWaitingThreads",
            "Current number of threads waiting for checkpoint read lock."
    );

    public CheckpointReadWriteLockMetricSource() {
        super("checkpoint.readwritelock", "Checkpoint read/wrtie lock metrics", "checkpoint");
    }

    /**
     * Records the duration of a lock acquisition in nanoseconds.
     */
    public void recordReadLockAcquisitionTime(long acquisitionDurationMillis) {
        Holder holder = holder();
        if (holder != null) {
            holder.readLockAcquisitionTime().add(acquisitionDurationMillis);
        }
    }

    /**
     * Records the duration of a lock hold in nanoseconds.
     */
    public void recordReadLockHoldDuration(long lockHoldDuration) {
        Holder holder = holder();
        if (holder != null) {
            holder.readLockHoldTime().add(lockHoldDuration);
        }
    }

    public LongAdderMetric readLockWaitingThreads() {
        return readLockWaitingThreads;
    }

    @Override
    protected Holder createHolder() {
        return new Holder(readLockWaitingThreads);
    }

    /** Metrics holder. */
    protected static class Holder implements AbstractMetricSource.Holder<Holder> {
        private final DistributionMetric readLockAcquisitionTime = new DistributionMetric(
                "ReadLockAcquisitionTime",
                "Time from requesting checkpoint read lock until acquisition in milliseconds.",
                LOCK_ACQUISITION_MILLIS
        );

        private final DistributionMetric readLockHoldTime = new DistributionMetric(
                "ReadLockHoldTime",
                "Duration between checkpoint read lock acquisition and release in milliseconds.",
                LOCK_HOLD_MILLIS
        );

        private final LongAdderMetric readLockWaitingThreads;

        /**
         * Constructor.
         *
         * @param readLockWaitingThreads Metric for the number of waiting threads.
         */
        Holder(LongAdderMetric readLockWaitingThreads) {
            this.readLockWaitingThreads = readLockWaitingThreads;
        }

        @Override
        public Iterable<Metric> metrics() {
            return List.of(readLockAcquisitionTime, readLockHoldTime, readLockWaitingThreads);
        }

        /** Returns acquisition time metric. */
        public DistributionMetric readLockAcquisitionTime() {
            return readLockAcquisitionTime;
        }

        /** Returns hold time metric. */
        public DistributionMetric readLockHoldTime() {
            return readLockHoldTime;
        }
    }
}
