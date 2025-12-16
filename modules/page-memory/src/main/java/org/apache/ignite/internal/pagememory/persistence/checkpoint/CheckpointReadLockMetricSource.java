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
import java.util.function.IntSupplier;
import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.DistributionMetric;
import org.apache.ignite.internal.metrics.IntGauge;
import org.apache.ignite.internal.metrics.LongAdderMetric;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.pagememory.metrics.MetricBounds;

/**
 * Metric source for checkpoint read lock operations.
 *
 * <p>This metric source tracks performance and contention characteristics of checkpoint read locks
 * acquired by normal operations during database operation. These locks coordinate with the checkpoint
 * write lock to ensure consistency.
 */
public class CheckpointReadLockMetricSource extends AbstractMetricSource<CheckpointReadLockMetricSource.Holder> {
    private final IntSupplier waitingThreadsSupplier;

    /**
     * Constructor.
     *
     * @param waitingThreadsSupplier Supplier for the number of waiting threads.
     */
    public CheckpointReadLockMetricSource(IntSupplier waitingThreadsSupplier) {
        super("checkpoint.readlock", "Checkpoint read lock metrics", "checkpoint");
        this.waitingThreadsSupplier = waitingThreadsSupplier;
    }

    @Override
    protected Holder createHolder() {
        return new Holder(waitingThreadsSupplier);
    }

    /** Metrics holder. */
    protected static class Holder implements AbstractMetricSource.Holder<Holder> {
        private final DistributionMetric acquisitionTime = new DistributionMetric(
                "CheckpointReadLockAcquisitionTime",
                "Time from requesting checkpoint read lock until acquisition in nanoseconds.",
                MetricBounds.LOCK_ACQUISITION_NANOS
        );

        private final DistributionMetric holdTime = new DistributionMetric(
                "CheckpointReadLockHoldTime",
                "Duration between checkpoint read lock acquisition and release in nanoseconds.",
                MetricBounds.LOCK_HOLD_NANOS
        );

        private final LongAdderMetric acquisitions = new LongAdderMetric(
                "CheckpointReadLockAcquisitions",
                "Total successful read lock acquisitions since startup."
        );

        private final LongAdderMetric contentionCount = new LongAdderMetric(
                "CheckpointReadLockContentionCount",
                "Number of times thread had to wait for read lock (lock not immediately available)."
        );

        private final IntGauge waitingThreads;

        /**
         * Constructor.
         *
         * @param waitingThreadsSupplier Supplier for the number of waiting threads.
         */
        Holder(IntSupplier waitingThreadsSupplier) {
            this.waitingThreads = new IntGauge(
                    "CheckpointReadLockWaitingThreads",
                    "Current number of threads waiting for checkpoint read lock.",
                    waitingThreadsSupplier
            );
        }

        @Override
        public Iterable<Metric> metrics() {
            return List.of(acquisitionTime, holdTime, acquisitions, contentionCount, waitingThreads);
        }

        /** Returns acquisition time metric. */
        public DistributionMetric acquisitionTime() {
            return acquisitionTime;
        }

        /** Returns hold time metric. */
        public DistributionMetric holdTime() {
            return holdTime;
        }

        /** Returns acquisitions metric. */
        public LongAdderMetric acquisitions() {
            return acquisitions;
        }

        /** Returns contention count metric. */
        public LongAdderMetric contentionCount() {
            return contentionCount;
        }

        /** Returns waiting threads metric. */
        public IntGauge waitingThreads() {
            return waitingThreads;
        }
    }
}
