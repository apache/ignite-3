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

import java.util.List;
import java.util.function.IntSupplier;
import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.DistributionMetric;
import org.apache.ignite.internal.metrics.IntGauge;
import org.apache.ignite.internal.metrics.LongAdderMetric;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.pagememory.metrics.MetricBounds;

/**
 * Metric source for storage consistency operations.
 *
 * <p>This metric source tracks runConsistently closure executions which are long-running
 * operations that need to span multiple checkpoints while maintaining consistency.
 */
public class StorageConsistencyMetricSource extends AbstractMetricSource<StorageConsistencyMetricSource.Holder> {
    private final IntSupplier activeCountSupplier;

    /**
     * Constructor.
     *
     * @param activeCountSupplier Supplier for the number of active runConsistently calls.
     */
    public StorageConsistencyMetricSource(IntSupplier activeCountSupplier) {
        super("storage.consistency", "Storage consistency operation metrics", "storage");
        this.activeCountSupplier = activeCountSupplier;
    }

    @Override
    protected Holder createHolder() {
        return new Holder(activeCountSupplier);
    }

    /** Metrics holder. */
    protected static class Holder implements AbstractMetricSource.Holder<Holder> {
        private final LongAdderMetric runConsistentlyExecutions = new LongAdderMetric(
                "RunConsistentlyExecutions",
                "Total number of runConsistently invocations since startup."
        );

        private final DistributionMetric runConsistentlyDuration = new DistributionMetric(
                "RunConsistentlyDuration",
                "Time spent in runConsistently closures in nanoseconds.",
                MetricBounds.RUN_CONSISTENTLY_NANOS
        );

        private final DistributionMetric runConsistentlyIoOperations = new DistributionMetric(
                "RunConsistentlyIoOperations",
                "Number of page I/O operations (reads + writes) per runConsistently call.",
                MetricBounds.IO_OPS_PER_CLOSURE
        );

        private final IntGauge runConsistentlyActiveCount;

        private final DistributionMetric runConsistentlyCheckpointWaitTime = new DistributionMetric(
                "RunConsistentlyCheckpointWaitTime",
                "Time spent waiting for checkpoint to complete within runConsistently in nanoseconds.",
                MetricBounds.LOCK_HOLD_NANOS
        );

        /**
         * Constructor.
         *
         * @param activeCountSupplier Supplier for the number of active runConsistently calls.
         */
        Holder(IntSupplier activeCountSupplier) {
            this.runConsistentlyActiveCount = new IntGauge(
                    "RunConsistentlyActiveCount",
                    "Current number of active runConsistently calls.",
                    activeCountSupplier
            );
        }

        @Override
        public Iterable<Metric> metrics() {
            return List.of(
                    runConsistentlyExecutions,
                    runConsistentlyDuration,
                    runConsistentlyIoOperations,
                    runConsistentlyActiveCount,
                    runConsistentlyCheckpointWaitTime
            );
        }

        /** Returns runConsistently executions metric. */
        public LongAdderMetric runConsistentlyExecutions() {
            return runConsistentlyExecutions;
        }

        /** Returns runConsistently duration metric. */
        public DistributionMetric runConsistentlyDuration() {
            return runConsistentlyDuration;
        }

        /** Returns runConsistently I/O operations metric. */
        public DistributionMetric runConsistentlyIoOperations() {
            return runConsistentlyIoOperations;
        }

        /** Returns runConsistently active count metric. */
        public IntGauge runConsistentlyActiveCount() {
            return runConsistentlyActiveCount;
        }

        /** Returns runConsistently checkpoint wait time metric. */
        public DistributionMetric runConsistentlyCheckpointWaitTime() {
            return runConsistentlyCheckpointWaitTime;
        }
    }
}
