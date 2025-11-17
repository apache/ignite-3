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

import org.apache.ignite.internal.metrics.DistributionMetric;
import org.apache.ignite.internal.metrics.IntGauge;
import org.apache.ignite.internal.metrics.LongAdderMetric;
import org.apache.ignite.internal.pagememory.metrics.MetricBounds;

/**
 * Storage consistency operation metrics.
 *
 * <p>Tracks runConsistently closure execution performance including duration,
 * I/O operations per closure, and concurrency.
 */
class StorageConsistencyMetrics {
    private final LongAdderMetric runConsistentlyExecutions;

    private final DistributionMetric runConsistentlyDuration;

    private final DistributionMetric runConsistentlyIoOperations;

    private final IntGauge runConsistentlyActiveCount;

    private final DistributionMetric runConsistentlyCheckpointWaitTime;

    /**
     * Constructor.
     *
     * @param source Metric source to register metrics with.
     * @param activeCountSupplier Supplier for the number of active runConsistently calls.
     */
    StorageConsistencyMetrics(
            StorageConsistencyMetricSource source,
            java.util.function.IntSupplier activeCountSupplier
    ) {
        runConsistentlyExecutions = source.addMetric(new LongAdderMetric(
                "RunConsistentlyExecutions",
                "Total number of runConsistently invocations since startup."
        ));

        runConsistentlyDuration = source.addMetric(new DistributionMetric(
                "RunConsistentlyDuration",
                "Time spent in runConsistently closures in nanoseconds.",
                MetricBounds.RUN_CONSISTENTLY_NANOS
        ));

        runConsistentlyIoOperations = source.addMetric(new DistributionMetric(
                "RunConsistentlyIoOperations",
                "Number of page I/O operations (reads + writes) per runConsistently call.",
                MetricBounds.IO_OPS_PER_CLOSURE
        ));

        runConsistentlyActiveCount = source.addMetric(new IntGauge(
                "RunConsistentlyActiveCount",
                "Current number of active runConsistently calls.",
                activeCountSupplier
        ));

        runConsistentlyCheckpointWaitTime = source.addMetric(new DistributionMetric(
                "RunConsistentlyCheckpointWaitTime",
                "Time spent waiting for checkpoint to complete within runConsistently in nanoseconds.",
                MetricBounds.LOCK_HOLD_NANOS
        ));
    }

    /**
     * Returns the runConsistently executions counter metric.
     *
     * @return RunConsistently executions metric.
     */
    public LongAdderMetric runConsistentlyExecutions() {
        return runConsistentlyExecutions;
    }

    /**
     * Returns the runConsistently duration distribution metric.
     *
     * @return RunConsistently duration metric.
     */
    public DistributionMetric runConsistentlyDuration() {
        return runConsistentlyDuration;
    }

    /**
     * Returns the runConsistently I/O operations distribution metric.
     *
     * @return RunConsistently I/O operations metric.
     */
    public DistributionMetric runConsistentlyIoOperations() {
        return runConsistentlyIoOperations;
    }

    /**
     * Returns the runConsistently active count gauge metric.
     *
     * @return RunConsistently active count metric.
     */
    public IntGauge runConsistentlyActiveCount() {
        return runConsistentlyActiveCount;
    }

    /**
     * Returns the runConsistently checkpoint wait time distribution metric.
     *
     * @return RunConsistently checkpoint wait time metric.
     */
    public DistributionMetric runConsistentlyCheckpointWaitTime() {
        return runConsistentlyCheckpointWaitTime;
    }
}
