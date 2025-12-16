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

/**
 * Storage consistency operation metrics.
 *
 * <p>Tracks runConsistently closure execution performance including duration,
 * I/O operations per closure, and concurrency.
 */
public class StorageConsistencyMetrics {
    private final LongAdderMetric runConsistentlyExecutions;
    private final DistributionMetric runConsistentlyDuration;
    private final DistributionMetric runConsistentlyIoOperations;
    private final IntGauge runConsistentlyActiveCount;
    private final DistributionMetric runConsistentlyCheckpointWaitTime;

    /**
     * Constructor.
     *
     * @param source Metric source to get metrics from.
     */
    public StorageConsistencyMetrics(StorageConsistencyMetricSource source) {
        // Enable the source immediately to create the holder
        source.enable();

        // Get the holder with metric instances
        StorageConsistencyMetricSource.Holder holder = source.holder();

        assert holder != null : "Holder must be non-null after enable()";

        this.runConsistentlyExecutions = holder.runConsistentlyExecutions();
        this.runConsistentlyDuration = holder.runConsistentlyDuration();
        this.runConsistentlyIoOperations = holder.runConsistentlyIoOperations();
        this.runConsistentlyActiveCount = holder.runConsistentlyActiveCount();
        this.runConsistentlyCheckpointWaitTime = holder.runConsistentlyCheckpointWaitTime();
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
