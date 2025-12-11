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
import org.apache.ignite.internal.metrics.IntGauge;
import org.apache.ignite.internal.metrics.LongAdderMetric;

/**
 * Checkpoint read lock metrics.
 *
 * <p>Tracks acquisition, hold time, and contention for checkpoint read locks used by normal operations
 * (as opposed to the checkpoint write lock held by the checkpointer thread).
 */
class CheckpointReadLockMetrics {
    private final DistributionMetric acquisitionTime;

    private final DistributionMetric holdTime;

    private final LongAdderMetric acquisitions;

    private final LongAdderMetric contentionCount;

    private final IntGauge waitingThreads;

    /**
     * Constructor.
     *
     * @param source Metric source to get metrics from.
     */
    CheckpointReadLockMetrics(CheckpointReadLockMetricSource source) {
        // Enable the source immediately to create the holder
        source.enable();

        // Get the holder with metric instances
        CheckpointReadLockMetricSource.Holder holder = source.holder();

        assert holder != null : "Holder must be non-null after enable()";

        this.acquisitionTime = holder.acquisitionTime();
        this.holdTime = holder.holdTime();
        this.acquisitions = holder.acquisitions();
        this.contentionCount = holder.contentionCount();
        this.waitingThreads = holder.waitingThreads();
    }

    /**
     * Returns the acquisition time distribution metric.
     *
     * @return Acquisition time metric.
     */
    public DistributionMetric acquisitionTime() {
        return acquisitionTime;
    }

    /**
     * Returns the hold time distribution metric.
     *
     * @return Hold time metric.
     */
    public DistributionMetric holdTime() {
        return holdTime;
    }

    /**
     * Returns the acquisitions counter metric.
     *
     * @return Acquisitions metric.
     */
    public LongAdderMetric acquisitions() {
        return acquisitions;
    }

    /**
     * Returns the contention count metric.
     *
     * @return Contention count metric.
     */
    public LongAdderMetric contentionCount() {
        return contentionCount;
    }

    /**
     * Returns the waiting threads gauge metric.
     *
     * @return Waiting threads metric.
     */
    public IntGauge waitingThreads() {
        return waitingThreads;
    }
}
