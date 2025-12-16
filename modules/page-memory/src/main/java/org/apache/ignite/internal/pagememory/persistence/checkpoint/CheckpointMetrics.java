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

import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.metrics.AtomicLongMetric;
import org.apache.ignite.internal.metrics.DistributionMetric;
import org.apache.ignite.internal.metrics.LongAdderMetric;
import org.apache.ignite.internal.pagememory.metrics.MetricBounds;

/** Checkpoint metrics. */
class CheckpointMetrics {
    private final AtomicLongMetric lastLockWaitDuration;

    private final AtomicLongMetric lastLockHoldDuration;

    private final AtomicLongMetric lastPagesWriteDuration;

    private final AtomicLongMetric lastFsyncDuration;

    private final AtomicLongMetric lastBeforeLockDuration;

    private final AtomicLongMetric lastReplicatorLogSyncDuration;

    private final AtomicLongMetric lastSplitAndSortCheckpointPagesDuration;

    private final AtomicLongMetric lastWaitPageReplacementDuration;

    private final AtomicLongMetric lastCheckpointDuration;

    private final AtomicLongMetric lastTotalPagesNumber;

    private final DistributionMetric checkpointDurationDistribution;

    private final DistributionMetric dirtyPagesCountDistribution;

    private final DistributionMetric checkpointIntervalDistribution;

    private final LongAdderMetric totalCheckpoints;

    CheckpointMetrics(CheckpointMetricSource source) {
        lastLockWaitDuration = source.addMetric(new AtomicLongMetric(
                "LastCheckpointLockWaitDuration",
                "Duration of the last checkpoint lock wait in milliseconds."
        ));

        lastLockHoldDuration = source.addMetric(new AtomicLongMetric(
                "LastCheckpointLockHoldDuration",
                "Duration of the last checkpoint lock hold in milliseconds."
        ));

        lastBeforeLockDuration = source.addMetric(new AtomicLongMetric(
                "LastCheckpointBeforeLockDuration",
                "Duration of actions before hold lock by the last checkpoint in milliseconds."
        ));

        lastPagesWriteDuration = source.addMetric(new AtomicLongMetric(
                "LastCheckpointPagesWriteDuration",
                "Duration of the last checkpoint pages write in milliseconds."
        ));

        lastFsyncDuration = source.addMetric(new AtomicLongMetric(
                "LastCheckpointFsyncDuration",
                "Duration of the sync phase of the last checkpoint in milliseconds."
        ));

        lastReplicatorLogSyncDuration = source.addMetric(new AtomicLongMetric(
                "LastCheckpointReplicatorLogSyncDuration",
                "Duration of the replicator log sync phase of the last checkpoint in milliseconds."
        ));

        lastSplitAndSortCheckpointPagesDuration = source.addMetric(new AtomicLongMetric(
                "LastCheckpointSplitAndSortPagesDuration",
                "Duration of the split and sort dirty pages phase of the last checkpoint in milliseconds."
        ));

        lastWaitPageReplacementDuration = source.addMetric(new AtomicLongMetric(
                "LastCheckpointWaitPageReplacementDuration",
                "Duration of the wait page replacement phase of the last checkpoint in milliseconds."
        ));

        lastCheckpointDuration = source.addMetric(new AtomicLongMetric(
                "LastCheckpointDuration",
                "Duration of the last checkpoint in milliseconds."
        ));

        lastTotalPagesNumber = source.addMetric(new AtomicLongMetric(
                "LastCheckpointTotalPagesNumber",
                "Total number of pages written during the last checkpoint."
        ));

        checkpointDurationDistribution = source.addMetric(new DistributionMetric(
                "CheckpointDuration",
                "Distribution of checkpoint durations in milliseconds.",
                MetricBounds.CHECKPOINT_DURATION_MILLIS
        ));

        dirtyPagesCountDistribution = source.addMetric(new DistributionMetric(
                "DirtyPagesCount",
                "Distribution of dirty pages count at checkpoint start.",
                MetricBounds.DIRTY_PAGES_COUNT
        ));

        checkpointIntervalDistribution = source.addMetric(new DistributionMetric(
                "CheckpointInterval",
                "Distribution of time between consecutive checkpoints in milliseconds.",
                MetricBounds.CHECKPOINT_INTERVAL_MILLIS
        ));

        totalCheckpoints = source.addMetric(new LongAdderMetric(
                "TotalCheckpoints",
                "Total number of checkpoints completed since startup."
        ));
    }

    /**
     * Updates metrics.
     *
     * @param tracker Checkpoint metrics tracker.
     * @param totalPages Total number of pages written during the checkpoint.
     */
    void update(CheckpointMetricsTracker tracker, long totalPages) {
        lastLockWaitDuration.value(tracker.writeLockWaitDuration(TimeUnit.MILLISECONDS));
        lastLockHoldDuration.value(tracker.writeLockHoldDuration(TimeUnit.MILLISECONDS));
        lastBeforeLockDuration.value(tracker.beforeWriteLockDuration(TimeUnit.MILLISECONDS));
        lastPagesWriteDuration.value(tracker.pagesWriteDuration(TimeUnit.MILLISECONDS));
        lastFsyncDuration.value(tracker.fsyncDuration(TimeUnit.MILLISECONDS));
        lastReplicatorLogSyncDuration.value(tracker.replicatorLogSyncDuration(TimeUnit.MILLISECONDS));
        lastSplitAndSortCheckpointPagesDuration.value(tracker.splitAndSortCheckpointPagesDuration(TimeUnit.MILLISECONDS));
        lastWaitPageReplacementDuration.value(tracker.waitPageReplacementDuration(TimeUnit.MILLISECONDS));

        long durationMs = tracker.checkpointDuration(TimeUnit.MILLISECONDS);
        lastCheckpointDuration.value(durationMs);

        lastTotalPagesNumber.value(totalPages);

        // Update distribution metrics
        checkpointDurationDistribution.add(durationMs);
        totalCheckpoints.increment();
    }

    /**
     * Records dirty pages count at checkpoint start.
     *
     * @param dirtyPagesCount Number of dirty pages at checkpoint start.
     */
    void recordDirtyPagesCount(long dirtyPagesCount) {
        dirtyPagesCountDistribution.add(dirtyPagesCount);
    }

    /**
     * Records time since last checkpoint.
     *
     * @param intervalMs Time since last checkpoint in milliseconds.
     */
    void recordCheckpointInterval(long intervalMs) {
        checkpointIntervalDistribution.add(intervalMs);
    }
}
