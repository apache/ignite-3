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

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.metrics.TestMetricManager;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** For {@link CheckpointMetrics} testing. */
public class CheckpointMetricsTest extends BaseIgniteAbstractTest {
    private final MetricManager metricManager = new TestMetricManager();

    private final CheckpointMetricSource metricSource = new CheckpointMetricSource("test");

    private final CheckpointMetrics metrics = new CheckpointMetrics(metricSource);

    @BeforeEach
    void setUp() {
        metricManager.registerSource(metricSource);
        metricManager.enable(metricSource);

        assertThat(metricManager.startAsync(new ComponentContext()), willCompleteSuccessfully());
    }

    @AfterEach
    void tearDown() {
        metricManager.beforeNodeStop();

        assertThat(metricManager.stopAsync(new ComponentContext()), willCompleteSuccessfully());
    }

    @Test
    void testUpdate() {
        var tracker = mock(CheckpointMetricsTracker.class);

        when(tracker.writeLockWaitDuration(TimeUnit.MILLISECONDS)).thenReturn(10L);
        when(tracker.writeLockHoldDuration(TimeUnit.MILLISECONDS)).thenReturn(20L);
        when(tracker.beforeWriteLockDuration(TimeUnit.MILLISECONDS)).thenReturn(30L);
        when(tracker.pagesWriteDuration(TimeUnit.MILLISECONDS)).thenReturn(40L);
        when(tracker.fsyncDuration(TimeUnit.MILLISECONDS)).thenReturn(50L);
        when(tracker.replicatorLogSyncDuration(TimeUnit.MILLISECONDS)).thenReturn(60L);
        when(tracker.splitAndSortCheckpointPagesDuration(TimeUnit.MILLISECONDS)).thenReturn(70L);
        when(tracker.waitPageReplacementDuration(TimeUnit.MILLISECONDS)).thenReturn(80L);
        when(tracker.checkpointDuration(TimeUnit.MILLISECONDS)).thenReturn(90L);

        metrics.update(tracker, 100);

        MetricSet metricSet = metricManager.metricSnapshot().metrics().get(metricSource.name());

        checkMetricValue(metricSet, "LastCheckpointLockWaitDuration", "10");
        checkMetricValue(metricSet, "LastCheckpointLockHoldDuration", "20");
        checkMetricValue(metricSet, "LastCheckpointBeforeLockDuration", "30");
        checkMetricValue(metricSet, "LastCheckpointPagesWriteDuration", "40");
        checkMetricValue(metricSet, "LastCheckpointFsyncDuration", "50");
        checkMetricValue(metricSet, "LastCheckpointReplicatorLogSyncDuration", "60");
        checkMetricValue(metricSet, "LastCheckpointSplitAndSortPagesDuration", "70");
        checkMetricValue(metricSet, "LastCheckpointWaitPageReplacementDuration", "80");
        checkMetricValue(metricSet, "LastCheckpointDuration", "90");

        checkMetricValue(metricSet, "LastCheckpointTotalPagesNumber", "100");

        // Verify distribution metrics and total checkpoints counter
        checkMetricExists(metricSet, "CheckpointDuration");
        checkMetricValue(metricSet, "TotalCheckpoints", "1");
    }

    @Test
    void testRecordDirtyPagesCount() {
        // Record dirty pages count
        metrics.recordDirtyPagesCount(1000);
        metrics.recordDirtyPagesCount(5000);
        metrics.recordDirtyPagesCount(10000);

        MetricSet metricSet = metricManager.metricSnapshot().metrics().get(metricSource.name());

        // Verify DirtyPagesCount distribution metric exists
        checkMetricExists(metricSet, "DirtyPagesCount");
    }

    @Test
    void testRecordCheckpointInterval() {
        // Record checkpoint intervals
        metrics.recordCheckpointInterval(1000);
        metrics.recordCheckpointInterval(5000);
        metrics.recordCheckpointInterval(10000);

        MetricSet metricSet = metricManager.metricSnapshot().metrics().get(metricSource.name());

        // Verify CheckpointInterval distribution metric exists
        checkMetricExists(metricSet, "CheckpointInterval");
    }

    @Test
    void testCheckpointDurationDistribution() {
        var tracker = mock(CheckpointMetricsTracker.class);

        // Simulate multiple checkpoints with different durations
        when(tracker.checkpointDuration(TimeUnit.MILLISECONDS))
                .thenReturn(100L)
                .thenReturn(500L)
                .thenReturn(1000L);

        when(tracker.writeLockWaitDuration(TimeUnit.MILLISECONDS)).thenReturn(10L);
        when(tracker.writeLockHoldDuration(TimeUnit.MILLISECONDS)).thenReturn(20L);
        when(tracker.beforeWriteLockDuration(TimeUnit.MILLISECONDS)).thenReturn(30L);
        when(tracker.pagesWriteDuration(TimeUnit.MILLISECONDS)).thenReturn(40L);
        when(tracker.fsyncDuration(TimeUnit.MILLISECONDS)).thenReturn(50L);
        when(tracker.replicatorLogSyncDuration(TimeUnit.MILLISECONDS)).thenReturn(60L);
        when(tracker.splitAndSortCheckpointPagesDuration(TimeUnit.MILLISECONDS)).thenReturn(70L);
        when(tracker.waitPageReplacementDuration(TimeUnit.MILLISECONDS)).thenReturn(80L);

        // Update metrics multiple times
        metrics.update(tracker, 100);
        metrics.update(tracker, 200);
        metrics.update(tracker, 300);

        MetricSet metricSet = metricManager.metricSnapshot().metrics().get(metricSource.name());

        // Verify distribution metric exists
        checkMetricExists(metricSet, "CheckpointDuration");

        // Verify total checkpoints counter incremented correctly
        checkMetricValue(metricSet, "TotalCheckpoints", "3");
    }

    private static void checkMetricValue(MetricSet metricSet, String metricName, String exp) {
        Metric metric = metricSet.get(metricName);

        assertNotNull(metric, metricName);

        assertEquals(exp, metric.getValueAsString(), metricName);
    }

    private static void checkMetricExists(MetricSet metricSet, String metricName) {
        Metric metric = metricSet.get(metricName);

        assertNotNull(metric, metricName);
    }
}
