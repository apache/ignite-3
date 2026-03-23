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
import org.apache.ignite.internal.pagememory.metrics.CollectionMetricSource;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** For {@link CheckpointMetrics} testing. */
public class CheckpointMetricsTest extends BaseIgniteAbstractTest {
    private final MetricManager metricManager = new TestMetricManager();

    private final CollectionMetricSource metricSource = new CollectionMetricSource("test", "storage", null);

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
    }

    private static void checkMetricValue(MetricSet metricSet, String metricName, String exp) {
        Metric metric = metricSet.get(metricName);

        assertNotNull(metric, metricName);

        assertEquals(exp, metric.getValueAsString(), metricName);
    }
}
