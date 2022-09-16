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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.util.FastTimestamps.coarseCurrentTimeMillis;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * For {@link CheckpointMetricsTracker} testing.
 */
public class CheckpointMetricsTrackerTest {
    @Test
    void testStartAndEndCheckpoint() throws Exception {
        CheckpointMetricsTracker tracker = new CheckpointMetricsTracker();

        long checkpointStartTime = tracker.checkpointStartTime();

        assertThat(checkpointStartTime, allOf(greaterThan(0L), lessThanOrEqualTo(coarseCurrentTimeMillis())));

        assertThat(tracker.totalDuration(), lessThan(0L));

        waitForChangeCoarseCurrentTimeMillis();

        tracker.onCheckpointEnd();

        assertThat(tracker.checkpointStartTime(), equalTo(checkpointStartTime));

        assertThat(tracker.totalDuration(), greaterThanOrEqualTo(10L));
    }

    @Test
    void testCopyOnWritePageWritten() {
        CheckpointMetricsTracker tracker = new CheckpointMetricsTracker();

        assertThat(tracker.copyOnWritePagesWritten(), equalTo(0));

        tracker.onCopyOnWritePageWritten();

        assertThat(tracker.copyOnWritePagesWritten(), equalTo(1));

        tracker.onCopyOnWritePageWritten();

        assertThat(tracker.copyOnWritePagesWritten(), equalTo(2));
    }

    @Test
    void testDataPagesWritten() {
        CheckpointMetricsTracker tracker = new CheckpointMetricsTracker();

        assertThat(tracker.dataPagesWritten(), equalTo(0));

        tracker.onDataPageWritten();

        assertThat(tracker.dataPagesWritten(), equalTo(1));

        tracker.onDataPageWritten();

        assertThat(tracker.dataPagesWritten(), equalTo(2));
    }

    @Test
    void testSplitAndSortCheckpointPages() throws Exception {
        CheckpointMetricsTracker tracker = new CheckpointMetricsTracker();

        assertThat(tracker.splitAndSortCheckpointPagesDuration(), equalTo(0L));

        tracker.onSplitAndSortCheckpointPagesStart();

        assertThat(tracker.splitAndSortCheckpointPagesDuration(), lessThan(0L));

        waitForChangeCoarseCurrentTimeMillis();

        tracker.onSplitAndSortCheckpointPagesEnd();

        assertThat(tracker.splitAndSortCheckpointPagesDuration(), greaterThanOrEqualTo(10L));
    }

    @Test
    void testFsync() throws Exception {
        CheckpointMetricsTracker tracker = new CheckpointMetricsTracker();

        assertThat(tracker.fsyncDuration(), equalTo(0L));

        tracker.onFsyncStart();

        assertThat(tracker.fsyncDuration(), lessThan(0L));

        waitForChangeCoarseCurrentTimeMillis();

        tracker.onCheckpointEnd();

        assertThat(tracker.fsyncDuration(), greaterThanOrEqualTo(10L));
    }

    @Test
    void testPagesWrite() throws Exception {
        CheckpointMetricsTracker tracker = new CheckpointMetricsTracker();

        assertThat(tracker.pagesWriteDuration(), equalTo(0L));

        tracker.onPagesWriteStart();

        assertThat(tracker.pagesWriteDuration(), lessThan(0L));

        waitForChangeCoarseCurrentTimeMillis();

        tracker.onFsyncStart();

        assertThat(tracker.pagesWriteDuration(), greaterThanOrEqualTo(10L));
    }

    @Test
    void testOnMarkCheckpointBegin() throws Exception {
        CheckpointMetricsTracker tracker = new CheckpointMetricsTracker();

        assertThat(tracker.onMarkCheckpointBeginDuration(), equalTo(0L));

        tracker.onMarkCheckpointBeginStart();

        assertThat(tracker.onMarkCheckpointBeginDuration(), lessThan(0L));

        waitForChangeCoarseCurrentTimeMillis();

        tracker.onMarkCheckpointBeginEnd();

        assertThat(tracker.onMarkCheckpointBeginDuration(), greaterThanOrEqualTo(10L));
    }

    @Test
    void testWriteLock() throws Exception {
        CheckpointMetricsTracker tracker = new CheckpointMetricsTracker();

        assertThat(tracker.beforeWriteLockDuration(), lessThan(0L));
        assertThat(tracker.writeLockWaitDuration(), equalTo(0L));
        assertThat(tracker.writeLockHoldDuration(), equalTo(0L));

        waitForChangeCoarseCurrentTimeMillis();

        tracker.onWriteLockWaitStart();

        long beforeWriteLockDuration = tracker.beforeWriteLockDuration();

        assertThat(beforeWriteLockDuration, greaterThanOrEqualTo(10L));
        assertThat(tracker.writeLockWaitDuration(), lessThan(0L));
        assertThat(tracker.writeLockHoldDuration(), equalTo(0L));

        waitForChangeCoarseCurrentTimeMillis();

        tracker.onMarkCheckpointBeginStart();

        long writeLockWaitDuration = tracker.writeLockWaitDuration();

        assertThat(tracker.beforeWriteLockDuration(), equalTo(beforeWriteLockDuration));
        assertThat(writeLockWaitDuration, greaterThanOrEqualTo(10L));
        assertThat(tracker.writeLockHoldDuration(), lessThan(0L));

        waitForChangeCoarseCurrentTimeMillis();

        tracker.onWriteLockRelease();

        assertThat(tracker.beforeWriteLockDuration(), equalTo(beforeWriteLockDuration));
        assertThat(tracker.writeLockWaitDuration(), equalTo(writeLockWaitDuration));
        assertThat(tracker.writeLockHoldDuration(), greaterThanOrEqualTo(10L));
    }

    private void waitForChangeCoarseCurrentTimeMillis() throws Exception {
        long coarseCurrentTimeMillis = coarseCurrentTimeMillis();

        assertTrue(waitForCondition(() -> coarseCurrentTimeMillis() != coarseCurrentTimeMillis, 5, 100));
    }
}
