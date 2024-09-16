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

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.ignite.internal.util.FastTimestamps.coarseCurrentTimeMillis;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

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

        waitForTimeChange();

        tracker.onCheckpointEnd();

        assertThat(tracker.checkpointStartTime(), equalTo(checkpointStartTime));

        assertThat(tracker.totalDuration(NANOSECONDS), greaterThanOrEqualTo(1L));
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

        assertThat(tracker.splitAndSortCheckpointPagesDuration(NANOSECONDS), equalTo(0L));

        tracker.onSplitAndSortCheckpointPagesStart();

        waitForTimeChange();

        tracker.onSplitAndSortCheckpointPagesEnd();

        assertThat(tracker.splitAndSortCheckpointPagesDuration(NANOSECONDS), greaterThanOrEqualTo(1L));
    }

    @Test
    void testFsync() throws Exception {
        CheckpointMetricsTracker tracker = new CheckpointMetricsTracker();

        assertThat(tracker.fsyncDuration(NANOSECONDS), equalTo(0L));

        tracker.onFsyncStart();

        waitForTimeChange();

        tracker.onCheckpointEnd();

        assertThat(tracker.fsyncDuration(NANOSECONDS), greaterThanOrEqualTo(1L));
    }

    @Test
    void testPagesWrite() throws Exception {
        CheckpointMetricsTracker tracker = new CheckpointMetricsTracker();

        assertThat(tracker.pagesWriteDuration(NANOSECONDS), equalTo(0L));

        tracker.onPagesWriteStart();

        waitForTimeChange();

        tracker.onFsyncStart();

        assertThat(tracker.pagesWriteDuration(NANOSECONDS), greaterThanOrEqualTo(1L));
    }

    @Test
    void testOnMarkCheckpointBegin() throws Exception {
        CheckpointMetricsTracker tracker = new CheckpointMetricsTracker();

        assertThat(tracker.onMarkCheckpointBeginDuration(NANOSECONDS), equalTo(0L));

        tracker.onMarkCheckpointBeginStart();

        waitForTimeChange();

        tracker.onMarkCheckpointBeginEnd();

        assertThat(tracker.onMarkCheckpointBeginDuration(NANOSECONDS), greaterThanOrEqualTo(1L));
    }

    @Test
    void testWriteLock() throws Exception {
        CheckpointMetricsTracker tracker = new CheckpointMetricsTracker();

        assertThat(tracker.writeLockWaitDuration(NANOSECONDS), equalTo(0L));
        assertThat(tracker.writeLockHoldDuration(NANOSECONDS), equalTo(0L));

        waitForTimeChange();

        tracker.onWriteLockWaitStart();

        long beforeWriteLockDuration = tracker.beforeWriteLockDuration(NANOSECONDS);

        assertThat(beforeWriteLockDuration, greaterThanOrEqualTo(1L));
        assertThat(tracker.writeLockHoldDuration(NANOSECONDS), equalTo(0L));

        waitForTimeChange();

        tracker.onMarkCheckpointBeginStart();

        long writeLockWaitDuration = tracker.writeLockWaitDuration(NANOSECONDS);

        assertThat(tracker.beforeWriteLockDuration(NANOSECONDS), equalTo(beforeWriteLockDuration));
        assertThat(writeLockWaitDuration, greaterThanOrEqualTo(1L));

        waitForTimeChange();

        tracker.onWriteLockRelease();

        assertThat(tracker.beforeWriteLockDuration(NANOSECONDS), equalTo(beforeWriteLockDuration));
        assertThat(tracker.writeLockWaitDuration(NANOSECONDS), equalTo(writeLockWaitDuration));
        assertThat(tracker.writeLockHoldDuration(NANOSECONDS), greaterThanOrEqualTo(1L));
    }

    private static void waitForTimeChange() throws Exception {
        long start = System.nanoTime();

        while (System.nanoTime() == start) {
            Thread.onSpinWait();
        }
    }
}
