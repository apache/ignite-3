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
    private final CheckpointMetricsTracker tracker = new CheckpointMetricsTracker();

    @Test
    void testCheckpoint() {
        assertThat(tracker.checkpointDuration(NANOSECONDS), equalTo(0L));

        tracker.onCheckpointStart();

        waitForTimeChange();

        tracker.onCheckpointEnd();

        assertThat(tracker.checkpointDuration(NANOSECONDS), greaterThanOrEqualTo(1L));
    }

    @Test
    void testCheckpointStartTime() {
        long checkpointStartTime = tracker.checkpointStartTime();

        assertThat(checkpointStartTime, allOf(greaterThan(0L), lessThanOrEqualTo(coarseCurrentTimeMillis())));
    }

    @Test
    void testCopyOnWritePageWritten() {
        assertThat(tracker.copyOnWritePagesWritten(), equalTo(0));

        tracker.onCopyOnWritePageWritten();

        assertThat(tracker.copyOnWritePagesWritten(), equalTo(1));

        tracker.onCopyOnWritePageWritten();

        assertThat(tracker.copyOnWritePagesWritten(), equalTo(2));
    }

    @Test
    void testDataPagesWritten() {
        assertThat(tracker.dataPagesWritten(), equalTo(0));

        tracker.onDataPageWritten();

        assertThat(tracker.dataPagesWritten(), equalTo(1));

        tracker.onDataPageWritten();

        assertThat(tracker.dataPagesWritten(), equalTo(2));
    }

    @Test
    void testSplitAndSortCheckpointPages() {
        assertThat(tracker.splitAndSortCheckpointPagesDuration(NANOSECONDS), equalTo(0L));

        tracker.onSplitAndSortCheckpointPagesStart();

        waitForTimeChange();

        tracker.onSplitAndSortCheckpointPagesEnd();

        assertThat(tracker.splitAndSortCheckpointPagesDuration(NANOSECONDS), greaterThanOrEqualTo(1L));
    }

    @Test
    void testFsync() {
        assertThat(tracker.fsyncDuration(NANOSECONDS), equalTo(0L));

        tracker.onFsyncStart();

        waitForTimeChange();

        tracker.onFsyncEnd();

        assertThat(tracker.fsyncDuration(NANOSECONDS), greaterThanOrEqualTo(1L));
    }

    @Test
    void testReplicatorLogSync() {
        assertThat(tracker.replicatorLogSyncDuration(NANOSECONDS), equalTo(0L));

        tracker.onReplicatorLogSyncStart();

        waitForTimeChange();

        tracker.onReplicatorLogSyncEnd();

        assertThat(tracker.replicatorLogSyncDuration(NANOSECONDS), greaterThanOrEqualTo(1L));
    }

    @Test
    void testPagesWrite() {
        assertThat(tracker.pagesWriteDuration(NANOSECONDS), equalTo(0L));

        tracker.onPagesWriteStart();

        waitForTimeChange();

        tracker.onPagesWriteEnd();

        assertThat(tracker.pagesWriteDuration(NANOSECONDS), greaterThanOrEqualTo(1L));
    }

    @Test
    void testOnMarkCheckpointBegin() {
        assertThat(tracker.onMarkCheckpointBeginDuration(NANOSECONDS), equalTo(0L));

        tracker.onMarkCheckpointBeginStart();

        waitForTimeChange();

        tracker.onMarkCheckpointBeginEnd();

        assertThat(tracker.onMarkCheckpointBeginDuration(NANOSECONDS), greaterThanOrEqualTo(1L));
    }

    @Test
    void testBeforeCheckpointBegin() {
        assertThat(tracker.onBeforeCheckpointBeginDuration(NANOSECONDS), equalTo(0L));

        tracker.onBeforeCheckpointBeginStart();

        waitForTimeChange();

        tracker.onBeforeCheckpointBeginEnd();

        assertThat(tracker.onBeforeCheckpointBeginDuration(NANOSECONDS), greaterThanOrEqualTo(1L));
    }

    @Test
    void testWriteLockHold() {
        assertThat(tracker.writeLockHoldDuration(NANOSECONDS), equalTo(0L));

        tracker.onWriteLockHoldStart();

        waitForTimeChange();

        tracker.onWriteLockHoldEnd();

        assertThat(tracker.writeLockHoldDuration(NANOSECONDS), greaterThanOrEqualTo(1L));
    }

    @Test
    void testWriteLockWait() {
        assertThat(tracker.writeLockWaitDuration(NANOSECONDS), equalTo(0L));

        tracker.onWriteLockWaitStart();

        waitForTimeChange();

        tracker.onWriteLockWaitEnd();

        assertThat(tracker.writeLockWaitDuration(NANOSECONDS), greaterThanOrEqualTo(1L));
    }

    @Test
    void testWaitPageReplacement() {
        assertThat(tracker.waitPageReplacementDuration(NANOSECONDS), equalTo(0L));

        tracker.onWaitPageReplacementStart();

        waitForTimeChange();

        tracker.onWaitPageReplacementEnd();

        assertThat(tracker.waitPageReplacementDuration(NANOSECONDS), greaterThanOrEqualTo(1L));
    }

    private static void waitForTimeChange() {
        long start = System.nanoTime();

        while (System.nanoTime() == start) {
            Thread.onSpinWait();
        }
    }
}
