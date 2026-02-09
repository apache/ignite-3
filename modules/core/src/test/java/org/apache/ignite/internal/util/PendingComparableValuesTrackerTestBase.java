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

package org.apache.ignite.internal.util;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.RunnableX;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings("rawtypes") // Because of test parametrization raw PendingComparableValuesTracker is used.
abstract class PendingComparableValuesTrackerTestBase {
    protected abstract <V extends Comparable<V>, R> void updateTracker(
            PendingComparableValuesTracker<V, R> tracker,
            V value,
            @Nullable R futureResult
    );

    protected void doTestSimpleWaitFor(PendingComparableValuesTracker trackerParam) {
        HybridTimestamp ts = new HybridTimestamp(1, 0);

        @SuppressWarnings("unchecked")
        PendingComparableValuesTracker<HybridTimestamp, Void> tracker = trackerParam;

        HybridTimestamp ts1 = new HybridTimestamp(ts.getPhysical() + 1_000_000, 0);
        HybridTimestamp ts2 = new HybridTimestamp(ts.getPhysical() + 2_000_000, 0);
        HybridTimestamp ts3 = new HybridTimestamp(ts.getPhysical() + 3_000_000, 0);

        CompletableFuture<Void> f0 = tracker.waitFor(ts1);
        CompletableFuture<Void> f1 = tracker.waitFor(ts2);
        CompletableFuture<Void> f2 = tracker.waitFor(ts3);

        assertFalse(f0.isDone());
        assertFalse(f1.isDone());
        assertFalse(f2.isDone());

        updateTracker(tracker, ts1, null);
        assertThat(f0, willCompleteSuccessfully());
        assertFalse(f1.isDone());
        assertFalse(f2.isDone());

        updateTracker(tracker, ts2, null);
        assertThat(f1, willCompleteSuccessfully());
        assertFalse(f2.isDone());

        updateTracker(tracker, ts3, null);
        assertThat(f2, willCompleteSuccessfully());
    }

    protected void doTestClose(PendingComparableValuesTracker trackerParam) {
        @SuppressWarnings("unchecked")
        PendingComparableValuesTracker<HybridTimestamp, Void> tracker = trackerParam;

        HybridTimestamp moreThanInitial = new HybridTimestamp(2, 0);

        CompletableFuture<Void> future0 = tracker.waitFor(moreThanInitial);

        // Close is called from dedicated stop worker.
        IgniteTestUtils.runAsync((RunnableX) tracker::close).join();

        assertThrows(TrackerClosedException.class, tracker::current);
        assertThrows(TrackerClosedException.class, () -> updateTracker(tracker, moreThanInitial, null));

        assertThat(future0, willThrowFast(TrackerClosedException.class));
        assertThat(tracker.waitFor(moreThanInitial), willThrowFast(TrackerClosedException.class));
    }
}
