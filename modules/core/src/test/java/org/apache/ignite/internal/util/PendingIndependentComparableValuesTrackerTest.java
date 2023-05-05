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

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.junit.jupiter.api.Test;

/**
 * Test for multiple waiters associated with same value in {@link PendingIndependentComparableValuesTracker}.
 */
public class PendingIndependentComparableValuesTrackerTest {
    @Test
    public void testSameTimeMultipleWaitersWithValue() {
        HybridTimestamp ts = new HybridTimestamp(1, 0);

        PendingComparableValuesTracker<HybridTimestamp, Integer> tracker = new PendingIndependentComparableValuesTracker<>(ts);

        HybridTimestamp ts1 = new HybridTimestamp(ts.getPhysical() + 1_000_000, 0);
        HybridTimestamp ts2 = new HybridTimestamp(ts.getPhysical() + 2_000_000, 0);

        CompletableFuture<Integer> f0 = tracker.waitFor(ts1);
        CompletableFuture<Integer> f1 = tracker.waitFor(ts1);
        CompletableFuture<Integer> f2 = tracker.waitFor(ts2);
        CompletableFuture<Integer> f3 = tracker.waitFor(ts2);
        CompletableFuture<Integer> f4 = tracker.waitFor(ts2);

        assertFalse(f0.isDone());
        assertFalse(f1.isDone());
        assertFalse(f2.isDone());
        assertFalse(f3.isDone());
        assertFalse(f4.isDone());

        tracker.update(ts1, 10);
        assertThat(f0, willBe(10));
        assertThat(f1, willBe(10));
        assertFalse(f2.isDone());
        assertFalse(f3.isDone());
        assertFalse(f4.isDone());

        // At this point we have three futures associated with same value - ts2.
        f2.cancel(false);
        f3.orTimeout(1, TimeUnit.MICROSECONDS);

        assertThat(f3, willTimeoutFast());

        tracker.update(ts2, 20);
        assertTrue(f2.isCancelled());
        assertThat(f4, willBe(20));
    }
}
