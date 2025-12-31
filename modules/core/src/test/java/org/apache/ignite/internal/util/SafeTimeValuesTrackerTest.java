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

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

class SafeTimeValuesTrackerTest extends PendingComparableValuesTrackerTestBase {
    @Override
    protected <V extends Comparable<V>, R> void updateTracker(
            PendingComparableValuesTracker<V, R> tracker,
            V value,
            @Nullable R futureResult
    ) {
        SafeTimeValuesTracker safeTimeValuesTracker = (SafeTimeValuesTracker) tracker;

        safeTimeValuesTracker.update((HybridTimestamp) value, 1, 1, new Object());
    }

    @Test
    void testSimpleWaitFor() {
        doTestSimpleWaitFor(newTracker());
    }

    private static SafeTimeValuesTracker newTracker() {
        return new SafeTimeValuesTracker(new HybridTimestamp(1, 0));
    }

    @RepeatedTest(100)
    void testConcurrentAccess() {
        var tracker = new SafeTimeValuesTracker(new HybridTimestamp(1, 0));

        HybridTimestamp moreThanInitial = new HybridTimestamp(2, 0);

        var barrier = new CyclicBarrier(2);

        CompletableFuture<Void> writerFuture = CompletableFuture.runAsync(() -> {
            try {
                barrier.await();
                updateTracker(tracker, moreThanInitial, null);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        CompletableFuture<Void> readerFuture = CompletableFuture.runAsync(() -> {
            try {
                barrier.await();
                tracker.waitFor(moreThanInitial).get(1, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        assertThat(writerFuture, willCompleteSuccessfully());
        assertThat(readerFuture, willCompleteSuccessfully());
    }

    @Test
    void testClose() {
        doTestClose(newTracker());
    }
}
