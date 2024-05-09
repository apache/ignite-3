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

package org.apache.ignite.internal.hlc;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedIn;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ClockWaiterTest {
    private ClockWaiter waiter;

    private final HybridClock clock = new HybridClockImpl();

    @BeforeEach
    void createWaiter() {
        waiter = new ClockWaiter("test", clock);

        assertThat(waiter.startAsync(), willCompleteSuccessfully());
    }

    @AfterEach
    void cleanup() {
        if (waiter != null) {
            assertThat(waiter.stopAsync(), willCompleteSuccessfully());
        }
    }

    @Test
    void futureCompletesImmediatelyOnPassedMoment() {
        CompletableFuture<Void> future = waiter.waitFor(clock.now());

        assertThat(future.isDone(), is(true));
    }

    @Test
    void futureCompletesWhenClockGetsUpdatedToSufficientTimestamp() {
        HybridTimestamp oneYearAhead = getOneYearAhead();

        CompletableFuture<Void> future = waiter.waitFor(oneYearAhead);

        assertThat(future.isDone(), is(false));

        clock.update(oneYearAhead);

        assertThat(future, willCompleteSuccessfully());
    }

    private HybridTimestamp getOneYearAhead() {
        return clock.now().addPhysicalTime(TimeUnit.DAYS.toMillis(365));
    }

    @Test
    void futureCompletesWithoutClockUpdates() {
        HybridTimestamp littleAhead = clock.now().addPhysicalTime(200);

        CompletableFuture<Void> future = waiter.waitFor(littleAhead);

        assertThat(future, willSucceedIn(10, TimeUnit.SECONDS));
    }

    @Test
    void futureGetsCancelledOnStop() {
        HybridTimestamp oneYearAhead = getOneYearAhead();

        CompletableFuture<Void> future = waiter.waitFor(oneYearAhead);

        assertThat(waiter.stopAsync(), willCompleteSuccessfully());

        assertThat(future, willThrow(CancellationException.class));
    }
}
