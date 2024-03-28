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

package org.apache.ignite.internal.thread;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.ignite.internal.util.IgniteUtils;
import org.hamcrest.core.CombinableMatcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class PublicApiThreadingTest {
    private final ExecutorService internalThreadPool = Executors.newSingleThreadExecutor(task -> new TestThread(task, true));
    private final ExecutorService asyncContinuationExecutor = Executors.newSingleThreadExecutor(task -> new TestThread(task, false));

    private static CompletableFuture<Thread> getCompletionThreadFuture(CompletableFuture<Void> publicFuture) {
        return publicFuture.thenApply(unused -> Thread.currentThread());
    }

    private static CombinableMatcher<Object> asyncContinuationThread() {
        return both(instanceOf(TestThread.class)).and(hasProperty("internal", is(false)));
    }

    @AfterEach
    void shutdownThreadPools() {
        IgniteUtils.shutdownAndAwaitTermination(asyncContinuationExecutor, 10, SECONDS);
        IgniteUtils.shutdownAndAwaitTermination(internalThreadPool, 10, SECONDS);
    }

    @Test
    void doesNotSwitchThreadWhenFutureIsCompleteRightAway() {
        CompletableFuture<Void> publicFuture = PublicApiThreading.preventThreadHijack(nullCompletedFuture(), asyncContinuationExecutor);
        CompletableFuture<Thread> completionThreadFuture = getCompletionThreadFuture(publicFuture);

        assertThat(completionThreadFuture, willBe(Thread.currentThread()));
    }

    @Test
    void switchesToAsyncContinuationPoolWhenFutureIsNotCompleteRightAway() {
        CompletableFuture<Void> internallyCompletedFuture = new CompletableFuture<>();

        CompletableFuture<Void> publicFuture = PublicApiThreading.preventThreadHijack(internallyCompletedFuture, asyncContinuationExecutor);
        CompletableFuture<Thread> completionThreadFuture = getCompletionThreadFuture(publicFuture);

        internallyCompletedFuture.completeAsync(() -> null, internalThreadPool);

        assertThat(completionThreadFuture, willBe(asyncContinuationThread()));
    }

    @SuppressWarnings("ClassExplicitlyExtendsThread")
    public static class TestThread extends Thread {
        private final boolean internal;

        TestThread(Runnable target, boolean internal) {
            super(target);
            this.internal = internal;
        }

        public boolean isInternal() {
            return internal;
        }
    }
}
