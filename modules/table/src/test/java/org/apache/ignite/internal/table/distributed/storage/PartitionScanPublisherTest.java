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

package org.apache.ignite.internal.table.distributed.storage;

import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.table.distributed.storage.PartitionScanPublisher.InflightBatchRequestTracker;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PartitionScanPublisherTest extends BaseIgniteAbstractTest {

    @Mock
    private InflightBatchRequestTracker tracker;

    /**
     * Tests that multiple {@link Subscription#request} calls are batched correctly.
     */
    @Test
    void testRequestNextOrder() {
        var batchInvoked = new AtomicInteger();

        var batchFuture = new CompletableFuture<Collection<Integer>>();

        var publisher = new PartitionScanPublisher<Integer>(tracker) {
            @Override
            protected CompletableFuture<Collection<Integer>> retrieveBatch(long scanId, int batchSize) {
                batchInvoked.incrementAndGet();

                return batchFuture;
            }

            @Override
            protected CompletableFuture<Void> onClose(boolean intentionallyClose, long scanId, @Nullable Throwable th) {
                return nullCompletedFuture();
            }
        };

        var result = new CompletableFuture<Void>();

        publisher.subscribe(new Subscriber<>() {
            @Override
            public void onSubscribe(Subscription subscription) {
                subscription.request(1);
                subscription.request(1);
                subscription.request(1);
            }

            @Override
            public void onNext(Integer item) {
            }

            @Override
            public void onError(Throwable throwable) {
                result.completeExceptionally(throwable);
            }

            @Override
            public void onComplete() {
                result.complete(null);
            }
        });

        assertThat(batchInvoked.get(), is(1));

        batchFuture.complete(List.of(1));

        assertThat(result, willCompleteSuccessfully());

        assertThat(batchInvoked.get(), is(2));

        verify(tracker, times(2)).onRequestBegin();
        verify(tracker, times(2)).onRequestEnd();
    }

    /**
     * Tests canceling an ongoing subscription.
     */
    @Test
    void requestAndCancelOrder() {
        var batchFuture = new CompletableFuture<Collection<Integer>>();

        var closeInvoked = new CompletableFuture<Boolean>();

        var publisher = new PartitionScanPublisher<Integer>(tracker) {
            @Override
            protected CompletableFuture<Collection<Integer>> retrieveBatch(long scanId, int batchSize) {
                return batchFuture;
            }

            @Override
            protected CompletableFuture<Void> onClose(boolean intentionallyClose, long scanId, @Nullable Throwable th) {
                closeInvoked.complete(true);

                return nullCompletedFuture();
            }
        };

        publisher.subscribe(new Subscriber<>() {
            @Override
            public void onSubscribe(Subscription subscription) {
                subscription.request(1);
                subscription.cancel();
            }

            @Override
            public void onNext(Integer item) {
            }

            @Override
            public void onError(Throwable throwable) {
            }

            @Override
            public void onComplete() {
            }
        });

        assertThat(closeInvoked.isDone(), is(false));

        batchFuture.complete(List.of(1));

        assertThat(closeInvoked, willBe(true));

        verify(tracker).onRequestBegin();
        verify(tracker).onRequestEnd();
    }

    /**
     * Tests a situation when a Subscriber throws an exception.
     */
    @Test
    void testErrorDuringOnNext() {
        var closeInvoked = new AtomicBoolean();

        var publisher = new PartitionScanPublisher<Integer>(tracker) {
            @Override
            protected CompletableFuture<Collection<Integer>> retrieveBatch(long scanId, int batchSize) {
                return supplyAsync(() -> List.of(1));
            }

            @Override
            protected CompletableFuture<Void> onClose(boolean intentionallyClose, long scanId, @Nullable Throwable th) {
                closeInvoked.set(true);

                return nullCompletedFuture();
            }
        };

        var result = new CompletableFuture<Void>();

        publisher.subscribe(new Subscriber<>() {
            @Override
            public void onSubscribe(Subscription subscription) {
                subscription.request(3);
            }

            @Override
            public void onNext(Integer item) {
                throw new RuntimeException("Test error");
            }

            @Override
            public void onError(Throwable throwable) {
                result.completeExceptionally(throwable);
            }

            @Override
            public void onComplete() {
                result.complete(null);
            }
        });

        assertThat(result, willThrow(RuntimeException.class, "Test error"));

        assertThat(closeInvoked.get(), is(true));
    }

    /**
     * Tests a situation when a remote call throws an exception.
     */
    @Test
    void testErrorDuringRequest() {
        var closeInvoked = new AtomicBoolean();

        var publisher = new PartitionScanPublisher<Integer>(tracker) {
            @Override
            protected CompletableFuture<Collection<Integer>> retrieveBatch(long scanId, int batchSize) {
                return failedFuture(new RuntimeException("Test error"));
            }

            @Override
            protected CompletableFuture<Void> onClose(boolean intentionallyClose, long scanId, @Nullable Throwable th) {
                closeInvoked.set(true);

                return nullCompletedFuture();
            }
        };

        var result = new CompletableFuture<Void>();

        publisher.subscribe(new Subscriber<>() {
            @Override
            public void onSubscribe(Subscription subscription) {
                subscription.request(3);
            }

            @Override
            public void onNext(Integer item) {
            }

            @Override
            public void onError(Throwable throwable) {
                result.completeExceptionally(throwable);
            }

            @Override
            public void onComplete() {
                result.complete(null);
            }
        });

        assertThat(result, willThrow(RuntimeException.class, "Test error"));

        assertThat(closeInvoked.get(), is(true));
    }

    @Test
    void testIllegalArgumentException() {
        var publisher = new PartitionScanPublisher<Integer>(tracker) {
            @Override
            protected CompletableFuture<Collection<Integer>> retrieveBatch(long scanId, int batchSize) {
                return supplyAsync(() -> List.of(1));
            }

            @Override
            protected CompletableFuture<Void> onClose(boolean intentionallyClose, long scanId, @Nullable Throwable th) {
                return nullCompletedFuture();
            }
        };

        var onNextCounter = new AtomicInteger();

        var result = new CompletableFuture<Void>();

        publisher.subscribe(new Subscriber<>() {
            @Override
            public void onSubscribe(Subscription subscription) {
                subscription.request(1);
                subscription.request(-1);
            }

            @Override
            public void onNext(Integer item) {
                onNextCounter.incrementAndGet();
            }

            @Override
            public void onError(Throwable throwable) {
                result.completeExceptionally(throwable);
            }

            @Override
            public void onComplete() {
                result.complete(null);
            }
        });

        assertThat(result, willThrow(IllegalArgumentException.class, "Invalid amount of items requested"));

        assertThat(onNextCounter.get(), is(1));
    }
}
