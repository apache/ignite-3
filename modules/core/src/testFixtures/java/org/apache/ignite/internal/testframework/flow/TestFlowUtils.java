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

package org.apache.ignite.internal.testframework.flow;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.subscription.ListAccumulator;

/**
 * Utility methods for extracting data from {@link Publisher}s.
 */
public class TestFlowUtils {
    /**
     * Subscribes to the given publisher, collecting all values into a list.
     */
    public static <T> CompletableFuture<List<T>> subscribeToList(Publisher<T> publisher) {
        var resultFuture = new CompletableFuture<List<T>>();

        publisher.subscribe(new ListAccumulator<T, T>(Function.identity()).toSubscriber(resultFuture));

        return resultFuture;
    }

    /**
     * Subscribes to the given publisher, extracting a single value from it.
     *
     * @return Future that completes with the first value that was produced by this publisher, or fails with
     *     {@link NoSuchElementException}, if the publisher did not produce any items.
     */
    public static <T> CompletableFuture<T> subscribeToValue(Publisher<T> publisher) {
        var resultFuture = new CompletableFuture<T>();

        publisher.subscribe(new Subscriber<>() {
            private Subscription subscription;

            @Override
            public void onSubscribe(Subscription subscription) {
                this.subscription = subscription;

                subscription.request(1);
            }

            @Override
            public void onNext(T item) {
                resultFuture.complete(item);

                subscription.cancel();
            }

            @Override
            public void onError(Throwable throwable) {
                resultFuture.completeExceptionally(throwable);
            }

            @Override
            public void onComplete() {
                resultFuture.completeExceptionally(new NoSuchElementException());
            }
        });

        return resultFuture;
    }

    /**
     * Subscribes to a cursor publisher.
     *
     * @param scannedRows List of rows, that were scanned.
     * @param publisher Publisher.
     * @param scanned A future that will be completed when the scan is finished.
     * @return Subscription, that can request rows from cluster.
     */
    public static <T> Subscription subscribeToPublisher(
            List<T> scannedRows,
            Publisher<T> publisher,
            CompletableFuture<Void> scanned
    ) {
        AtomicReference<Subscription> subscriptionRef = new AtomicReference<>();

        publisher.subscribe(new Subscriber<>() {
            @Override
            public void onSubscribe(Subscription subscription) {
                subscriptionRef.set(subscription);
            }

            @Override
            public void onNext(T item) {
                scannedRows.add(item);
            }

            @Override
            public void onError(Throwable throwable) {
                scanned.completeExceptionally(throwable);
            }

            @Override
            public void onComplete() {
                scanned.complete(null);
            }
        });

        return subscriptionRef.get();
    }

    /**
     * Creates a Publisher that provides data from a given cursor.
     */
    public static <T> Publisher<T> fromCursor(Cursor<T> cursor) {
        return subscriber -> subscriber.onSubscribe(new Subscription() {
            private long demand;

            private boolean isStarted;

            private boolean isCancelled;

            @Override
            public void request(long n) {
                demand += n;

                if (!isStarted) {
                    isStarted = true;

                    start();
                }
            }

            private void start() {
                ForkJoinPool.commonPool().execute(() -> {
                    try {
                        while (demand > 0 && !isCancelled && cursor.hasNext()) {
                            subscriber.onNext(cursor.next());
                        }

                        if (!cursor.hasNext() && !isCancelled) {
                            cursor.close();

                            subscriber.onComplete();
                        }
                    } catch (Exception e) {
                        cancel();

                        subscriber.onError(e);
                    }
                });
            }

            @Override
            public void cancel() {
                try {
                    if (!isCancelled) {
                        isCancelled = true;

                        cursor.close();
                    }
                } catch (Exception e) {
                    subscriber.onError(e);
                }
            }
        });
    }
}
