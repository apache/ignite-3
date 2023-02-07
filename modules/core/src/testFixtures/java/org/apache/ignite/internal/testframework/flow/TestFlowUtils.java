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

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.ForkJoinPool;
import org.apache.ignite.internal.util.Cursor;

/**
 * Utility methods for extracting data from {@link Publisher}s.
 */
public class TestFlowUtils {
    /**
     * Subscribes to the given publisher, collecting all values into a list.
     */
    public static <T> CompletableFuture<List<T>> subscribeToList(Publisher<T> publisher) {
        var resultFuture = new CompletableFuture<List<T>>();

        publisher.subscribe(new Subscriber<>() {
            private final List<T> items = new ArrayList<>();

            @Override
            public void onSubscribe(Subscription subscription) {
                subscription.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(T item) {
                items.add(item);
            }

            @Override
            public void onError(Throwable throwable) {
                resultFuture.completeExceptionally(throwable);
            }

            @Override
            public void onComplete() {
                resultFuture.complete(items);
            }
        });

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
            @Override
            public void onSubscribe(Subscription subscription) {
                subscription.request(1);
            }

            @Override
            public void onNext(T item) {
                resultFuture.complete(item);
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
