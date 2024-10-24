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

package org.apache.ignite.internal.util.subscription;

import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Helper class for working with {@link Publisher}.
 */
public class Publishers {
    /**
     * Returns an {@code Publisher} that applies a specified function to each item emitted by the current {@code Publisher} and
     * emits the results of these function applications.
     * <p>
     * <img width="640" height="305" src="https://raw.github.com/wiki/ReactiveX/RxJava/images/rx-operators/map.v3.png" alt="">
     * </p>
     *
     * @param <U> the output type
     * @param mapper
     *            a function to apply to each item emitted by the current {@code Publisher}
     * @return the new {@code Publisher} instance
     */
    public static <T, U> Publisher<U> map(Publisher<T> upstream, Function<T, U> mapper) {
        return new TransformingPublisher<T, U>(upstream, mapper);
    }

    /**
     * Filters items emitted by the current {@code Publisher} by only emitting those that satisfy a specified {@link Predicate}.
     *
     * <p>
     * <img width="640" height="310" src="https://raw.github.com/wiki/ReactiveX/RxJava/images/rx-operators/filter.v3.png" alt="">
     * </p>
     *
     * @param predicate
     *            a function that evaluates each item emitted by the current {@code Publisher}, returning {@code true}
     *            if it passes the filter
     * @return the new {@code Observable} instance
     * @throws NullPointerException if {@code predicate} is {@code null}
     */
    public static <T> Publisher<T> filter(Publisher<T> upstream, Predicate<T> predicate) {
        return new FilteringPublisher<>(upstream, predicate);
    }

    /**
     * Concatenates elements of each {@link Publisher} provided via an {@link Iterable} sequence into a single sequence
     * of elements without interleaving them.
     *
     * <p>
     * <img width="640" height="380" src="https://raw.github.com/wiki/ReactiveX/RxJava/images/rx-operators/concat.v3.png" alt="">
     * </p>
     *
     * @param <T> the common value type of the sources
     * @param upstream the {@code Iterable} sequence of {@code Publisher}s
     * @return the new {@code Publisher} instance
     */
    public static <T> Publisher<T> concat(Iterable<Publisher<? extends T>> upstream) {
        return new ConcatenatedPublisher<>(upstream.iterator());
    }

    /**
     * Combines two source {@link Publisher}s by emitting an item that aggregates the latest values of each of the
     * {@code Publisher}s each time an item is received from either of the {@code Subscription}s, where this
     * aggregation is defined by a specified function.
     * <p>
     * If any of the sources never produces an item but only terminates (normally or with an error), the
     * resulting sequence terminates immediately.
     * </p>
     *
     * <p>
     * <img width="640" height="380"
     *     src="https://raw.github.com/wiki/ReactiveX/RxJava/images/rx-operators/combineLatest.v3.png"
     *     alt="Marble diagram">
     * </p>
     */
    public static <L, R, T> Publisher<T> combineLatest(Publisher<L> leftUpstream, Publisher<R> rightUpstream, BiFunction<L, R, T> zipFunction) {
        return new CombineLatestPublisher<>(leftUpstream, rightUpstream, zipFunction);
    }

    /**
     * Returns a {@link CompletableFuture} that will complete with this {@link Publisher} first item, or exception, whenever this
     * {@link Publisher} terminates (normally or with an error) before emitting any items.
     *
     * @param producer upstream to subscribe to.
     * @return {@link CompletableFuture} that will be populated with the firt item of provided {@link Publisher}.
     * @param <T> type of items of {@link Publisher}.
     */
    public static <T> CompletableFuture<T> firstItem(Publisher<T> producer) {
        CompletableFuture<T> result = new CompletableFuture<>();

        producer.subscribe(new Subscriber<>() {
            private final SubscriptionHolder subscription = new SubscriptionHolder();

            @Override
            public void onSubscribe(Subscription subscription) {
                this.subscription.setSubscription(subscription);
                subscription.request(Long.MAX_VALUE); // Eager subscription with no backpressure: futures are always hot.
            }

            @Override
            public void onNext(T item) {
                if (subscription.isNotCancelled()) {
                    result.complete(item);
                    subscription.cancel();
                }
            }

            @Override
            public void onError(Throwable throwable) {
                if (subscription.isNotCancelled()) {
                    result.completeExceptionally(throwable);
                    subscription.cancel();
                }
            }

            @Override
            public void onComplete() {
                if (subscription.isNotCancelled()) {
                    subscription.cancel();
                    result.completeExceptionally(
                            new NoSuchElementException("Producer did not produced necessary element")
                    );
                }
            }
        });

        return result;
    }
}
