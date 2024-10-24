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

import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.BiFunction;


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
 *
 * @param <L> type of left upstream elements.
 * @param <R> type of right upstream elements.
 * @param <U> type of resulting items.
 */
public class CombineLatestPublisher<L, R, U> implements Publisher<U> {
    private static final Object NO_VALUE = new Object();

    private final Publisher<L> leftUpstream;
    private final Publisher<R> rightUpstream;
    private final BiFunction<L, R, U> zipFunction;

    /**
     * Constructs a ZipPublisher.
     *
     * @param leftUpstream left upstream.
     * @param rightUpstream right upstream.
     * @param zipFunction function to produce resulting items.
     */
    public CombineLatestPublisher(Publisher<L> leftUpstream, Publisher<R> rightUpstream, BiFunction<L, R, U> zipFunction) {
        this.leftUpstream = leftUpstream;
        this.rightUpstream = rightUpstream;
        this.zipFunction = zipFunction;
    }

    @Override
    public void subscribe(Subscriber<? super U> subscriber) {
        Subscription thisSubscription = new CombineLatestSubscription(subscriber);
        subscriber.onSubscribe(thisSubscription);

    }


    private class CombineLatestSubscription implements Subscription {
        private final Subscriber<? super U> subscriber;
        private SubscriptionHolder leftUpstreamSubscription;
        private SubscriptionHolder rightUpstreamSubscription;
        private Object lastLeftValue = NO_VALUE;
        private Object lastRightValue = NO_VALUE;


        private CombineLatestSubscription(Subscriber<? super U> subscriber) {
            this.subscriber = subscriber;
            subscribeUpstream();
        }

        @Override
        public void request(long n) {
            if (isNotCancelled()) {
                leftUpstreamSubscription.request(n);
                rightUpstreamSubscription.request(n);
            }
        }

        @Override
        public void cancel() {
            leftUpstreamSubscription.cancel();
            rightUpstreamSubscription.cancel();
        }

        private boolean isCancelled() {
            return leftUpstreamSubscription.isCancelled() || rightUpstreamSubscription.isCancelled();
        }

        private boolean isNotCancelled() {
            return !isCancelled();
        }

        private void subscribeUpstream() {
            leftUpstream.subscribe(new Subscriber<L>() {
                @Override
                public void onSubscribe(Subscription subscription) {
                    leftUpstreamSubscription.setSubscription(subscription);
                }

                @Override
                public void onNext(L item) {
                    lastLeftValue = item;
                    if (lastRightValue != NO_VALUE) {
                        CombineLatestSubscription.this.onNext(item, (R) lastRightValue);
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    if (isNotCancelled()) {
                        cancel();
                        subscriber.onError(throwable);
                    }
                }

                @Override
                public void onComplete() {
                    if (isNotCancelled()) {
                        cancel();
                        subscriber.onComplete();
                    }
                }
            });

            rightUpstream.subscribe(new Subscriber<R>() {
                @Override
                public void onSubscribe(Subscription subscription) {
                    rightUpstreamSubscription.setSubscription(subscription);
                }

                @Override
                public void onNext(R item) {
                    lastRightValue = item;
                    if (lastLeftValue != NO_VALUE) {
                        CombineLatestSubscription.this.onNext((L) lastLeftValue, item);
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    if (isNotCancelled()) {
                        cancel();
                        subscriber.onError(throwable);
                    }
                }

                @Override
                public void onComplete() {
                    if (isNotCancelled()) {
                        cancel();
                        subscriber.onComplete();
                    }
                }
            });

        }

        private void onNext(L left, R right) {
            if (isNotCancelled()) {
                subscriber.onNext(zipFunction.apply(left, right));
            }
        }
    }
}
