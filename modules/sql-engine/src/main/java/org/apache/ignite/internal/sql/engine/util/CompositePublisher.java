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

package org.apache.ignite.internal.sql.engine.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.jetbrains.annotations.Nullable;

/**
 * Composite publisher.
 */
public class CompositePublisher<T> implements Publisher<T> {
    /** List of upstream publishers. */
    final Collection<? extends Publisher<T>> publishers;

    /**
     * Constructor.
     *
     * @param publishers List of upstream publishers.
     */
    public CompositePublisher(Collection<? extends Publisher<T>> publishers) {
        this.publishers = publishers;
    }

    @Override
    public void subscribe(Subscriber<? super T> downstream) {
        subscribe(new CompositeSubscription<>(downstream, publishers.size()), downstream);
    }

    void subscribe(CompositeSubscription<T> subscription, Subscriber<? super T> downstream) {
        subscription.subscribe(publishers);

        downstream.onSubscribe(subscription);
    }

    /**
     * Sequential composite subscription.
     *
     * <p>Sequentially receives data from each registered subscription
     * until the total number of requested items has been received.
     */
    public static class CompositeSubscription<T> implements Subscription {
        /** List of subscriptions. */
        private final List<Subscription> subscriptions = new ArrayList<>();

        /** Downstream subscriber. */
        protected final Subscriber<? super T> downstream;

        /** Current subscription index. */
        private final AtomicInteger subscriptionIdx = new AtomicInteger();

        /** Total number of remaining items. */
        private final AtomicLong remaining = new AtomicLong();

        /** Flag indicating that the subscription has been cancelled. */
        private volatile boolean cancelled;

        private final int cnt;

        public CompositeSubscription(Subscriber<? super T> downstream, int cnt) {
            this.downstream = downstream;
            this.cnt = cnt;
        }

        /**
         * Subscribe multiple publishers.
         *
         * @param sources Publishers.
         */
        public void subscribe(Collection<? extends Publisher<? extends T>> sources) {
            for (Publisher<? extends T> publisher : sources) {
                publisher.subscribe(new PlainSubscriber());
            }
        }

        /** {@inheritDoc} */
        @Override
        public void request(long n) {
            remaining.set(n);

            requestInternal();
        }

        /** {@inheritDoc} */
        @Override
        public void cancel() {
            cancelled = true;

            Subscription subscription = activeSubscription();

            if (subscription != null) {
                subscription.cancel();
            }
        }

        /** Request data from a subscription. */
        private void requestInternal() {
            Subscription subscription = activeSubscription();

            if (subscription != null) {
                subscription.request(remaining.get());
            }
        }

        private @Nullable Subscription activeSubscription() {
            int subsIdx = subscriptionIdx.get();

            if (subsIdx >= cnt) {
                return null;
            }

            Subscription res = subscriptions.get(subsIdx);

            if (res == null)
                System.err.println(">xxx> visibility error");

            return res;
        }

        /**
         * Plain subscriber.
         */
        protected class PlainSubscriber implements Subscriber<T> {
            /** {@inheritDoc} */
            @Override
            public void onSubscribe(Subscription subscription) {
                subscriptions.add(subscription);
            }

            /** {@inheritDoc} */
            @Override
            public void onNext(T item) {
                remaining.decrementAndGet();

                downstream.onNext(item);
            }

            /** {@inheritDoc} */
            @Override
            public void onError(Throwable throwable) {
                downstream.onError(throwable);
            }

            /** {@inheritDoc} */
            @Override
            public void onComplete() {
                if (cancelled) {
                    return;
                }

                if (subscriptionIdx.incrementAndGet() == cnt) {
                    downstream.onComplete();

                    return;
                }

                if (remaining.get() > 0) {
                    requestInternal();
                }
            }
        }
    }
}
