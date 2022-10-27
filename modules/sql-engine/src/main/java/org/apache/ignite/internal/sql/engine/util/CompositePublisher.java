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
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jetbrains.annotations.Nullable;

/**
 * Composite publisher.
 */
public class CompositePublisher<T> implements Flow.Publisher<T> {
    /** List of registered publishers. */
    private final Collection<? extends Publisher<T>> publishers;

    /** Flag indicating the state of the subscription. */
    private final AtomicBoolean subscribed = new AtomicBoolean();

    /** Items comparator. */
    private final Comparator<T> comp;

    public CompositePublisher(Collection<? extends Publisher<T>> publishers, @Nullable Comparator<T> comp) {
        this.publishers = publishers;
        this.comp = comp;
    }

    /** {@inheritDoc} */
    @Override
    public void subscribe(Subscriber<? super T> delegate) {
        if (!subscribed.compareAndSet(false, true)) {
            throw new IllegalStateException("Multiple subscribers are not supported.");
        }

        AbstractCompositeSubscriptionStrategy<T> subscriptionStrategy = comp != null
                ? new MergeSortSubscriptionStrategy<>(comp, delegate)
                : new SequentialSubscriptionStrategy<>(delegate);

        int subscriberIdx = 0;

        for (Publisher<T> publisher : publishers) {
            publisher.subscribe(subscriptionStrategy.subscriberProxy(subscriberIdx++));
        }

        // Subscribe delegated (target) subscriber to composite subscription.
        delegate.onSubscribe(subscriptionStrategy);
    }

    /**
     * Composite subscription strategy template.
     */
    protected abstract static class AbstractCompositeSubscriptionStrategy<T> implements Subscription {
        /** List of subscriptions. */
        final List<Subscription> subscriptions = new ArrayList<>();

        /** Delegated subscriber. */
        final Subscriber<? super T> delegate;

        AbstractCompositeSubscriptionStrategy(Subscriber<? super T> delegate) {
            this.delegate = delegate;
        }

        /**
         * Add new subscription.
         *
         * @param subscription Subscription.
         */
        void addSubscription(Subscription subscription) {
            subscriptions.add(subscription);
        }

        /**
         * Create a new subscriber proxy to receive data from the subscription.
         *
         * @param subscriberId Subscriber ID.
         * @return Subscriber proxy.
         */
        public Subscriber<T> subscriberProxy(int subscriberId) {
            return new PlainSubscriberProxy(subscriberId);
        }

        /**
         * Called when one of the subscriptions is completed.
         *
         * @param subscribeId Subscription ID.
         */
        public abstract void onSubscriptionComplete(int subscribeId);

        /**
         * Called when the subscriber receives a new item.
         *
         * @param subscriberId Subscriber ID.
         * @param item Item.
         */
        public abstract void onReceive(int subscriberId, T item);

        /**
         * Plain subscriber.
         */
        protected class PlainSubscriberProxy implements Subscriber<T> {
            /** Subscriber ID. */
            protected final int id;

            PlainSubscriberProxy(int id) {
                this.id = id;
            }

            /** {@inheritDoc} */
            @Override
            public void onSubscribe(Subscription subscription) {
                addSubscription(subscription);
            }

            /** {@inheritDoc} */
            @Override
            public void onNext(T item) {
                onReceive(id, item);
            }

            /** {@inheritDoc} */
            @Override
            public void onError(Throwable throwable) {
                cancel();

                delegate.onError(throwable);
            }

            /** {@inheritDoc} */
            @Override
            public void onComplete() {
                onSubscriptionComplete(id);
            }
        }
    }

    /**
     * Sequential subscription strategy.
     * <br>
     * Sequentially receives data from each registered subscription
     * until the total number of requested items has been received.
     */
    public static class SequentialSubscriptionStrategy<T> extends AbstractCompositeSubscriptionStrategy<T> {
        /** Current subscription index. */
        int subscriptionIdx = 0;

        /** Total number of remaining items. */
        private long remaining;

        SequentialSubscriptionStrategy(Subscriber<? super T> delegate) {
            super(delegate);
        }

        /** {@inheritDoc} */
        @Override
        public void onReceive(int subscriberId, T item) {
            --remaining;

            delegate.onNext(item);
        }

        /** {@inheritDoc} */
        @Override
        public void onSubscriptionComplete(int subscribeId) {
            if (++subscriptionIdx == subscriptions.size()) {
                delegate.onComplete();

                return;
            }

            if (remaining > 0) {
                requestInternal();
            }
        }

        /** {@inheritDoc} */
        @Override
        public void request(long n) {
            remaining = n;

            requestInternal();
        }

        /** {@inheritDoc} */
        @Override
        public void cancel() {
            activeSubscription().cancel();
        }

        /** Request data from a subscription. */
        private void requestInternal() {
            activeSubscription().request(remaining);
        }

        private Subscription activeSubscription() {
            return subscriptions.get(subscriptionIdx);
        }
    }
}
