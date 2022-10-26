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

public class CompositePublisher<T> implements Flow.Publisher<T> {
    private final Collection<? extends Publisher<T>> publishers;

    private final AtomicBoolean subscribed = new AtomicBoolean();

    private final Comparator<T> comp;

    public CompositePublisher(Collection<? extends Publisher<T>> publishers, @Nullable Comparator<T> comp) {
        this.publishers = publishers;
        this.comp = comp;
    }

    @Override
    public void subscribe(Subscriber<? super T> delegate) {
        if (!subscribed.compareAndSet(false, true)) {
            throw new IllegalStateException("Multiple subscribers are not supported.");
        }

        SubscriptionManagementStrategy<T> subscriptionStrategy = comp != null ?
                new MergeSortSubscriptionStrategy<>(comp, delegate) :
                new SequentialSubscriptionStrategy<>(delegate);

        int subscriberIdx = 0;

        for (Publisher<T> publisher : publishers) {
            publisher.subscribe(subscriptionStrategy.subscriberProxy(subscriberIdx++));
        }

        // Subscribe delegated (target) subscriber to composite subscription.
        delegate.onSubscribe(subscriptionStrategy);
    }

    private static class PlainSubscriberProxy<T> implements Subscriber<T> {
        private final Subscriber<? super T> delegate;

        private final SubscriptionManagementStrategy<T> subscriptionStrategy;

        private final int id;

        PlainSubscriberProxy(Subscriber<? super T> delegate, SubscriptionManagementStrategy<T> subscriptionStrategy, int id) {
            assert delegate != null;

            this.delegate = delegate;
            this.subscriptionStrategy = subscriptionStrategy;
            this.id = id;
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            subscriptionStrategy.addSubscription(subscription);
        }

        @Override
        public void onNext(T item) {
            subscriptionStrategy.onReceive(id, item);
        }

        @Override
        public void onError(Throwable throwable) {
            subscriptionStrategy.cancel();

            delegate.onError(throwable);
        }

        @Override
        public void onComplete() {
            subscriptionStrategy.onSubscriptionComplete(id);
        }
    }

    private static class SequentialSubscriptionStrategy<T> implements SubscriptionManagementStrategy<T> {
        List<Subscription> subscriptions = new ArrayList<>();
        int subscriptionIdx = 0;

        private final Subscriber<? super T> delegate;

        private long remaining;

        public SequentialSubscriptionStrategy(Subscriber<? super T> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void addSubscription(Subscription subscription) {
            subscriptions.add(subscription);
        }

        @Override
        public void onReceive(int subscriberId, T item) {
            --remaining;

            delegate.onNext(item);
        }

        @Override
        public void onSubscriptionComplete(int subscriberId) {
            if (++subscriptionIdx == subscriptions.size()) {
                delegate.onComplete();

                return;
            }

            if (remaining > 0)
                requestInternal();
        }

        @Override
        public void request(long n) {
            remaining = n;

            requestInternal();
        }

        private void requestInternal() {
            activeSubscription().request(remaining);
        }


        @Override
        public void cancel() {
            activeSubscription().cancel();
        }

        private Subscription activeSubscription() {
            return subscriptions.get(subscriptionIdx);
        }

        @Override
        public void onRequestCompleted(int subscriberId) {
            // No-op.
        }

        @Override
        public Subscriber<T> subscriberProxy(int subscriberId) {
            return new PlainSubscriberProxy<>(delegate, this, subscriberId);
        }
    }
}
