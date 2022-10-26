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

        AbstractCompositeSubscriptionStrategy<T> subscriptionStrategy = comp != null ?
                new MergeSortSubscriptionStrategy<>(comp, delegate) :
                new SequentialSubscriptionStrategy<>(delegate);

        int subscriberIdx = 0;

        for (Publisher<T> publisher : publishers) {
            publisher.subscribe(subscriptionStrategy.subscriberProxy(subscriberIdx++));
        }

        // Subscribe delegated (target) subscriber to composite subscription.
        delegate.onSubscribe(subscriptionStrategy);
    }

    protected abstract static class AbstractCompositeSubscriptionStrategy<T> implements Subscription {
        protected List<Subscription> subscriptions = new ArrayList<>();

        protected final Subscriber<? super T> delegate;

        protected AbstractCompositeSubscriptionStrategy(Subscriber<? super T> delegate) {
            this.delegate = delegate;
        }

        public void addSubscription(Subscription subscription) {
            subscriptions.add(subscription);
        }

        public Subscriber<T> subscriberProxy(int subscriberId) {
            return new PlainSubscriberProxy(delegate, subscriberId);
        }

        public abstract void onSubscriptionComplete(int subscriberId);

        public abstract void onReceive(int subscriberId, T item);

        protected class PlainSubscriberProxy implements Subscriber<T> {
            protected final Subscriber<? super T> delegate;

            protected final int id;

            public PlainSubscriberProxy(Subscriber<? super T> delegate, int id) {
                assert delegate != null;

                this.delegate = delegate;
                this.id = id;
            }

            @Override
            public void onSubscribe(Subscription subscription) {
                addSubscription(subscription);
            }

            @Override
            public void onNext(T item) {
                onReceive(id, item);
            }

            @Override
            public void onError(Throwable throwable) {
                cancel();

                delegate.onError(throwable);
            }

            @Override
            public void onComplete() {
                onSubscriptionComplete(id);
            }
        }
    }

    public static class SequentialSubscriptionStrategy<T> extends AbstractCompositeSubscriptionStrategy<T> {
        int subscriptionIdx = 0;

        private long remaining;

        public SequentialSubscriptionStrategy(Subscriber<? super T> delegate) {
            super(delegate);
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

        @Override
        public void cancel() {
            activeSubscription().cancel();
        }

        private void requestInternal() {
            activeSubscription().request(remaining);
        }

        private Subscription activeSubscription() {
            return subscriptions.get(subscriptionIdx);
        }
    }
}
