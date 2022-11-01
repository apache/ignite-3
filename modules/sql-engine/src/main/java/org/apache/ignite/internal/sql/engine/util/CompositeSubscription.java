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
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import org.jetbrains.annotations.Nullable;

/**
 * Sequential composite subscription.
 * <br>
 * Sequentially receives data from each registered subscription
 * until the total number of requested items has been received.
 */
public class CompositeSubscription<T> implements Subscription {
    /** List of subscriptions. */
    private final List<Subscription> subscriptions = new ArrayList<>();

    /** Downstream subscriber. */
    protected final Subscriber<? super T> downstream;

    /** Current subscription index. */
    private int subscriptionIdx = 0;

    /** Total number of remaining items. */
    private long remaining;

    public CompositeSubscription(Subscriber<? super T> downstream) {
        this.downstream = downstream;
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
        remaining = n;

        requestInternal();
    }

    /** {@inheritDoc} */
    @Override
    public void cancel() {
        Subscription subscription = activeSubscription();

        if (subscription != null) {
            subscription.cancel();
        }
    }

    /** Request data from a subscription. */
    private void requestInternal() {
        Subscription subscription = activeSubscription();

        if (subscription != null) {
            subscription.request(remaining);
        }
    }

    private @Nullable Subscription activeSubscription() {
        if (subscriptionIdx >= subscriptions.size()) {
            return null;
        }

        return subscriptions.get(subscriptionIdx);
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
            --remaining;

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
            if (++subscriptionIdx == subscriptions.size()) {
                downstream.onComplete();

                return;
            }

            if (remaining > 0) {
                requestInternal();
            }
        }
    }
}
