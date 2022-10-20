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

package org.apache.ignite.internal.sql.engine.exec.comp;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.jetbrains.annotations.Nullable;

public class CompositePublisher<T> implements Flow.Publisher<T> {
    private final Collection<Publisher<T>> publishers = new ArrayList<>();

    private final CompositeSubscription<T> compSubscription;

    private final AtomicBoolean subscribed = new AtomicBoolean();

    private final boolean ordered;

    private final PriorityBlockingQueue<T> queue;

    public CompositePublisher(@Nullable Comparator<T> comp) {
        this.queue = new PriorityBlockingQueue<>(1, comp);

        compSubscription = new CompositeSubscription<>(comp, queue);
        ordered = comp != null;
    }

    public void add(Publisher<T> publisher) {
        if (subscribed.get())
            throw new IllegalStateException("Cannot add publisher after subscription.");

        publishers.add(publisher);
    }

    @Override
    public void subscribe(Subscriber<? super T> delegate) {
        if (!subscribed.compareAndSet(false, true)) {
            throw new IllegalStateException("Multiple subscribers are not supported.");
        }

        int idx = 0;

        for (Publisher<T> publisher : publishers)
            publisher.subscribe(wrap((Subscriber<T>) delegate, idx++));

        delegate.onSubscribe(compSubscription);
    }

    public Subscriber<T> wrap(Subscriber<T> subscriber, int idx) {
        if (ordered)
            return new SortingSubscriber<>(subscriber, idx, compSubscription, queue);
        else
            return new PlainSubscriber<>(subscriber, compSubscription);
    }

    private static class PlainSubscriber<T> implements Subscriber<T> {
        private final Subscriber<T> delegate;

        private final CompositeSubscription<T> compSubscription;

        private final AtomicInteger completed = new AtomicInteger();

        PlainSubscriber(Subscriber<T> delegate, CompositeSubscription<T> compSubscription) {
            assert delegate != null;

            this.delegate = delegate;
            this.compSubscription = compSubscription;
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            compSubscription.add(subscription, null);
        }

        @Override
        public void onNext(T item) {
            delegate.onNext(item);
        }

        @Override
        public void onError(Throwable throwable) {
            compSubscription.cancel();

            delegate.onError(throwable);
        }

        @Override
        public void onComplete() {
            if (completed.incrementAndGet() == compSubscription.subscriptions().size())
                delegate.onComplete();
        }
    }
}
