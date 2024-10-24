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
import java.util.function.Predicate;

/**
 * Publisher that filters items of upstream.
 */
public final class FilteringPublisher<T> implements Publisher<T> {

    private final Publisher<T> publisher;

    private final Predicate<T> predicate;

    public FilteringPublisher(Publisher<T> publisher, Predicate<T> predicate) {
        this.publisher = publisher;
        this.predicate = predicate;
    }

    /** {@inheritDoc} */
    @Override
    public void subscribe(Subscriber<? super T> subscriber) {
        this.publisher.subscribe(new SubscriberImpl(subscriber));
    }

    private class SubscriberImpl implements Subscriber<T> {

        private final Subscriber<? super T> subscriber;

        SubscriberImpl(Subscriber<? super T> subscriber) {
            this.subscriber = subscriber;
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            subscriber.onSubscribe(subscription);
        }

        @Override
        public void onNext(T item) {
            if (predicate.test(item)) {
                subscriber.onNext(item);
            }
        }

        @Override
        public void onError(Throwable t) {
            subscriber.onError(t);
        }

        @Override
        public void onComplete() {
            subscriber.onComplete();
        }
    }
}
