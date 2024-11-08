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

package org.apache.ignite.internal.streamer;

import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Direct publisher that ignores backpressure and sends items directly to the subscribers.
 */
public class DirectPublisher<T> implements Publisher<T>, Subscription, AutoCloseable {
    private final AtomicLong requested = new AtomicLong();

    private Subscriber<? super T> subscriber;

    @Override
    public void close() {
        if (subscriber != null) {
            subscriber.onComplete();
        }
    }

    @Override
    public void subscribe(Subscriber<? super T> subscriber) {
        assert this.subscriber == null : "Only one subscriber is supported";
        this.subscriber = subscriber;
        subscriber.onSubscribe(this);
    }

    @Override
    public void request(long n) {
        requested.addAndGet(n);
    }

    @Override
    public void cancel() {
        // No-op.
    }

    public void submit(T item) {
        subscriber.onNext(item);
    }

    public long requested() {
        return requested.get();
    }
}
