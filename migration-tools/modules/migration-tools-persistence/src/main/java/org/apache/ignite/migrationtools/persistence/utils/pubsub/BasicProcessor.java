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

package org.apache.ignite.migrationtools.persistence.utils.pubsub;

import java.util.concurrent.Flow;

/** Base class for building {@link Flow.Processor} which provides a transparent layer between a single publisher and a single subscriber. */
public abstract class BasicProcessor<S, T> implements Flow.Processor<S, T> {

    protected Flow.Subscriber<? super T> subscriber;

    protected Flow.Subscription subscription;

    @Override
    public void subscribe(Flow.Subscriber<? super T> subscriber) {
        if (this.subscriber != null) {
            throw new IllegalStateException("Only one subscriber is supported");
        }

        this.subscriber = subscriber;
        if (this.subscription != null) {
            this.subscriber.onSubscribe(subscription);
        }
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        if (this.subscription != null) {
            throw new IllegalStateException("Only one subscription is supported. Cannot subscribe to multiple subscribers");
        }

        this.subscription = subscription;
        if (this.subscriber != null) {
            this.subscriber.onSubscribe(subscription);
        }
    }

    @Override
    public void onError(Throwable throwable) {
        this.subscriber.onError(throwable);
    }

    @Override
    public void onComplete() {
        this.subscriber.onComplete();
    }
}
