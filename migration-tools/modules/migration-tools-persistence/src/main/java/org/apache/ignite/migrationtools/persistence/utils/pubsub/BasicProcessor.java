/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
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
