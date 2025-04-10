/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.persistence.utils.pubsub;

import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.SubmissionPublisher;
import org.apache.commons.lang3.mutable.MutableLong;

/**
 * Publisher for streamer. A lighter alternative to {@link SubmissionPublisher}.
 * Single-threaded.
 * Only supports one subscriber.
 */
public class StreamerPublisher<T> implements Publisher<T>, Subscription, AutoCloseable {

    private final MutableLong requested = new MutableLong(0);

    private Subscriber<? super T> subscriber;

    @Override
    public void subscribe(Subscriber<? super T> subscriber) {
        synchronized (this.requested) {
            if (this.requested.getValue() == -1 || this.subscriber != null) {
                throw new IllegalStateException("Only one subscription is supported");
            }
        }

        this.subscriber = subscriber;

        subscriber.onSubscribe(this);
    }

    @Override
    public void request(long n) {
        assert n > 0 : "Requested count must be positive";

        // This method is called from arbitrary thread in StreamerSubscriber
        synchronized (this.requested) {
            this.requested.add(n);
            this.requested.notifyAll();
        }
    }

    @Override
    public void cancel() {
        setCancelledStatus();
    }

    /**
     * Offer a new item to the subscriber.
     *
     * @param item Item to be offered.
     * @return Whether the element was successfully published or not.
     * @throws InterruptedException if the thread was interrupted.
     */
    public boolean offer(T item) throws InterruptedException {
        // This method is called from the same thread in sink task.
        // request() method is called from arbitrary thread in StreamerSubscriber, but it can only increment the counter.
        synchronized (this.requested) {
            while (this.requested.getValue() <= 0) {
                // Is effectively not subscribed
                if (this.requested.getValue() <= -1) {
                    return false;
                }

                if (this.requested.getValue() == 0) {
                    this.requested.wait();
                }
            }

            this.requested.decrementAndGet();
        }

        subscriber.onNext(item);

        return true;
    }

    /** Close with error. */
    public void closeExceptionally(Throwable error) {
        if (this.subscriber == null) {
            throw new IllegalStateException("Subscriber already closed");
        }

        this.subscriber.onError(error);
        this.subscriber = null;
        setCancelledStatus();
    }

    @Override
    public void close() {
        if (subscriber != null) {
            subscriber.onComplete();
            subscriber = null;
            setCancelledStatus();
        }
    }

    private void setCancelledStatus() {
        synchronized (this.requested) {
            this.requested.setValue(-1);
            this.requested.notifyAll();
        }
    }
}
