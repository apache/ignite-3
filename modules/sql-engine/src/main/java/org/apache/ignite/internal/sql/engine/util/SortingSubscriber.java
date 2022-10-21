package org.apache.ignite.internal.sql.engine.util;

import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

class SortingSubscriber<T> implements Subscriber<T> {
    private final Subscriber<T> delegate;

    private final SubscriptionManagementStrategy<T> subscriptionStrategy;

    private volatile T lastItem;

    private final int id;

    private final AtomicLong remainingCnt = new AtomicLong();

    private final AtomicBoolean finished = new AtomicBoolean();

    SortingSubscriber(Subscriber<T> delegate, int id, SubscriptionManagementStrategy<T> subscriptionStrategy) {
        assert delegate != null;

        this.delegate = delegate;
        this.id = id;
        this.subscriptionStrategy = subscriptionStrategy;
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscriptionStrategy.addSubscription(subscription);
    }

    @Override
    public void onNext(T item) {
        // todo optimize
        lastItem = item;

        subscriptionStrategy.onReceive(id, item);

        long val = remainingCnt.decrementAndGet();

        if (val <= 0) {
            assert val == 0 : "remain=" + val
                    + ", id=" + id
                    + ", item=" + item
                    + ", threadId=" + Thread.currentThread().getName();

            subscriptionStrategy.onRequestCompleted(id);
        }
    }

    @Override
    public void onError(Throwable throwable) {
        // todo
        throwable.printStackTrace();

        subscriptionStrategy.cancel();

        delegate.onError(throwable);
    }

    @Override
    public void onComplete() {
        // todo assertion?
        if (finished.compareAndSet(false, true)) {
            // todo think
//            remainingCnt.set(0);

            subscriptionStrategy.onSubscriptionComplete(id);
        }
    }

    // todo move related code to subscription strategy
    @Deprecated
    T lastItem() {
        return lastItem;
    }

    // todo move related code to subscription strategy
    @Deprecated
    public void onDataRequested(long n) {
        // todo add assertion?
        if (finished.get())
            return;

        remainingCnt.set(n);
    }
}
