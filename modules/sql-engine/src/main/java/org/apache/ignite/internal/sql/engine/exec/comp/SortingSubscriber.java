package org.apache.ignite.internal.sql.engine.exec.comp;

import java.util.Comparator;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.jetbrains.annotations.Nullable;

class SortingSubscriber<T> implements Subscriber<T> {
    private final Subscriber<T> delegate;

    private final SubscriptionManagementStrategy<T> subscriptionStrategy;

//    private final Queue<T> inBuf;

    private volatile T lastItem;

    private final int id;

    private final AtomicLong remainingCnt = new AtomicLong();

    private final AtomicBoolean finished = new AtomicBoolean();

    private final int subscriptionsCnt;

    SortingSubscriber(Subscriber<T> delegate, int id, SubscriptionManagementStrategy<T> subscriptionStrategy, int subscriptionsCnt) {
        assert delegate != null;

        this.delegate = delegate;
        this.id = id;
        this.subscriptionStrategy = subscriptionStrategy;
        this.subscriptionsCnt = subscriptionsCnt;
    }

    T lastItem() {
        return lastItem;
    }

    boolean finished() {
        return finished.get();
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscriptionStrategy.addSubscription(subscription);
    }

    @Override
    public void onNext(T item) {
        // todo optimize
        lastItem = item;

        subscriptionStrategy.push(id, item);

        if (remainingCnt.decrementAndGet() <= 0) {
            assert remainingCnt.get() == 0 : "!!!!remaining failed " + remainingCnt.get();

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

    public void onDataRequested(long n) {
        // todo add assertion?
        if (finished.get())
            return;

        remainingCnt.set(n);
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
}
