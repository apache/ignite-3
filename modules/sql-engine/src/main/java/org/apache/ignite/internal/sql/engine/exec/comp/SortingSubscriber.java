package org.apache.ignite.internal.sql.engine.exec.comp;

import java.util.Comparator;
import java.util.Queue;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.jetbrains.annotations.Nullable;

class SortingSubscriber<T> implements Subscriber<T> {
    private final Subscriber<T> delegate;

    private final CompositeSubscription<T> compSubscription;

    private final Queue<T> inBuf;

    private volatile T lastItem;

    private final int idx;

    private final AtomicLong remainingCnt = new AtomicLong();

    private final AtomicBoolean finished = new AtomicBoolean();

    SortingSubscriber(Subscriber<T> delegate, int idx, CompositeSubscription<T> compSubscription, Queue<T> inBuf) {
        assert delegate != null;

        this.delegate = delegate;
        this.idx = idx;
        this.compSubscription = compSubscription;
        this.inBuf = inBuf;
    }

    T lastItem() {
        return lastItem;
    }

    boolean finished() {
        return finished.get();
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        compSubscription.add(subscription, this);
    }

    @Override
    public void onNext(T item) {
        // todo optimize
        lastItem = item;

        inBuf.add(item);

        if (remainingCnt.decrementAndGet() <= 0) {
            assert remainingCnt.get() == 0 : "!!!!remaining failed " + remainingCnt.get();

            compSubscription.onRequestCompleted(idx);
        }
    }

    @Override
    public void onError(Throwable throwable) {
        // todo
        throwable.printStackTrace();

        compSubscription.cancel();

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
        if (finished.compareAndSet(false, true)) {
            // todo think
//            remainingCnt.set(0);

            compSubscription.cancel(idx);
        }
    }

    public long pushQueue(long remain, @Nullable Comparator<T> comp) {
        boolean done = false;
        T r;

        while (remain > 0 && (r = inBuf.peek()) != null) {
            int cmpRes = comp == null ? 0 : comp.compare(lastItem, r);

            if (cmpRes < 0) {
                return remain;
            }

            boolean same = comp != null && cmpRes == 0;

            if (!done && same) {
                done = true;
            }

            if (!done || same) {
                delegate.onNext(inBuf.poll());

                --remain;
            }

            if (done && !same) {
                break;
            }
        }

        if (comp == null && inBuf.isEmpty()) {
            delegate.onComplete();
        }

        return remain;
    }
}
