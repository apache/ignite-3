package org.apache.ignite.internal.sql.engine.util;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.calcite.util.Pair;
import org.apache.ignite.raft.jraft.util.concurrent.ConcurrentHashSet;
import org.jetbrains.annotations.Nullable;

public class OrderedSubscriptionStrategy<T> implements SubscriptionManagementStrategy<T>, Subscription {
    /** Items comparator. */
    private final Comparator<T> comp;

    /** Internal ordered buffer. */
    private final PriorityBlockingQueue<T> inBuf;

    private final List<Subscription> subscriptions = new ArrayList<>();

    private final List<SortingSubscriber<T>> subscribers = new ArrayList<>();

    private final ConcurrentHashSet<Integer> finished = new ConcurrentHashSet<>();

    private final AtomicInteger finishedCnt = new AtomicInteger();

    private final AtomicBoolean completed = new AtomicBoolean();

    /** Count of remaining items. */
    private volatile long remain = 0;

    /** Count of requested items. */
    private volatile long requested = 0;

    private Subscriber<? super T> delegate;

    private final Set<Integer> waitResponse = new ConcurrentHashSet<>();

    public OrderedSubscriptionStrategy(Comparator<T> comp) {
        this.comp = comp;
        this.inBuf = new PriorityBlockingQueue<>(1, comp);
    }

    @Override
    public void addSubscription(Subscription subscription) {
        // todo subscriptions and subscribers must be in the same order
        subscriptions.add(subscription);
    }

    @Override
    public void addSubscriber(Subscriber<T> subscriber) {
        // todo
        subscribers.add((SortingSubscriber<T>) subscriber);
    }

    @Override
    public void subscribe(Subscriber<? super T> delegate) {
        this.delegate = delegate;

        delegate.onSubscribe(this);
    }

    @Override
    public void onReceive(int subscriberId, T item) {
        inBuf.offer(item);
        // todo calc estmations
    }

    /** {@inheritDoc} */
    @Override
    public void request(long n) {
        // todo we may return result before all publishers has finished publishing
        assert waitResponse.isEmpty() : waitResponse;

        synchronized (this) {
            remain = n;
            requested = n;
        }

        // Perhaps we can return something from internal buffer?
        if (inBuf.size() > 0) {
            if (finished.size() == subscriptions.size()) { // all data has been received?
                if (pushQueue(n, null, null) == 0)
                    return;
            }
            else { // Someone still alive.
                onRequestCompleted0();

                return;
            }
        }

        long requestCnt = estimateSubscriptionRequestAmount(n);

        for (int i = 0; i < subscribers.size(); i++) {
            SortingSubscriber<T> subscriber = subscribers.get(i);

            if (finished.contains(i))
                continue;

            waitResponse.add(i);
            subscriber.onDataRequested(requestCnt);
        }

        for (int i = 0; i < subscriptions.size(); i++) {
            if (finished.contains(i)) {
                waitResponse.remove(i);

                continue;
            }

            subscriptions.get(i).request(requestCnt);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void cancel() {
        for (Subscription subscription : subscriptions) {
            subscription.cancel();
        }
    }

    // synchronized is needed because "request" can be executed in parallel
    // can be replaced with retry
    @Override
    public synchronized void onSubscriptionComplete(int subscriberId) {
        debug(">xxx> onComplete " + subscriberId);

        finished.add(subscriberId);

        if (finishedCnt.incrementAndGet() == subscriptions.size() && (remain > 0 || inBuf.size() == 0)) {
            waitResponse.remove(subscriberId);

            if (completed.compareAndSet(false, true)) {
                debug(">xxx> push queue, remain=" + remain + " queue=" + inBuf.size());

                pushQueue(remain, null, null);
            }

            // all work done
            return;
        }

        onRequestCompleted(subscriberId);

        debug(">xxx> finished " + subscriberId + " t=" + Thread.currentThread().getId());
    }

    private long estimateSubscriptionRequestAmount(long total) {
        return Math.max(1, total / (subscriptions.size() - finished.size()));
    }

    private long pushQueue(long remain, @Nullable Comparator<T> comp, @Nullable T minBound) {
        boolean done = false;
        T r;

        while (remain > 0 && (r = inBuf.peek()) != null) {
            int cmpRes = comp == null ? 0 : comp.compare(minBound, r);

            if (cmpRes < 0) {
                return remain;
            }

            boolean same = comp != null && cmpRes == 0;

            if (!done && same) {
                done = true;
            }

            if (!done || same) {
                T r0 = inBuf.poll();

                assert r == r0;

                delegate.onNext(r);

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

    public boolean onRequestCompleted(int subscriberId) {
        if (waitResponse.remove(subscriberId) && waitResponse.isEmpty()) {
            onRequestCompleted0();

            return true;
        }

        return false;
    }

    // can be called from different threads
    public synchronized boolean onRequestCompleted0() {
        Pair<T, List<Integer>> minItemAndIds = chooseRequestedSubscriptionsIds();
        T minItem = minItemAndIds.left;
        List<Integer> subsIds = minItemAndIds.right;

        if (minItem == null)
            return false;

        remain = pushQueue(remain, comp, minItem);

        debug(">xxx> pushQueue :: end");

        if (remain > 0) {
            long dataAmount = estimateSubscriptionRequestAmount(requested);

            for (Integer idx : subsIds) {
                waitResponse.add(idx);

                // todo remove this usage
                subscribers.get(idx).onDataRequested(dataAmount);
            }

            for (Integer idx : subsIds) {
                debug(">xxx> idx=" + idx + " requested=" + dataAmount);

                subscriptions.get(idx).request(dataAmount);
            }
        }

        return true;
    }

    /**
     * Choose which subscription to request.
     *
     * @return Identifiers of subscriptions to be requested.
     */
    private Pair<T, List<Integer>> chooseRequestedSubscriptionsIds() {
        T minItem = null;
        List<Integer> minIdxs = new ArrayList<>();

        for (int i = 0; i < subscribers.size(); i++) {
            SortingSubscriber<T> subscriber = subscribers.get(i);

            if (finished.contains(i)) {
                continue;
            }

            T item = subscriber.lastItem();

            int cmpRes = 0;

            if (minItem == null || (cmpRes = comp.compare(minItem, item)) >= 0) {
                minItem = item;

                if (cmpRes != 0) {
                    minIdxs.clear();
                }

                minIdxs.add(i);
            }
        }

        return Pair.of(minItem, minIdxs);
    }

    private static boolean debug = false;

    private static void debug(String msg) {
        if (debug)
            System.out.println(msg);
    }
}
