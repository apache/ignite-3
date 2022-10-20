package org.apache.ignite.internal.sql.engine.exec.comp;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.raft.jraft.util.concurrent.ConcurrentHashSet;

public class CompositeSubscription<T> implements Subscription {
    private final Comparator<T> comp;

    private final PriorityBlockingQueue<T> queue;

    private final List<Subscription> subscriptions = new ArrayList<>();

    private final List<SortingSubscriber<T>> subscribers = new ArrayList<>();

    private final ConcurrentHashSet<Integer> finished = new ConcurrentHashSet<>();

    private final AtomicInteger finishedCnt = new AtomicInteger();

    private final AtomicBoolean completed = new AtomicBoolean();

    /** Count of remaining items. */
    private volatile long remain = 0;

    /** Count of requested items. */
    private volatile long requested = 0;

    private final Set<Integer> waitResponse = new ConcurrentHashSet<>();

    CompositeSubscription(Comparator<T> comp, PriorityBlockingQueue<T> queue) {
        this.comp = comp;
        this.queue = queue;
    }

    public boolean onRequestCompleted(int idx) {
        // Internal buffers has been filled.
        if (waitResponse.remove(idx) && waitResponse.isEmpty()) {
            onRequestCompleted0();

            return true;
        }

        return false;
    }

    // can be called from different threads
    public synchronized boolean onRequestCompleted0() {
        List<Integer> minIdxs = selectMinIdx();

        if (minIdxs.isEmpty())
            return false;

        SortingSubscriber<T> subscr = subscribers.get(minIdxs.get(0));

        debug(">xxx> pushQueue :: start, t=" + Thread.currentThread().getId());

        remain = subscr.pushQueue(remain, comp);

        debug(">xxx> pushQueue :: end");

        if (remain > 0) {
            long dataAmount = Math.max(1, requested / (subscriptions.size() - finished.size()));

            for (Integer idx : minIdxs) {
                waitResponse.add(idx);

                subscribers.get(idx).onDataRequested(dataAmount);
            }

            for (Integer idx : minIdxs) {
                debug(">xxx> idx=" + idx + " requested=" + dataAmount);

                subscriptions.get(idx).request(dataAmount);
            }
        }

        return true;
    }

    private synchronized List<Integer> selectMinIdx() {
        T minItem = null;
        List<Integer> minIdxs = new ArrayList<>();

        for (int i = 0; i < subscribers.size(); i++) {
            SortingSubscriber<T> subscriber = subscribers.get(i);

            if (subscriber == null || subscriber.finished()) {
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

        return minIdxs;
    }

    public List<Subscription> subscriptions() {
        return subscriptions;
    }

    public void add(Subscription subscription, SortingSubscriber<T> subscriber) {
        subscriptions.add(subscription);

        subscribers.add(subscriber);
    }

    // todo sync
    public boolean remove(Subscription subscription) {
        return subscriptions.remove(subscription);
    }

    @Override
    public void request(long n) {
        // todo we may return result before all publishers has finished publishing
        assert waitResponse.isEmpty();

        synchronized (this) {
            remain = n;
            requested = n;
        }

        // Perhaps we can return something from internal buffer?
        if (queue.size() > 0) {
            if (finished.size() == subscriptions.size()) { // all data has been received
                if (subscribers.get(0).pushQueue(n, null) == 0)
                    return;
            }
            else { // we have someone alive
                onRequestCompleted0();

                return;
            }
        }

        long requestCnt = Math.max(1, n / subscriptions.size());

        for (int i = 0; i < subscriptions.size(); i++) {
            SortingSubscriber<T> subscriber = subscribers.get(i);

            if (subscriber.finished())
                continue;

            waitResponse.add(i);
            subscriber.onDataRequested(requestCnt);
        }

        for (int i = 0; i< subscriptions.size(); i++) {
            if (subscribers.get(i).finished())
                continue;

            subscriptions.get(i).request(requestCnt);
        }
    }

    @Override
    public void cancel() {
        for (Subscription subscription : subscriptions) {
            subscription.cancel();
        }
    }

    // synchronized is needed because "request" can be executed in parallel
    // can be replaced with retry
    public synchronized void cancel(int idx) {
        debug(">xxx> onComplete " + idx);

        finished.add(idx);

        if (finishedCnt.incrementAndGet() == subscriptions.size() && (remain > 0 || queue.size() == 0)) {
            waitResponse.remove(idx);

            if (completed.compareAndSet(false, true)) {
                debug(">xxx> push queue, remain=" + remain + " queue=" + queue.size());

                subscribers.get(0).pushQueue(remain, null);
            }

            // all work done
            return;
        }

        onRequestCompleted(idx);

        debug(">xxx> finished " + idx + " t=" + Thread.currentThread().getId());
    }

    private static boolean debug = false;

    private static void debug(String msg) {
        if (debug)
            System.out.println(msg);
    }
}
