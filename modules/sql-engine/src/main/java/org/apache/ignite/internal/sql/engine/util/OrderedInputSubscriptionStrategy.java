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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.util.Pair;
import org.apache.ignite.raft.jraft.util.concurrent.ConcurrentHashSet;
import org.jetbrains.annotations.Nullable;

public class OrderedInputSubscriptionStrategy<T> implements SubscriptionManagementStrategy<T>, Subscription {
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
    private long remain = 0;

    /** Count of requested items. */
    private long requested = 0;

    private Subscriber<? super T> delegate;

    /** The IDs of the subscribers we are waiting for. */
    private final Set<Integer> waitResponse = new ConcurrentHashSet<>();

    /** Count of subscribers we are waiting for. */
    private final AtomicInteger waitResponseCnt = new AtomicInteger();

    public OrderedInputSubscriptionStrategy(Comparator<T> comp) {
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
    }

    /** {@inheritDoc} */
    @Override
    public void request(long n) {
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
                requestNext();

                return;
            }
        }

        List<Integer> subsIds = IntStream.rangeClosed(0, subscriptions.size() - 1).boxed().collect(Collectors.toList());

        requestInternal(subsIds, n);
    }

    /** {@inheritDoc} */
    @Override
    public void cancel() {
        for (Subscription subscription : subscriptions) {
            subscription.cancel();
        }
    }

    // Synchronized is needed because "request" can be executed in parallel with "onComplete".
    // For example (supplier has only one item left):
    //      user-thread: request(1)
    //  supplier-thread: subscriber -> onNext() -> return result to user
    //
    //  supplier-thread: onComplete -\
    //                                |-------> can be executed in parallel
    //      user-thread: request(1) -/
    //
    @Override
    public synchronized void onSubscriptionComplete(int subscriberId) {
        debug(">xxx> onComplete " + subscriberId);

        if (finished.add(subscriberId) && finishedCnt.incrementAndGet() == subscriptions.size() && (remain > 0 || inBuf.size() == 0)) {
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

    @Override
    public void onRequestCompleted(int subscriberId) {
        if (waitResponse.remove(subscriberId) && waitResponseCnt.decrementAndGet() == 0) {
            requestNext();
        }
        else
            debug(">Xxx> onRequestCompleted " + waitResponse + ", cntr=" + waitResponseCnt.get() + ", id=" + subscriberId);
    }

    // can be called from different threads
    public synchronized void requestNext() {
        Pair<T, List<Integer>> minItemAndIds = chooseRequestedSubscriptionsIds();
        T minItem = minItemAndIds.left;
        List<Integer> subsIds = minItemAndIds.right;

        if (minItem == null)
            return;

        remain = pushQueue(remain, comp, minItem);

        debug(">xxx> pushQueue :: end");

        if (remain > 0) {
            requestInternal(subsIds, requested);
        }
    }

    private void requestInternal(List<Integer> subsIds, long cnt) {
        long dataAmount = estimateSubscriptionRequestAmount(cnt);

        for (Integer id : subsIds) {
            if (finished.contains(id))
                continue;

            boolean added = waitResponse.add(id);

            assert added : "concurrent request call [id=" + id + ']';

            waitResponseCnt.incrementAndGet();
        }

        for (Integer id : waitResponse) {
            debug(">xxx> idx=" + id + " requested=" + dataAmount);

            // todo remove this usage
            subscribers.get(id).onDataRequested(dataAmount);
            subscriptions.get(id).request(dataAmount);
        }
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
            System.out.println(Thread.currentThread().getId() + " " + msg);
    }
}
