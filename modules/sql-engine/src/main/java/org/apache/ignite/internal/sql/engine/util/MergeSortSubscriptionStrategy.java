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

package org.apache.ignite.internal.sql.engine.util;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.sql.engine.util.CompositePublisher.AbstractCompositeSubscriptionStrategy;
import org.apache.ignite.raft.jraft.util.concurrent.ConcurrentHashSet;
import org.jetbrains.annotations.Nullable;

/**
 * Merge sort subscription strategy.
 */
public class MergeSortSubscriptionStrategy<T> extends AbstractCompositeSubscriptionStrategy<T> {
    /** Items comparator. */
    private final Comparator<T> comp;

    /** Internal ordered buffer. */
    private final PriorityBlockingQueue<T> inBuf;

    private final List<MergeSortStrategySubscriber> subscribers = new ArrayList<>();

    private final ConcurrentHashSet<Integer> finished = new ConcurrentHashSet<>();

    private final AtomicInteger finishedCnt = new AtomicInteger();

    private final AtomicBoolean completed = new AtomicBoolean();

    /** Count of remaining items. */
    private long remain = 0;

    /** Count of requested items. */
    private long requested = 0;

    /** The IDs of the subscribers we are waiting for. */
    private final Set<Integer> waitResponse = new ConcurrentHashSet<>();

    /** Count of subscribers we are waiting for. */
    private final AtomicInteger waitResponseCnt = new AtomicInteger();

    /**
     * Constructor.
     *
     * @param comp Items comparator.
     * @param delegate Delegated subscriber.
     */
    public MergeSortSubscriptionStrategy(Comparator<T> comp, Subscriber<? super T> delegate) {
        super(delegate);

        this.comp = comp;
        this.inBuf = new PriorityBlockingQueue<>(1, comp);
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
                if (pushQueue(n, null, null) == 0) {
                    return;
                }
            } else { // Someone still alive.
                requestNext();

                return;
            }
        }

        List<Integer> subsIds = IntStream.rangeClosed(0, subscriptions.size() - 1).boxed().collect(Collectors.toList());

        requestInternal(subsIds, n);
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void cancel() {
        for (int i = 0; i < subscriptions.size(); i++) {
            if (finished.add(i)) {
                subscriptions.get(i).cancel();
            }
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

    private void onRequestCompleted(int subscriberId) {
        if (waitResponse.remove(subscriberId) && waitResponseCnt.decrementAndGet() == 0) {
            requestNext();
        }
    }

    /** {@inheritDoc} */
    @Override
    public Subscriber<T> subscriberProxy(int subscriberId) {
        MergeSortStrategySubscriber subscriber = new MergeSortStrategySubscriber(delegate, subscriberId);

        subscribers.add(subscriber);

        return subscriber;
    }

    private synchronized void requestNext() {
        Pair<T, List<Integer>> minItemAndIds = chooseRequestedSubscriptionsIds();
        T minItem = minItemAndIds.left;
        List<Integer> subsIds = minItemAndIds.right;

        if (minItem == null) {
            return;
        }

        remain = pushQueue(remain, comp, minItem);

        if (remain > 0) {
            requestInternal(subsIds, requested);
        }
    }

    private void requestInternal(List<Integer> subsIds, long cnt) {
        long dataAmount = estimateSubscriptionRequestAmount(cnt);

        for (Integer id : subsIds) {
            if (finished.contains(id)) {
                continue;
            }

            boolean added = waitResponse.add(id);

            assert added : "concurrent request call [id=" + id + ']';

            waitResponseCnt.incrementAndGet();
        }

        for (Integer id : waitResponse) {
            debug(">xxx> idx=" + id + " requested=" + dataAmount);

            MergeSortStrategySubscriber subscriber = subscribers.get(id);

            subscriber.remainingCnt.set(dataAmount);
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
            if (finished.contains(i)) {
                continue;
            }

            MergeSortStrategySubscriber subscriber = subscribers.get(i);

            T item = subscriber.lastItem;

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

    /**
     * Merge sort subscription strategy subscriber.
     */
    public class MergeSortStrategySubscriber extends AbstractCompositeSubscriptionStrategy<T>.PlainSubscriberProxy {
        private final AtomicLong remainingCnt = new AtomicLong();

        private final AtomicBoolean finished = new AtomicBoolean();

        private volatile T lastItem;

        public MergeSortStrategySubscriber(Subscriber<? super T> delegate, int id) {
            super(delegate, id);
        }

        @Override
        public void onNext(T item) {
            lastItem = item;

            onReceive(id, item);

            long val = remainingCnt.decrementAndGet();

            if (val <= 0) {
                assert val == 0 : "remain=" + val
                        + ", id=" + id
                        + ", item=" + item
                        + ", threadId=" + Thread.currentThread().getName();

                onRequestCompleted(id);
            }
        }

        @Override
        public void onComplete() {
            if (finished.compareAndSet(false, true)) {
                onSubscriptionComplete(id);
            }
        }
    }

    private static boolean debug = false;

    private static void debug(String msg) {
        if (debug) {
            System.out.println(Thread.currentThread().getId() + " " + msg);
        }
    }
}
