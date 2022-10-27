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
 * <br>
 * Merges ordered streams.
 */
public class MergeSortSubscriptionStrategy<T> extends AbstractCompositeSubscriptionStrategy<T> {
    /** Items comparator. */
    private final Comparator<T> comp;

    /** Internal ordered buffer. */
    private final PriorityBlockingQueue<T> inBuf;

    /** List of subscribers. */
    private final List<MergeSortStrategySubscriber> subscribers = new ArrayList<>();

    /** Identifiers of completed subscriptions. */
    private final ConcurrentHashSet<Integer> finished = new ConcurrentHashSet<>();

    /** Number of completed subscriptions. */
    private final AtomicInteger finishedCnt = new AtomicInteger();

    /** Composite subscription completed flag. */
    private final AtomicBoolean completed = new AtomicBoolean();

    /** Number of remaining items. */
    private long remain = 0;

    /** Number of requested items. */
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
    MergeSortSubscriptionStrategy(Comparator<T> comp, Subscriber<? super T> delegate) {
        super(delegate);

        this.comp = comp;
        this.inBuf = new PriorityBlockingQueue<>(1, comp);
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
        if (!inBuf.isEmpty()) {
            if (finished.size() == subscriptions.size()) { // all data has been received?
                if (pushData(n, null, null) == 0) {
                    return;
                }
            } else { // Someone still alive.
                processReceivedData();

                return;
            }
        }

        List<Integer> subsIds = IntStream.range(0, subscriptions.size()).boxed().collect(Collectors.toList());

        requestNext(subsIds, n);
    }

    /** {@inheritDoc} */
    @Override
    public void onReceive(int subscriberId, T item) {
        inBuf.offer(item);
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

    /** {@inheritDoc} */
    @Override
    public synchronized void onSubscriptionComplete(int subscribeId) {
        /*
         * Synchronized is needed because "request" can be executed in parallel with "onComplete".
         * For example (supplier has only one item left):
         *      user-thread: request(1)
         *  supplier-thread: subscriber -> onNext() -> return result to user
         *
         *  supplier-thread: onComplete -\
         *                                |-------> can be executed in parallel
         *      user-thread: request(1) -/
         */
        if (finished.add(subscribeId) && finishedCnt.incrementAndGet() == subscriptions.size() && (remain > 0 || inBuf.isEmpty())) {
            waitResponse.remove(subscribeId);

            if (completed.compareAndSet(false, true)) {
                pushData(remain, null, null);
            }

            // all work done
            return;
        }

        onRequestCompleted(subscribeId);
    }

    /**
     * Called when single subscription request is completed.
     *
     * @param subscriberId Subscriber ID.
     */
    private void onRequestCompleted(int subscriberId) {
        if (waitResponse.remove(subscriberId) && waitResponseCnt.decrementAndGet() == 0) {
            processReceivedData();
        }
    }

    /** {@inheritDoc} */
    @Override
    public Subscriber<T> subscriberProxy(int subscriberId) {
        MergeSortStrategySubscriber subscriber = new MergeSortStrategySubscriber(subscriberId);

        subscribers.add(subscriber);

        return subscriber;
    }

    /**
     * Estimate amount of data for each of the registered subscriptions.
     *
     * @param total Total number of requested items.
     * @return Estimated amount of data for each of the registered subscriptions.
     */
    private long estimateSingleSubscriptionRequestAmount(long total) {
        return Math.max(1, total / (subscriptions.size() - finished.size()));
    }

    /**
     * Push available internal data to the delegated subscriber.
     *
     * @param cnt Maximum number of items to push.
     * @param comp Items comparator.
     * @param minBound Minimum bound up to which we can return data.
     * @return Number of remaining items.
     */
    private long pushData(long cnt, @Nullable Comparator<T> comp, @Nullable T minBound) {
        boolean done = false;
        T r;

        while (cnt > 0 && (r = inBuf.peek()) != null) {
            int cmpRes = comp == null ? 0 : comp.compare(minBound, r);

            if (cmpRes < 0) {
                return cnt;
            }

            boolean same = comp != null && cmpRes == 0;

            if (!done && same) {
                done = true;
            }

            if (!done || same) {
                T r0 = inBuf.poll();

                assert r == r0;

                delegate.onNext(r);

                --cnt;
            }

            if (done && !same) {
                break;
            }
        }

        if (comp == null && inBuf.isEmpty()) {
            delegate.onComplete();
        }

        return cnt;
    }

    private synchronized void processReceivedData() {
        Pair<T, List<Integer>> minItemAndIds = chooseRequestedSubscriptionsIds();
        T minItem = minItemAndIds.left;
        List<Integer> subsIds = minItemAndIds.right;

        if (minItem == null) {
            return;
        }

        remain = pushData(remain, comp, minItem);

        if (remain > 0) {
            requestNext(subsIds, requested);
        }
    }

    private void requestNext(List<Integer> subsIds, long cnt) {
        long dataAmount = estimateSingleSubscriptionRequestAmount(cnt);

        for (Integer id : subsIds) {
            if (finished.contains(id)) {
                continue;
            }

            boolean added = waitResponse.add(id);

            assert added : "concurrent request call [id=" + id + ']';

            waitResponseCnt.incrementAndGet();
        }

        for (Integer id : waitResponse) {
            MergeSortStrategySubscriber subscriber = subscribers.get(id);

            long val = subscriber.remainCntr.compareAndExchange(0, dataAmount);

            assert val == 0 : "request busy subscription [id=" + id + ", remain=" + val + ']';

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
        /** The counter of the remaining number of elements. */
        private final AtomicLong remainCntr = new AtomicLong();

        /** Last received item. */
        private volatile T lastItem;

        MergeSortStrategySubscriber(int id) {
            super(id);
        }

        /** {@inheritDoc} */
        @Override
        public void onNext(T item) {
            lastItem = item;

            onReceive(id, item);

            long val = remainCntr.decrementAndGet();

            if (val <= 0) {
                assert val == 0 : "remain=" + val + ", id=" + id + ", item=" + item;

                onRequestCompleted(id);
            }
        }
    }
}
