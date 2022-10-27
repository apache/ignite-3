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
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.util.Pair;
import org.apache.ignite.raft.jraft.util.concurrent.ConcurrentHashSet;
import org.jetbrains.annotations.Nullable;

/**
 * Composite publisher.
 */
public class CompositePublisher<T> implements Flow.Publisher<T> {
    /** List of registered publishers. */
    private final Collection<? extends Publisher<T>> publishers;

    /** Flag indicating the state of the subscription. */
    private final AtomicBoolean subscribed = new AtomicBoolean();

    /** Items comparator. */
    private final Comparator<T> comp;

    public CompositePublisher(Collection<? extends Publisher<T>> publishers, @Nullable Comparator<T> comp) {
        this.publishers = publishers;
        this.comp = comp;
    }

    /** {@inheritDoc} */
    @Override
    public void subscribe(Subscriber<? super T> delegate) {
        if (!subscribed.compareAndSet(false, true)) {
            throw new IllegalStateException("Multiple subscribers are not supported.");
        }

        AbstractCompositeSubscriptionStrategy<T> subscriptionStrategy = comp != null
                ? new MergeSortSubscriptionStrategy<>(comp, delegate)
                : new SequentialSubscriptionStrategy<>(delegate);

        int subscriberIdx = 0;

        for (Publisher<T> publisher : publishers) {
            publisher.subscribe(subscriptionStrategy.subscriberProxy(subscriberIdx++));
        }

        // Subscribe delegated (target) subscriber to composite subscription.
        delegate.onSubscribe(subscriptionStrategy);
    }

    /**
     * Composite subscription strategy template.
     */
    protected abstract static class AbstractCompositeSubscriptionStrategy<T> implements Subscription {
        /** List of subscriptions. */
        final List<Subscription> subscriptions = new ArrayList<>();

        /** Delegated subscriber. */
        final Subscriber<? super T> delegate;

        AbstractCompositeSubscriptionStrategy(Subscriber<? super T> delegate) {
            this.delegate = delegate;
        }

        /**
         * Add new subscription.
         *
         * @param subscription Subscription.
         */
        void addSubscription(Subscription subscription) {
            subscriptions.add(subscription);
        }

        /**
         * Create a new subscriber proxy to receive data from the subscription.
         *
         * @param subscriberId Subscriber ID.
         * @return Subscriber proxy.
         */
        public Subscriber<T> subscriberProxy(int subscriberId) {
            return new PlainSubscriberProxy(subscriberId);
        }

        /**
         * Called when one of the subscriptions is completed.
         *
         * @param subscribeId Subscription ID.
         */
        public abstract void onSubscriptionComplete(int subscribeId);

        /**
         * Called when the subscriber receives a new item.
         *
         * @param subscriberId Subscriber ID.
         * @param item Item.
         */
        public abstract void onReceive(int subscriberId, T item);

        /**
         * Plain subscriber.
         */
        protected class PlainSubscriberProxy implements Subscriber<T> {
            /** Subscriber ID. */
            protected final int id;

            PlainSubscriberProxy(int id) {
                this.id = id;
            }

            /** {@inheritDoc} */
            @Override
            public void onSubscribe(Subscription subscription) {
                addSubscription(subscription);
            }

            /** {@inheritDoc} */
            @Override
            public void onNext(T item) {
                onReceive(id, item);
            }

            /** {@inheritDoc} */
            @Override
            public void onError(Throwable throwable) {
                cancel();

                delegate.onError(throwable);
            }

            /** {@inheritDoc} */
            @Override
            public void onComplete() {
                onSubscriptionComplete(id);
            }
        }
    }

    /**
     * Sequential subscription strategy.
     * <br>
     * Sequentially receives data from each registered subscription
     * until the total number of requested items has been received.
     */
    public static class SequentialSubscriptionStrategy<T> extends AbstractCompositeSubscriptionStrategy<T> {
        /** Current subscription index. */
        int subscriptionIdx = 0;

        /** Total number of remaining items. */
        private long remaining;

        SequentialSubscriptionStrategy(Subscriber<? super T> delegate) {
            super(delegate);
        }

        /** {@inheritDoc} */
        @Override
        public void onReceive(int subscriberId, T item) {
            --remaining;

            delegate.onNext(item);
        }

        /** {@inheritDoc} */
        @Override
        public void onSubscriptionComplete(int subscribeId) {
            if (++subscriptionIdx == subscriptions.size()) {
                delegate.onComplete();

                return;
            }

            if (remaining > 0) {
                requestInternal();
            }
        }

        /** {@inheritDoc} */
        @Override
        public void request(long n) {
            remaining = n;

            requestInternal();
        }

        /** {@inheritDoc} */
        @Override
        public void cancel() {
            activeSubscription().cancel();
        }

        /** Request data from a subscription. */
        private void requestInternal() {
            activeSubscription().request(remaining);
        }

        private Subscription activeSubscription() {
            return subscriptions.get(subscriptionIdx);
        }
    }

    /**
     * Merge sort subscription strategy.
     * <br>
     * Merges multiple concurrent sorted data streams into one.
     */
    private static class MergeSortSubscriptionStrategy<T> extends AbstractCompositeSubscriptionStrategy<T> {
        /** Items comparator. */
        private final Comparator<T> comp;

        /** Internal ordered buffer. */
        private final PriorityBlockingQueue<T> inBuf;

        /** List of subscribers. */
        private final List<MergeSortSubscriberProxy> subscribers = new ArrayList<>();

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
                if (finished.size() == subscriptions.size()) { // all possible data has been received?
                    pushData(n);
                } else { // Someone still alive.
                    processReceivedData();
                }

                return;
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
            if (finished.add(subscribeId) && finishedCnt.incrementAndGet() == subscribers.size()) {
                // It could be a completely dummy request (no data),in which case
                // the wait-set must be also updated.
                waitResponse.remove(subscribeId);

                if (completed.compareAndSet(false, true)) {
                    pushData(remain);
                }

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
            MergeSortSubscriberProxy subscriber = new MergeSortSubscriberProxy(subscriberId);

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
         */
        private void pushData(long cnt) {
            T item;

            while (cnt-- > 0 && (item = inBuf.poll()) != null) {
                delegate.onNext(item);
            }

            if (inBuf.isEmpty()) {
                delegate.onComplete();
            }
        }

        private synchronized void processReceivedData() {
            Pair<T, List<Integer>> minItemAndIds = chooseRequestedSubscriptionsIds();
            T minItem = minItemAndIds.left;
            List<Integer> subsIds = minItemAndIds.right;

            if (minItem == null) {
                return;
            }

            T item;
            // Pass the data from the internal buffer to the delegate.
            while (remain > 0 && (item = inBuf.peek()) != null && comp.compare(minItem, item) >= 0) {
                T item0 = inBuf.poll();

                assert item == item0;

                delegate.onNext(item);

                --remain;
            }

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
                MergeSortSubscriberProxy subscriber = subscribers.get(id);

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

                MergeSortSubscriberProxy subscriber = subscribers.get(i);

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
        public class MergeSortSubscriberProxy extends AbstractCompositeSubscriptionStrategy<T>.PlainSubscriberProxy {
            /** The counter of the remaining number of elements. */
            private final AtomicLong remainCntr = new AtomicLong();

            /** Last received item. */
            private volatile T lastItem;

            MergeSortSubscriberProxy(int id) {
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
}
