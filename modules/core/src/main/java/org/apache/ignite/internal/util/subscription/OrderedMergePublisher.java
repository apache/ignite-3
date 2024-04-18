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

package org.apache.ignite.internal.util.subscription;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.jetbrains.annotations.Nullable;

/**
 * Sorting composite publisher.
 *
 * <p>Merges multiple publishers using merge-sort algorithm.
 *
 * <p>Note: upstream publishers must be sources of sorted data.
 */
public class OrderedMergePublisher<T> implements Publisher<T> {
    /** Rows comparator. */
    private final Comparator<? super T> comp;

    /** Array of upstream publishers. */
    private final Publisher<? extends T>[] sources;

    /** Prefetch size. */
    private final int prefetch;

    /**
     * Constructor.
     *
     * @param comp Rows comparator.
     * @param prefetch Prefetch size.
     * @param sources List of upstream publishers.
     */
    public OrderedMergePublisher(
            Comparator<? super T> comp,
            int prefetch,
            Publisher<? extends T>... sources) {
        this.sources = sources;
        this.prefetch = prefetch;
        this.comp = comp;
    }

    /** {@inheritDoc} */
    @Override
    public void subscribe(Subscriber<? super T> downstream) {
        OrderedMergeSubscription<? super T> subscription = new OrderedMergeSubscription<>(downstream, comp, prefetch, sources.length);

        subscription.subscribe(sources);
        downstream.onSubscribe(subscription);
        subscription.drain();
    }

    /**
     * Sorting composite subscription.
     *
     * <p>Merges multiple ordered data streams into single ordered stream using merge-sort algorithm.
     */
    static final class OrderedMergeSubscription<T> implements Subscription {
        /** Marker object with means publisher is completed. */
        private static final Object DONE = new Object();

        final Subscriber<? super T> downstream;

        /** Counter to prevent concurrent execution of a critical section. */
        private final AtomicInteger guardCntr = new AtomicInteger();

        /** Subscribers. */
        private final OrderedMergeSubscriber<T>[] subscribers;

        /** Rows comparator. */
        private final Comparator<? super T> comp;

        /** Last received values. */
        private final Object[] values;

        /** Error. */
        @SuppressWarnings({"unused", "FieldMayBeFinal"})
        private ErrorChain errorChain;

        /** Cancelled flag. */
        @SuppressWarnings({"unused", "FieldMayBeFinal"})
        private boolean cancelled;

        /** Number of requested rows. */
        @SuppressWarnings({"unused", "FieldMayBeFinal"})
        private long requested;

        /** Number of emitted rows (guarded by {@link #guardCntr}). */
        private long emitted;

        static final VarHandle ERROR_CHAIN;

        static final VarHandle CANCELLED;

        static final VarHandle REQUESTED;

        static {
            Lookup lk = MethodHandles.lookup();

            try {
                ERROR_CHAIN = lk.findVarHandle(OrderedMergeSubscription.class, "errorChain", ErrorChain.class);
                CANCELLED = lk.findVarHandle(OrderedMergeSubscription.class, "cancelled", boolean.class);
                REQUESTED = lk.findVarHandle(OrderedMergeSubscription.class, "requested", long.class);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }

        /**
         * Constructor.
         *
         * @param downstream Downstream subscriber.
         * @param comp Rows comparator.
         * @param prefetch Prefetch size.
         * @param cnt Count of subscriptions.
         */
        OrderedMergeSubscription(Subscriber<? super T> downstream, Comparator<? super T> comp, int prefetch, int cnt) {
            this.downstream = downstream;
            this.comp = comp;
            this.subscribers = new OrderedMergeSubscriber[cnt];

            for (int i = 0; i < cnt; i++) {
                this.subscribers[i] = new OrderedMergeSubscriber<>(this, prefetch);
            }

            this.values = new Object[cnt];
        }

        void subscribe(Publisher<? extends T>[] sources) {
            for (int i = 0; i < sources.length; i++) {
                sources[i].subscribe(subscribers[i]);
            }
        }

        /** {@inheritDoc} */
        @Override
        public void request(long n) {
            for (; ; ) {
                long current = (long) REQUESTED.getAcquire(this);
                long next = current + n;

                if (next < 0L) {
                    next = Long.MAX_VALUE;
                }

                if (REQUESTED.compareAndSet(this, current, next)) {
                    break;
                }
            }

            drain();
        }

        /** {@inheritDoc} */
        @Override
        public void cancel() {
            if (CANCELLED.compareAndSet(this, false, true)) {
                for (OrderedMergeSubscriber<T> inner : subscribers) {
                    inner.cancel();
                }

                if (guardCntr.getAndIncrement() == 0) {
                    Arrays.fill(values, null);

                    for (OrderedMergeSubscriber<T> inner : subscribers) {
                        inner.queue.clear();
                    }
                }
            }
        }

        private void onInnerError(OrderedMergeSubscriber<T> sender, Throwable ex) {
            updateError(ex);

            sender.done = true;

            drain();
        }

        private void updateError(Throwable throwable) {
            for (; ; ) {
                ErrorChain current = (ErrorChain) ERROR_CHAIN.getAcquire(this);
                ErrorChain next = new ErrorChain(throwable, current);

                if (ERROR_CHAIN.compareAndSet(this, current, next)) {
                    break;
                }
            }
        }

        private void drain() {
            // Only one thread can pass below.
            if (guardCntr.getAndIncrement() != 0) {
                return;
            }

            // Frequently accessed fields.
            Subscriber<? super T> downstream = this.downstream;
            OrderedMergeSubscriber<T>[] subscribers = this.subscribers;
            int subsCnt = subscribers.length;
            Object[] values = this.values;
            long emitted = this.emitted;

            for (; ; ) {
                long requested = (long) REQUESTED.getAcquire(this);

                for (; ; ) {
                    if ((boolean) CANCELLED.getAcquire(this)) {
                        Arrays.fill(values, null);

                        for (OrderedMergeSubscriber<T> inner : subscribers) {
                            inner.queue.clear();
                        }

                        return;
                    }

                    int completed = 0;
                    boolean waitResponse = false;

                    for (int i = 0; i < subsCnt; i++) {
                        Object obj = values[i];

                        if (obj == DONE) {
                            completed++;
                        } else if (obj == null) {
                            boolean innerDone = subscribers[i].done;

                            obj = subscribers[i].queue.poll();

                            if (obj != null) {
                                values[i] = obj;
                            } else if (innerDone) {
                                values[i] = DONE;

                                completed++;
                            } else {
                                // Subscriber has not received a response yet.
                                waitResponse = true;

                                break;
                            }
                        }
                    }

                    if (completed == subsCnt) {
                        ErrorChain chain = (ErrorChain) ERROR_CHAIN.getAcquire(this);

                        if (chain == null) {
                            downstream.onComplete();
                        } else {
                            downstream.onError(chain.buildThrowable());
                        }

                        return;
                    }

                    if (waitResponse || emitted == requested) {
                        break;
                    }

                    T min = null;
                    int minIndex = -1;

                    for (int i = 0; i < values.length; i++) {
                        Object obj = values[i];

                        if (obj != DONE && (min == null || comp.compare(min, (T) obj) > 0)) {
                            min = (T) obj;
                            minIndex = i;
                        }
                    }

                    values[minIndex] = null;

                    downstream.onNext(min);

                    emitted++;
                    subscribers[minIndex].request(1);
                }

                this.emitted = emitted;

                // Retry if any other thread has incremented the counter.
                if (guardCntr.decrementAndGet() == 0) {
                    break;
                }
            }
        }

        /**
         * Merge sort subscriber.
         */
        static final class OrderedMergeSubscriber<T> extends AtomicReference<Subscription> implements Subscriber<T>, Subscription {
            /** Parent subscription. */
            private final OrderedMergeSubscription<T> parent;

            /** Prefetch size. */
            private final int prefetch;

            /** Number of requests to buffer. */
            private final int limit;

            /** Inner data buffer. */
            private final Queue<T> queue;

            /** Count of consumed requests. */
            private int consumed;

            /** Flag indicating that the subscription has completed. */
            private volatile boolean done;

            OrderedMergeSubscriber(OrderedMergeSubscription<T> parent, int prefetch) {
                assert prefetch > 0;

                this.parent = parent;
                this.prefetch = prefetch;
                this.limit = prefetch - (prefetch >> 2);
                this.queue = new ConcurrentLinkedQueue<>();
            }

            /** {@inheritDoc} */
            @Override
            public void onSubscribe(Subscription subscription) {
                if (compareAndSet(null, subscription)) {
                    subscription.request(prefetch);
                } else {
                    subscription.cancel();
                }
            }

            /** {@inheritDoc} */
            @Override
            public void onNext(T item) {
                queue.offer(item);

                parent.drain();
            }

            /** {@inheritDoc} */
            @Override
            public void onError(Throwable throwable) {
                parent.onInnerError(this, throwable);
            }

            /** {@inheritDoc} */
            @Override
            public void onComplete() {
                done = true;

                parent.drain();
            }

            /** {@inheritDoc} */
            @Override
            public void request(long n) {
                int c = consumed + 1;

                if (c == limit) {
                    consumed = 0;
                    Subscription subscription = get();

                    // If the subscription has not yet been cancelled - request upstream.
                    if (subscription != this) {
                        subscription.request(c);
                    }
                } else {
                    consumed = c;
                }
            }

            /** {@inheritDoc} */
            @Override
            public void cancel() {
                Subscription subscription = getAndSet(this);

                if (subscription != null && subscription != this) {
                    subscription.cancel();
                }
            }
        }
    }

    private static class ErrorChain {
        private final Throwable error;
        @Nullable
        private final ErrorChain next;

        private boolean built = false;

        private ErrorChain(Throwable error, @Nullable ErrorChain next) {
            this.error = error;
            this.next = next;
        }

        synchronized Throwable buildThrowable() {
            if (built) {
                // Already built, so error already contains all subsequent exceptions attached.
                return error;
            }

            ErrorChain chain = next;

            while (chain != null) {
                error.addSuppressed(chain.error);
                chain = chain.next;
            }

            built = true;

            return error;
        }
    }
}
