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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Sorting composite publisher.
 *
 * <p>Merges multiple concurrent ordered data streams into one.
 */
public class SortingCompositePublisher<T> extends CompositePublisher<T> {
    /** Rows comparator. */
    private final Comparator<T> comp;

    /** Prefetch size. */
    private final int prefetch;

    /**
     * Constructor.
     *
     * @param publishers List of upstream publishers.
     * @param comp Rows comparator.
     * @param prefetch Prefetch size.
     */
    public SortingCompositePublisher(Collection<? extends Publisher<T>> publishers, Comparator<T> comp, int prefetch) {
        super(publishers);

        this.comp = comp;
        this.prefetch = prefetch;
    }

    @Override
    public void subscribe(Subscriber<? super T> downstream) {
        subscribe(new OrderedMergeCompositeSubscription<>(downstream, comp, prefetch, publishers.size()), downstream);
    }

    /**
     * Sorting composite subscription.
     *
     * <p>Merges multiple concurrent ordered data streams into one.
     */
    public static class OrderedMergeCompositeSubscription<T> extends CompositePublisher.CompositeSubscription<T> {
        /** Marker to indicate completed subscription. */
        private static final Object DONE = new Object();

        /** Atomic updater for {@link #error} field. */
        private static final VarHandle ERROR;

        /** Atomic updater for {@link #cancelled} field. */
        private static final VarHandle CANCELLED;

        /** Atomic updater for {@link #requested} field. */
        private static final VarHandle REQUESTED;

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
        private Throwable error;

        /** Cancelled flag. */
        @SuppressWarnings({"unused", "FieldMayBeFinal"})
        private boolean cancelled;

        /** Number of requested rows. */
        @SuppressWarnings({"unused", "FieldMayBeFinal"})
        private long requested;

        /** Number of emitted rows (guarded by {@link #guardCntr}). */
        private long emitted;

        static {
            Lookup lk = MethodHandles.lookup();

            try {
                ERROR = lk.findVarHandle(OrderedMergeCompositeSubscription.class, "error", Throwable.class);
                CANCELLED = lk.findVarHandle(OrderedMergeCompositeSubscription.class, "cancelled", boolean.class);
                REQUESTED = lk.findVarHandle(OrderedMergeCompositeSubscription.class, "requested", long.class);
            } catch (NoSuchFieldException | IllegalAccessException ex) {
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
        public OrderedMergeCompositeSubscription(Subscriber<? super T> downstream, Comparator<? super T> comp, int prefetch, int cnt) {
            super(downstream, cnt);

            this.comp = comp;
            this.subscribers = new OrderedMergeSubscriber[cnt];

            for (int i = 0; i < cnt; i++) {
                this.subscribers[i] = new OrderedMergeSubscriber<>(this, prefetch);
            }

            this.values = new Object[cnt];
        }

        @Override
        public void subscribe(Collection<? extends Publisher<? extends T>> sources) {
            int i = 0;

            for (Publisher<? extends T> publisher : sources) {
                publisher.subscribe(subscribers[i++]);
            }
        }

        @Override
        public void request(long n) {
            for (;;) {
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
            for (;;) {
                Throwable current = (Throwable) ERROR.getAcquire(this);
                Throwable next;

                if (current == null) {
                    next = throwable;
                } else {
                    next = new Throwable();
                    next.addSuppressed(current);
                    next.addSuppressed(throwable);
                }

                if (ERROR.compareAndSet(this, current, next)) {
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

            for (;;) {
                long requested = (long) REQUESTED.getAcquire(this);

                for (;;) {
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
                        Throwable ex = (Throwable) ERROR.getAcquire(this);

                        if (ex == null) {
                            downstream.onComplete();
                        } else {
                            downstream.onError(ex);
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
        private static final class OrderedMergeSubscriber<T> extends AtomicReference<Subscription> implements Subscriber<T>, Subscription {
            /** Parent subscription. */
            private final OrderedMergeCompositeSubscription<T> parent;

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

            OrderedMergeSubscriber(OrderedMergeCompositeSubscription<T> parent, int prefetch) {
                this.parent = parent;
                this.prefetch = Math.max(1, prefetch);
                this.limit = this.prefetch - (this.prefetch >> 2);
                this.queue = new ConcurrentLinkedQueue<>();
            }

            @Override
            public void onSubscribe(Subscription s) {
                if (compareAndSet(null, s)) {
                    s.request(prefetch);
                } else {
                    s.cancel();
                }
            }

            @Override
            public void onNext(T item) {
                queue.offer(item);

                parent.drain();
            }

            @Override
            public void onError(Throwable throwable) {
                parent.onInnerError(this, throwable);
            }

            @Override
            public void onComplete() {
                done = true;

                parent.drain();
            }

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

            @Override
            public void cancel() {
                Subscription subscription = getAndSet(this);

                if (subscription != null && subscription != this) {
                    subscription.cancel();
                }
            }
        }
    }
}
