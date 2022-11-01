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
 * Sorting composite subscription.
 * <br>
 * Merges multiple concurrent sorted data streams into one.
 */
public class MergeSortCompositeSubscription<T> extends AtomicInteger implements CompositeSubscription<T> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    private final Subscriber<? super T> downstream;

    private final MergeSortSubscriber<T>[] subscribers;

    private final Comparator<? super T> comp;

    private final Object[] values;

    private static final Object DONE = new Object();

    private Throwable error;

    private boolean cancelled;

    private long requested;

    private long emitted;

    private static final VarHandle ERROR;

    private static final VarHandle CANCELLED;

    private static final VarHandle REQUESTED;

    static {
        Lookup lk = MethodHandles.lookup();
        try {
            ERROR = lk.findVarHandle(MergeSortCompositeSubscription.class, "error", Throwable.class);
            CANCELLED = lk.findVarHandle(MergeSortCompositeSubscription.class, "cancelled", boolean.class);
            REQUESTED = lk.findVarHandle(MergeSortCompositeSubscription.class, "requested", long.class);
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
    public MergeSortCompositeSubscription(Subscriber<? super T> downstream, Comparator<? super T> comp, int prefetch, int cnt) {
        this.downstream = downstream;
        this.comp = comp;
        this.subscribers = new MergeSortSubscriber[cnt];

        for (int i = 0; i < cnt; i++) {
            this.subscribers[i] = new MergeSortSubscriber<>(this, prefetch);
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
            for (MergeSortSubscriber<T> inner : subscribers) {
                inner.cancel();
            }

            if (getAndIncrement() == 0) {
                Arrays.fill(values, null);

                for (MergeSortSubscriber<T> inner : subscribers) {
                    inner.queue.clear();
                }
            }
        }
    }

    private void onInnerError(MergeSortSubscriber<T> sender, Throwable ex) {
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
        if (getAndIncrement() != 0) {
            return;
        }

        int missed = 1;
        Subscriber<? super T> downstream = this.downstream;

        Comparator<? super T> comparator = this.comp;

        MergeSortSubscriber<T>[] subscribers = this.subscribers;
        int n = subscribers.length;

        Object[] values = this.values;

        long e = emitted;

        for (;;) {
            long r = (long) REQUESTED.getAcquire(this);

            for (;;) {
                if ((boolean) CANCELLED.getAcquire(this)) {
                    Arrays.fill(values, null);

                    for (MergeSortSubscriber<T> inner : subscribers) {
                        inner.queue.clear();
                    }
                    return;
                }

                int done = 0;
                int nonEmpty = 0;

                for (int i = 0; i < n; i++) {
                    Object o = values[i];
                    if (o == DONE) {
                        done++;
                        nonEmpty++;
                    } else if (o == null) {
                        boolean innerDone = subscribers[i].done;
                        o = subscribers[i].queue.poll();

                        if (o != null) {
                            values[i] = o;
                            nonEmpty++;
                        } else if (innerDone) {
                            values[i] = DONE;
                            done++;
                            nonEmpty++;
                        }
                    } else {
                        nonEmpty++;
                    }
                }

                if (done == n) {
                    Throwable ex = (Throwable) ERROR.getAcquire(this);

                    if (ex == null) {
                        downstream.onComplete();
                    } else {
                        downstream.onError(ex);
                    }

                    return;
                }

                if (nonEmpty != n || e == r) {
                    break;
                }

                T min = null;
                int minIndex = -1;
                int i = 0;

                for (Object o : values) {
                    if (o != DONE) {
                        if (min == null || comparator.compare(min, (T) o) > 0) {
                            min = (T) o;
                            minIndex = i;
                        }
                    }
                    i++;
                }

                values[minIndex] = null;

                downstream.onNext(min);

                e++;
                subscribers[minIndex].request(1);
            }

            emitted = e;
            missed = addAndGet(-missed);
            if (missed == 0) {
                break;
            }
        }
    }

    /**
     * Merge sort subscriber.
     */
    public static final class MergeSortSubscriber<T> extends AtomicReference<Subscription> implements Subscriber<T>, Subscription {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        private final MergeSortCompositeSubscription<T> parent;

        private final int prefetch;

        private final int limit;

        private final Queue<T> queue;

        private int consumed;

        private volatile boolean done;

        MergeSortSubscriber(MergeSortCompositeSubscription<T> parent, int prefetch) {
            this.parent = parent;
            this.prefetch = prefetch;
            this.limit = prefetch - (prefetch >> 2);
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
                Subscription s = get();
                if (s != this) {
                    s.request(c);
                }
            } else {
                consumed = c;
            }
        }

        @Override
        public void cancel() {
            Subscription s = getAndSet(this);
            if (s != null && s != this) {
                s.cancel();
            }
        }
    }
}
