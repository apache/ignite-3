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
import java.util.Iterator;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Thread-safe realization of combine multiple publishers. Generally, start consuming a source once the previous source has terminated,
 * building a chain.
 */
public class ConcatPublisher<T> implements Publisher<T> {
    /** Iterator of upstream publishers. */
    private final Iterator<Publisher<? extends T>> sources;

    /**
     * Constructor.
     *
     * @param sources Iterator of upstream publishers.
     */
    public ConcatPublisher(Iterator<Publisher<? extends T>> sources) {
        this.sources = sources;
    }

    /** {@inheritDoc} */
    @Override
    public void subscribe(Subscriber<? super T> downstream) {
        ConcatCoordinator<T> subscription = new ConcatCoordinator<>(downstream, sources);

        downstream.onSubscribe(subscription);
        subscription.drain();
    }

    /**
     * Concatenation composite subscription.
     */
    static final class ConcatCoordinator<T> extends SubscriptionArbiter implements Subscriber<T> {
        /** Counter to prevent concurrent execution of a critical section. */
        private final AtomicInteger guardCntr = new AtomicInteger();

        final Subscriber downstream;

        final Iterator<Publisher<? extends T>> sources;

        long consumed;

        ConcatCoordinator(
                Subscriber downstream,
                Iterator<Publisher<? extends T>> sources
        ) {
            this.downstream = downstream;
            this.sources = sources;
        }

        @Override
        public void onSubscribe(Subscription s) {
            setSubscription(s);
        }

        @Override
        public void onNext(T item) {
            consumed++;
            downstream.onNext(item);
        }

        @Override
        public void onError(Throwable throwable) {
            downstream.onError(throwable);
        }

        @Override
        public void onComplete() {
            drain();
        }

        void drain() {
            if (guardCntr.getAndIncrement() == 0) {
                do {
                    if (isCancelled()) {
                        return;
                    }

                    if (!sources.hasNext()) {
                        downstream.onComplete();
                        return;
                    }

                    long c = consumed;
                    if (c != 0L) {
                        consumed = 0L;
                        setProduced(c);
                    }

                    sources.next().subscribe(this);

                } while ((guardCntr.getAndDecrement() - 1) != 0);
            }
        }
    }

    static class SubscriptionArbiter implements Subscription {
        Subscription current;
        static final VarHandle CURRENT;

        Subscription next;
        static final VarHandle NEXT;

        long requested;

        long downstreamRequested;
        static final VarHandle DOWNSTREAM_REQUESTED;

        long produced;
        static final VarHandle PRODUCED;

        int wip;
        static final VarHandle WIP;

        static {
            Lookup lk = MethodHandles.lookup();

            try {
                CURRENT = lk.findVarHandle(SubscriptionArbiter.class, "current", Subscription.class);
                NEXT = lk.findVarHandle(SubscriptionArbiter.class, "next", Subscription.class);
                DOWNSTREAM_REQUESTED = lk.findVarHandle(SubscriptionArbiter.class, "downstreamRequested", long.class);
                PRODUCED = lk.findVarHandle(SubscriptionArbiter.class, "produced", long.class);
                WIP = lk.findVarHandle(SubscriptionArbiter.class, "wip", int.class);

            } catch (NoSuchFieldException | IllegalAccessException ex) {
                throw new InternalError(ex);
            }
        }


        @Override
        public final void request(long n) {
            for (; ; ) {
                long r = (long) DOWNSTREAM_REQUESTED.getAcquire(this);
                long u = r + n;
                if (u < 0L) {
                    u = Long.MAX_VALUE;
                }
                if (DOWNSTREAM_REQUESTED.compareAndSet(this, r, u)) {
                    arbiterDrain();
                    break;
                }
            }
        }

        @Override
        public void cancel() {
            Subscription s = (Subscription) CURRENT.getAndSet(this, this);
            if (s != null && s != this) {
                s.cancel();
            }
            s = (Subscription) NEXT.getAndSet(this, this);
            if (s != null && s != this) {
                s.cancel();
            }
        }

        public final boolean isCancelled() {
            return CURRENT.getAcquire(this) == this;
        }

        public final void setSubscription(Subscription subscription) {
            if (NEXT.compareAndSet(this, null, subscription)) {
                arbiterDrain();
            } else {
                subscription.cancel();
            }
        }

        public final void setProduced(long n) {
            PRODUCED.setRelease(this, n);
            arbiterDrain();
        }

        final void arbiterDrain() {
            if ((int) WIP.getAndAdd(this, 1) != 0) {
                return;
            }

            Subscription requestFrom = null;
            long requestAmount = 0L;

            for (; ; ) {
                Subscription currentSub = (Subscription) CURRENT.getAcquire(this);
                if (currentSub != this) {

                    long req = requested;

                    long downstreamReq = (long) DOWNSTREAM_REQUESTED.getAndSet(this, 0L);

                    long prod = (long) PRODUCED.getAndSet(this, 0L);

                    Subscription nextSub = (Subscription) NEXT.getAcquire(this); // null
                    if (nextSub != null && nextSub != this) {
                        NEXT.compareAndSet(this, nextSub, null);
                    }

                    if (downstreamReq != 0L) {
                        req += downstreamReq;
                        if (req < 0L) {
                            req = Long.MAX_VALUE;
                        }
                    }

                    if (prod != 0L && req != Long.MAX_VALUE) {
                        req -= prod;
                    }

                    requested = req;

                    if (nextSub != null && nextSub != this) {
                        requestFrom = nextSub;
                        requestAmount = req;
                        CURRENT.compareAndSet(this, currentSub, nextSub);
                    } else {
                        // (6) --------------------------------------
                        requestFrom = currentSub;
                        requestAmount += downstreamReq;
                        if (requestAmount < 0L) {
                            requestAmount = Long.MAX_VALUE;
                        }
                    }
                }

                if ((int) WIP.getAndAdd(this, -1) - 1 == 0) {
                    break;
                }
            }

            if (requestFrom != null && requestFrom != this
                    && requestAmount != 0L) {
                requestFrom.request(requestAmount);
            }
        }
    }
}

