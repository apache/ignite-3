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
import java.util.Iterator;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Thread-safe implementation of combining multiple publishers.
 * Starts to consume the next source only after the previous has been completed.
 */
public class ConcatenatedPublisher<T> implements Publisher<T> {
    /** Iterator of upstream publishers. */
    private final Iterator<Publisher<? extends T>> sources;

    /**
     * Constructor.
     *
     * @param sources Iterator of upstream publishers.
     */
    public ConcatenatedPublisher(Iterator<Publisher<? extends T>> sources) {
        this.sources = sources;
    }

    /** {@inheritDoc} */
    @Override
    public void subscribe(Subscriber<? super T> downstream) {
        ConcatenatedSubscriber<T> subscription = new ConcatenatedSubscriber<>(downstream, sources);

        downstream.onSubscribe(subscription);
        subscription.drain();
    }

    /**
     * Sequentially processes a list of subscriptions.
     *
     * <p>Subscribes to the next subscription only after the previous subscription completes.
     */
    static final class ConcatenatedSubscriber<T> extends ConcatenatedSubscription implements Subscriber<T> {
        /** Counter to prevent concurrent execution of a critical section. */
        private final AtomicInteger guardCntr = new AtomicInteger();

        /** Downstream {@link Subscriber} that receives the signals. */
        private final Subscriber<? super T> downstream;

        /** List of {@link Publisher publishers} to be consumed one by one. */
        private final Iterator<Publisher<? extends T>> sources;

        /**
         * Tracks how many items the current source has produced (and has the coordinator consumed). It is used to update the arbiter
         * at the completion of the current source instead of doing it for each item received.
         */
        private long consumed;

        ConcatenatedSubscriber(
                Subscriber<? super T> downstream,
                Iterator<Publisher<? extends T>> sources
        ) {
            this.downstream = downstream;
            this.sources = sources;
        }

        /** {@inheritDoc} */
        @Override
        public void onSubscribe(Subscription s) {
            setSubscription(s);
        }

        /** {@inheritDoc} */
        @Override
        public void onNext(T item) {
            consumed++;

            downstream.onNext(item);
        }

        /** {@inheritDoc} */
        @Override
        public void onError(Throwable throwable) {
            downstream.onError(throwable);
        }

        /** {@inheritDoc} */
        @Override
        public void onComplete() {
            drain();
        }

        void drain() {
            // Ensures only one thread is busy setting up the consumption of the next (or the previous) source.
            if (guardCntr.getAndIncrement() != 0) {
                return;
            }

            do {
                if (isCancelled()) {
                    return;
                }

                if (!sources.hasNext()) {
                    downstream.onComplete();
                    return;
                }

                // Indicate to the arbiter the number of items consumed from the previous source so it can update its outstanding (current)
                // request amount. Note that "consumed" is not concurrently updated because onNext() and onComplete() (and thus drain())
                // on the same source can't be executed concurrently.
                long c = consumed;

                if (c != 0L) {
                    consumed = 0L;
                    setProduced(c);
                }

                sources.next().subscribe(this);
            } while (guardCntr.decrementAndGet() != 0); // Resume with the subsequent sources if there was synchronous or racy onComplete().
        }
    }

    /**
     * Concatenation composite subscription.
     */
    private static class ConcatenatedSubscription implements Subscription {
        /** Used to update {@link #current} with proper visibility. */
        private static final VarHandle CURRENT;

        /** Used to update {@link #next} with proper visibility. */
        private static final VarHandle NEXT;

        /** Used to update {@link #downstreamRequested} with proper visibility. */
        private static final VarHandle DOWNSTREAM_REQUESTED;

        /** Used to update {@link #produced} with proper visibility. */
        private static final VarHandle PRODUCED;

        /** Used to update {@link #wip} with proper visibility. */
        private static final VarHandle WIP;

        static {
            Lookup lk = MethodHandles.lookup();

            try {
                CURRENT = lk.findVarHandle(ConcatenatedSubscription.class, "current", Subscription.class);
                NEXT = lk.findVarHandle(ConcatenatedSubscription.class, "next", Subscription.class);
                DOWNSTREAM_REQUESTED = lk.findVarHandle(ConcatenatedSubscription.class, "downstreamRequested", long.class);
                PRODUCED = lk.findVarHandle(ConcatenatedSubscription.class, "produced", long.class);
                WIP = lk.findVarHandle(ConcatenatedSubscription.class, "wip", int.class);
            } catch (NoSuchFieldException | IllegalAccessException ex) {
                throw new InternalError(ex);
            }
        }

        /**
         * Holds the current {@link Subscription}.
         *
         * <p>Updated with the {@link #CURRENT}.
         */
        @SuppressWarnings({"unused"})
        private Subscription current;

        /**
         * Temporarily holds the next {@link Subscription} to replace current instance.
         * Direct replacement can't work due to the required request accounting.
         *
         * <p>Updated with the {@link #NEXT}.
         */
        @SuppressWarnings({"unused"})
        private Subscription next;

        /**
         * Holds the current outstanding request count.
         * It doesn't have any VarHandle because it will be only accessed from within a drain-loop.
         */
        private long requested;

        /**
         * Accumulates the downstream requests in case there the drain loop is executing.
         *
         * <p>Updated with the {@link #DOWNSTREAM_REQUESTED}.
         */
        @SuppressWarnings({"unused"})
        private long downstreamRequested;

        /**
         * Holds the number of items produced by the previous source which has to be deduced from requested before switching to the next
         * source happens.
         *
         * <p>Updated with the {@link #PRODUCED}.
         */
        @SuppressWarnings({"unused"})
        private long produced;

        /**
         * Protects the drain loop from being called in parallel.
         *
         * <p>Updated with the {@link #WIP}.
         */
        @SuppressWarnings({"unused"})
        private int wip;

        /** {@inheritDoc} */
        @Override
        public final void request(long n) {
            // Call the arbiter drain loop only if the atomic increment succeeds.
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

        /**
         * {@inheritDoc}
         *
         * <p>Atomically swap in both the {@link #current} and the {@link #next} subscription instances
         * with the cancelled indicator of {@code this}.
         * To support some eagerness in cancellation, the {@link #isCancelled()} can be called by
         * the subclass (i.e., concat an array of Flow.Publishers) to quit its trampolined looping.
         */
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

        final boolean isCancelled() {
            return CURRENT.getAcquire(this) == this;
        }

        /**
         * Sets next subscription.
         *
         * <p>It is expected that there will be only one thread calling this method and that call happens before the termination
         * of the associated source. A failed CAS can only mean that the arbiter has been canceled in the meantime, and therefore
         * the subscription needs to be cancelled. But you still need to relay the unfulfilled request amount to this new subscription,
         * which will be done in {@link #arbiterDrain()}.
         *
         * @param subscription New subscription.
         */
        final void setSubscription(Subscription subscription) {
            if (NEXT.compareAndSet(this, null, subscription)) {
                arbiterDrain();
            } else {
                subscription.cancel();
            }
        }

        /**
         * Atomically updates the number of items produced by the previous source
         * which has to be deduced from requested before switching to the next source happens.
         *
         * @param n Number of items.
         */
        final void setProduced(long n) {
            PRODUCED.setRelease(this, n);
            arbiterDrain();
        }

        private void arbiterDrain() {
            if ((int) WIP.getAndAdd(this, 1) != 0) {
                // Another thread already in the loop.
                return;
            }

            Subscription requestFrom;
            long requestAmount = 0L;

            for (; ; ) {
                Subscription currentSub = (Subscription) CURRENT.getAcquire(this);

                // Subscription has been cancelled.
                if (currentSub == this) {
                    return;
                }

                // Read out the current and queued state: the current outstanding requested amount, the request amount from
                // the downstream if any, the produced item count by the previous source and the potential next Flow.Subscription instance.
                long req = requested;
                long downstreamReq = (long) DOWNSTREAM_REQUESTED.getAndSet(this, 0L);
                long prod = (long) PRODUCED.getAndSet(this, 0L);

                // Swapping to null unconditionally may overwrite the cancelled indicator
                // and the setSubscription() won't cancel its argument.
                Subscription nextSub = (Subscription) NEXT.getAcquire(this);
                if (nextSub != null && nextSub != this) {
                    NEXT.compareAndSet(this, nextSub, null);
                }

                // If there was an asynchronous request() call, we need to add the downstreamReq amount to the current requested amount,
                // capped at Long.MAX_VALUE (unbounded indicator).
                if (downstreamReq != 0L) {
                    req += downstreamReq;

                    if (req < 0L) {
                        req = Long.MAX_VALUE;
                    }
                }

                // If there was a produced amount by the previous source, we need to subtract it from the requested.
                if (prod != 0L && req != Long.MAX_VALUE) {
                    req -= prod;
                }

                requested = req;

                // If there was a new Flow.Subscription set via setSubscription(), we need to indicate where to request from outside
                // the loop and we indicate the whole current requested amount should be used (now including any async downstream request
                // and upstream produced count). The CAS will make sure the next Flow.Subscription only becomes the current one if there
                // was no cancellation in the meantime.
                if (nextSub != null && nextSub != this) {
                    requestFrom = nextSub;
                    requestAmount = req;
                    CURRENT.compareAndSet(this, currentSub, nextSub);
                } else {
                    // Otherwise, we target the current Flow.Subscription, add up the downstream extra requests capped at Long.MAX_VALUE.
                    // The reason for this is that the downstream may issue multiple requests (r1, r2) in a quick succession which makes
                    // the logic to loop back again, now having r1 + r2 items outstanding from the downstream perspective.
                    requestFrom = currentSub;
                    requestAmount += downstreamReq;
                    if (requestAmount < 0L) {
                        requestAmount = Long.MAX_VALUE;
                    }
                }

                // Retry the loop if another thread has requested.
                if ((int) WIP.getAndAdd(this, -1) - 1 == 0) {
                    break;
                }
            }

            if (requestFrom != null && requestAmount != 0L) {
                // It is possible and not a problem if the current subscription is outdated or the arbiter has been canceled at that time.
                requestFrom.request(requestAmount);
            }
        }
    }
}

