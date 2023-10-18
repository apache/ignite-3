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
import java.util.concurrent.Executor;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;

/**
 * An adapter that issues a new iterator for every subscription and drains that iterator on a given
 * executor, emitting up to given {@code batchSize} entries at time. The drain task will be re-scheduled
 * until demand is fulfilled or iterator has no more items.
 *
 * @param <T> The type of the entry this publisher will emit.
 */
public class IterableToPublisherAdapter<T> implements Publisher<T> {
    private final Iterable<T> iterable;
    private final Executor executor;
    private final int batchSize;

    /**
     * Constructor.
     *
     * @param iterable An iterable to issue iterator for every incoming subscription.
     * @param executor This executor will be used to drain iterator and supply entries to the subscription.
     * @param batchSize An amount of entries to supply during a single iteration. It's always good idea
     *      to provide some reasonable value here in order to give am ability to other publishers which share the same
     *      executor to make progress.
     */
    public IterableToPublisherAdapter(Iterable<T> iterable, Executor executor, int batchSize) {
        this.iterable = iterable;
        this.executor = executor;
        this.batchSize = batchSize;
    }

    @Override
    public void subscribe(Subscriber<? super T> subscriber) {
        Iterator<T> it = iterable.iterator();

        Subscription subscription = new SubscriptionImpl<>(it, subscriber, executor, batchSize);

        subscriber.onSubscribe(subscription);
    }

    @SuppressWarnings("FieldMayBeFinal")
    private static class SubscriptionImpl<T> implements Subscription {
        private static final VarHandle CANCELLED_HANDLE;
        private static final VarHandle REQUESTED_HANDLE;
        private static final VarHandle WIP_HANDLE;

        static {
            try {
                Lookup lookup = MethodHandles.lookup();

                CANCELLED_HANDLE = lookup.findVarHandle(SubscriptionImpl.class, "cancelled", boolean.class);
                REQUESTED_HANDLE = lookup.findVarHandle(SubscriptionImpl.class, "requested", long.class);
                WIP_HANDLE = lookup.findVarHandle(SubscriptionImpl.class, "wip", boolean.class);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        private final Iterator<T> it;
        private final Subscriber<? super T> subscriber;
        private final Executor executor;
        private final int batchSize;

        private boolean cancelled = false;
        private long requested = 0;
        private boolean wip = false;

        SubscriptionImpl(Iterator<T> it, Subscriber<? super T> subscriber, Executor executor, int batchSize) {
            this.it = it;
            this.subscriber = subscriber;
            this.executor = executor;
            this.batchSize = batchSize;
        }

        @Override
        public void request(long n) {
            if (n <= 0) {
                notifyError(new IllegalArgumentException("N should be positive:" + n));

                return;
            }

            if ((boolean) CANCELLED_HANDLE.getAcquire(this)) {
                return;
            }

            long oldValue;
            long newValue;
            do {
                oldValue = (long) REQUESTED_HANDLE.getAcquire(this);

                newValue = oldValue + n;

                if (newValue < 0) {
                    newValue = Long.MAX_VALUE;
                }
            } while (!REQUESTED_HANDLE.compareAndSet(this, oldValue, newValue));

            // the task may be scheduled several times, but it's ok, since we will deal with this
            // inside task itself
            executor.execute(this::drain);
        }

        @Override
        public void cancel() {
            CANCELLED_HANDLE.setRelease(this, true);
        }

        private void drain() {
            if (!WIP_HANDLE.compareAndSet(this, false, true)) {
                return;
            }

            if ((boolean) CANCELLED_HANDLE.getAcquire(this)) {
                return;
            }

            long amount = amountToDrain();

            try {
                while (amount-- > 0 && it.hasNext()) {
                    subscriber.onNext(it.next());
                }
            } catch (Throwable th) {
                notifyError(th);
            }

            if (amount > 0) {
                // according to javadoc, no need to send onComplete signal if subscription has been cancelled
                if (CANCELLED_HANDLE.compareAndSet(this, false, true)) {
                    subscriber.onComplete();
                }

                return;
            }

            WIP_HANDLE.setRelease(this, false);

            if (((long) REQUESTED_HANDLE.getAcquire(this)) > 0) {
                executor.execute(this::drain);
            }
        }

        private long amountToDrain() {
            long oldRequested;
            long newRequested;
            do {
                oldRequested = (long) REQUESTED_HANDLE.getAcquire(this);

                if (oldRequested <= batchSize) {
                    newRequested = 0;
                } else {
                    newRequested = oldRequested - batchSize;
                }
            } while (!REQUESTED_HANDLE.compareAndSet(this, oldRequested, newRequested));

            return oldRequested - newRequested;
        }

        private void notifyError(Throwable th) {
            // according to javadoc, no need to send onError signal if subscription has been cancelled
            if (!CANCELLED_HANDLE.compareAndSet(this, false, true)) {
                return;
            }

            subscriber.onError(th);
        }
    }
}
