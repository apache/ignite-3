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

package org.apache.ignite.internal.table.distributed.storage;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.jetbrains.annotations.Nullable;

/**
 * Partition Scan Publisher.
 *
 * @param <T> The type of the elements.
 */
public abstract class PartitionScanPublisher<T> implements Publisher<T> {
    /** Cursor id generator. */
    private static final AtomicLong CURSOR_ID_GENERATOR = new AtomicLong();

    /** True when the publisher has a subscriber, false otherwise. */
    private final AtomicBoolean subscribed;

    private final InflightBatchRequestTracker inflightBatchRequestTracker;

    /**
     * The constructor.
     *
     * @param inflightBatchRequestTracker {@link InflightBatchRequestTracker} to track betch requests completion.
     */
    public PartitionScanPublisher(
            InflightBatchRequestTracker inflightBatchRequestTracker
    ) {
        this.inflightBatchRequestTracker = inflightBatchRequestTracker;

        this.subscribed = new AtomicBoolean(false);
    }

    @Override
    public void subscribe(Subscriber<? super T> subscriber) {
        if (subscriber == null) {
            throw new NullPointerException("Subscriber is null");
        }

        if (!subscribed.compareAndSet(false, true)) {
            subscriber.onError(new IllegalStateException("Scan publisher does not support multiple subscriptions."));
        }

        subscriber.onSubscribe(new PartitionScanSubscription(subscriber));
    }

    /**
     * Gets a new batch from the remote replica.
     *
     * @param scanId The scan id to uniquely identify it on server side.
     * @param batchSize The size of the batch to retrieve.
     * @return A future with a batch of rows.
     */
    protected abstract CompletableFuture<Collection<T>> retrieveBatch(Long scanId, Integer batchSize);

    /**
     * The function will be applied when {@link Subscription#cancel} is invoked directly or the cursor is finished.
     *
     * @param intentionallyClose {@code true} if the subscription is closed for the client side.
     * @param scanId The scan id to uniquely identify it on server side.
     * @param th An exception which was thrown when entries were retrieving from the cursor.
     * @return A future which will be completed when the cursor is closed.
     */
    protected abstract CompletableFuture<Void> onClose(Boolean intentionallyClose, Long scanId, @Nullable Throwable th);

    /**
     * Partition Scan Subscription.
     */
    private class PartitionScanSubscription implements Subscription {
        private final Subscriber<? super T> subscriber;

        private final AtomicBoolean canceled;

        /**
         * Scan id to uniquely identify it on server side.
         */
        private final Long scanId;

        private final AtomicLong requestedItemsCnt;

        private static final int INTERNAL_BATCH_SIZE = 10_000;

        /**
         * The constructor. TODO: IGNITE-15544 Close partition scans on node left.
         *
         * @param subscriber The subscriber.
         */
        private PartitionScanSubscription(Subscriber<? super T> subscriber) {
            this.subscriber = subscriber;
            this.canceled = new AtomicBoolean(false);
            this.scanId = CURSOR_ID_GENERATOR.getAndIncrement();
            this.requestedItemsCnt = new AtomicLong(0);
        }

        @Override
        public void request(long n) {
            if (n <= 0) {
                cancel(null, true);

                subscriber.onError(new IllegalArgumentException(
                        format("Invalid requested amount of items [requested={}, minValue=1].", n)));
            }

            if (canceled.get()) {
                return;
            }

            long prevVal = requestedItemsCnt.getAndUpdate(origin -> {
                try {
                    return Math.addExact(origin, n);
                } catch (ArithmeticException e) {
                    return Long.MAX_VALUE;
                }
            });

            if (prevVal == 0) {
                scanBatch((int) Math.min(n, INTERNAL_BATCH_SIZE));
            }
        }

        @Override
        public void cancel() {
            cancel(null, true); // Explicit cancel.
        }

        /**
         * After the method is called, a subscriber won't be received updates from the publisher.
         *
         * @param t An exception which was thrown when entries were retrieving from the cursor.
         * @param intentionallyClose True if the subscription is closed for the client side.
         */
        private void cancel(@Nullable Throwable t, boolean intentionallyClose) {
            if (!canceled.compareAndSet(false, true)) {
                return;
            }

            onClose(intentionallyClose, scanId, t).whenComplete((ignore, th) -> {
                if (th != null) {
                    subscriber.onError(th);
                } else {
                    subscriber.onComplete();
                }
            });
        }

        /**
         * Requests and processes n requested elements where n is an integer.
         *
         * @param n Amount of items to request and process.
         */
        private void scanBatch(int n) {
            if (canceled.get()) {
                return;
            }

            inflightBatchRequestTracker.onRequestBegin();

            retrieveBatch(scanId, n).thenAccept(binaryRows -> {
                assert binaryRows != null;
                assert binaryRows.size() <= n : "Rows more then requested " + binaryRows.size() + " " + n;

                inflightBatchRequestTracker.onRequestEnd();

                binaryRows.forEach(subscriber::onNext);

                if (binaryRows.size() < n) {
                    cancel(null, false);
                } else {
                    long remaining = requestedItemsCnt.addAndGet(Math.negateExact(binaryRows.size()));

                    if (remaining > 0) {
                        scanBatch((int) Math.min(remaining, INTERNAL_BATCH_SIZE));
                    }
                }
            }).exceptionally(t -> {
                inflightBatchRequestTracker.onRequestEnd();

                cancel(t, false);

                return null;
            });
        }
    }

    /**
     * Tracks every inflight batch request.
     */
    interface InflightBatchRequestTracker {
        /**
         * Called right before a batch request is started.
         */
        void onRequestBegin();

        /**
         * Called right after a batch request is completed.
         */
        void onRequestEnd();
    }
}

