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
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

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
    private final AtomicBoolean subscribed = new AtomicBoolean(false);

    private final InflightBatchRequestTracker inflightBatchRequestTracker;

    /**
     * The constructor.
     *
     * @param inflightBatchRequestTracker {@link InflightBatchRequestTracker} to track batch requests completion.
     */
    public PartitionScanPublisher(InflightBatchRequestTracker inflightBatchRequestTracker) {
        this.inflightBatchRequestTracker = inflightBatchRequestTracker;
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
    protected abstract CompletableFuture<Collection<T>> retrieveBatch(long scanId, int batchSize);

    /**
     * The function will be applied when {@link Subscription#cancel} is invoked directly or the cursor is finished.
     *
     * @param intentionallyClose {@code true} if the subscription is closed for the client side.
     * @param scanId The scan id to uniquely identify it on server side.
     * @param th An exception which was thrown when entries were retrieving from the cursor.
     * @return A future which will be completed when the cursor is closed.
     */
    protected abstract CompletableFuture<Void> onClose(boolean intentionallyClose, long scanId, @Nullable Throwable th);

    /**
     * Partition Scan Subscription.
     */
    private class PartitionScanSubscription implements Subscription {
        private static final int INTERNAL_BATCH_SIZE = 10_000;

        private final Subscriber<? super T> subscriber;

        /**
         * Scan id to uniquely identify it on server side.
         */
        private final long scanId;

        private final Object lock = new Object();

        private boolean canceled;

        private long requestedItemsCnt;

        private CompletableFuture<Void> serializationFuture = nullCompletedFuture();

        /**
         * The constructor. TODO: IGNITE-15544 Close partition scans on node left.
         *
         * @param subscriber The subscriber.
         */
        private PartitionScanSubscription(Subscriber<? super T> subscriber) {
            this.subscriber = subscriber;
            this.scanId = CURSOR_ID_GENERATOR.getAndIncrement();
        }

        @Override
        public void request(long n) {
            synchronized (lock) {
                if (n <= 0) {
                    var e = new IllegalArgumentException(format("Invalid requested amount of items [requested={}, minValue=1].", n));

                    failSubscription(e);

                    return;
                }

                if (canceled) {
                    return;
                }

                boolean shouldRetrieveBatch = requestedItemsCnt == 0;

                requestedItemsCnt += n;

                // Handle overflow.
                if (requestedItemsCnt < 0) {
                    requestedItemsCnt = Long.MAX_VALUE;
                }

                if (shouldRetrieveBatch) {
                    int batchSize = (int) Math.min(n, INTERNAL_BATCH_SIZE);

                    serializationFuture = serializationFuture.thenCompose(v -> retrieveAndProcessBatch(batchSize));
                }
            }
        }

        @Override
        public void cancel() {
            synchronized (lock) {
                if (canceled) {
                    return;
                }

                canceled = true;

                serializationFuture = serializationFuture.thenCompose(v -> onClose(true, scanId, null));
            }
        }

        private CompletableFuture<Void> completeSubscription() {
            return onClose(false, scanId, null).thenRun(subscriber::onComplete);
        }

        private void failSubscription(Throwable t) {
            synchronized (lock) {
                if (canceled) {
                    return;
                }

                canceled = true;

                onClose(false, scanId, t)
                        .whenComplete((v, e) -> subscriber.onError(t));
            }
        }

        /**
         * Requests and processes n requested elements.
         *
         * <p>Must be executed under {@link PartitionScanSubscription#lock}.
         *
         * @param n Amount of items to request and process.
         */
        private CompletableFuture<Void> retrieveAndProcessBatch(int n) {
            inflightBatchRequestTracker.onRequestBegin();

            return retrieveBatch(scanId, n)
                    .whenComplete((batch, err) -> inflightBatchRequestTracker.onRequestEnd())
                    .thenCompose(batch -> processBatch(batch, n))
                    .whenComplete((v, err) -> {
                        if (err != null) {
                            failSubscription(err);
                        }
                    });
        }

        private CompletableFuture<Void> processBatch(Collection<T> batch, int requestedCnt) {
            assert batch != null;
            assert batch.size() <= requestedCnt : "Rows more than requested " + batch.size() + " " + requestedCnt;

            batch.forEach(subscriber::onNext);

            synchronized (lock) {
                if (canceled) {
                    return nullCompletedFuture();
                }

                if (batch.size() < requestedCnt) {
                    return completeSubscription();
                }

                requestedItemsCnt -= batch.size();

                return requestedItemsCnt == 0
                        ? nullCompletedFuture()
                        : retrieveAndProcessBatch((int) Math.min(requestedItemsCnt, INTERNAL_BATCH_SIZE));
            }
        }
    }

    /**
     * Tracks every inflight batch request.
     */
    public interface InflightBatchRequestTracker {
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
