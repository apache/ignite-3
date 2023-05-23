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

package org.apache.ignite.internal.client.table;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.table.DataStreamerOptions;
import org.jetbrains.annotations.Nullable;

/**
 * Data streamer subscriber.
 */
class StreamerSubscriber<T> implements Subscriber<T> {
    private final StreamerBatchSender<T> batchSender;

    private final DataStreamerOptions options;

    private final CompletableFuture<Void> completionFut = new CompletableFuture<>();

    private @Nullable Flow.Subscription subscription;

    private @Nullable Collection<T> currentBatch; // TODO: per-node buffers.

    private final AtomicInteger pendingItemCount = new AtomicInteger();

    private final Set<CompletableFuture<Void>> pendingFuts = ConcurrentHashMap.newKeySet();

    /**
     * Constructor.
     *
     * @param batchSender Batch sender.
     * @param options Data streamer options.
     */
    StreamerSubscriber(StreamerBatchSender<T> batchSender, @Nullable DataStreamerOptions options) {
        assert batchSender != null;

        if (options != null && options.batchSize() <= 0) {
            throw new IllegalArgumentException("Batch size must be positive: " + options.batchSize());
        }

        this.batchSender = batchSender;
        this.options = options == null ? new DataStreamerOptions() : null;
    }

    /** {@inheritDoc} */
    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscription = subscription;

        requestNextBatch(subscription);
    }

    /** {@inheritDoc} */
    @Override
    public void onNext(T item) {
        // TODO: This method should be called from a single thread - is that correct?
        if (pendingItemCount.decrementAndGet() == 0) {
            requestNextBatch(subscription);
        }

        // TODO: Update per-node buffers.
        // TODO: Request more data once current batch is processed.
        if (currentBatch == null) {
            currentBatch = new ArrayList<>(options.batchSize());
        }

        currentBatch.add(item);

        if (currentBatch.size() == options.batchSize()) {
            sendBatch(currentBatch);
            currentBatch = null;
        }
    }

    /** {@inheritDoc} */
    @Override
    public void onError(Throwable throwable) {
        close();
    }

    /** {@inheritDoc} */
    @Override
    public void onComplete() {
        close();
    }

    /**
     * Returns a future that will be completed once all the data is sent.
     *
     * @return Completion future.
     */
    CompletableFuture<Void> completionFuture() {
        return completionFut;
    }

    private void sendBatch(Collection<T> batch) {
        // TODO: Backpressure control - no more than 1 batch in flight (per node).
        CompletableFuture<Void> fut = new CompletableFuture<>();
        pendingFuts.add(fut);

        batchSender.sendAsync(batch).whenComplete((res, err) -> {
            if (err != null) {
                // TODO: Retry - what should be the logic? When do we give up?
                // TODO: Log error.
                sendBatch(batch);
            }
            else {
                fut.complete(null);
                pendingFuts.remove(fut);
            }
        });
    }

    private void close() {
        var s = subscription;

        if (s != null) {
            s.cancel();
        }

        var batch = currentBatch;

        if (batch != null) {
            sendBatch(batch);
        }

        // TODO: Thread synchronization - make sure no new futures are added.
        var futs = pendingFuts.toArray(new CompletableFuture[0]);

        CompletableFuture.allOf(futs).whenComplete((res, err) -> {
            if (err != null) {
                completionFut.completeExceptionally(err);
            }
            else {
                completionFut.complete(null);
            }
        });
    }

    private void requestNextBatch(@Nullable Subscription subscription) {
        if (subscription == null) {
            return;
        }

        int batchSize = options.batchSize();
        subscription.request(batchSize);
        pendingItemCount.addAndGet(batchSize);
    }
}
