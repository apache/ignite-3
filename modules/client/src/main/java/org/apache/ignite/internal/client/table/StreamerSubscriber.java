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

import java.util.Map.Entry;
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
class StreamerSubscriber<T, TPartition> implements Subscriber<T> {
    private final StreamerBatchSender<T, TPartition> batchSender;

    private final StreamerPartitionAwarenessProvider<T, TPartition> partitionAwarenessProvider;

    private final DataStreamerOptions options;

    private final CompletableFuture<Void> completionFut = new CompletableFuture<>();

    private final AtomicInteger pendingItemCount = new AtomicInteger();

    private final Set<CompletableFuture<Void>> pendingFuts = ConcurrentHashMap.newKeySet();

    private final ConcurrentHashMap<TPartition, StreamerBuffer<T>> buffers = new ConcurrentHashMap<>();

    private @Nullable Flow.Subscription subscription;

    /**
     * Constructor.
     *
     * @param batchSender Batch sender.
     * @param options Data streamer options.
     */
    StreamerSubscriber(
            StreamerBatchSender<T, TPartition> batchSender,
            StreamerPartitionAwarenessProvider<T, TPartition> partitionAwarenessProvider,
            @Nullable DataStreamerOptions options) {
        assert batchSender != null;
        assert partitionAwarenessProvider != null;

        if (options != null && options.batchSize() <= 0) {
            throw new IllegalArgumentException("Batch size must be positive: " + options.batchSize());
        }

        this.batchSender = batchSender;
        this.partitionAwarenessProvider = partitionAwarenessProvider;
        this.options = options != null ? options : new DataStreamerOptions();
    }

    /** {@inheritDoc} */
    @Override
    public void onSubscribe(Subscription subscription) {
        if (this.subscription != null) {
            throw new IllegalStateException("Subscription is already set.");
        }

        this.subscription = subscription;

        // Request initial batch times 2 (every per-node buffer can hold 2x items - one for flushing and one for adding).
        requestMore(options.batchSize() * 2);
    }

    /** {@inheritDoc} */
    @Override
    public void onNext(T item) {
        if (pendingItemCount.decrementAndGet() == 0 && pendingFuts.isEmpty()) {
            // No batches are being flushed, so we can request more items.
            requestMore(options.batchSize());
        }

        TPartition partition = partitionAwarenessProvider.partition(item);
        StreamerBuffer<T> buf = buffers.computeIfAbsent(partition, p -> new StreamerBuffer<>(options.batchSize()));

        boolean shouldFlush = buf.add(item);

        if (shouldFlush) {
            sendBatch(partition, buf);
            buf = buffers.computeIfAbsent(partition, p -> new StreamerBuffer<>(options.batchSize()));
            buf.add(item);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void onError(Throwable throwable) {
        close(throwable);
    }

    /** {@inheritDoc} */
    @Override
    public void onComplete() {
        close(null);
    }

    /**
     * Returns a future that will be completed once all the data is sent.
     *
     * @return Completion future.
     */
    CompletableFuture<Void> completionFuture() {
        return completionFut;
    }

    private void sendBatch(TPartition partition, StreamerBuffer<T> batch) {
        if (batch.items().isEmpty()) {
            return;
        }

        CompletableFuture<Void> fut = new CompletableFuture<>();
        pendingFuts.add(fut);

        batchSender.sendAsync(partition, batch.items()).whenComplete((res, err) -> {
            if (err != null) {
                // TODO: Retry only connection issues. Connection issue indicates channel failure.
                // We have to re-add items from the current buffer.
                // - When do we give up?
                // - How does it combine with RetryPolicy?
                // TODO: Log error.
                sendBatch(partition, batch);
            }
            else {
                int itemsSent = batch.items().size();
                fut.complete(null);
                pendingFuts.remove(fut);
                batch.onSent();

                // Backpressure control: request as many items as we sent.
                requestMore(itemsSent);
            }
        });
    }

    private void close(@Nullable Throwable throwable) {
        // TODO: RW lock.
        var s = subscription;

        if (s != null) {
            s.cancel();
        }

        for (Entry<TPartition, StreamerBuffer<T>> buf : buffers.entrySet()) {
            sendBatch(buf.getKey(), buf.getValue());
        }

        // TODO: Thread synchronization - make sure no new futures are added.
        var futs = pendingFuts.toArray(new CompletableFuture[0]);

        CompletableFuture.allOf(futs).whenComplete((res, err) -> {
            if (throwable != null) {
                completionFut.completeExceptionally(throwable);
            } else if (err != null) {
                completionFut.completeExceptionally(err);
            } else {
                completionFut.complete(null);
            }
        });
    }

    private void requestMore(int count) {
        if (subscription == null) {
            return;
        }

        // This method controls backpressure. We won't get more items than we requested.
        subscription.request(count);
        pendingItemCount.addAndGet(count);
    }
}
