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

import java.util.Collection;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
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

    private final AtomicInteger inFlightItemCount = new AtomicInteger();

    private final Set<CompletableFuture<Void>> pendingFuts = ConcurrentHashMap.newKeySet();

    // TODO: This can accumulate huge number of buffers for dropped connections over time.
    // We should have some logic to check if a buffer is still needed.
    private final ConcurrentHashMap<TPartition, StreamerBuffer<T>> buffers = new ConcurrentHashMap<>();

    private @Nullable Flow.Subscription subscription;

    private @Nullable Timer flushTimer;

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

        // Refresh schemas and partition assignment, then request initial batch.
        partitionAwarenessProvider.refreshAsync()
                .whenComplete((res, err) -> {
                    if (err != null) {
                        close(err);
                    }
                    else {
                        requestMore();

                        if (options.autoFlushFrequency() > 0) {
                            flushTimer = initFlushTimer();
                        }
                    }
                });
    }

    /** {@inheritDoc} */
    @Override
    public void onNext(T item) {
        TPartition partition = partitionAwarenessProvider.partition(item);

        StreamerBuffer<T> buf = buffers.computeIfAbsent(
                partition,
                p -> new StreamerBuffer<>(options.batchSize(), items -> sendBatch(p, items)));

        buf.add(item);

        requestMore();
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

    private CompletableFuture<Void> sendBatch(TPartition partition, Collection<T> batch) {
        int batchSize = batch.size();
        assert batchSize > 0;

        CompletableFuture<Void> fut = new CompletableFuture<>();
        pendingFuts.add(fut);
        inFlightItemCount.addAndGet(batchSize);

        // If a connection fails, the batch goes to default connection thanks to built-it retry mechanism.
        batchSender.sendAsync(partition, batch).whenComplete((res, err) -> {
            if (err != null) {
                // Retry is handled by RetryPolicy as usual in ReliableChannel.
                // If we get here, then retries are exhausted and we should fail the streamer.
                close(err);
            }
            else {
                fut.complete(null);
                pendingFuts.remove(fut);

                inFlightItemCount.addAndGet(-batchSize);
                requestMore();

                // Refresh partition assignment asynchronously.
                partitionAwarenessProvider.refreshAsync().exceptionally(refreshErr -> {
                    close(refreshErr);
                    return null;
                });
            }
        });

        return fut;
    }

    private void close(@Nullable Throwable throwable) {
        if (flushTimer != null) {
            flushTimer.cancel();
        }

        var s = subscription;

        if (s != null) {
            s.cancel();
        }

        for (StreamerBuffer<T> buf : buffers.values()) {
            buf.flushAndClose();
        }

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

    private void requestMore() {
        // This method controls backpressure. We won't get more items than we requested.
        // The idea is to have perNodeParallelOperations batches in flight for every connection.
        var pending = pendingItemCount.get();
        var desiredInFlight = Math.max(1, buffers.size()) * options.batchSize() * options.perNodeParallelOperations();
        var inFlight = inFlightItemCount.get();
        var count = desiredInFlight - inFlight - pending;

        if (count <= 0) {
            return;
        }

        assert subscription != null;
        subscription.request(count);
        pendingItemCount.addAndGet(count);
    }

    private Timer initFlushTimer() {
        Timer timer = new Timer("client-data-streamer-flush-" + hashCode());

        int interval = options.autoFlushFrequency();
        timer.schedule(new PeriodicFlushTask(), interval, interval);

        return timer;
    }

    /**
     * Periodically flushes buffers.
     */
    private class PeriodicFlushTask extends TimerTask {
        @Override
        public void run() {
            for (StreamerBuffer<T> buf : buffers.values()) {
                buf.flush(options.autoFlushFrequency());
            }
        }
    }
}
