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

package org.apache.ignite.internal.streamer;

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
import org.apache.ignite.internal.logger.IgniteLogger;
import org.jetbrains.annotations.Nullable;

/**
 * Data streamer subscriber.
 *
 * @param <T> Item type.
 * @param <P> Partition type.
 */
public class StreamerSubscriber<T, P> implements Subscriber<T> {
    private final StreamerBatchSender<T, P> batchSender;

    private final StreamerPartitionAwarenessProvider<T, P> partitionAwarenessProvider;

    private final StreamerOptions options;

    private final CompletableFuture<Void> completionFut = new CompletableFuture<>();

    private final AtomicInteger pendingItemCount = new AtomicInteger();

    private final AtomicInteger inFlightItemCount = new AtomicInteger();

    private final Set<CompletableFuture<Void>> pendingFuts = ConcurrentHashMap.newKeySet();

    // NOTE: This can accumulate empty buffers for stopped/failed nodes. Cleaning up is not trivial in concurrent scenario.
    // We don't expect thousands of node failures, so it should be fine.
    private final ConcurrentHashMap<P, StreamerBuffer<T>> buffers = new ConcurrentHashMap<>();

    private final IgniteLogger log;

    private @Nullable Flow.Subscription subscription;

    private @Nullable Timer flushTimer;

    /**
     * Constructor.
     *
     * @param batchSender Batch sender.
     * @param options Data streamer options.
     */
    public StreamerSubscriber(
            StreamerBatchSender<T, P> batchSender,
            StreamerPartitionAwarenessProvider<T, P> partitionAwarenessProvider,
            StreamerOptions options,
            IgniteLogger log) {
        assert batchSender != null;
        assert partitionAwarenessProvider != null;
        assert options != null;
        assert log != null;

        this.batchSender = batchSender;
        this.partitionAwarenessProvider = partitionAwarenessProvider;
        this.options = options;
        this.log = log;
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
                        log.error("Failed to refresh schemas and partition assignment: " + err.getMessage(), err);
                        close(err);
                    } else {
                        requestMore();

                        flushTimer = initFlushTimer();
                    }
                });
    }

    /** {@inheritDoc} */
    @Override
    public void onNext(T item) {
        pendingItemCount.decrementAndGet();

        P partition = partitionAwarenessProvider.partition(item);

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
    public CompletableFuture<Void> completionFuture() {
        return completionFut;
    }

    private CompletableFuture<Void> sendBatch(P partition, Collection<T> batch) {
        int batchSize = batch.size();
        assert batchSize > 0 : "Batch size must be positive.";

        CompletableFuture<Void> fut = new CompletableFuture<>();
        pendingFuts.add(fut);
        inFlightItemCount.addAndGet(batchSize);

        // If a connection fails, the batch goes to default connection thanks to built-it retry mechanism.
        try {
            batchSender.sendAsync(partition, batch).whenComplete((res, err) -> {
                if (err != null) {
                    // Retry is handled by RetryPolicy as usual in ReliableChannel.
                    // If we get here, then retries are exhausted and we should fail the streamer.
                    log.error("Failed to send batch to partition " + partition + ": " + err.getMessage(), err);
                    close(err);
                } else {
                    fut.complete(null);
                    pendingFuts.remove(fut);

                    inFlightItemCount.addAndGet(-batchSize);
                    requestMore();

                    // Refresh partition assignment asynchronously.
                    partitionAwarenessProvider.refreshAsync().exceptionally(refreshErr -> {
                        log.error("Failed to refresh schemas and partition assignment: " + refreshErr.getMessage(), refreshErr);
                        close(refreshErr);
                        return null;
                    });
                }
            });

            return fut;
        } catch (Exception e) {
            log.error("Failed to send batch to partition " + partition + ": " + e.getMessage(), e);
            close(e);
            return CompletableFuture.failedFuture(e);
        }
    }

    private void close(@Nullable Throwable throwable) {
        if (flushTimer != null) {
            flushTimer.cancel();
        }

        var s = subscription;

        if (s != null) {
            s.cancel();
        }

        if (throwable == null) {
            for (StreamerBuffer<T> buf : buffers.values()) {
                pendingFuts.add(buf.flushAndClose());
            }

            var futs = pendingFuts.toArray(new CompletableFuture[0]);

            CompletableFuture.allOf(futs).whenComplete((res, err) -> {
                if (err != null) {
                    completionFut.completeExceptionally(err);
                } else {
                    completionFut.complete(null);
                }
            });
        } else {
            completionFut.completeExceptionally(throwable);
        }
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

    private @Nullable Timer initFlushTimer() {
        int interval = options.autoFlushFrequency();

        if (interval <= 0) {
            return null;
        }

        Timer timer = new Timer("client-data-streamer-flush-" + hashCode());

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
