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

import java.util.BitSet;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.jetbrains.annotations.Nullable;

/**
 * Data streamer subscriber.
 *
 * @param <T> Key type.
 * @param <E> Element type.
 * @param <V> Value (payload) type.
 * @param <R> Result type.
 * @param <P> Partition type.
 */
public class StreamerSubscriber<T, E, V, R, P> implements Subscriber<E> {
    private final StreamerBatchSender<V, P, R> batchSender;

    private final @Nullable Subscriber<R> resultSubscriber;

    private final Function<E, T> keyFunc;

    private final Function<E, V> payloadFunc;

    private final Function<E, Boolean> deleteFunc;

    private final StreamerPartitionAwarenessProvider<T, P> partitionAwarenessProvider;

    private final StreamerOptions options;

    private final CompletableFuture<Void> completionFut = new CompletableFuture<>();

    private final AtomicInteger pendingItemCount = new AtomicInteger();

    private final AtomicInteger inFlightItemCount = new AtomicInteger();

    // NOTE: This can accumulate empty buffers for stopped/failed nodes. Cleaning up is not trivial in concurrent scenario.
    // We don't expect thousands of node failures, so it should be fine.
    private final ConcurrentHashMap<P, StreamerBuffer<V>> buffers = new ConcurrentHashMap<>();

    private final ConcurrentMap<P, CompletableFuture<Collection<R>>> pendingRequests = new ConcurrentHashMap<>();

    private final IgniteLogger log;

    private final StreamerMetricSink metrics;

    private final ScheduledExecutorService flushExecutor;

    private @Nullable Flow.Subscription subscription;

    private @Nullable ResultSubscription resultSubscription;

    private @Nullable ScheduledFuture<?> flushTask;

    private boolean closed;

    /**
     * Constructor.
     *
     * @param batchSender Batch sender.
     * @param resultSubscriber Result subscriber.
     * @param keyFunc Key function.
     * @param payloadFunc Payload function.
     * @param deleteFunc Delete function.
     * @param partitionAwarenessProvider Partition awareness provider.
     * @param options Streamer options.
     * @param flushExecutor Flush executor.
     * @param log Logger.
     * @param metrics Metrics.
     */
    public StreamerSubscriber(
            StreamerBatchSender<V, P, R> batchSender,
            @Nullable Flow.Subscriber<R> resultSubscriber,
            Function<E, T> keyFunc,
            Function<E, V> payloadFunc,
            Function<E, Boolean> deleteFunc,
            StreamerPartitionAwarenessProvider<T, P> partitionAwarenessProvider,
            StreamerOptions options,
            ScheduledExecutorService flushExecutor,
            IgniteLogger log,
            @Nullable StreamerMetricSink metrics) {
        assert batchSender != null;
        assert keyFunc != null;
        assert payloadFunc != null;
        assert partitionAwarenessProvider != null;
        assert options != null;
        assert flushExecutor != null;
        assert log != null;

        this.batchSender = batchSender;
        this.resultSubscriber = resultSubscriber;
        this.keyFunc = keyFunc;
        this.payloadFunc = payloadFunc;
        this.deleteFunc = deleteFunc;
        this.partitionAwarenessProvider = partitionAwarenessProvider;
        this.options = options;
        this.flushExecutor = flushExecutor;
        this.log = log;
        this.metrics = getMetrics(metrics);
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void onSubscribe(Subscription subscription) {
        if (this.subscription != null) {
            throw new IllegalStateException("Subscription is already set.");
        }

        this.subscription = subscription;

        if (resultSubscriber != null) {
            resultSubscription = new ResultSubscription();
            resultSubscriber.onSubscribe(resultSubscription);
        }

        // Refresh schemas and partition assignment, then request initial batch.
        partitionAwarenessProvider.refreshAsync()
                .whenComplete((res, err) -> {
                    if (err != null) {
                        log.error("Failed to refresh schemas and partition assignment: " + err.getMessage(), err);
                        close(err);
                    } else {
                        initFlushTimer();

                        requestMore();
                    }
                });
    }

    /** {@inheritDoc} */
    @Override
    public void onNext(E item) {
        pendingItemCount.decrementAndGet();

        T key = keyFunc.apply(item);
        P partition = partitionAwarenessProvider.partition(key);

        StreamerBuffer<V> buf = buffers.computeIfAbsent(
                partition,
                p -> new StreamerBuffer<>(options.pageSize(), (items, deleted) -> enlistBatch(p, items, deleted)));

        V payload = payloadFunc.apply(item);
        boolean delete = deleteFunc.apply(item);

        buf.add(payload, delete);
        this.metrics.streamerItemsQueuedAdd(1);

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

    private void enlistBatch(P partition, Collection<V> batch, BitSet deleted) {
        int batchSize = batch.size();
        assert batchSize > 0 : "Batch size must be positive.";
        assert partition != null : "Partition must not be null.";

        inFlightItemCount.addAndGet(batchSize);
        metrics.streamerBatchesActiveAdd(1);

        pendingRequests.compute(
                partition,
                // Chain existing futures to preserve request order.
                (part, fut) -> fut == null ? sendBatch(part, batch, deleted) : fut.thenCompose(v -> sendBatch(part, batch, deleted))
        );
    }

    private CompletableFuture<Collection<R>> sendBatch(P partition, Collection<V> batch, BitSet deleted) {
        // If a connection fails, the batch goes to default connection thanks to built-it retry mechanism.
        try {
            return batchSender.sendAsync(partition, batch, deleted).whenComplete((res, err) -> {
                if (err != null) {
                    // Retry is handled by the sender (RetryPolicy in ReliableChannel on the client, sendWithRetry on the server).
                    // If we get here, then retries are exhausted and we should fail the streamer.
                    log.error("Failed to send batch to partition " + partition + ": " + err.getMessage(), err);
                    close(err);
                } else {
                    int batchSize = batch.size();

                    this.metrics.streamerBatchesSentAdd(1);
                    this.metrics.streamerBatchesActiveAdd(-1);
                    this.metrics.streamerItemsSentAdd(batchSize);
                    this.metrics.streamerItemsQueuedAdd(-batchSize);

                    inFlightItemCount.addAndGet(-batchSize);
                    requestMore();

                    // Refresh partition assignment asynchronously.
                    partitionAwarenessProvider.refreshAsync().exceptionally(refreshErr -> {
                        log.error("Failed to refresh schemas and partition assignment: " + refreshErr.getMessage(), refreshErr);
                        close(refreshErr);
                        return null;
                    });

                    invokeResultSubscriber(res);
                }
            });
        } catch (Exception e) {
            log.error("Failed to send batch to partition " + partition + ": " + e.getMessage(), e);
            close(e);
            return CompletableFuture.failedFuture(e);
        }
    }

    private void invokeResultSubscriber(Collection<R> res) {
        if (res == null || resultSubscriber == null) {
            return;
        }

        ResultSubscription sub = resultSubscription();

        if (sub == null) {
            return;
        }

        for (R r : res) {
            if (sub.cancelled.get()) {
                return;
            }

            resultSubscriber.onNext(r);
        }
    }

    private synchronized @Nullable ResultSubscription resultSubscription() {
        return resultSubscription;
    }

    private synchronized void close(@Nullable Throwable throwable) {
        if (flushTask != null) {
            flushTask.cancel(false);
        }

        var sub = subscription;
        if (sub != null) {
            sub.cancel();
        }

        if (throwable == null) {
            buffers.values().forEach(StreamerBuffer::flushAndClose);

            var futs = pendingRequests.values().toArray(new CompletableFuture[0]);

            CompletableFuture.allOf(futs).whenComplete((v, e) -> {
                if (e != null) {
                    if (resultSubscriber != null) {
                        resultSubscriber.onError(e);
                    }

                    completionFut.completeExceptionally(e);
                } else {
                    if (resultSubscriber != null) {
                        resultSubscriber.onComplete();
                    }

                    completionFut.complete(null);
                }
            });
        } else {
            completionFut.completeExceptionally(throwable);

            if (resultSubscriber != null) {
                resultSubscriber.onError(throwable);
            }
        }

        closed = true;
    }

    private synchronized void requestMore() {
        if (closed || subscription == null) {
            return;
        }

        // This method controls backpressure. We won't get more items than we requested.
        // The idea is to have perPartitionParallelOperations batches in flight for every connection.
        var pending = pendingItemCount.get();
        var desiredInFlight = Math.max(1, buffers.size()) * options.pageSize() * options.perPartitionParallelOperations();
        var inFlight = inFlightItemCount.get();
        var count = desiredInFlight - inFlight - pending;

        if (count <= 0) {
            return;
        }

        subscription.request(count);
        pendingItemCount.addAndGet(count);
    }

    private synchronized void initFlushTimer() {
        if (closed) {
            return;
        }

        int interval = options.autoFlushFrequency();

        if (interval <= 0) {
            return;
        }

        flushTask = flushExecutor.scheduleAtFixedRate(this::flushBuffers, interval, interval, TimeUnit.MILLISECONDS);
    }

    private void flushBuffers() {
        buffers.values().forEach(StreamerBuffer::flush);
    }

    private static StreamerMetricSink getMetrics(@Nullable StreamerMetricSink metrics) {
        return metrics != null ? metrics : new StreamerMetricSink() {
            @Override
            public void streamerBatchesSentAdd(long batches) {
                // No-op.
            }

            @Override
            public void streamerItemsSentAdd(long items) {
                // No-op.

            }

            @Override
            public void streamerBatchesActiveAdd(long batches) {
                // No-op.

            }

            @Override
            public void streamerItemsQueuedAdd(long items) {
                // No-op.
            }
        };
    }

    private static class ResultSubscription implements Subscription {
        AtomicBoolean cancelled = new AtomicBoolean();

        @Override
        public void request(long n) {
            // No-op: result subscriber ignores backpressure and follows the pace of the user-defined publisher.
        }

        @Override
        public void cancel() {
            cancelled.set(true);
        }
    }
}
