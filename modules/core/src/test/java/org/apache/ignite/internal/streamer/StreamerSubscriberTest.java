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

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongFunction;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.table.DataStreamerItem;
import org.apache.ignite.table.DataStreamerOperationType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class StreamerSubscriberTest extends BaseIgniteAbstractTest {
    private static ScheduledExecutorService flushExecutor;

    @BeforeAll
    public static void flushExecutorInit() {
        flushExecutor = Executors.newSingleThreadScheduledExecutor(
                new NamedThreadFactory("flushExecutor", Loggers.forClass(StreamerSubscriberTest.class)));
    }

    @AfterAll
    public static void flushExecutorShutdown() {
        if (flushExecutor != null) {
            flushExecutor.shutdown();
        }
    }

    private static class Metrics implements StreamerMetricSink {
        private final LongAdder batchesSent = new LongAdder();
        private final LongAdder itemsSent = new LongAdder();
        private final LongAdder batchesActive = new LongAdder();
        private final LongAdder itemsQueued = new LongAdder();

        @Override
        public void streamerBatchesSentAdd(long batches) {
            batchesSent.add(batches);
        }

        @Override
        public void streamerItemsSentAdd(long items) {
            itemsSent.add(items);
        }

        @Override
        public void streamerBatchesActiveAdd(long batches) {
            batchesActive.add(batches);
        }

        @Override
        public void streamerItemsQueuedAdd(long items) {
            itemsQueued.add(items);
        }
    }

    private static class Options implements StreamerOptions {
        private final int batchSize;
        private final int perPartitionParallelOperations;
        private final int autoFlushFrequency;

        Options(int batchSize, int perPartitionParallelOperations, int autoFlushFrequency) {
            this.batchSize = batchSize;
            this.perPartitionParallelOperations = perPartitionParallelOperations;
            this.autoFlushFrequency = autoFlushFrequency;
        }

        @Override
        public int pageSize() {
            return batchSize;
        }

        @Override
        public int perPartitionParallelOperations() {
            return perPartitionParallelOperations;
        }

        @Override
        public int autoFlushFrequency() {
            return autoFlushFrequency;
        }
    }

    /**
     * Publisher that generates a {@code limit} number of items and dispatches them in the same thread.
     */
    private static class LimitedPublisher<T> implements Publisher<DataStreamerItem<T>> {
        private final long limit;

        private final LongFunction<T> generator;

        LimitedPublisher(long limit, LongFunction<T> generator) {
            this.limit = limit;
            this.generator = generator;
        }

        @Override
        public void subscribe(Subscriber<? super DataStreamerItem<T>> subscriber) {
            subscriber.onSubscribe(new Subscription() {
                private long produced = 0;

                @Override
                public void request(long n) {
                    if (produced == limit) {
                        produced++;
                        subscriber.onComplete();
                    } else if (produced < limit) {
                        for (int i = 0; i < Math.min(n, limit - produced); i++) {
                            produced++;
                            T item = generator.apply(produced);
                            subscriber.onNext(DataStreamerItem.of(item));
                        }
                    }
                }

                @Override
                public void cancel() {
                }
            });
        }
    }

    /**
     * Tests the backpressure algorithm when batch sending is stuck.
     */
    @Test
    void testBackpressureWithDelay() {
        long itemsCount = 10;

        var metrics = new Metrics();

        var options = new Options(2, 1, 1000);

        long expectedBatches = itemsCount / options.batchSize;

        var partitionProvider = new StreamerPartitionAwarenessProvider<Long, String>() {
            @Override
            public String partition(Long item) {
                return "foo";
            }

            @Override
            public CompletableFuture<Void> refreshAsync() {
                return nullCompletedFuture();
            }
        };

        var sendFuture = new CompletableFuture<Collection<Object>>();

        StreamerSubscriber<Long, DataStreamerItem<Long>, Long, Object, String> subscriber = new StreamerSubscriber<>(
                (part, batch, deleted) -> sendFuture,
                DataStreamerItem::get,
                DataStreamerItem::get,
                x -> x.operationType() == DataStreamerOperationType.REMOVE,
                partitionProvider,
                options,
                flushExecutor,
                log,
                metrics
        );

        LimitedPublisher<Long> publisher = new LimitedPublisher<>(itemsCount, i -> i);

        publisher.subscribe(subscriber);

        assertThat(metrics.batchesActive.longValue(), is(expectedBatches));
        assertThat(metrics.batchesSent.longValue(), is(0L));
        assertThat(metrics.itemsQueued.longValue(), is(itemsCount));
        assertThat(metrics.itemsSent.longValue(), is(0L));

        sendFuture.complete(null);

        assertThat(subscriber.completionFuture(), willCompleteSuccessfully());

        assertThat(metrics.batchesActive.longValue(), is(0L));
        assertThat(metrics.batchesSent.longValue(), is(expectedBatches));
        assertThat(metrics.itemsQueued.longValue(), is(0L));
        assertThat(metrics.itemsSent.longValue(), is(itemsCount));
    }
}
