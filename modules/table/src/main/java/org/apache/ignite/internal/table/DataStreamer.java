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

package org.apache.ignite.internal.table;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.streamer.StreamerBatchSender;
import org.apache.ignite.internal.streamer.StreamerOptions;
import org.apache.ignite.internal.streamer.StreamerPartitionAwarenessProvider;
import org.apache.ignite.internal.streamer.StreamerSubscriber;
import org.apache.ignite.table.DataStreamerItem;
import org.apache.ignite.table.DataStreamerOperationType;
import org.apache.ignite.table.DataStreamerOptions;
import org.jetbrains.annotations.Nullable;

class DataStreamer {
    private static final IgniteLogger LOG = Loggers.forClass(DataStreamer.class);

    static <R> CompletableFuture<Void> streamData(
            Publisher<DataStreamerItem<R>> publisher,
            @Nullable DataStreamerOptions options,
            StreamerBatchSender<R, Integer, Void> batchSender,
            StreamerPartitionAwarenessProvider<R, Integer> partitionAwarenessProvider,
            ScheduledExecutorService flushExecutor) {
        return streamData(
                publisher,
                DataStreamerItem::get,
                DataStreamerItem::get,
                x -> x.operationType() == DataStreamerOperationType.REMOVE,
                options,
                batchSender,
                null,
                partitionAwarenessProvider,
                flushExecutor);
    }

    // T = key, E = element, V = payload, R = result.
    static <T, E, V, R> CompletableFuture<Void> streamData(
            Publisher<E> publisher,
            Function<E, T> keyFunc,
            Function<E, V> payloadFunc,
            Function<E, Boolean> deleteFunc,
            @Nullable DataStreamerOptions options,
            StreamerBatchSender<V, Integer, R> batchSender,
            @Nullable Flow.Subscriber<R> resultSubscriber,
            StreamerPartitionAwarenessProvider<T, Integer> partitionAwarenessProvider,
            ScheduledExecutorService flushExecutor) {
        StreamerOptions streamerOpts = streamerOptions(options);
        StreamerSubscriber<T, E, V, R, Integer> subscriber = new StreamerSubscriber<>(
                batchSender,
                resultSubscriber,
                keyFunc,
                payloadFunc,
                deleteFunc,
                partitionAwarenessProvider,
                streamerOpts,
                flushExecutor,
                LOG,
                null);

        publisher.subscribe(subscriber);

        return subscriber.completionFuture();
    }

    private static StreamerOptions streamerOptions(@Nullable DataStreamerOptions options) {
        var options0 = options == null ? DataStreamerOptions.DEFAULT : options;

        return new StreamerOptions() {
            @Override
            public int pageSize() {
                return options0.pageSize();
            }

            @Override
            public int perPartitionParallelOperations() {
                return options0.perPartitionParallelOperations();
            }

            @Override
            public int autoFlushInterval() {
                return options0.autoFlushInterval();
            }
        };
    }
}
