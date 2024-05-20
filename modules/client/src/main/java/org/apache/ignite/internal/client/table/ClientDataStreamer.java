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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.function.Function;
import org.apache.ignite.internal.client.ClientUtils;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.streamer.StreamerBatchSender;
import org.apache.ignite.internal.streamer.StreamerOptions;
import org.apache.ignite.internal.streamer.StreamerPartitionAwarenessProvider;
import org.apache.ignite.internal.streamer.StreamerSubscriber;
import org.apache.ignite.table.DataStreamerItem;
import org.apache.ignite.table.DataStreamerOptions;
import org.apache.ignite.table.Tuple;

/**
 * Client data streamer.
 */
class ClientDataStreamer {
    @SuppressWarnings("resource")
    static <T, E, V, R> CompletableFuture<Void> streamData( // T = key, E = element, V = payload, R = result
            Publisher<DataStreamerItem<E>> publisher,
            Function<E, T> keyFunc,
            Function<E, V> payloadFunc,
            DataStreamerOptions options,
            StreamerBatchSender<T, Integer> batchSender,
            StreamerPartitionAwarenessProvider<T, Integer> partitionAwarenessProvider,
            ClientTable tbl) {
        IgniteLogger log = ClientUtils.logger(tbl.channel().configuration(), StreamerSubscriber.class);
        StreamerOptions streamerOpts = streamerOptions(options);
        StreamerSubscriber<T, Integer> subscriber = new StreamerSubscriber<>(
                batchSender,
                partitionAwarenessProvider,
                streamerOpts,
                tbl.channel().streamerFlushExecutor(),
                log,
                tbl.channel().metrics());

        publisher.subscribe(subscriber);

        return subscriber.completionFuture();
    }

    private static StreamerOptions streamerOptions(DataStreamerOptions options) {
        return new StreamerOptions() {
            @Override
            public int pageSize() {
                return options.pageSize();
            }

            @Override
            public int perPartitionParallelOperations() {
                return options.perPartitionParallelOperations();
            }

            @Override
            public int autoFlushFrequency() {
                return options.autoFlushFrequency();
            }
        };
    }
}
