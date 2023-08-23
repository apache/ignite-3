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
import org.apache.ignite.internal.client.ClientUtils;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.streamer.StreamerBatchSender;
import org.apache.ignite.internal.streamer.StreamerOptions;
import org.apache.ignite.internal.streamer.StreamerPartitionAwarenessProvider;
import org.apache.ignite.internal.streamer.StreamerSubscriber;
import org.apache.ignite.table.DataStreamerOptions;

/**
 * Client data streamer.
 */
class ClientDataStreamer {
    @SuppressWarnings("resource")
    static <R> CompletableFuture<Void> streamData(
            Publisher<R> publisher,
            DataStreamerOptions options,
            StreamerBatchSender<R, String> batchSender,
            StreamerPartitionAwarenessProvider<R, String> partitionAwarenessProvider,
            ClientTable tbl) {
        IgniteLogger log = ClientUtils.logger(tbl.channel().configuration(), StreamerSubscriber.class);
        StreamerOptions streamerOpts = streamerOptions(options);
        StreamerSubscriber<R, String> subscriber = new StreamerSubscriber<>(
                batchSender, partitionAwarenessProvider, streamerOpts, log, tbl.channel().metrics());

        publisher.subscribe(subscriber);

        return subscriber.completionFuture();
    }

    private static StreamerOptions streamerOptions(DataStreamerOptions options) {
        return new StreamerOptions() {
            @Override
            public int batchSize() {
                return options.batchSize();
            }

            @Override
            public int perNodeParallelOperations() {
                return options.perNodeParallelOperations();
            }

            @Override
            public int autoFlushFrequency() {
                return options.autoFlushFrequency();
            }

            @Override
            public int retryLimit() {
                return options.retryLimit();
            }
        };
    }
}
