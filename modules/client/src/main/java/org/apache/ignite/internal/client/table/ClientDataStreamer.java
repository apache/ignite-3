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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Publisher;
import java.util.function.Function;
import org.apache.ignite.client.RetryLimitPolicy;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.internal.client.ClientUtils;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.client.proto.StreamerReceiverSerializer;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.streamer.StreamerBatchSender;
import org.apache.ignite.internal.streamer.StreamerOptions;
import org.apache.ignite.internal.streamer.StreamerPartitionAwarenessProvider;
import org.apache.ignite.internal.streamer.StreamerSubscriber;
import org.apache.ignite.marshalling.Marshaller;
import org.apache.ignite.table.DataStreamerItem;
import org.apache.ignite.table.DataStreamerOperationType;
import org.apache.ignite.table.DataStreamerOptions;
import org.jetbrains.annotations.Nullable;

/**
 * Client data streamer.
 */
class ClientDataStreamer {
    static <T> CompletableFuture<Void> streamData(
            Publisher<DataStreamerItem<T>> publisher,
            DataStreamerOptions options,
            StreamerBatchSender<T, Integer, Void> batchSender,
            StreamerPartitionAwarenessProvider<T, Integer> partitionAwarenessProvider,
            ClientTable tbl) {
        return streamData(
                publisher,
                DataStreamerItem::get,
                DataStreamerItem::get,
                x -> x.operationType() == DataStreamerOperationType.REMOVE,
                options,
                batchSender,
                null,
                partitionAwarenessProvider,
                tbl);
    }

    // T = key, E = element, V = payload, R = result, A = Argument.
    @SuppressWarnings("resource")
    static <T, E, V, R, A> CompletableFuture<Void> streamData(
            Publisher<E> publisher,
            Function<E, T> keyFunc,
            Function<E, V> payloadFunc,
            Function<E, Boolean> deleteFunc,
            DataStreamerOptions options,
            StreamerPartitionAwarenessProvider<T, Integer> partitionAwarenessProvider,
            ClientTable tbl,
            @Nullable Flow.Subscriber<R> resultSubscriber,
            List<DeploymentUnit> deploymentUnits,
            String receiverClassName,
            A receiverArgs,
            @Nullable Marshaller<A, byte[]> receiverArgsMarshaller
    ) {
        StreamerBatchSender<V, Integer, R> batchSender = (partitionId, items, deleted) ->
                tbl.getPartitionAssignment().thenCompose(
                        partitionAssignment -> tbl.channel().serviceAsync(
                                ClientOp.STREAMER_WITH_RECEIVER_BATCH_SEND,
                                out -> {
                                    assert deleted == null || deleted.isEmpty() : "Deletion is not supported with receiver.";

                                    ClientMessagePacker w = out.out();
                                    w.packInt(tbl.tableId());
                                    w.packInt(partitionId);
                                    w.packDeploymentUnits(deploymentUnits);
                                    w.packBoolean(resultSubscriber != null); // receiveResults

                                    StreamerReceiverSerializer.serializeReceiverInfoOnClient(
                                            w, receiverClassName, receiverArgs, receiverArgsMarshaller, items);
                                },
                                in -> resultSubscriber != null
                                        ? StreamerReceiverSerializer.deserializeReceiverResultsOnClient(in.in())
                                        : null,
                                partitionAssignment.get(partitionId),
                                new RetryLimitPolicy().retryLimit(options.retryLimit()),
                                false)
                );

        return streamData(
                publisher,
                keyFunc,
                payloadFunc,
                deleteFunc,
                options,
                batchSender,
                resultSubscriber,
                partitionAwarenessProvider,
                tbl);
    }


    // T = key, E = element, V = payload, R = result.
    @SuppressWarnings("resource")
    private static <T, E, V, R> CompletableFuture<Void> streamData(
            Publisher<E> publisher,
            Function<E, T> keyFunc,
            Function<E, V> payloadFunc,
            Function<E, Boolean> deleteFunc,
            DataStreamerOptions options,
            StreamerBatchSender<V, Integer, R> batchSender,
            @Nullable Flow.Subscriber<R> resultSubscriber,
            StreamerPartitionAwarenessProvider<T, Integer> partitionAwarenessProvider,
            ClientTable tbl) {
        IgniteLogger log = ClientUtils.logger(tbl.channel().configuration(), StreamerSubscriber.class);
        StreamerOptions streamerOpts = streamerOptions(options);
        StreamerSubscriber<T, E, V, R, Integer> subscriber = new StreamerSubscriber<>(
                batchSender,
                resultSubscriber,
                keyFunc,
                payloadFunc,
                deleteFunc,
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
            public int autoFlushInterval() {
                return options.autoFlushInterval();
            }
        };
    }
}
