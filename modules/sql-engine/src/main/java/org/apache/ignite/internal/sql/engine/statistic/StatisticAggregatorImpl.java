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

package org.apache.ignite.internal.sql.engine.statistic;

import static java.util.concurrent.CompletableFuture.allOf;
import static org.apache.ignite.lang.ErrorGroups.Replicator.REPLICA_UNAVAILABLE_ERR;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongObjectImmutablePair;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.network.ChannelType;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessageGroup;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.message.GetEstimatedSizeWithLastModifiedTsRequest;
import org.apache.ignite.internal.partition.replicator.network.message.GetEstimatedSizeWithLastModifiedTsResponse;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.jetbrains.annotations.Nullable;

/** Statistic aggregator. */
public class StatisticAggregatorImpl implements
        StatisticAggregator<InternalTable, CompletableFuture<LongObjectImmutablePair<HybridTimestamp>>> {
    private final PlacementDriver placementDriver;
    private final Supplier<HybridTimestamp> currentClock;
    private final MessagingService messagingService;
    private static final PartitionReplicationMessagesFactory PARTITION_REPLICATION_MESSAGES_FACTORY =
            new PartitionReplicationMessagesFactory();
    private static final long REQUEST_ESTIMATION_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(3);
    private final Map<Integer, Map<Integer, CompletableFuture<LongObjectImmutablePair<HybridTimestamp>>>>
            requestsCompletion = new HashMap<>();
    private final Map<Integer, CompletableFuture<LongObjectImmutablePair<HybridTimestamp>>>
            requestsInbound = new ConcurrentHashMap<>();

    /** Constructor. */
    public StatisticAggregatorImpl(
            PlacementDriver placementDriver,
            Supplier<HybridTimestamp> currentClock,
            MessagingService messagingService
    ) {
        this.placementDriver = placementDriver;
        this.currentClock = currentClock;
        this.messagingService = messagingService;

        messagingService.addMessageHandler(PartitionReplicationMessageGroup.class, this::handleMessage);
    }

    private void handleMessage(NetworkMessage message, InternalClusterNode sender, @Nullable Long correlationId) {
        if (message instanceof GetEstimatedSizeWithLastModifiedTsResponse) {
            GetEstimatedSizeWithLastModifiedTsResponse response
                    = (GetEstimatedSizeWithLastModifiedTsResponse) message;

            CompletableFuture<LongObjectImmutablePair<HybridTimestamp>> fut = requestsCompletion.get(response.tableId())
                    .get(response.partitionId());

            fut.complete(LongObjectImmutablePair.of(response.estimatedSize(), response.lastModified()));
        }
    }

    /**
     * Returns the pair<<em>last modification timestamp</em>, <em>estimated size</em>> of this table.
     *
     * @return Estimated size of this table with last modification timestamp.
     */
    @Override
    public CompletableFuture<LongObjectImmutablePair<HybridTimestamp>> estimatedSizeWithLastUpdate(InternalTable table) {
        //return requestsInbound.computeIfAbsent(table.tableId(), t -> estimatedSizeWithLastUpdateInternal(table));
        return estimatedSizeWithLastUpdateInternal(table);
    }

    private CompletableFuture<LongObjectImmutablePair<HybridTimestamp>> estimatedSizeWithLastUpdateInternal(InternalTable table) {
        int partitions = table.partitions();

        Map<String, Set<Integer>> peers = new HashMap<>();

        HybridTimestamp clockNow = currentClock.get();

        Map<Integer, CompletableFuture<LongObjectImmutablePair<HybridTimestamp>>> partRequests = new Int2ObjectOpenHashMap<>();

        for (int p = 0; p < partitions; ++p) {
            ReplicationGroupId replicationGroupId = table.targetReplicationGroupId(p);

            ReplicaMeta repl = placementDriver.getCurrentPrimaryReplica(replicationGroupId, clockNow);

            if (repl != null && repl.getLeaseholder() != null) {
                Set<Integer> peer = peers.computeIfAbsent(repl.getLeaseholder(), k -> new HashSet<>());
                peer.add(p);
            } else {
                return CompletableFuture.failedFuture(
                        new IgniteInternalException(REPLICA_UNAVAILABLE_ERR, "Failed to get the primary replica"
                        + " [replicationGroupId=" + replicationGroupId + ']'));
            }

            partRequests.put(p, new CompletableFuture<>());
        }

        if (peers.isEmpty()) {
            return CompletableFuture.failedFuture(new IgniteInternalException(Common.INTERNAL_ERR, "Table peers are not available"
                    + " [tableId=" + table.tableId() + ']'));
        }

        requestsCompletion.put(table.tableId(), partRequests);

        CompletableFuture<?>[] sendFutures = peers.entrySet().stream()
                .map(ent -> {
                    GetEstimatedSizeWithLastModifiedTsRequest request =
                            PARTITION_REPLICATION_MESSAGES_FACTORY.getEstimatedSizeWithLastModifiedTsRequest()
                            .tableId(table.tableId()).partitions(ent.getValue()).build();

                    System.err.println("!!!!send: getEstimatedSizeWithLastModifiedTsRequest " + table.tableId());

                    return messagingService.send(ent.getKey(), ChannelType.DEFAULT, request);
                })
                .toArray(CompletableFuture[]::new);

        CompletableFuture<LongObjectImmutablePair<HybridTimestamp>>[] responses = requestsCompletion.get(
                table.tableId()).values().toArray(new CompletableFuture[0]);

         return allOf(sendFutures)
                 .orTimeout(REQUEST_ESTIMATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                 .thenCompose(r -> allOf(responses))
                 .orTimeout(REQUEST_ESTIMATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                 .thenApply(unused -> {
                    HybridTimestamp last = HybridTimestamp.MIN_VALUE;
                    long count = 0L;

                    for (CompletableFuture<LongObjectImmutablePair<HybridTimestamp>> requestFut : responses) {
                        LongObjectImmutablePair<HybridTimestamp> partitionState = requestFut.join();

                        if (partitionState.value().compareTo(last) > 0) {
                            last = partitionState.value();
                        }

                        count += partitionState.keyLong();
                    }

                    return LongObjectImmutablePair.of(count, last);
                })
                .whenComplete((ignore, ex) -> {
                    requestsCompletion.remove(table.tableId());
                    requestsInbound.remove(table.tableId());
                });
    }
}
