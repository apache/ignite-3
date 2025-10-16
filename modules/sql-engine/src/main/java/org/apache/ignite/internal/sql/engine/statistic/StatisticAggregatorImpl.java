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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.message.GetEstimatedSizeWithLastModifiedTsRequest;
import org.apache.ignite.internal.partition.replicator.network.message.GetEstimatedSizeWithLastModifiedTsResponse;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.lang.ErrorGroups.Common;

/** Statistic aggregator. */
public class StatisticAggregatorImpl implements
        StatisticAggregator<InternalTable, CompletableFuture<LongObjectImmutablePair<HybridTimestamp>>> {
    private final PlacementDriver placementDriver;
    private final Supplier<HybridTimestamp> currentClock;
    private final MessagingService messagingService;
    private static final PartitionReplicationMessagesFactory PARTITION_REPLICATION_MESSAGES_FACTORY =
            new PartitionReplicationMessagesFactory();
    private static final long REQUEST_ESTIMATION_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(3);

    /** Constructor. */
    public StatisticAggregatorImpl(
            PlacementDriver placementDriver,
            Supplier<HybridTimestamp> currentClock,
            MessagingService messagingService
    ) {
        this.placementDriver = placementDriver;
        this.currentClock = currentClock;
        this.messagingService = messagingService;
    }

    /**
     * Returns the pair<<em>last modification timestamp</em>, <em>estimated size</em>> of this table.
     *
     * @return Estimated size of this table with last modification timestamp.
     */
    @Override
    public CompletableFuture<LongObjectImmutablePair<HybridTimestamp>> estimatedSizeWithLastUpdate(InternalTable table) {
        assert messagingService != null;

        int partitions = table.partitions();

        Map<Integer, String> peers = new Int2ObjectOpenHashMap<>(partitions);

        for (int p = 0; p < partitions; ++p) {
            ReplicationGroupId replicationGroupId = table.targetReplicationGroupId(p);

            ReplicaMeta repl = placementDriver.getCurrentPrimaryReplica(
                    replicationGroupId, currentClock.get());

            if (repl != null && repl.getLeaseholder() != null) {
                peers.put(p, repl.getLeaseholder());
            } else {
                return CompletableFuture.failedFuture(
                        new IgniteInternalException(REPLICA_UNAVAILABLE_ERR, "Failed to get the primary replica"
                        + " [replicationGroupId=" + replicationGroupId + ']'));
            }
        }

        if (peers.isEmpty()) {
            return CompletableFuture.failedFuture(new IgniteInternalException(Common.INTERNAL_ERR, "Table peers are not available"
                    + " [tableId=" + table.tableId() + ']'));
        }

        CompletableFuture<LongObjectImmutablePair<HybridTimestamp>>[] invokeFutures = peers.entrySet().stream()
                .map(ent -> {
                    GetEstimatedSizeWithLastModifiedTsRequest request =
                            PARTITION_REPLICATION_MESSAGES_FACTORY.getEstimatedSizeWithLastModifiedTsRequest()
                            .tableId(table.tableId()).partitionId(ent.getKey()).build();

                    return messagingService.invoke(ent.getValue(), request, REQUEST_ESTIMATION_TIMEOUT_MILLIS)
                            .thenApply(networkMessage -> {
                                assert networkMessage instanceof GetEstimatedSizeWithLastModifiedTsResponse : networkMessage;

                                GetEstimatedSizeWithLastModifiedTsResponse response
                                        = (GetEstimatedSizeWithLastModifiedTsResponse) networkMessage;

                                return LongObjectImmutablePair.of(response.estimatedSize(), response.lastModified());
                            });
                })
                .toArray(CompletableFuture[]::new);

        return allOf(invokeFutures).thenApply(unused -> {
            HybridTimestamp last = HybridTimestamp.MIN_VALUE;
            long count = 0L;

            for (CompletableFuture<LongObjectImmutablePair<HybridTimestamp>> requestFut : invokeFutures) {
                LongObjectImmutablePair<HybridTimestamp> partitionState = requestFut.join();

                if (partitionState.value().compareTo(last) > 0) {
                    last = partitionState.value();
                }

                count += partitionState.keyLong();
            }

            return LongObjectImmutablePair.of(count, last);
        });
    }
}
