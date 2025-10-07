package org.apache.ignite.internal.sql.engine.statistic;

import static java.util.concurrent.CompletableFuture.allOf;
import static org.apache.ignite.internal.util.IgniteUtils.newHashMap;
import static org.apache.ignite.lang.ErrorGroups.Replicator.REPLICA_UNAVAILABLE_ERR;

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
import org.jetbrains.annotations.Nullable;

public class StatisticAggregator {
    private final PlacementDriver placementDriver;
    private final Supplier<HybridTimestamp> currentClock;
    private @Nullable MessagingService messagingService;
    private static final PartitionReplicationMessagesFactory PARTITION_REPLICATION_MESSAGES_FACTORY =
            new PartitionReplicationMessagesFactory();
    private static final long REQUEST_ESTIMATION_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(3);

    public StatisticAggregator(
            PlacementDriver placementDriver,
            Supplier<HybridTimestamp> currentClock
    ) {
        this.placementDriver = placementDriver;
        this.currentClock = currentClock;
    }

    public void messaging(MessagingService messagingService) {
        this.messagingService = messagingService;
    }

    /**
     * Returns the pair<<em>last modification timestamp</em>, <em>estimated size</em>> of this table.
     *
     * @return Estimated size of this table with last modification timestamp.
     */
    CompletableFuture<LongObjectImmutablePair<HybridTimestamp>> estimatedSizeWithLastUpdate(InternalTable table) {
        assert messagingService != null;

        int partitions = table.partitions();

        Map<Integer, String> peers = newHashMap(partitions);

        for (int p = 0; p < partitions; ++p) {
            ReplicationGroupId replicationGroupId = table.targetReplicationGroupId(p);

            ReplicaMeta repl = placementDriver.getCurrentPrimaryReplica(
                    replicationGroupId, currentClock.get());

            if (repl != null && repl.getLeaseholder() != null) {
                peers.put(p, repl.getLeaseholder());
            } else {
                System.err.println("!!!! Failed to get the primary replica");
                CompletableFuture.failedFuture(new IgniteInternalException(REPLICA_UNAVAILABLE_ERR, "Failed to get the primary replica"
                        + " [replicationGroupId=" + replicationGroupId + ']'));
            }
        }

        if (peers.isEmpty()) {
            throw new IgniteInternalException(Common.INTERNAL_ERR, "Table peers are not available"
                    + " [tableId=" + table.tableId() + ']');
        }

        CompletableFuture<LongObjectImmutablePair<HybridTimestamp>>[] invokeFutures = peers.entrySet().stream()
                .map(ent -> {
                    GetEstimatedSizeWithLastModifiedTsRequest request = PARTITION_REPLICATION_MESSAGES_FACTORY.getEstimatedSizeWithLastModifiedTsRequest()
                            .tableId(table.tableId()).partitionId(ent.getKey()).build();

                    return messagingService.invoke(ent.getValue(), request, REQUEST_ESTIMATION_TIMEOUT_MILLIS)
                            .thenApply(networkMessage -> {
                                assert networkMessage instanceof GetEstimatedSizeWithLastModifiedTsResponse : networkMessage;

                                GetEstimatedSizeWithLastModifiedTsResponse response = (GetEstimatedSizeWithLastModifiedTsResponse) networkMessage;

                                return LongObjectImmutablePair.of(response.estimatedSize(), response.lastModified());
                            });
                })
                .toArray(CompletableFuture[]::new);;

        return allOf(invokeFutures).thenApply(unused -> {
            HybridTimestamp last = HybridTimestamp.MIN_VALUE;
            long count = 0L;

            System.err.println("!!!! invokeFutures " + invokeFutures.length);

            for (CompletableFuture<LongObjectImmutablePair<HybridTimestamp>> requestFut : invokeFutures) {
                LongObjectImmutablePair<HybridTimestamp> result = requestFut.join();

                if (last == null) {
                    last = result.value();
                } else {
                    if (result.value().compareTo(last) > 0) {
                        last = result.value();
                    }
                }
                count += result.keyLong();
            }

            return LongObjectImmutablePair.of(count, last);
        });
    }
}
