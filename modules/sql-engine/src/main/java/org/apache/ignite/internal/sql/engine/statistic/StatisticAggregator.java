package org.apache.ignite.internal.sql.engine.statistic;

import static java.util.concurrent.CompletableFuture.allOf;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongObjectImmutablePair;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.message.GetEstimatedSizeWithLastModifiedTsRequest;
import org.apache.ignite.internal.partition.replicator.network.message.GetEstimatedSizeWithLastModifiedTsResponse;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.table.InternalTable;
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
    public CompletableFuture<LongObjectImmutablePair<HybridTimestamp>> estimatedSizeWithLastUpdate(InternalTable table) {
        assert messagingService != null;

        int partitions = table.partitions();

        Map<Integer, String> peers = new HashMap<>();

        for (int p = 0; p < partitions; ++p) {
            ReplicaMeta repl = placementDriver.getCurrentPrimaryReplica(
                    table.targetReplicationGroupId(p), currentClock.get());

            if (repl != null) {
                peers.put(p, repl.getLeaseholder());
            } else {
                //assert false; // !!! delete
            }
        }

        if (peers.isEmpty()) {
            return CompletableFuture.completedFuture(LongObjectImmutablePair.of(0, HybridTimestamp.MIN_VALUE));
        }

/*        GetEstimatedSizeWithLastModifiedTsRequest request = PARTITION_REPLICATION_MESSAGES_FACTORY.getEstimatedSizeWithLastModifiedTsRequest()
                .tableId(table.tableId()).build();*/

        CompletableFuture<LongObjectImmutablePair<HybridTimestamp>>[] invokeFutures = peers.entrySet().stream()
                .map(ent -> {
                    GetEstimatedSizeWithLastModifiedTsRequest request = PARTITION_REPLICATION_MESSAGES_FACTORY.getEstimatedSizeWithLastModifiedTsRequest()
                            .tableId(table.tableId()).partitionId(ent.getKey()).build();

                    return messagingService.invoke(ent.getValue(), request, REQUEST_ESTIMATION_TIMEOUT_MILLIS)
                            .thenApply(response -> {
                                assert response instanceof GetEstimatedSizeWithLastModifiedTsResponse : response;

                                GetEstimatedSizeWithLastModifiedTsResponse response0 = (GetEstimatedSizeWithLastModifiedTsResponse) response;

                                return LongObjectImmutablePair.of(response0.estimatedSize(), response0.ts());
                            })
                            .exceptionally(unused -> LongObjectImmutablePair.of(0, HybridTimestamp.MIN_VALUE));
                })
                .toArray(CompletableFuture[]::new);;

        return allOf(invokeFutures).thenApply(unused -> {
            HybridTimestamp last = HybridTimestamp.MIN_VALUE;
            long count = 0L;

            System.err.println("invokeFutures size: " + invokeFutures.length);

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
                System.err.println("!!!!! requestFut " + result.keyLong());
            }

            System.err.println("!!!!! requestFut final:" + count);
            return LongObjectImmutablePair.of(count, last);
        });
    }
}
