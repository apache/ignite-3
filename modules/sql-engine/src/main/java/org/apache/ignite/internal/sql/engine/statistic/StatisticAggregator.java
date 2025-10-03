package org.apache.ignite.internal.sql.engine.statistic;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.stream.Collectors.toList;

import it.unimi.dsi.fastutil.longs.LongObjectImmutablePair;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessageGroup;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.message.DataPresence;
import org.apache.ignite.internal.partition.replicator.network.message.HasDataResponse;
import org.apache.ignite.internal.partition.replicator.network.replication.GetEstimatedSizeWithLastModifiedTsRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.GetEstimatedSizeWithLastModifiedTsResponse;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.sql.engine.message.SqlQueryMessagesFactory;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.jetbrains.annotations.Nullable;

public class StatisticAggregator {
    private static final IgniteLogger LOG = Loggers.forClass(StatisticAggregator.class);
    private final PlacementDriver placementDriver;
    private final Supplier<HybridTimestamp> currentClock;
    private @Nullable MessagingService messagingService;
    private @Nullable String nodeName;
    private final TableManager tableManager;
    private static final SqlQueryMessagesFactory MSG_FACTORY = new SqlQueryMessagesFactory();
    private static final PartitionReplicationMessagesFactory PARTITION_REPLICATION_MESSAGES_FACTORY =
            new PartitionReplicationMessagesFactory();
    private TopologyService topologyService;
    private static final long REQUEST_ESTIMATION_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(3);

    public StatisticAggregator(
            PlacementDriver placementDriver,
            Supplier<HybridTimestamp> currentClock,
            TableManager tableManager,
            TopologyService topologyService
    ) {
        this.placementDriver = placementDriver;
        this.currentClock = currentClock;
        this.tableManager = tableManager;
        this.topologyService = topologyService;
    }

    public void nodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public void messaging(MessagingService messagingService) {
        this.messagingService = messagingService;

        messagingService.addMessageHandler(PartitionReplicationMessageGroup.class, this::handleMessage);

/*        messagingService.addMessageHandler(SqlQueryMessageGroup.class, (message, sender, correlationId) -> {
            if (message instanceof GetEstimatedSizeWithLastModifiedTsRequest) {
                GetEstimatedSizeWithLastModifiedTsRequest msg = (GetEstimatedSizeWithLastModifiedTsRequest) message;

                TableViewInternal tableView = tableManager.cachedTable(msg.tableId());

                if (tableView == null) {
                    LOG.debug("No table found to update statistics [id={}].", msg.tableId());

                    return;
                }

                InternalTable table = tableView.internalTable();

                for (int p = 0 ; p < table.partitions(); ++p) {
                    MvPartitionStorage mvPartition = table.storage().getMvPartition(p);

                    if (mvPartition != null) {
                        LeaseInfo info = mvPartition.leaseInfo();

                        if (info != null) {
                            if (info.primaryReplicaNodeName().equals(nodeName)) {
                                mvPartition.estimatedSize();
                            }
                        }
                    }
                }

                storageAccessExecutor.execute(() -> handleHasDataRequest(msg, sender, correlationId));
            }
        });*/

        //InternalClusterNode localNode = clusterSrvc.topologyService().localMember();
        //String nodeName = localNode.name();
    }

    private void handleMessage(NetworkMessage message, InternalClusterNode sender, @Nullable Long correlationId) {
        if (message instanceof GetEstimatedSizeWithLastModifiedTsResponse) {
            System.err.println("!!!!");
        }
    }

    /**
     * Returns the pair<<em>last modification timestamp</em>, <em>estimated size</em>> of this table.
     *
     * @return Estimated size of this table with last modification timestamp.
     */
    public CompletableFuture<LongObjectImmutablePair<HybridTimestamp>> estimatedSizeWithLastUpdate(InternalTable table) {
        assert messagingService != null;

        int partitions = table.partitions();

        Set<String> peers = new HashSet<>();

        for (int p = 0; p < partitions; ++p) {
            ReplicaMeta repl = placementDriver.getCurrentPrimaryReplica(
                    table.targetReplicationGroupId(p), currentClock.get());

            if (repl != null) {
                peers.add(repl.getLeaseholder());
            } else {
                //assert false; // !!! delete
            }
        }

        if (peers.isEmpty()) {
            return CompletableFuture.completedFuture(LongObjectImmutablePair.of(0, HybridTimestamp.MIN_VALUE));
        }

        GetEstimatedSizeWithLastModifiedTsRequest request = PARTITION_REPLICATION_MESSAGES_FACTORY.getEstimatedSizeWithLastModifiedTsRequest()
                .tableId(table.tableId()).build();

/*        for (String p : peers) {
            @Nullable InternalClusterNode cons = topologyService.getByConsistentId(p);
            if (cons == null)
                continue;

            CompletableFuture<NetworkMessage> fut = messagingService.invoke(cons, request, REQUEST_ESTIMATION_TIMEOUT_MILLIS);

            fut.thenApply(res -> {
                System.err.println();
                return null;
            }).exceptionally(ex -> {
                System.err.println();
                return null;
            });

            fut.join();
        }*/

        CompletableFuture<LongObjectImmutablePair<HybridTimestamp>>[] invokeFutures = peers.stream()
                .map(topologyService::getByConsistentId)
                .filter(Objects::nonNull)
                .map(node -> messagingService
                        .invoke(node, request, REQUEST_ESTIMATION_TIMEOUT_MILLIS)
                        .thenApply(response -> {
                            assert response instanceof GetEstimatedSizeWithLastModifiedTsResponse : response;

                            GetEstimatedSizeWithLastModifiedTsResponse response0 = (GetEstimatedSizeWithLastModifiedTsResponse) response;

                            return LongObjectImmutablePair.of(response0.estimatedSize(), response0.ts());
                        })
                        .exceptionally(unused -> LongObjectImmutablePair.of(0, HybridTimestamp.MIN_VALUE)))
                .toArray(CompletableFuture[]::new);

        return allOf(invokeFutures).thenApply(unused -> {
            HybridTimestamp last = HybridTimestamp.MIN_VALUE;
            long count = 0L;

            for (CompletableFuture<?> fut : invokeFutures) {
                CompletableFuture<LongObjectImmutablePair<HybridTimestamp>> requestFut =
                        (CompletableFuture<LongObjectImmutablePair<HybridTimestamp>>) fut;
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
