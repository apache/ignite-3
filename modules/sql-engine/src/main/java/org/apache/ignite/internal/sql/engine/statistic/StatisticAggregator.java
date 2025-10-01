package org.apache.ignite.internal.sql.engine.statistic;

import it.unimi.dsi.fastutil.longs.LongObjectImmutablePair;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.sql.engine.message.GetEstimatedSizeWithLastModifiedTsRequest;
import org.apache.ignite.internal.sql.engine.message.SqlQueryMessageGroup;
import org.apache.ignite.internal.sql.engine.message.SqlQueryMessagesFactory;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.lease.LeaseInfo;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableViewInternal;
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

    public StatisticAggregator(
            PlacementDriver placementDriver,
            Supplier<HybridTimestamp> currentClock,
            TableManager tableManager
    ) {
        this.placementDriver = placementDriver;
        this.currentClock = currentClock;
        this.tableManager = tableManager;
    }

    public void nodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public void messaging(MessagingService messagingService) {
        this.messagingService = messagingService;

        messagingService.addMessageHandler(SqlQueryMessageGroup.class, (message, sender, correlationId) -> {
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
        });
    }

    private void onMessage(InternalClusterNode node, GetEstimatedSizeWithLastModifiedTsRequest msg) {
        assert node != null && msg != null;
    }

    /**
     * Returns the pair<<em>last modification timestamp</em>, <em>estimated size</em>> of this table.
     *
     * @return Estimated size of this table with last modification timestamp.
     */
    public LongObjectImmutablePair<HybridTimestamp> estimatedSizeWithLastUpdate(InternalTable table) {
        int partitions = table.partitions();

        Set<String> peers = new HashSet<>();

        for (int p = 0; p < partitions; ++p) {
            ReplicaMeta repl = placementDriver.getCurrentPrimaryReplica(
                    table.targetReplicationGroupId(p), currentClock.get());

            if (repl != null) {
                peers.add(repl.getLeaseholder());
            } else {
                assert false; // !!! delete
            }
        }

        GetEstimatedSizeWithLastModifiedTsRequest request = MSG_FACTORY.getEstimatedSizeWithLastModifiedTsRequest()
                .tableId(table.tableId()).build();

        for (String node : peers) {
            messageService.send(node, request);
        }
    }
}
