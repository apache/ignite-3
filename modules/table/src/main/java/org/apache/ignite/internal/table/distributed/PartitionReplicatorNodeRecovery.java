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

package org.apache.ignite.internal.table.distributed;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.IntFunction;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.message.HasDataRequest;
import org.apache.ignite.internal.table.distributed.message.HasDataResponse;
import org.apache.ignite.internal.utils.RebalanceUtil;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.MessagingService;

/**
 * Code specific to recovering a partition replicator group node. This includes a case when we lost metadata
 * that is required for the replication protocol (for instance, for RAFT it's about group metadata).
 */
class PartitionReplicatorNodeRecovery {
    private static final long QUERY_DATA_NODES_COUNT_TIMEOUT = TimeUnit.SECONDS.toMillis(3);

    private static final TableMessagesFactory TABLE_MESSAGES_FACTORY = new TableMessagesFactory();

    private final MetaStorageManager metaStorageManager;

    private final MessagingService messagingService;

    /** Resolver that resolves a node consistent ID to cluster node. */
    private final Function<String, ClusterNode> clusterNodeResolver;

    private final IntFunction<TableImpl> tableSupplier;

    PartitionReplicatorNodeRecovery(
            MetaStorageManager metaStorageManager,
            MessagingService messagingService,
            Function<String, ClusterNode> clusterNodeResolver,
            IntFunction<TableImpl> tableSupplier) {
        this.metaStorageManager = metaStorageManager;
        this.messagingService = messagingService;
        this.clusterNodeResolver = clusterNodeResolver;
        this.tableSupplier = tableSupplier;
    }

    /**
     * Starts the component.
     */
    void start() {
        addMessageHandler();
    }

    private void addMessageHandler() {
        messagingService.addMessageHandler(TableMessageGroup.class, (message, sender, correlationId) -> {
            if (message instanceof HasDataRequest) {
                // This message queries if a node has any data for a specific partition of a table
                assert correlationId != null;

                HasDataRequest msg = (HasDataRequest) message;

                int tableId = msg.tableId();
                int partitionId = msg.partitionId();

                boolean contains = false;

                TableImpl table = tableSupplier.apply(tableId);

                if (table != null) {
                    MvTableStorage storage = table.internalTable().storage();

                    MvPartitionStorage mvPartition = storage.getMvPartition(partitionId);

                    // If node's recovery process is incomplete (no partition storage), then we consider this node's
                    // partition storage empty.
                    if (mvPartition != null) {
                        contains = mvPartition.closestRowId(RowId.lowestRowId(partitionId)) != null;
                    }
                }

                messagingService.respond(sender, TABLE_MESSAGES_FACTORY.hasDataResponse().result(contains).build(), correlationId);
            }
        });
    }

    /**
     * Returns a future that completes with a decision: should we start the corresponding group locally or not.
     *
     * @param tablePartitionId ID of the table partition.
     * @param internalTable Table we are working with.
     * @param newConfiguration New configuration that is going to be applied if we'll start the group.
     * @param localMemberAssignment Assignment of this node in this group.
     */
    CompletableFuture<Boolean> shouldStartGroup(
            TablePartitionId tablePartitionId,
            InternalTable internalTable,
            PeersAndLearners newConfiguration,
            Assignment localMemberAssignment
    ) {
        int tableId = tablePartitionId.tableId();
        int partId = tablePartitionId.partitionId();

        // If Raft is running in in-memory mode or the PDS has been cleared, we need to remove the current node
        // from the Raft group in order to avoid the double vote problem.
        // <MUTED> See https://issues.apache.org/jira/browse/IGNITE-16668 for details.
        // TODO: https://issues.apache.org/jira/browse/IGNITE-19046 Restore "|| !hasData"
        if (internalTable.storage().isVolatile()) {
            // No majority and not a full partition restart - need to restart nodes
            // with current partition.
            return queryDataNodesCount(tableId, partId, newConfiguration.peers())
                        .thenApply(dataNodesCount -> {
                            boolean fullPartitionRestart = dataNodesCount == 0;

                            if (fullPartitionRestart) {
                                return true;
                            }

                            boolean majorityAvailable = dataNodesCount >= (newConfiguration.peers().size() / 2) + 1;

                            if (majorityAvailable) {
                                RebalanceUtil.startPeerRemoval(tablePartitionId, localMemberAssignment, metaStorageManager);

                                return false;
                            } else {
                                // No majority and not a full partition restart - need to restart nodes
                                // with current partition.
                                String msg = "Unable to start partition " + partId + ". Majority not available.";

                                throw new IgniteInternalException(msg);
                            }
                        });
        }

        return completedFuture(true);
    }

    /**
     * Calculates the quantity of the data nodes for the partition of the table.
     *
     * @param tblId Table id.
     * @param partId Partition id.
     * @param peers Raft peers.
     * @return A future that will hold the quantity of data nodes.
     */
    private CompletableFuture<Long> queryDataNodesCount(int tblId, int partId, Collection<Peer> peers) {
        HasDataRequest request = TABLE_MESSAGES_FACTORY.hasDataRequest().tableId(tblId).partitionId(partId).build();

        //noinspection unchecked
        CompletableFuture<Boolean>[] requestFutures = peers.stream()
                .map(Peer::consistentId)
                .map(clusterNodeResolver)
                .filter(Objects::nonNull)
                .map(node -> messagingService
                        .invoke(node, request, QUERY_DATA_NODES_COUNT_TIMEOUT)
                        .thenApply(response -> {
                            assert response instanceof HasDataResponse : response;

                            return ((HasDataResponse) response).result();
                        })
                        .exceptionally(unused -> false))
                .toArray(CompletableFuture[]::new);

        return allOf(requestFutures)
                .thenApply(unused -> Arrays.stream(requestFutures).filter(CompletableFuture::join).count());
    }
}
