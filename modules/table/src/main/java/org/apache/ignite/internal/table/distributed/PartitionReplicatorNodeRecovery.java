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
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.trueCompletedFuture;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.IntFunction;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.message.HasDataRequest;
import org.apache.ignite.internal.table.distributed.message.HasDataResponse;
import org.apache.ignite.internal.utils.RebalanceUtilEx;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.TopologyEventHandler;
import org.apache.ignite.network.TopologyService;

/**
 * Code specific to recovering a partition replicator group node. This includes a case when we lost metadata
 * that is required for the replication protocol (for instance, for RAFT it's about group metadata).
 */
class PartitionReplicatorNodeRecovery {
    private static final long QUERY_DATA_NODES_COUNT_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(3);

    private static final long PEERS_IN_TOPOLOGY_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(3);

    private static final TableMessagesFactory TABLE_MESSAGES_FACTORY = new TableMessagesFactory();

    private final MetaStorageManager metaStorageManager;

    private final MessagingService messagingService;

    private final TopologyService topologyService;

    /** Obtains a TableImpl instance by a table ID. */
    private final IntFunction<TableViewInternal> tableById;

    PartitionReplicatorNodeRecovery(
            MetaStorageManager metaStorageManager,
            MessagingService messagingService,
            TopologyService topologyService,
            IntFunction<TableViewInternal> tableById
    ) {
        this.metaStorageManager = metaStorageManager;
        this.messagingService = messagingService;
        this.topologyService = topologyService;
        this.tableById = tableById;
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

                boolean storageHasData = false;

                TableViewInternal table = tableById.apply(tableId);

                if (table != null) {
                    MvTableStorage storage = table.internalTable().storage();

                    MvPartitionStorage mvPartition = storage.getMvPartition(partitionId);

                    // If node's recovery process is incomplete (no partition storage), then we consider this node's
                    // partition storage empty.
                    if (mvPartition != null) {
                        storageHasData = mvPartition.closestRowId(RowId.lowestRowId(partitionId)) != null;
                    }
                }

                messagingService.respond(sender, TABLE_MESSAGES_FACTORY.hasDataResponse().result(storageHasData).build(), correlationId);
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
        // If Raft is running in in-memory mode or the PDS has been cleared, we need to remove the current node
        // from the Raft group in order to avoid the double vote problem.
        if (mightNeedGroupRecovery(internalTable)) {
            return performGroupRecovery(tablePartitionId, newConfiguration, localMemberAssignment);
        }

        return trueCompletedFuture();
    }

    private static boolean mightNeedGroupRecovery(InternalTable internalTable) {
        // <MUTED> See https://issues.apache.org/jira/browse/IGNITE-16668 for details.
        // TODO: https://issues.apache.org/jira/browse/IGNITE-19046 Restore "|| !hasData"
        return internalTable.storage().isVolatile();
    }

    private CompletableFuture<Boolean> performGroupRecovery(
            TablePartitionId tablePartitionId,
            PeersAndLearners newConfiguration,
            Assignment localMemberAssignment
    ) {
        int tableId = tablePartitionId.tableId();
        int partId = tablePartitionId.partitionId();

        // No majority and not a full partition restart - need to 'remove, then add' nodes
        // with current partition.
        return waitForPeersAndQueryDataNodesCount(tableId, partId, newConfiguration.peers())
                .thenApply(dataNodesCount -> {
                    boolean fullPartitionRestart = dataNodesCount == 0;

                    if (fullPartitionRestart) {
                        return true;
                    }

                    boolean majorityAvailable = dataNodesCount >= (newConfiguration.peers().size() / 2) + 1;

                    if (majorityAvailable) {
                        RebalanceUtilEx.startPeerRemoval(tablePartitionId, localMemberAssignment, metaStorageManager);

                        return false;
                    } else {
                        // No majority and not a full partition restart - need to restart nodes
                        // with current partition.
                        String msg = "Unable to start partition " + partId + ". Majority not available.";

                        throw new IgniteInternalException(msg);
                    }
                });
    }

    /**
     * Calculates the quantity of the data nodes for the partition of the table.
     *
     * @param tblId Table id.
     * @param partId Partition id.
     * @param peers Raft peers.
     * @return A future that will hold the quantity of data nodes.
     */
    private CompletableFuture<Long> waitForPeersAndQueryDataNodesCount(int tblId, int partId, Collection<Peer> peers) {
        HasDataRequest request = TABLE_MESSAGES_FACTORY.hasDataRequest().tableId(tblId).partitionId(partId).build();

        return allPeersAreInTopology(peers)
                .thenCompose(unused -> queryDataNodesCount(peers, request));
    }

    private CompletableFuture<?> allPeersAreInTopology(Collection<Peer> peers) {
        Set<String> peerConsistentIds = peers.stream()
                .map(Peer::consistentId)
                .collect(toSet());

        Map<String, ClusterNode> peerNodesByConsistentIds = new ConcurrentHashMap<>();

        for (Peer peer : peers) {
            ClusterNode node = topologyService.getByConsistentId(peer.consistentId());

            if (node != null) {
                peerNodesByConsistentIds.put(peer.consistentId(), node);
            }
        }

        if (peerNodesByConsistentIds.size() >= peers.size()) {
            return nullCompletedFuture();
        }

        CompletableFuture<Void> allPeersAreSeenInTopology = new CompletableFuture<>();

        TopologyEventHandler eventHandler = new TopologyEventHandler() {
            @Override
            public void onAppeared(ClusterNode member) {
                if (peerConsistentIds.contains(member.name())) {
                    peerNodesByConsistentIds.put(member.name(), member);
                }

                if (peerNodesByConsistentIds.size() >= peers.size()) {
                    allPeersAreSeenInTopology.complete(null);
                }
            }
        };

        topologyService.addEventHandler(eventHandler);

        // Check again for peers that could appear in the topology since last check, but before we installed the handler.
        for (Peer peer : peers) {
            if (!peerNodesByConsistentIds.containsKey(peer.consistentId())) {
                ClusterNode node = topologyService.getByConsistentId(peer.consistentId());

                if (node != null) {
                    peerNodesByConsistentIds.put(peer.consistentId(), node);
                }
            }
        }

        if (peerNodesByConsistentIds.size() >= peers.size()) {
            return nullCompletedFuture();
        }

        // TODO: remove the handler after https://issues.apache.org/jira/browse/IGNITE-14519 is implemented.

        return withTimeout(allPeersAreSeenInTopology);
    }

    private static CompletableFuture<Void> withTimeout(CompletableFuture<Void> future) {
        return future.orTimeout(PEERS_IN_TOPOLOGY_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .handle((res, ex) -> {
                    if (ex instanceof TimeoutException) {
                        return completedFuture(res);
                    }

                    if (ex != null) {
                        return CompletableFuture.<Void>failedFuture(ex);
                    }

                    return completedFuture(res);
                })
                .thenCompose(identity());
    }

    private CompletableFuture<Long> queryDataNodesCount(Collection<Peer> peers, HasDataRequest request) {
        // noinspection unchecked
        CompletableFuture<Boolean>[] requestFutures = peers.stream()
                .map(Peer::consistentId)
                .map(topologyService::getByConsistentId)
                .filter(Objects::nonNull)
                .map(node -> messagingService
                        .invoke(node, request, QUERY_DATA_NODES_COUNT_TIMEOUT_MILLIS)
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
