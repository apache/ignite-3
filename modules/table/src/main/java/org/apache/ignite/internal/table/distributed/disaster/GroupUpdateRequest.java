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

package org.apache.ignite.internal.table.distributed.disaster;

import static java.util.Collections.emptySet;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.UpdateStatus.ASSIGNMENT_NOT_UPDATED;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.UpdateStatus.OUTDATED_UPDATE_RECEIVED;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.UpdateStatus.PENDING_KEY_UPDATED;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.pendingChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.pendingPartAssignmentsKey;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.plannedPartAssignmentsKey;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.tableStableAssignments;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.value;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.metastorage.dsl.Operations.remove;
import static org.apache.ignite.internal.metastorage.dsl.Statements.iif;
import static org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStateEnum.CATCHING_UP;
import static org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStateEnum.HEALTHY;
import static org.apache.ignite.internal.partitiondistribution.PartitionDistributionUtils.calculateAssignmentForPartition;
import static org.apache.ignite.internal.table.distributed.disaster.DisasterRecoveryRequestType.SINGLE_NODE;
import static org.apache.ignite.internal.util.ByteUtils.longToBytesKeepingOrder;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.DisasterRecovery.CLUSTER_NOT_IDLE_ERR;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.distributionzones.NodeWithAttributes;
import org.apache.ignite.internal.distributionzones.rebalance.AssignmentUtil;
import org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil;
import org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.UpdateStatus;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStateEnum;
import org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStateMessage;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.table.distributed.disaster.exceptions.DisasterRecoveryException;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.CollectionUtils;
import org.jetbrains.annotations.Nullable;

class GroupUpdateRequest implements DisasterRecoveryRequest {
    private static final IgniteLogger LOG = Loggers.forClass(GroupUpdateRequest.class);

    private final UUID operationId;

    /**
     * Catalog version at the moment of operation creation. Must match catalog version at the moment of operation execution.
     */
    private final int catalogVersion;

    private final int zoneId;

    private final int tableId;

    private final Set<Integer> partitionIds;

    private final boolean manualUpdate;

    GroupUpdateRequest(UUID operationId, int catalogVersion, int zoneId, int tableId, Set<Integer> partitionIds, boolean manualUpdate) {
        this.operationId = operationId;
        this.catalogVersion = catalogVersion;
        this.zoneId = zoneId;
        this.tableId = tableId;
        this.partitionIds = Set.copyOf(partitionIds);
        this.manualUpdate = manualUpdate;
    }

    @Override
    public UUID operationId() {
        return operationId;
    }

    @Override
    public int zoneId() {
        return zoneId;
    }

    @Override
    public DisasterRecoveryRequestType type() {
        return SINGLE_NODE;
    }

    public int catalogVersion() {
        return catalogVersion;
    }

    public int tableId() {
        return tableId;
    }

    public Set<Integer> partitionIds() {
        return partitionIds;
    }

    public boolean manualUpdate() {
        return manualUpdate;
    }

    @Override
    public CompletableFuture<Void> handle(DisasterRecoveryManager disasterRecoveryManager, long msRevision, HybridTimestamp msTimestamp) {
        int catalogVersion = disasterRecoveryManager.catalogManager.activeCatalogVersion(msTimestamp.longValue());

        if (this.catalogVersion != catalogVersion) {
            return failedFuture(
                    new DisasterRecoveryException(CLUSTER_NOT_IDLE_ERR, "Cluster is not idle, concurrent DDL update detected.")
            );
        }

        Catalog catalog = disasterRecoveryManager.catalogManager.catalog(catalogVersion);

        CatalogZoneDescriptor zoneDescriptor = catalog.zone(zoneId);
        CatalogTableDescriptor tableDescriptor = catalog.table(tableId);

        CompletableFuture<Map<TablePartitionId, LocalPartitionStateMessageByNode>> localStates
                = disasterRecoveryManager.localPartitionStatesInternal(Set.of(zoneDescriptor.name()), emptySet(), partitionIds, catalog);

        CompletableFuture<Set<String>> dataNodesFuture = disasterRecoveryManager.dzManager.dataNodes(msRevision, catalogVersion, zoneId);

        return dataNodesFuture.thenCombine(localStates, (dataNodes, localStatesMap) -> {
            Set<String> nodeConsistentIds = disasterRecoveryManager.dzManager.logicalTopology()
                    .stream()
                    .map(NodeWithAttributes::nodeName)
                    .collect(toSet());

            int[] partitionIdsArray = AssignmentUtil.partitionIds(partitionIds, zoneDescriptor.partitions());

            return forceAssignmentsUpdate(
                    tableDescriptor,
                    zoneDescriptor,
                    dataNodes,
                    nodeConsistentIds,
                    msRevision,
                    disasterRecoveryManager.metaStorageManager,
                    localStatesMap,
                    catalog.time(),
                    partitionIdsArray,
                    manualUpdate
            );
        })
        .thenCompose(Function.identity())
        .whenComplete((unused, throwable) -> {
            // TODO: IGNITE-23635 Add fail handling for failed resetPeers
            if (throwable != null) {
                LOG.error("Failed to reset partition", throwable);
            }
        });
    }

    /**
     * Sets force assignments for the zone/table if it's required. The condition for force reassignment is the absence of stable
     * assignments' majority within the set of currently alive nodes. In this case we calculate new assignments that include all alive
     * stable nodes, and try to save ot with a {@link Assignments#force()} flag enabled.
     *
     * @param tableDescriptor Table descriptor.
     * @param zoneDescriptor Zone descriptor.
     * @param dataNodes Current DZ data nodes.
     * @param aliveNodesConsistentIds Set of alive nodes according to logical topology.
     * @param revision Meta-storage revision to be associated with reassignment.
     * @param metaStorageManager Meta-storage manager.
     * @param localStatesMap Local partition states retrieved by
     *         {@link DisasterRecoveryManager#localPartitionStates(Set, Set, Set)}.
     * @return A future that will be completed when reassignments data is written into a meta-storage, if that's required.
     */
    private static CompletableFuture<Void> forceAssignmentsUpdate(
            CatalogTableDescriptor tableDescriptor,
            CatalogZoneDescriptor zoneDescriptor,
            Set<String> dataNodes,
            Set<String> aliveNodesConsistentIds,
            long revision,
            MetaStorageManager metaStorageManager,
            Map<TablePartitionId, LocalPartitionStateMessageByNode> localStatesMap,
            long assignmentsTimestamp,
            int[] partitionIds,
            boolean manualUpdate
    ) {
        return tableStableAssignments(metaStorageManager, tableDescriptor.id(), partitionIds)
                .thenCompose(tableAssignments -> {
                    if (tableAssignments.isEmpty()) {
                        return nullCompletedFuture();
                    }

                    return updateAssignments(
                            tableDescriptor,
                            zoneDescriptor,
                            dataNodes,
                            aliveNodesConsistentIds,
                            revision,
                            metaStorageManager,
                            localStatesMap,
                            assignmentsTimestamp,
                            partitionIds,
                            tableAssignments,
                            manualUpdate
                    );
                });
    }

    private static CompletableFuture<Void> updateAssignments(
            CatalogTableDescriptor tableDescriptor,
            CatalogZoneDescriptor zoneDescriptor,
            Set<String> dataNodes,
            Set<String> aliveNodesConsistentIds,
            long revision,
            MetaStorageManager metaStorageManager,
            Map<TablePartitionId, LocalPartitionStateMessageByNode> localStatesMap,
            long assignmentsTimestamp,
            int[] partitionIds,
            Map<Integer, Assignments> tableAssignments,
            boolean manualUpdate
    ) {
        Set<String> aliveDataNodes = CollectionUtils.intersect(dataNodes, aliveNodesConsistentIds);

        CompletableFuture<?>[] futures = new CompletableFuture[partitionIds.length];

        for (int i = 0; i < partitionIds.length; i++) {
            TablePartitionId replicaGrpId = new TablePartitionId(tableDescriptor.id(), partitionIds[i]);

            futures[i] = partitionUpdate(
                    replicaGrpId,
                    aliveDataNodes,
                    aliveNodesConsistentIds,
                    zoneDescriptor.replicas(),
                    revision,
                    metaStorageManager,
                    tableAssignments.get(replicaGrpId.partitionId()).nodes(),
                    localStatesMap.get(replicaGrpId),
                    assignmentsTimestamp,
                    manualUpdate
            ).thenAccept(res -> {
                DisasterRecoveryManager.LOG.info(
                        "Partition {} returned {} status on reset attempt", replicaGrpId, UpdateStatus.valueOf(res)
                );
            });
        }

        return allOf(futures);
    }

    private static CompletableFuture<Integer> partitionUpdate(
            TablePartitionId partId,
            Collection<String> aliveDataNodes,
            Set<String> aliveNodesConsistentIds,
            int replicas,
            long revision,
            MetaStorageManager metaStorageMgr,
            Set<Assignment> currentAssignments,
            LocalPartitionStateMessageByNode localPartitionStateMessageByNode,
            long assignmentsTimestamp,
            boolean manualUpdate
    ) {
        Set<Assignment> partAssignments = getAliveNodesWithData(aliveNodesConsistentIds, localPartitionStateMessageByNode);
        Set<Assignment> aliveStableNodes = CollectionUtils.intersect(currentAssignments, partAssignments);

        if (aliveStableNodes.size() >= (replicas / 2 + 1)) {
            return completedFuture(ASSIGNMENT_NOT_UPDATED.ordinal());
        }

        if (aliveStableNodes.isEmpty() && !manualUpdate) {
            return completedFuture(ASSIGNMENT_NOT_UPDATED.ordinal());
        }

        if (manualUpdate) {
            enrichAssignments(partId, aliveDataNodes, replicas, partAssignments);
        }

        Assignment nextAssignment = nextAssignment(localPartitionStateMessageByNode, partAssignments);

        boolean isProposedPendingEqualsProposedPlanned = partAssignments.size() == 1;

        assert partAssignments.contains(nextAssignment) : IgniteStringFormatter.format(
                "Recovery nodes set doesn't contain the reset node assignment [partAssignments={}, nextAssignment={}]",
                partAssignments,
                nextAssignment
        );

        // There are nodes with data, and we set pending assignments to this set of nodes. It'll be the source of peers for
        // "resetPeers", and after that new assignments with restored replica factor wil be picked up from planned assignments
        // for the case of the manual update, that was triggered by a user.
        Iif invokeClosure = prepareMsInvokeClosure(
                partId,
                longToBytesKeepingOrder(revision),
                Assignments.forced(Set.of(nextAssignment), assignmentsTimestamp).toBytes(),
                // If planned nodes set consists of reset node assignment only then we shouldn't schedule the same planned rebalance.
                isProposedPendingEqualsProposedPlanned
                        ? null
                        : Assignments.toBytes(partAssignments, assignmentsTimestamp)
        );

        return metaStorageMgr.invoke(invokeClosure).thenApply(StatementResult::getAsInt);
    }

    /**
     * Returns an assignment with the most up to date log index, if there are more than one node with the same index,
     * returns the first one in the lexicographic order.
     */
    private static Assignment nextAssignment(
            LocalPartitionStateMessageByNode localPartitionStateByNode,
            Set<Assignment> assignments
    ) {
        // For nodes that we know log index for (having any data), we choose the node with the highest log index.
        // If there are more than one node with same log index, we choose the one with the first consistent id in the lexicographic order.
        Optional<Assignment> nodeWithMaxLogIndex = assignments.stream()
                .filter(assignment -> localPartitionStateByNode.partitionState(assignment.consistentId()) != null)
                .min(Comparator.<Assignment>comparingLong(
                                node -> localPartitionStateByNode.partitionState(node.consistentId()).logIndex()
                        )
                        .reversed()
                        .thenComparing(Assignment::consistentId))
                // If there are no nodes with data, we choose the node with the first consistent id in the lexicographic order.
                .or(() -> assignments.stream().min(Comparator.comparing(Assignment::consistentId)));

        // TODO: IGNITE-23737 The case with no suitable nodes should be handled.
        return nodeWithMaxLogIndex.orElseThrow();
    }

    /**
     * Creates an {@link Iif} instance for meta-storage's {@link MetaStorageManager#invoke(Iif)} call. Does the following:
     * <ul>
     *     <li>Guards the condition with a standard {@link RebalanceUtil#pendingChangeTriggerKey(TablePartitionId)} check.</li>
     *     <li>Adds additional guard with comparison of real and proposed values of
     *          {@link RebalanceUtil#pendingPartAssignmentsKey(TablePartitionId)}, just in case.</li>
     *     <li>Updates the value of {@link RebalanceUtil#pendingChangeTriggerKey(TablePartitionId)}.</li>
     *     <li>Updates the value of {@link RebalanceUtil#pendingPartAssignmentsKey(TablePartitionId)}.</li>
     *     <li>Updates the value of {@link RebalanceUtil#plannedPartAssignmentsKey(TablePartitionId)} or removes it, if
     *          {@code plannedAssignmentsBytes} is {@code null}.</li>
     * </ul>
     *
     * @param partId Partition ID.
     * @param revisionBytes Properly serialized current meta-storage revision.
     * @param pendingAssignmentsBytes Value for {@link RebalanceUtil#pendingPartAssignmentsKey(TablePartitionId)}.
     * @param plannedAssignmentsBytes Value for {@link RebalanceUtil#plannedPartAssignmentsKey(TablePartitionId)} or {@code null}.
     * @return {@link Iif} instance.
     */
    private static Iif prepareMsInvokeClosure(
            TablePartitionId partId,
            byte[] revisionBytes,
            byte[] pendingAssignmentsBytes,
            byte @Nullable [] plannedAssignmentsBytes
    ) {
        ByteArray pendingChangeTriggerKey = pendingChangeTriggerKey(partId);
        ByteArray partAssignmentsPendingKey = pendingPartAssignmentsKey(partId);
        ByteArray partAssignmentsPlannedKey = plannedPartAssignmentsKey(partId);

        return iif(notExists(pendingChangeTriggerKey).or(value(pendingChangeTriggerKey).lt(revisionBytes)),
                iif(
                        value(partAssignmentsPendingKey).ne(pendingAssignmentsBytes),
                        ops(
                                put(pendingChangeTriggerKey, revisionBytes),
                                put(partAssignmentsPendingKey, pendingAssignmentsBytes),
                                plannedAssignmentsBytes == null
                                        ? remove(partAssignmentsPlannedKey)
                                        : put(partAssignmentsPlannedKey, plannedAssignmentsBytes)
                        ).yield(PENDING_KEY_UPDATED.ordinal()),
                        ops().yield(ASSIGNMENT_NOT_UPDATED.ordinal())
                ),
                ops().yield(OUTDATED_UPDATE_RECEIVED.ordinal())
        );
    }

    /**
     * Returns a set of nodes that are both alive and either {@link LocalPartitionStateEnum#HEALTHY} or
     * {@link LocalPartitionStateEnum#CATCHING_UP}.
     */
    private static Set<Assignment> getAliveNodesWithData(
            Set<String> aliveNodesConsistentIds,
            @Nullable LocalPartitionStateMessageByNode localPartitionStateMessageByNode
    ) {
        if (localPartitionStateMessageByNode == null) {
            return Set.of();
        }

        var partAssignments = new HashSet<Assignment>();

        for (Entry<String, LocalPartitionStateMessage> entry : localPartitionStateMessageByNode.entrySet()) {
            String nodeName = entry.getKey();
            LocalPartitionStateEnum state = entry.getValue().state();

            if (aliveNodesConsistentIds.contains(nodeName) && (state == HEALTHY || state == CATCHING_UP)) {
                partAssignments.add(Assignment.forPeer(nodeName));
            }
        }

        return partAssignments;
    }

    /**
     * Adds more nodes into {@code partAssignments} until it matches the number of replicas or we run out of nodes.
     */
    private static void enrichAssignments(
            TablePartitionId partId,
            Collection<String> aliveDataNodes,
            int replicas,
            Set<Assignment> partAssignments
    ) {
        Set<Assignment> calcAssignments = calculateAssignmentForPartition(aliveDataNodes, partId.partitionId(), replicas);

        for (Assignment calcAssignment : calcAssignments) {
            if (partAssignments.size() == replicas) {
                break;
            }

            partAssignments.add(calcAssignment);
        }
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
