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
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.tableAssignments;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.value;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.metastorage.dsl.Operations.remove;
import static org.apache.ignite.internal.metastorage.dsl.Statements.iif;
import static org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStateEnum.CATCHING_UP;
import static org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStateEnum.HEALTHY;
import static org.apache.ignite.internal.table.distributed.disaster.DisasterRecoveryRequestType.SINGLE_NODE;
import static org.apache.ignite.internal.util.ByteUtils.longToBytesKeepingOrder;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.DisasterRecovery.CLUSTER_NOT_IDLE_ERR;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.ignite.internal.affinity.AffinityUtils;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.affinity.Assignments;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.distributionzones.NodeWithAttributes;
import org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.UpdateStatus;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStateMessage;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.table.distributed.disaster.exceptions.DisasterRecoveryException;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.CollectionUtils;

class ManualGroupUpdateRequest implements DisasterRecoveryRequest {
    /** Serial version UID. */
    private static final long serialVersionUID = 0L;

    private final UUID operationId;

    /**
     * Catalog version at the moment of operation creation. Must match catalog version at the moment of operation execution.
     */
    private final int catalogVersion;

    private final int zoneId;

    private final int tableId;

    private final Set<Integer> partitionIds;

    ManualGroupUpdateRequest(UUID operationId, int catalogVersion, int zoneId, int tableId, Set<Integer> partitionIds) {
        this.operationId = operationId;
        this.catalogVersion = catalogVersion;
        this.zoneId = zoneId;
        this.tableId = tableId;
        this.partitionIds = Set.copyOf(partitionIds);
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

    @Override
    public CompletableFuture<Void> handle(DisasterRecoveryManager disasterRecoveryManager, long msRevision) {
        HybridTimestamp msSafeTime = disasterRecoveryManager.metaStorageManager.timestampByRevision(msRevision);

        int catalogVersion = disasterRecoveryManager.catalogManager.activeCatalogVersion(msSafeTime.longValue());

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

            CompletableFuture<?>[] futures = forceAssignmentsUpdate(
                    tableDescriptor,
                    zoneDescriptor,
                    dataNodes,
                    nodeConsistentIds,
                    msRevision,
                    disasterRecoveryManager.metaStorageManager,
                    localStatesMap
            );

            return allOf(futures);
        }).thenCompose(Function.identity());
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
     * @param localStatesMap Local partition states retrieved by {@link DisasterRecoveryManager#localPartitionStates(Set, Set, Set)}.
     * @return A future that will be completed when reassignments data is written into a meta-storage, if that's required.
     */
    private CompletableFuture<?>[] forceAssignmentsUpdate(
            CatalogTableDescriptor tableDescriptor,
            CatalogZoneDescriptor zoneDescriptor,
            Set<String> dataNodes,
            Set<String> aliveNodesConsistentIds,
            long revision,
            MetaStorageManager metaStorageManager,
            Map<TablePartitionId, LocalPartitionStateMessageByNode> localStatesMap
    ) {
        CompletableFuture<Map<Integer, Assignments>> tableAssignmentsFut = tableAssignments(
                metaStorageManager,
                tableDescriptor.id(),
                partitionIds,
                zoneDescriptor.partitions()
        );

        Set<String> aliveDataNodes = CollectionUtils.intersect(dataNodes, aliveNodesConsistentIds);

        int[] partitionIdsArray = partitionIds.isEmpty()
                ? IntStream.range(0, zoneDescriptor.partitions()).toArray()
                : partitionIds.stream().mapToInt(Integer::intValue).toArray();

        CompletableFuture<?>[] futures = new CompletableFuture[partitionIdsArray.length];

        for (int partitionId = 0; partitionId < partitionIdsArray.length; partitionId++) {
            TablePartitionId replicaGrpId = new TablePartitionId(tableDescriptor.id(), partitionIdsArray[partitionId]);

            futures[partitionId] = tableAssignmentsFut.thenCompose(tableAssignments ->
                    tableAssignments.isEmpty() ? nullCompletedFuture() : manualPartitionUpdate(
                            replicaGrpId,
                            aliveDataNodes,
                            aliveNodesConsistentIds,
                            zoneDescriptor.replicas(),
                            revision,
                            metaStorageManager,
                            tableAssignments.get(replicaGrpId.partitionId()).nodes(),
                            localStatesMap.get(replicaGrpId)
                    )).thenAccept(res -> {
                        DisasterRecoveryManager.LOG.info(
                                "Partition {} returned {} status on reset attempt", replicaGrpId, UpdateStatus.valueOf(res)
                        );
                    }
            );
        }

        return futures;
    }

    private static CompletableFuture<Integer> manualPartitionUpdate(
            TablePartitionId partId,
            Collection<String> aliveDataNodes,
            Set<String> aliveNodesConsistentIds,
            int replicas,
            long revision,
            MetaStorageManager metaStorageMgr,
            Set<Assignment> currentAssignments,
            LocalPartitionStateMessageByNode localPartitionStateMessageByNode
    ) {
        // TODO https://issues.apache.org/jira/browse/IGNITE-21303
        //  This is a naive approach that doesn't exclude nodes in error state, if they exist.
        Set<Assignment> partAssignments = getAliveNodesWithData(aliveNodesConsistentIds, localPartitionStateMessageByNode);

        Set<Assignment> aliveStableNodes = CollectionUtils.intersect(currentAssignments, partAssignments);

        if (aliveStableNodes.size() >= (replicas / 2 + 1)) {
            return completedFuture(ASSIGNMENT_NOT_UPDATED.ordinal());
        }

        enrichAssignments(partId, aliveDataNodes, replicas, partAssignments);

        byte[] partAssignmentsBytes = Assignments.forced(partAssignments).toBytes();
        byte[] revisionBytes = longToBytesKeepingOrder(revision);

        ByteArray partChangeTriggerKey = pendingChangeTriggerKey(partId);
        ByteArray partAssignmentsPendingKey = pendingPartAssignmentsKey(partId);
        ByteArray partAssignmentsPlannedKey = plannedPartAssignmentsKey(partId);

        Iif iif = iif(
                notExists(partChangeTriggerKey).or(value(partChangeTriggerKey).lt(revisionBytes)),
                iif(
                        value(partAssignmentsPendingKey).ne(partAssignmentsBytes),
                        ops(
                                put(partAssignmentsPendingKey, partAssignmentsBytes),
                                put(partChangeTriggerKey, revisionBytes),
                                remove(partAssignmentsPlannedKey)
                        ).yield(PENDING_KEY_UPDATED.ordinal()),
                        ops().yield(ASSIGNMENT_NOT_UPDATED.ordinal())
                ),
                ops().yield(OUTDATED_UPDATE_RECEIVED.ordinal())
        );

        return metaStorageMgr.invoke(iif).thenApply(StatementResult::getAsInt);
    }

    /**
     * Returns a set of nodes that are both alive and either {@link LocalPartitionStateEnum#HEALTHY} or
     * {@link LocalPartitionStateEnum#CATCHING_UP}.
     */
    private static Set<Assignment> getAliveNodesWithData(
            Set<String> aliveNodesConsistentIds,
            LocalPartitionStateMessageByNode localPartitionStateMessageByNode
    ) {
        Set<Assignment> partAssignments = new HashSet<>();
        if (localPartitionStateMessageByNode != null) {
            for (Entry<String, LocalPartitionStateMessage> entry : localPartitionStateMessageByNode.entrySet()) {
                if (aliveNodesConsistentIds.contains(entry.getKey())
                        && (entry.getValue().state() == HEALTHY || entry.getValue().state() == CATCHING_UP)
                ) {
                    partAssignments.add(Assignment.forPeer(entry.getKey()));
                }
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
        Set<Assignment> calcAssignments = AffinityUtils.calculateAssignmentForPartition(aliveDataNodes, partId.partitionId(), replicas);

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
