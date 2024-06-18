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
import static org.apache.ignite.internal.table.distributed.disaster.DisasterRecoveryRequestType.SINGLE_NODE;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.Arrays;
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
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.table.distributed.disaster.messages.LocalPartitionStateMessage;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.lang.IgniteException;

class ManualGroupUpdateRequest implements DisasterRecoveryRequest {
    /** Serial version UID. */
    private static final long serialVersionUID = 0L;

    private final UUID operationId;

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
            return CompletableFuture.failedFuture(new IgniteException("Foo"));
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
     * @param partitionIds Partitions IDs to force assignments for. If empty, reassigns all zone's partitions.
     * @param dataNodes Current DZ data nodes.
     * @param aliveNodesConsistentIds Set of alive nodes according to logical topology.
     * @param revision Meta-storage revision to be associated with reassignment.
     * @param metaStorageManager Meta-storage manager.
     * @param localStatesMap
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

        int[] ids = partitionIds.isEmpty()
                ? IntStream.range(0, zoneDescriptor.partitions()).toArray()
                : partitionIds.stream().mapToInt(Integer::intValue).toArray();

        CompletableFuture<?>[] futures = new CompletableFuture[ids.length];

        for (int i = 0; i < ids.length; i++) {
            TablePartitionId replicaGrpId = new TablePartitionId(tableDescriptor.id(), ids[i]);

            futures[i] = tableAssignmentsFut.thenCompose(tableAssignments ->
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
        //        Set<Integer> tablesInZone = findTablesByZoneId(zoneId, catalogVersion, catalogService).stream()
//                .map(CatalogObjectDescriptor::id)
//                .collect(toSet());
//
//        byte[] countersValue = toBytes(tablesInZone);

        // TODO https://issues.apache.org/jira/browse/IGNITE-21303
        //  This is a naive approach that doesn't exclude nodes in error state, if they exist.
        Set<Assignment> partAssignments = new HashSet<>();
        if (localPartitionStateMessageByNode != null) {
            for (Entry<String, LocalPartitionStateMessage> entry : localPartitionStateMessageByNode.entrySet()) {
                if (aliveNodesConsistentIds.contains(entry.getKey()) && (entry.getValue().state() == LocalPartitionStateEnum.HEALTHY
                        || entry.getValue().state() == LocalPartitionStateEnum.CATCHING_UP)) {
                    partAssignments.add(Assignment.forPeer(entry.getKey()));
                }
            }
        }

        Set<Assignment> aliveStableNodes = CollectionUtils.intersect(currentAssignments, partAssignments);
//        for (Assignment currentAssignment : currentAssignments) {
//            if (aliveNodesConsistentIds.contains(currentAssignment.consistentId())) {
//                partAssignments.add(currentAssignment);
//            }
//        }

        if (aliveStableNodes.size() >= (replicas / 2 + 1)) {
            return CompletableFuture.completedFuture(ASSIGNMENT_NOT_UPDATED.ordinal());
        }

        Set<Assignment> calcAssignments = AffinityUtils.calculateAssignmentForPartition(aliveDataNodes, partId.partitionId(), replicas);

        for (Assignment calcAssignment : calcAssignments) {
            if (partAssignments.size() == replicas) {
                break;
            }

            partAssignments.add(calcAssignment);
        }

//        if (currentAssignments.equals(calcAssignments)) {
//            return CompletableFuture.completedFuture(ASSIGNMENT_NOT_UPDATED.ordinal());
//        }

        byte[] partAssignmentsBytes = Assignments.forced(partAssignments).toBytes();
        byte[] revisionBytes = ByteUtils.longToBytesKeepingOrder(revision);

        ByteArray partChangeTriggerKey = pendingChangeTriggerKey(partId);
        ByteArray partAssignmentsPendingKey = pendingPartAssignmentsKey(partId);
        ByteArray partAssignmentsPlannedKey = plannedPartAssignmentsKey(partId);

        System.out.println("<$> revision bytes = " + Arrays.toString(revisionBytes));
        Iif iif = iif(
                notExists(partChangeTriggerKey).or(value(partChangeTriggerKey).lt(revisionBytes)),
                iif(
                        value(partAssignmentsPendingKey).ne(partAssignmentsBytes),
                        ops(
                                put(partAssignmentsPendingKey, partAssignmentsBytes),
                                put(partChangeTriggerKey, revisionBytes),
//                                put(tablesCounterKey(zoneId, partId.partitionId()), ByteUtils.toBytes(Set.of(partId.tableId()))),
                                remove(partAssignmentsPlannedKey)
                        ).yield(PENDING_KEY_UPDATED.ordinal()),
                        ops().yield(ASSIGNMENT_NOT_UPDATED.ordinal())
                ),
                ops().yield(OUTDATED_UPDATE_RECEIVED.ordinal())
        );

        return metaStorageMgr.invoke(iif).thenApply(StatementResult::getAsInt);
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
