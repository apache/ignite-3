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
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.tableStableAssignments;
import static org.apache.ignite.internal.table.distributed.disaster.DisasterRecoveryManager.tableState;
import static org.apache.ignite.internal.table.distributed.disaster.DisasterRecoveryRequestType.MULTI_NODE;
import static org.apache.ignite.internal.table.distributed.disaster.GroupUpdateRequestHandler.getAliveNodesWithData;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.DisasterRecovery.RESTART_WITH_CLEAN_UP_ERR;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;
import org.apache.ignite.internal.distributionzones.NodeWithAttributes;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.table.distributed.disaster.exceptions.DisasterRecoveryException;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.CollectionUtils;

class ManualGroupRestartRequest implements DisasterRecoveryRequest {
    private final UUID operationId;

    private final int zoneId;

    private final int tableId;

    private final Set<Integer> partitionIds;

    private final Set<String> nodeNames;

    private final long assignmentsTimestamp;

    private final boolean cleanUp;

    ManualGroupRestartRequest(
            UUID operationId,
            int zoneId,
            int tableId,
            Set<Integer> partitionIds,
            Set<String> nodeNames,
            long assignmentsTimestamp,
            boolean cleanUp
    ) {
        this.operationId = operationId;
        this.zoneId = zoneId;
        this.tableId = tableId;
        this.partitionIds = Set.copyOf(partitionIds);
        this.nodeNames = Set.copyOf(nodeNames);
        this.assignmentsTimestamp = assignmentsTimestamp;
        this.cleanUp = cleanUp;
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
        return MULTI_NODE;
    }

    public int tableId() {
        return tableId;
    }

    public Set<Integer> partitionIds() {
        return partitionIds;
    }

    public Set<String> nodeNames() {
        return nodeNames;
    }

    long assignmentsTimestamp() {
        return assignmentsTimestamp;
    }

    public boolean cleanUp() {
        return cleanUp;
    }

    @Override
    public CompletableFuture<Void> handle(DisasterRecoveryManager disasterRecoveryManager, long revision, HybridTimestamp timestamp) {
        if (!nodeNames.isEmpty() && !nodeNames.contains(disasterRecoveryManager.localNode().name())) {
            return nullCompletedFuture();
        }

        var restartFutures = new ArrayList<CompletableFuture<?>>();

        if (cleanUp) {
            restartPartitionWithCleanup(disasterRecoveryManager, revision, timestamp, restartFutures);
        } else {
            restartPartition(disasterRecoveryManager, revision, restartFutures);
        }

        return restartFutures.isEmpty() ? nullCompletedFuture() : allOf(restartFutures.toArray(CompletableFuture[]::new));
    }

    private void restartPartition(
            DisasterRecoveryManager disasterRecoveryManager,
            long revision,
            ArrayList<CompletableFuture<?>> restartFutures
    ) {
        disasterRecoveryManager.raftManager.forEach((raftNodeId, raftGroupService) -> {
            ReplicationGroupId replicationGroupId = raftNodeId.groupId();

            if (replicationGroupId instanceof TablePartitionId) {
                TablePartitionId groupId = (TablePartitionId) replicationGroupId;

                if (groupId.tableId() == tableId && partitionIds.contains(groupId.partitionId())) {
                    restartFutures.add(disasterRecoveryManager.tableManager.restartPartition(groupId, revision, assignmentsTimestamp));
                }
            } else {
                if (replicationGroupId instanceof ZonePartitionId) {
                    ZonePartitionId groupId = (ZonePartitionId) replicationGroupId;

                    if (groupId.zoneId() == zoneId && partitionIds.contains(groupId.partitionId())) {
                        restartFutures.add(
                                disasterRecoveryManager.partitionReplicaLifecycleManager.restartPartition(
                                        groupId,
                                        revision,
                                        assignmentsTimestamp
                                )
                        );
                    }
                }
            }
        });
    }

    private void restartPartitionWithCleanup(
            DisasterRecoveryManager disasterRecoveryManager,
            long revision,
            HybridTimestamp timestamp,
            ArrayList<CompletableFuture<?>> restartFutures
    ) {
        disasterRecoveryManager.raftManager.forEach((raftNodeId, raftGroupService) -> {
            ReplicationGroupId replicationGroupId = raftNodeId.groupId();
            CatalogManager catalogManager = disasterRecoveryManager.catalogManager;
            Catalog catalog = catalogManager.activeCatalog(timestamp.longValue());
            CatalogZoneDescriptor zoneDescriptor = catalog.zone(zoneId);

            if (replicationGroupId instanceof TablePartitionId) {
                TablePartitionId groupId = (TablePartitionId) replicationGroupId;

                if (groupId.tableId() == tableId && partitionIds.contains(groupId.partitionId())) {

                    if (zoneDescriptor.consistencyMode() == ConsistencyMode.HIGH_AVAILABILITY) {
                        if (zoneDescriptor.replicas() >= 2) {
                            restartFutures.add(disasterRecoveryManager.tableManager.restartPartitionWithCleanUp(
                                    groupId,
                                    revision,
                                    assignmentsTimestamp
                            ));
                        }
                    } else {
                        restartFutures.add(
                                enoughAliveNodesToRestartWithCleanUp(
                                    disasterRecoveryManager,
                                    revision,
                                    replicationGroupId,
                                    zoneDescriptor,
                                    catalog
                                ).thenCompose(
                                    enoughNodes -> {
                                        if (enoughNodes) {
                                            return disasterRecoveryManager.tableManager.restartPartitionWithCleanUp(
                                                    groupId,
                                                    revision,
                                                    assignmentsTimestamp
                                            );
                                        } else {
                                            throw new DisasterRecoveryException(RESTART_WITH_CLEAN_UP_ERR, "Not enough alive node "
                                                    + "to perform reset with clean up.");
                                        }
                                    }
                                )
                        );
                    }
                }
            } else {
                if (replicationGroupId instanceof ZonePartitionId) {
                    // todo support zone partitions
                }
            }
        });
    }

    private CompletableFuture<Boolean> enoughAliveNodesToRestartWithCleanUp(
            DisasterRecoveryManager disasterRecoveryManager,
            long msRevision,
            ReplicationGroupId replicationGroupId,
            CatalogZoneDescriptor zoneDescriptor,
            Catalog catalog
    ) {
        if (zoneDescriptor.replicas() <= 2) {
            return CompletableFuture.completedFuture(false);
        }

        TablePartitionId tablePartitionId = (TablePartitionId) replicationGroupId;

        MetaStorageManager metaStorageManager = disasterRecoveryManager.metaStorageManager;

        Set<String> aliveNodesConsistentIds = disasterRecoveryManager.dzManager.logicalTopology(msRevision)
                .stream()
                .map(NodeWithAttributes::nodeName)
                .collect(toSet());

        CompletableFuture<Map<TablePartitionId, LocalPartitionStateMessageByNode>> localStatesFuture =
                disasterRecoveryManager.localPartitionStatesInternal(
                        Set.of(zoneDescriptor.name()),
                        emptySet(),
                        Set.of(tablePartitionId.partitionId()),
                        catalog,
                        tableState()
                );

        CompletableFuture<Map<Integer, Assignments>> stableAssignments =
                tableStableAssignments(metaStorageManager, tableId, new int[]{tablePartitionId.partitionId()});

        return localStatesFuture.thenCombine(stableAssignments, (localPartitionStatesMap, currentAssignments) -> {
            LocalPartitionStateMessageByNode localPartitionStateMessageByNode = localPartitionStatesMap.get(tablePartitionId);

            Set<Assignment> partAssignments = getAliveNodesWithData(aliveNodesConsistentIds, localPartitionStateMessageByNode);

            Set<Assignment> currentStableAssignments = currentAssignments.get(tablePartitionId.partitionId()).nodes();

            Set<Assignment> aliveStableNodes = CollectionUtils.intersect(currentStableAssignments, partAssignments);

            return aliveStableNodes.size() > (zoneDescriptor.replicas() / 2 + 1);
        });
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
