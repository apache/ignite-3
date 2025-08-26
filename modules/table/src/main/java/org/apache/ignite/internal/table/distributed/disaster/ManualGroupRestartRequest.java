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
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.tableStableAssignments;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.zoneStableAssignments;
import static org.apache.ignite.internal.table.distributed.disaster.DisasterRecoveryManager.tableState;
import static org.apache.ignite.internal.table.distributed.disaster.DisasterRecoveryManager.zoneState;
import static org.apache.ignite.internal.table.distributed.disaster.DisasterRecoveryRequestType.MULTI_NODE;
import static org.apache.ignite.internal.table.distributed.disaster.GroupUpdateRequestHandler.getAliveNodesWithData;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.DisasterRecovery.RESTART_WITH_CLEAN_UP_ERR;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;
import org.apache.ignite.internal.distributionzones.NodeWithAttributes;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStateMessage;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.replicator.PartitionGroupId;
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

        Catalog catalog = disasterRecoveryManager.catalogManager.activeCatalog(timestamp.longValue());
        CatalogZoneDescriptor zoneDescriptor = catalog.zone(zoneId);

        var restartFutures = new ArrayList<CompletableFuture<?>>();

        disasterRecoveryManager.raftManager.forEach((raftNodeId, raftGroupService) -> {
            ReplicationGroupId replicationGroupId = raftNodeId.groupId();

            if (shouldProcessPartition(replicationGroupId)) {
                if (cleanUp) {
                    restartFutures.add(
                            createRestartWithCleanupFuture(disasterRecoveryManager, replicationGroupId, revision, zoneDescriptor, catalog)
                    );
                } else {
                    restartFutures.add(createRestartFuture(disasterRecoveryManager, replicationGroupId, revision));
                }
            }
        });

        return restartFutures.isEmpty() ? nullCompletedFuture() : allOf(restartFutures.toArray(CompletableFuture[]::new));
    }

    private boolean shouldProcessPartition(ReplicationGroupId replicationGroupId) {
        if (replicationGroupId instanceof TablePartitionId) {
            TablePartitionId groupId = (TablePartitionId) replicationGroupId;
            return groupId.tableId() == tableId && partitionIds.contains(groupId.partitionId());
        } else if (replicationGroupId instanceof ZonePartitionId) {
            ZonePartitionId groupId = (ZonePartitionId) replicationGroupId;
            return groupId.zoneId() == zoneId && partitionIds.contains(groupId.partitionId());
        }
        return false;
    }

    private CompletableFuture<?> createRestartFuture(
            DisasterRecoveryManager disasterRecoveryManager,
            ReplicationGroupId replicationGroupId,
            long revision
    ) {
        if (replicationGroupId instanceof TablePartitionId) {
            return disasterRecoveryManager.tableManager.restartPartition(
                    (TablePartitionId) replicationGroupId,
                    revision,
                    assignmentsTimestamp
            );
        } else if (replicationGroupId instanceof ZonePartitionId) {
            return disasterRecoveryManager.partitionReplicaLifecycleManager.restartPartition(
                    (ZonePartitionId) replicationGroupId,
                    revision,
                    assignmentsTimestamp
            );
        }
        throw new IllegalStateException("Unexpected replication group id: " + replicationGroupId);
    }

    private CompletableFuture<?> createCleanupRestartFuture(
            DisasterRecoveryManager disasterRecoveryManager,
            ReplicationGroupId replicationGroupId,
            long revision
    ) {
        if (replicationGroupId instanceof TablePartitionId) {
            return disasterRecoveryManager.tableManager.restartPartitionWithCleanUp(
                    (TablePartitionId) replicationGroupId,
                    revision,
                    assignmentsTimestamp
            );
        } else if (replicationGroupId instanceof ZonePartitionId) {
            return disasterRecoveryManager.partitionReplicaLifecycleManager.restartPartitionWithCleanUp(
                    (ZonePartitionId) replicationGroupId,
                    revision,
                    assignmentsTimestamp
            );
        }
        throw new IllegalStateException("Unexpected replication group id: " + replicationGroupId);
    }

    private CompletableFuture<?> createRestartWithCleanupFuture(
            DisasterRecoveryManager disasterRecoveryManager,
            ReplicationGroupId replicationGroupId,
            long revision,
            CatalogZoneDescriptor zoneDescriptor,
            Catalog catalog
    ) {
        if (zoneDescriptor.consistencyMode() == ConsistencyMode.HIGH_AVAILABILITY) {
            if (zoneDescriptor.replicas() >= 2) {
                return createCleanupRestartFuture(disasterRecoveryManager, replicationGroupId, revision);
            } else {
                return notEnoughAliveNodes();
            }
        } else {
            if (zoneDescriptor.replicas() <= 2) {
                return notEnoughAliveNodes();
            }

            return enoughAliveNodesToRestartWithCleanUp(
                    disasterRecoveryManager,
                    revision,
                    replicationGroupId,
                    zoneDescriptor,
                    catalog
            ).thenCompose(enoughNodes -> {
                if (enoughNodes) {
                    return createCleanupRestartFuture(disasterRecoveryManager, replicationGroupId, revision);
                } else {
                    return notEnoughAliveNodes();
                }
            });
        }
    }

    private static <U> CompletableFuture<U> notEnoughAliveNodes() {
        return CompletableFuture.failedFuture(
                new DisasterRecoveryException(RESTART_WITH_CLEAN_UP_ERR, "Not enough alive nodes "
                        + "to perform reset with clean up.")
        );
    }

    private static CompletableFuture<Boolean> enoughAliveNodesToRestartWithCleanUp(
            DisasterRecoveryManager disasterRecoveryManager,
            long msRevision,
            ReplicationGroupId replicationGroupId,
            CatalogZoneDescriptor zoneDescriptor,
            Catalog catalog
    ) {
        if (replicationGroupId instanceof TablePartitionId) {
            TablePartitionId tablePartitionId = (TablePartitionId) replicationGroupId;

            return checkPartitionAliveNodes(
                    disasterRecoveryManager,
                    tablePartitionId,
                    zoneDescriptor,
                    catalog,
                    msRevision,
                    tableState(),
                    tableStableAssignments(
                            disasterRecoveryManager.metaStorageManager,
                            tablePartitionId.tableId(),
                            new int[]{tablePartitionId.partitionId()}
                    )
            );
        } else if (replicationGroupId instanceof ZonePartitionId) {
            ZonePartitionId zonePartitionId = (ZonePartitionId) replicationGroupId;

            return checkPartitionAliveNodes(
                    disasterRecoveryManager,
                    zonePartitionId,
                    zoneDescriptor,
                    catalog,
                    msRevision,
                    zoneState(),
                    zoneStableAssignments(
                            disasterRecoveryManager.metaStorageManager,
                            zonePartitionId.zoneId(),
                            new int[]{zonePartitionId.partitionId()}
                    )
            );
        } else {
            throw new IllegalArgumentException("Unsupported replication group type: " + replicationGroupId.getClass());
        }
    }

    private static <T extends PartitionGroupId> CompletableFuture<Boolean> checkPartitionAliveNodes(
            DisasterRecoveryManager disasterRecoveryManager,
            T partitionGroupId,
            CatalogZoneDescriptor zoneDescriptor,
            Catalog catalog,
            long msRevision,
            Function<LocalPartitionStateMessage, T> keyExtractor,
            CompletableFuture<Map<Integer, Assignments>> stableAssignments
    ) {
        Set<String> aliveNodesConsistentIds = disasterRecoveryManager.dzManager.logicalTopology(msRevision)
                .stream()
                .map(NodeWithAttributes::nodeName)
                .collect(Collectors.toSet());

        CompletableFuture<Map<T, LocalPartitionStateMessageByNode>> localStatesFuture =
                disasterRecoveryManager.localPartitionStatesInternal(
                        Set.of(zoneDescriptor.name()),
                        emptySet(),
                        Set.of(partitionGroupId.partitionId()),
                        catalog,
                        keyExtractor
                );

        return localStatesFuture.thenCombine(stableAssignments, (localPartitionStatesMap, currentAssignments) -> {
            LocalPartitionStateMessageByNode localPartitionStateMessageByNode = localPartitionStatesMap.get(partitionGroupId);

            Set<Assignment> partAssignments = getAliveNodesWithData(aliveNodesConsistentIds, localPartitionStateMessageByNode);
            Set<Assignment> currentStableAssignments = currentAssignments.get(partitionGroupId.partitionId()).nodes();

            Set<Assignment> aliveStableNodes = CollectionUtils.intersect(currentStableAssignments, partAssignments);

            return aliveStableNodes.size() > (zoneDescriptor.replicas() / 2 + 1);
        });
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
