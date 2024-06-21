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

package org.apache.ignite.internal.index;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.AVAILABLE;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.BUILDING;
import static org.apache.ignite.internal.index.IndexManagementUtils.AWAIT_PRIMARY_REPLICA_TIMEOUT_SEC;
import static org.apache.ignite.internal.index.IndexManagementUtils.isLocalNode;
import static org.apache.ignite.internal.index.IndexManagementUtils.isPrimaryReplica;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.RemoveIndexEventParameters;
import org.apache.ignite.internal.catalog.events.StartBuildingIndexEventParameters;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.PrimaryReplicaAwaitTimeoutException;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * Component is responsible for starting and stopping the building of indexes on primary replicas.
 *
 * <p>Component handles the following events (indexes are started and stopped by {@link CatalogIndexDescriptor#tableId()} ==
 * {@link TablePartitionId#tableId()}): </p>
 * <ul>
 *     <li>{@link CatalogEvent#INDEX_BUILDING} - starts building indexes for the corresponding local primary replicas.</li>
 *     <li>{@link CatalogEvent#INDEX_REMOVED} - stops building indexes for the corresponding local primary replicas.</li>
 *     <li>{@link PrimaryReplicaEvent#PRIMARY_REPLICA_ELECTED} - for a new local primary replica, starts the building of all corresponding
 *     indexes, for an expired primary replica, stops the building of all corresponding indexes.</li>
 * </ul>
 *
 * <p>A few words about restoring the building of indexes on node recovery: the building of indexes will be resumed by
 * {@link PrimaryReplicaEvent#PRIMARY_REPLICA_ELECTED}, which will fire due to a change in the {@link ReplicaMeta#getLeaseholderId()} on
 * node restart but after {@link ReplicaMeta#getExpirationTime()}.</p>
 */
class IndexBuildController implements ManuallyCloseable {
    private final IndexBuilder indexBuilder;

    private final IndexManager indexManager;

    private final CatalogService catalogService;

    private final ClusterService clusterService;

    private final PlacementDriver placementDriver;

    private final ClockService clockService;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean closeGuard = new AtomicBoolean();

    private final Set<TablePartitionId> primaryReplicaIds = ConcurrentHashMap.newKeySet();

    /** Constructor. */
    IndexBuildController(
            IndexBuilder indexBuilder,
            IndexManager indexManager,
            CatalogService catalogService,
            ClusterService clusterService,
            PlacementDriver placementDriver,
            ClockService clockService
    ) {
        this.indexBuilder = indexBuilder;
        this.indexManager = indexManager;
        this.catalogService = catalogService;
        this.clusterService = clusterService;
        this.placementDriver = placementDriver;
        this.clockService = clockService;

        addListeners();
    }

    @Override
    public void close() {
        if (!closeGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        indexBuilder.close();
    }

    private void addListeners() {
        catalogService.listen(CatalogEvent.INDEX_BUILDING, (StartBuildingIndexEventParameters parameters) -> {
            return onIndexBuilding(parameters).thenApply(unused -> false);
        });

        catalogService.listen(CatalogEvent.INDEX_REMOVED, (RemoveIndexEventParameters parameters) -> {
            return onIndexRemoved(parameters).thenApply(unused -> false);
        });

        placementDriver.listen(PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED, parameters -> {
            return onPrimaryReplicaElected(parameters).thenApply(unused -> false);
        });
    }

    private CompletableFuture<?> onIndexBuilding(StartBuildingIndexEventParameters parameters) {
        return inBusyLockAsync(busyLock, () -> {
            CatalogIndexDescriptor indexDescriptor = catalogService.index(parameters.indexId(), parameters.catalogVersion());

            var startBuildIndexFutures = new ArrayList<CompletableFuture<?>>();

            for (TablePartitionId primaryReplicaId : primaryReplicaIds) {
                if (primaryReplicaId.tableId() == indexDescriptor.tableId()) {
                    CompletableFuture<?> startBuildIndexFuture = getMvTableStorageFuture(parameters.causalityToken(), primaryReplicaId)
                            .thenCompose(mvTableStorage -> awaitPrimaryReplica(primaryReplicaId, clockService.now())
                                    .thenAccept(replicaMeta -> tryScheduleBuildIndex(
                                            primaryReplicaId,
                                            indexDescriptor,
                                            mvTableStorage,
                                            replicaMeta
                                    ))
                            );

                    startBuildIndexFutures.add(startBuildIndexFuture);
                }
            }

            return allOf(startBuildIndexFutures.toArray(CompletableFuture[]::new));
        });
    }

    private CompletableFuture<?> onIndexRemoved(RemoveIndexEventParameters parameters) {
        return inBusyLockAsync(busyLock, () -> {
            indexBuilder.stopBuildingIndexes(parameters.indexId());

            return nullCompletedFuture();
        });
    }

    private CompletableFuture<?> onPrimaryReplicaElected(PrimaryReplicaEventParameters parameters) {
        return inBusyLockAsync(busyLock, () -> {
            TablePartitionId primaryReplicaId = (TablePartitionId) parameters.groupId();

            if (isLocalNode(clusterService, parameters.leaseholderId())) {
                primaryReplicaIds.add(primaryReplicaId);

                // It is safe to get the latest version of the catalog because the PRIMARY_REPLICA_ELECTED event is handled on the
                // metastore thread.
                int catalogVersion = catalogService.latestCatalogVersion();

                return getMvTableStorageFuture(parameters.causalityToken(), primaryReplicaId)
                        .thenCompose(mvTableStorage -> awaitPrimaryReplica(primaryReplicaId, parameters.startTime())
                                .thenAccept(replicaMeta -> tryScheduleBuildIndexesForNewPrimaryReplica(
                                        catalogVersion,
                                        primaryReplicaId,
                                        mvTableStorage,
                                        replicaMeta
                                ))
                        );
            } else {
                stopBuildingIndexesIfPrimaryExpired(primaryReplicaId);

                return nullCompletedFuture();
            }
        });
    }

    private void tryScheduleBuildIndexesForNewPrimaryReplica(
            int catalogVersion,
            TablePartitionId primaryReplicaId,
            MvTableStorage mvTableStorage,
            ReplicaMeta replicaMeta
    ) {
        inBusyLock(busyLock, () -> {
            if (isLeaseExpire(replicaMeta)) {
                stopBuildingIndexesIfPrimaryExpired(primaryReplicaId);

                return;
            }

            for (CatalogIndexDescriptor indexDescriptor : catalogService.indexes(catalogVersion, primaryReplicaId.tableId())) {
                if (indexDescriptor.status() == BUILDING) {
                    scheduleBuildIndex(primaryReplicaId, indexDescriptor, mvTableStorage, enlistmentConsistencyToken(replicaMeta));
                } else if (indexDescriptor.status() == AVAILABLE) {
                    scheduleBuildIndexAfterDisasterRecovery(
                            primaryReplicaId,
                            indexDescriptor,
                            mvTableStorage,
                            enlistmentConsistencyToken(replicaMeta)
                    );
                }
            }
        });
    }

    private void tryScheduleBuildIndex(
            TablePartitionId primaryReplicaId,
            CatalogIndexDescriptor indexDescriptor,
            MvTableStorage mvTableStorage,
            ReplicaMeta replicaMeta
    ) {
        inBusyLock(busyLock, () -> {
            if (isLeaseExpire(replicaMeta)) {
                stopBuildingIndexesIfPrimaryExpired(primaryReplicaId);

                return;
            }

            scheduleBuildIndex(primaryReplicaId, indexDescriptor, mvTableStorage, enlistmentConsistencyToken(replicaMeta));
        });
    }

    /**
     * Stops building indexes for a replica that has expired, if it has not been done before.
     *
     * <p>We need to stop building indexes at the event of a change of primary replica or after executing asynchronous code, we understand
     * that the {@link ReplicaMeta#getExpirationTime() expiration time} has come.</p>
     *
     * @param replicaId Replica ID.
     */
    private void stopBuildingIndexesIfPrimaryExpired(TablePartitionId replicaId) {
        if (primaryReplicaIds.remove(replicaId)) {
            // Primary replica is no longer current, we need to stop building indexes for it.
            indexBuilder.stopBuildingIndexes(replicaId.tableId(), replicaId.partitionId());
        }
    }

    private CompletableFuture<MvTableStorage> getMvTableStorageFuture(long causalityToken, TablePartitionId replicaId) {
        return indexManager.getMvTableStorage(causalityToken, replicaId.tableId())
                .thenApply(mvTableStorage -> requireMvTableStorageNonNull(mvTableStorage, replicaId.tableId()));
    }

    private static MvTableStorage requireMvTableStorageNonNull(@Nullable MvTableStorage mvTableStorage, int tableId) {
        if (mvTableStorage == null) {
            throw new IgniteInternalException(
                    INTERNAL_ERR,
                    "Table storage for the specified table cannot be null [tableId = {}]",
                    tableId
            );
        }

        return mvTableStorage;
    }

    private CompletableFuture<ReplicaMeta> awaitPrimaryReplica(TablePartitionId replicaId, HybridTimestamp timestamp) {
        return placementDriver
                .awaitPrimaryReplica(replicaId, timestamp, AWAIT_PRIMARY_REPLICA_TIMEOUT_SEC, SECONDS)
                .handle((replicaMeta, throwable) -> {
                    if (throwable != null) {
                        Throwable unwrapThrowable = ExceptionUtils.unwrapCause(throwable);

                        if (unwrapThrowable instanceof PrimaryReplicaAwaitTimeoutException) {
                            return awaitPrimaryReplica(replicaId, timestamp);
                        } else {
                            return CompletableFuture.<ReplicaMeta>failedFuture(unwrapThrowable);
                        }
                    }

                    return completedFuture(replicaMeta);
                }).thenCompose(Function.identity());
    }

    /** Shortcut to schedule index building. */
    private void scheduleBuildIndex(
            TablePartitionId replicaId,
            CatalogIndexDescriptor indexDescriptor,
            MvTableStorage mvTableStorage,
            long enlistmentConsistencyToken
    ) {
        MvPartitionStorage mvPartition = mvPartitionStorage(mvTableStorage, replicaId);

        assert mvPartition != null : "Partition storage is missing, replicaId=" + replicaId;

        IndexStorage indexStorage = indexStorage(mvTableStorage, replicaId, indexDescriptor);

        indexBuilder.scheduleBuildIndex(
                replicaId.tableId(),
                replicaId.partitionId(),
                indexDescriptor.id(),
                indexStorage,
                mvPartition,
                localNode(),
                enlistmentConsistencyToken
        );
    }

    /** Shortcut to schedule {@link CatalogIndexStatus#AVAILABLE available} index building after disaster recovery. */
    private void scheduleBuildIndexAfterDisasterRecovery(
            TablePartitionId replicaId,
            CatalogIndexDescriptor indexDescriptor,
            MvTableStorage mvTableStorage,
            long enlistmentConsistencyToken
    ) {
        MvPartitionStorage mvPartition = mvPartitionStorage(mvTableStorage, replicaId);

        assert mvPartition != null : "Partition storage is missing, replicaId=" + replicaId;

        IndexStorage indexStorage = indexStorage(mvTableStorage, replicaId, indexDescriptor);

        indexBuilder.scheduleBuildIndexAfterDisasterRecovery(
                replicaId.tableId(),
                replicaId.partitionId(),
                indexDescriptor.id(),
                indexStorage,
                mvPartition,
                localNode(),
                enlistmentConsistencyToken
        );
    }

    private ClusterNode localNode() {
        return IndexManagementUtils.localNode(clusterService);
    }

    private boolean isLeaseExpire(ReplicaMeta replicaMeta) {
        return !isPrimaryReplica(replicaMeta, localNode(), clockService.now());
    }

    private static long enlistmentConsistencyToken(ReplicaMeta replicaMeta) {
        return replicaMeta.getStartTime().longValue();
    }

    private static @Nullable MvPartitionStorage mvPartitionStorage(MvTableStorage mvTableStorage, TablePartitionId replicaId) {
        MvPartitionStorage mvPartition = mvTableStorage.getMvPartition(replicaId.partitionId());

        assert mvPartition != null : "Partition storage is missing, replicaId=" + replicaId;

        return mvPartition;
    }

    private static IndexStorage indexStorage(
            MvTableStorage mvTableStorage,
            TablePartitionId replicaId,
            CatalogIndexDescriptor indexDescriptor
    ) {
        IndexStorage indexStorage = mvTableStorage.getIndex(replicaId.partitionId(), indexDescriptor.id());

        assert indexStorage != null : "replicaId=" + replicaId + ", indexId=" + indexDescriptor.id();

        return indexStorage;
    }
}
