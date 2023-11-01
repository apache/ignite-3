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
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.index.IndexManagementUtils.getPartitionCountFromCatalog;
import static org.apache.ignite.internal.index.IndexManagementUtils.isPrimaryReplica;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CreateIndexEventParameters;
import org.apache.ignite.internal.catalog.events.DropIndexEventParameters;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.PrimaryReplicaAwaitTimeoutException;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.table.distributed.index.IndexBuilder;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;

/**
 * Сomponent is responsible for starting and stopping the building of indexes on primary replicas.
 *
 * <p>Component handles the following events (indexes are started and stopped by {@link CatalogIndexDescriptor#tableId()} ==
 * {@link TablePartitionId#tableId()}): </p>
 * <ul>
 *     <li>{@link CatalogEvent#INDEX_CREATE} - starts building indexes for the corresponding local primary replicas.</li>
 *     <li>{@link CatalogEvent#INDEX_DROP} - stops building indexes for the corresponding local primary replicas.</li>
 *     <li>{@link PrimaryReplicaEvent#PRIMARY_REPLICA_ELECTED} - for a new local primary replica, starts the building of all corresponding
 *     indexes, for an expired primary replica, stops the building of all corresponding indexes.</li>
 * </ul>
 *
 * <p>To recover the building of indexes on node recovery, you need to use {@link #recoverBuildIndexes(long)}.</p>
 */
public class IndexBuildController implements IgniteComponent {
    private static final long AWAIT_PRIMARY_REPLICA_TIMEOUT_SEC = 10;

    private final IndexBuilder indexBuilder;

    private final IndexManager indexManager;

    private final CatalogService catalogService;

    private final ClusterService clusterService;

    private final PlacementDriver placementDriver;

    private final HybridClock clock;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean closeGuard = new AtomicBoolean();

    private final Set<TablePartitionId> primaryReplicaIds = ConcurrentHashMap.newKeySet();

    /** Constructor. */
    public IndexBuildController(
            IndexBuilder indexBuilder,
            IndexManager indexManager,
            CatalogService catalogService,
            ClusterService clusterService,
            PlacementDriver placementDriver,
            HybridClock clock
    ) {
        this.indexBuilder = indexBuilder;
        this.indexManager = indexManager;
        this.catalogService = catalogService;
        this.clusterService = clusterService;
        this.placementDriver = placementDriver;
        this.clock = clock;

        addListeners();
    }

    @Override
    public void start() {
        // No-op.
    }

    @Override
    public void stop() {
        if (!closeGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        indexBuilder.close();
    }

    /**
     * Recovers the building of {@link CatalogIndexDescriptor#available() registered} indexes on node recovery. Building of indexes will be 
     * recovered only for those partitions for which the local node is the primary replica.
     *
     * @param recoveryRevision Metastore revision on recovery.
     * @return Future of recovery execution.
     */
    // TODO: IGNITE-20638 Use when adding the IndexBuildingManager
    public CompletableFuture<Void> recoverBuildIndexes(long recoveryRevision) {
        return inBusyLockAsync(busyLock, () -> {
            // // It is expected that the method will only be called on recovery, when the deploy of metastore watches has not yet occurred.
            int catalogVersion = catalogService.latestCatalogVersion();

            List<CompletableFuture<?>> futures = new ArrayList<>();

            for (CatalogIndexDescriptor indexDescriptor : catalogService.indexes(catalogVersion)) {
                if (!indexDescriptor.available()) {
                    int partitions = getPartitionCountFromCatalog(catalogService, indexDescriptor.id(), catalogVersion);

                    futures.add(recoverBuildIndexBusy(indexDescriptor, recoveryRevision, partitions));
                }
            }

            return allOf(futures.toArray(CompletableFuture[]::new));
        });
    }

    private void addListeners() {
        catalogService.listen(CatalogEvent.INDEX_CREATE, (parameters, exception) -> {
            if (exception != null) {
                return failedFuture(exception);
            }

            return onIndexCreate(((CreateIndexEventParameters) parameters)).thenApply(unused -> false);
        });

        catalogService.listen(CatalogEvent.INDEX_DROP, (parameters, exception) -> {
            if (exception != null) {
                return failedFuture(exception);
            }

            return onIndexDrop(((DropIndexEventParameters) parameters)).thenApply(unused -> false);
        });

        placementDriver.listen(PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED, (parameters, exception) -> {
            if (exception != null) {
                return failedFuture(exception);
            }

            return onPrimaryReplicaElected(parameters).thenApply(unused -> false);
        });
    }

    private CompletableFuture<?> onIndexCreate(CreateIndexEventParameters parameters) {
        return inBusyLockAsync(busyLock, () -> {
            var startBuildIndexFutures = new ArrayList<CompletableFuture<?>>();

            for (TablePartitionId primaryReplicaId : primaryReplicaIds) {
                if (primaryReplicaId.tableId() == parameters.indexDescriptor().tableId()) {
                    CompletableFuture<?> startBuildIndexFuture = getMvTableStorageFuture(parameters.causalityToken(), primaryReplicaId)
                            .thenCompose(mvTableStorage -> awaitPrimaryReplicaForNow(primaryReplicaId)
                                    .thenAccept(replicaMeta -> tryScheduleBuildIndex(
                                            primaryReplicaId,
                                            parameters.indexDescriptor(),
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

    private CompletableFuture<?> onIndexDrop(DropIndexEventParameters parameters) {
        return inBusyLockAsync(busyLock, () -> {
            indexBuilder.stopBuildingIndexes(parameters.indexId());

            return completedFuture(null);
        });
    }

    private CompletableFuture<?> onPrimaryReplicaElected(PrimaryReplicaEventParameters parameters) {
        return inBusyLockAsync(busyLock, () -> {
            TablePartitionId primaryReplicaId = (TablePartitionId) parameters.groupId();

            if (isLocalNode(parameters.leaseholder())) {
                primaryReplicaIds.add(primaryReplicaId);

                // It is safe to get the latest version of the catalog because the PRIMARY_REPLICA_ELECTED event is handled on the
                // metastore thread.
                int catalogVersion = catalogService.latestCatalogVersion();

                return getMvTableStorageFuture(parameters.causalityToken(), primaryReplicaId)
                        .thenCompose(mvTableStorage -> awaitPrimaryReplicaForNow(primaryReplicaId)
                                .thenAccept(replicaMeta -> tryScheduleBuildIndexesForNewPrimaryReplica(
                                        catalogVersion,
                                        primaryReplicaId,
                                        mvTableStorage,
                                        replicaMeta
                                ))
                        );
            } else {
                stopBuildingIndexesIfPrimaryExpired(primaryReplicaId);

                return completedFuture(null);
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
            if (!isPrimaryReplica(replicaMeta, localNode(), clock.now())) {
                stopBuildingIndexesIfPrimaryExpired(primaryReplicaId);

                return;
            }

            for (CatalogIndexDescriptor indexDescriptor : catalogService.indexes(catalogVersion)) {
                if (!indexDescriptor.available() && primaryReplicaId.tableId() == indexDescriptor.tableId()) {
                    scheduleBuildIndex(primaryReplicaId, indexDescriptor, mvTableStorage, enlistmentConsistencyToken(replicaMeta));
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
            if (!isPrimaryReplica(replicaMeta, localNode(), clock.now())) {
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
        return indexManager.getMvTableStorage(causalityToken, replicaId.tableId());
    }

    private CompletableFuture<ReplicaMeta> awaitPrimaryReplicaForNow(TablePartitionId replicaId) {
        return placementDriver
                .awaitPrimaryReplica(replicaId, clock.now(), AWAIT_PRIMARY_REPLICA_TIMEOUT_SEC, SECONDS)
                .handle((replicaMeta, throwable) -> {
                    if (throwable != null) {
                        Throwable unwrapThrowable = ExceptionUtils.unwrapCause(throwable);

                        if (unwrapThrowable instanceof PrimaryReplicaAwaitTimeoutException) {
                            return awaitPrimaryReplicaForNow(replicaId);
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
        int partitionId = replicaId.partitionId();

        MvPartitionStorage mvPartition = mvTableStorage.getMvPartition(partitionId);

        assert mvPartition != null : replicaId;

        int indexId = indexDescriptor.id();

        IndexStorage indexStorage = mvTableStorage.getIndex(partitionId, indexId);

        assert indexStorage != null : "replicaId=" + replicaId + ", indexId=" + indexId;

        indexBuilder.scheduleBuildIndex(
                replicaId.tableId(),
                partitionId,
                indexId,
                indexStorage,
                mvPartition,
                localNode(),
                enlistmentConsistencyToken
        );
    }

    private boolean isLocalNode(String nodeConsistentId) {
        return nodeConsistentId.equals(localNode().name());
    }

    private ClusterNode localNode() {
        return clusterService.topologyService().localMember();
    }

    private static long enlistmentConsistencyToken(ReplicaMeta replicaMeta) {
        return replicaMeta.getStartTime().longValue();
    }

    private CompletableFuture<Void> recoverBuildIndexBusy(CatalogIndexDescriptor indexDescriptor, long recoveryRevision, int partitions) {
        assert !indexDescriptor.available() : indexDescriptor.id();

        List<CompletableFuture<?>> futures = new ArrayList<>();

        for (int partitionId = 0; partitionId < partitions; partitionId++) {
            int finalPartitionId = partitionId;
            int tableId = indexDescriptor.tableId();

            CompletableFuture<?> future = indexManager.getMvTableStorage(recoveryRevision, tableId)
                    .thenCompose(mvTableStorage -> inBusyLockAsync(busyLock, () -> {
                        TablePartitionId replicaGroupId = new TablePartitionId(tableId, finalPartitionId);

                        return placementDriver.getPrimaryReplica(replicaGroupId, clock.now())
                                .thenCompose(primaryReplicaMeta -> inBusyLockAsync(busyLock, () -> {
                                    if (primaryReplicaMeta == null) {
                                        return completedFuture(null);
                                    }

                                    if (isPrimaryReplica(primaryReplicaMeta, localNode(), clock.now())) {
                                        primaryReplicaIds.add(replicaGroupId);
                                    }

                                    tryScheduleBuildIndex(replicaGroupId, indexDescriptor, mvTableStorage, primaryReplicaMeta);

                                    return completedFuture(null);
                                }));
                    }));

            futures.add(future);
        }

        return allOf(futures.toArray(CompletableFuture[]::new));
    }
}
