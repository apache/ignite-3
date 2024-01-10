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
import static org.apache.ignite.internal.index.IndexManagementUtils.isPrimaryReplica;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;

import java.util.ArrayList;
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
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
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
 * <p>A few words about restoring the building of indexes on node recovery: the building of indexes will be resumed by
 * {@link PrimaryReplicaEvent#PRIMARY_REPLICA_ELECTED}, which will fire due to a change in the {@link ReplicaMeta#getLeaseholderId()} on
 * node restart but after {@link ReplicaMeta#getExpirationTime()}.</p>
 */
class IndexBuildController implements ManuallyCloseable {
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
    IndexBuildController(
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
    public void close() {
        if (!closeGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        indexBuilder.close();
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
            if (parameters.indexDescriptor().available()) {
                return nullCompletedFuture();
            }

            var startBuildIndexFutures = new ArrayList<CompletableFuture<?>>();

            for (TablePartitionId primaryReplicaId : primaryReplicaIds) {
                if (primaryReplicaId.tableId() == parameters.indexDescriptor().tableId()) {
                    CompletableFuture<?> startBuildIndexFuture = getMvTableStorageFuture(parameters.causalityToken(), primaryReplicaId)
                            .thenCompose(mvTableStorage -> awaitPrimaryReplica(primaryReplicaId, clock.now())
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

            return nullCompletedFuture();
        });
    }

    private CompletableFuture<?> onPrimaryReplicaElected(PrimaryReplicaEventParameters parameters) {
        return inBusyLockAsync(busyLock, () -> {
            TablePartitionId primaryReplicaId = (TablePartitionId) parameters.groupId();

            if (isLocalNode(parameters.leaseholderId())) {
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
                if (!indexDescriptor.available()) {
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
        return indexManager.getMvTableStorage(causalityToken, replicaId.tableId());
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

    private boolean isLocalNode(String nodeId) {
        return nodeId.equals(localNode().id());
    }

    private ClusterNode localNode() {
        return clusterService.topologyService().localMember();
    }

    private boolean isLeaseExpire(ReplicaMeta replicaMeta) {
        return !isPrimaryReplica(replicaMeta, localNode(), clock.now());
    }

    private static long enlistmentConsistencyToken(ReplicaMeta replicaMeta) {
        return replicaMeta.getStartTime().longValue();
    }
}
