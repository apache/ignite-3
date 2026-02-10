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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.AVAILABLE;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.BUILDING;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.INDEX_BUILDING;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.INDEX_REMOVED;
import static org.apache.ignite.internal.index.IndexManagementUtils.AWAIT_PRIMARY_REPLICA_TIMEOUT_SEC;
import static org.apache.ignite.internal.index.IndexManagementUtils.isLocalNode;
import static org.apache.ignite.internal.index.IndexManagementUtils.isPrimaryReplica;
import static org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;

import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.RemoveIndexEventParameters;
import org.apache.ignite.internal.catalog.events.StartBuildingIndexEventParameters;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.failure.FailureType;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.partition.replicator.PartitionReplicaLifecycleManager;
import org.apache.ignite.internal.partition.replicator.TableTxRwOperationTracker;
import org.apache.ignite.internal.partition.replicator.ZonePartitionReplicaListener;
import org.apache.ignite.internal.partition.replicator.ZoneResourcesManager.ZonePartitionResources;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.PrimaryReplicaAwaitTimeoutException;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageClosedException;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.jetbrains.annotations.Nullable;

/**
 * Component is responsible for starting and stopping the building of indexes on primary replicas.
 *
 * <p>Component handles the following events (indexes are started and stopped by {@link CatalogIndexDescriptor#tableId()}): </p>
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
    private static final IgniteLogger LOG = Loggers.forClass(IndexBuildController.class);

    private final IndexBuilder indexBuilder;

    private final IndexManager indexManager;

    private final CatalogService catalogService;

    private final ClusterService clusterService;

    private final PlacementDriver placementDriver;

    private final ClockService clockService;

    private final PartitionReplicaLifecycleManager partitionReplicaLifecycleManager;

    private final FailureProcessor failureProcessor;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean closeGuard = new AtomicBoolean();

    private final Set<ZonePartitionId> primaryReplicaIds = ConcurrentHashMap.newKeySet();

    /** Constructor. */
    IndexBuildController(
            IndexBuilder indexBuilder,
            IndexManager indexManager,
            CatalogService catalogService,
            ClusterService clusterService,
            PlacementDriver placementDriver,
            ClockService clockService,
            PartitionReplicaLifecycleManager partitionReplicaLifecycleManager,
            FailureProcessor failureProcessor
    ) {
        this.indexBuilder = indexBuilder;
        this.indexManager = indexManager;
        this.catalogService = catalogService;
        this.clusterService = clusterService;
        this.placementDriver = placementDriver;
        this.clockService = clockService;
        this.partitionReplicaLifecycleManager = partitionReplicaLifecycleManager;
        this.failureProcessor = failureProcessor;
    }

    /** Starts component. */
    public void start() {
        inBusyLock(busyLock, this::addListeners);
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
        catalogService.listen(INDEX_BUILDING, EventListener.fromConsumer(this::onIndexBuilding));

        catalogService.listen(INDEX_REMOVED, EventListener.fromConsumer(this::onIndexRemoved));

        placementDriver.listen(PRIMARY_REPLICA_ELECTED, EventListener.fromConsumer(this::onPrimaryReplicaElected));
    }

    private void onIndexBuilding(StartBuildingIndexEventParameters parameters) {
        inBusyLockAsync(busyLock, () -> {
            Catalog catalog = catalogService.catalog(parameters.catalogVersion());

            assert catalog != null : "Failed to find a catalog for the specified version [version=" + parameters.catalogVersion()
                    + ", earliestVersion=" + catalogService.earliestCatalogVersion()
                    + ", latestVersion=" + catalogService.latestCatalogVersion()
                    + "].";

            CatalogIndexDescriptor indexDescriptor = catalog.index(parameters.indexId());

            assert indexDescriptor != null : "Failed to find an index descriptor for the specified index [indexId="
                    + parameters.indexId() + ", catalogVersion=" + parameters.catalogVersion() + "].";

            CatalogTableDescriptor tableDescriptor = catalog.table(indexDescriptor.tableId());
            assert tableDescriptor != null : "Failed to find a table descriptor for the specified index [indexId="
                    + parameters.indexId() + ", tableId=" + indexDescriptor.tableId()
                    + ", catalogVersion=" + parameters.catalogVersion() + "].";

            CatalogZoneDescriptor zoneDescriptor = catalog.zone(tableDescriptor.zoneId());

            assert zoneDescriptor != null : "Failed to find a zone descriptor for the specified table [indexId="
                    + parameters.indexId() + ", tableId=" + indexDescriptor.tableId()
                    + ", catalogVersion=" + parameters.catalogVersion() + "].";

            var startBuildIndexFutures = new ArrayList<CompletableFuture<?>>();

            for (ZonePartitionId zoneReplicaId : primaryReplicaIds) {
                int zoneId = zoneReplicaId.zoneId();
                int partitionId = zoneReplicaId.partitionId();

                boolean needToProcessPartition = zoneReplicaId.zoneId() == zoneDescriptor.id();

                if (needToProcessPartition) {
                    CompletableFuture<?> startBuildIndexFuture = indexManager
                            .getMvTableStorage(parameters.causalityToken(), indexDescriptor.tableId())
                            .thenCompose(mvTableStorage -> {
                                        HybridTimestamp buildAttemptTimestamp = clockService.now();
                                        return awaitPrimaryReplica(zoneReplicaId, buildAttemptTimestamp)
                                                .thenAccept(replicaMeta -> tryScheduleBuildIndex(
                                                        zoneId,
                                                        indexDescriptor.tableId(),
                                                        partitionId,
                                                        zoneReplicaId,
                                                        indexDescriptor,
                                                        mvTableStorage,
                                                        replicaMeta,
                                                        buildAttemptTimestamp
                                                ));
                                        }
                            );

                    startBuildIndexFutures.add(startBuildIndexFuture);
                }
            }

            return CompletableFutures.allOf(startBuildIndexFutures);
        }).whenComplete((res, ex) -> {
            // StorageClosedException happens on node stop, so we can ignore it.
            if (ex != null && !hasCause(ex, NodeStoppingException.class, StorageClosedException.class)) {
                failureProcessor.process(new FailureContext(FailureType.CRITICAL_ERROR, ex));
            }
        });
    }

    private void onIndexRemoved(RemoveIndexEventParameters parameters) {
        inBusyLockAsync(busyLock, () -> {
            indexBuilder.stopBuildingIndexes(parameters.indexId());

            return nullCompletedFuture();
        }).whenComplete((res, ex) -> {
            if (ex != null) {
                failureProcessor.process(new FailureContext(FailureType.CRITICAL_ERROR, ex));
            }
        });
    }

    private void onPrimaryReplicaElected(PrimaryReplicaEventParameters parameters) {
        inBusyLockAsync(busyLock, () -> {
            assert parameters.groupId() instanceof ZonePartitionId :
                    "Primary replica ID must be of type ZonePartitionId [groupId="
                            + parameters.groupId() + ", class=" + parameters.groupId().getClass().getSimpleName() + "].";

            ZonePartitionId primaryReplicaId = (ZonePartitionId) parameters.groupId();

            if (isLocalNode(clusterService, parameters.leaseholderId())) {
                primaryReplicaIds.add(primaryReplicaId);

                // It is safe to get the latest version of the catalog because the PRIMARY_REPLICA_ELECTED event is handled on the
                // metastore thread.
                Catalog catalog = catalogService.latestCatalog();

                CatalogZoneDescriptor zoneDescriptor = catalog.zone(primaryReplicaId.zoneId());
                // TODO: IGNITE-22656 It is necessary not to generate an event for a destroyed zone by LWM
                if (zoneDescriptor == null) {
                    return nullCompletedFuture();
                }

                var indexFutures = new ArrayList<CompletableFuture<?>>();
                for (CatalogTableDescriptor tableDescriptor : catalog.tables(zoneDescriptor.id())) {
                    // Perhaps, it makes sense to get primary replica future first and then get table storage future,
                    // because, it will be the same for all tables in the zone for the given partition.
                    CompletableFuture<?> future =
                            indexManager.getMvTableStorage(parameters.causalityToken(), tableDescriptor.id())
                                    .thenCompose(mvTableStorage -> {
                                                HybridTimestamp buildAttemptTimestamp = clockService.now();
                                                return awaitPrimaryReplica(primaryReplicaId, buildAttemptTimestamp)
                                                        .thenAccept(replicaMeta -> tryScheduleBuildIndexesForNewPrimaryReplica(
                                                                catalog,
                                                                tableDescriptor,
                                                                primaryReplicaId,
                                                                mvTableStorage,
                                                                replicaMeta,
                                                                buildAttemptTimestamp
                                                        ));
                                            }
                                    );

                    indexFutures.add(future);
                }

                return CompletableFutures.allOf(indexFutures);
            } else {
                stopBuildingIndexesIfPrimaryExpired(primaryReplicaId);

                return nullCompletedFuture();
            }
        }).whenComplete((res, ex) -> {
            if (ex != null) {
                // StorageClosedException happens on node stop, so we can ignore it.
                if (!hasCause(ex, NodeStoppingException.class, StorageClosedException.class)) {
                    failureProcessor.process(new FailureContext(FailureType.CRITICAL_ERROR, ex));
                }
            }
        });
    }

    private void tryScheduleBuildIndexesForNewPrimaryReplica(
            Catalog catalog,
            CatalogTableDescriptor tableDescriptor,
            ZonePartitionId primaryReplicaId,
            MvTableStorage mvTableStorage,
            ReplicaMeta replicaMeta,
            HybridTimestamp buildAttemptTimestamp
    ) {
        inBusyLock(busyLock, () -> {
            if (isLeaseExpired(replicaMeta, buildAttemptTimestamp)) {
                LOG.info("Lease has expired (on new primary), stopping build index process [groupId={}, localNode={},"
                        + " primaryReplica={}.", primaryReplicaId, localNode(), replicaMeta);
                stopBuildingIndexesIfPrimaryExpired(primaryReplicaId);

                return;
            }

            for (CatalogIndexDescriptor indexDescriptor : catalog.indexes(tableDescriptor.id())) {
                if (indexDescriptor.status() == BUILDING) {
                    scheduleBuildIndex(
                            tableDescriptor.zoneId(),
                            tableDescriptor.id(),
                            primaryReplicaId.partitionId(),
                            indexDescriptor,
                            mvTableStorage,
                            enlistmentConsistencyToken(replicaMeta),
                            clockService.current()
                    );
                } else if (indexDescriptor.status() == AVAILABLE) {
                    scheduleBuildIndexAfterDisasterRecovery(
                            tableDescriptor.zoneId(),
                            tableDescriptor.id(),
                            primaryReplicaId.partitionId(),
                            indexDescriptor,
                            mvTableStorage,
                            enlistmentConsistencyToken(replicaMeta)
                    );
                }
            }
        });
    }

    private void tryScheduleBuildIndex(
            int zoneId,
            int tableId,
            int partitionId,
            ZonePartitionId primaryReplicaId,
            CatalogIndexDescriptor indexDescriptor,
            MvTableStorage mvTableStorage,
            ReplicaMeta replicaMeta,
            HybridTimestamp buildAttemptTimestamp
    ) {
        assert primaryReplicaId.zoneId() == zoneId && primaryReplicaId.partitionId() == partitionId
                : "Primary replica identifier mismatched [zoneId=" + zoneId + ", tableId=" + tableId
                + ", partitionId=" + partitionId + ", primaryReplicaId=" + primaryReplicaId
                + ", primaryReplicaCls=" + primaryReplicaId.getClass().getSimpleName() + "].";

        inBusyLock(busyLock, () -> {
            if (isLeaseExpired(replicaMeta, buildAttemptTimestamp)) {
                stopBuildingIndexesIfPrimaryExpired(primaryReplicaId);

                return;
            }

            scheduleBuildIndex(
                    zoneId,
                    tableId,
                    partitionId,
                    indexDescriptor,
                    mvTableStorage,
                    enlistmentConsistencyToken(replicaMeta),
                    clockService.current()
            );
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
    private void stopBuildingIndexesIfPrimaryExpired(ZonePartitionId replicaId) {
        if (primaryReplicaIds.remove(replicaId)) {
            // Primary replica is no longer current, we need to stop building indexes for it.
            indexBuilder.stopBuildingZoneIndexes(replicaId.zoneId(), replicaId.partitionId());
        }
    }

    private CompletableFuture<ReplicaMeta> awaitPrimaryReplica(ZonePartitionId replicaId, HybridTimestamp timestamp) {
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
            int zoneId,
            int tableId,
            int partitionId,
            CatalogIndexDescriptor indexDescriptor,
            MvTableStorage mvTableStorage,
            long enlistmentConsistencyToken,
            HybridTimestamp initialOperationTimestamp
    ) {
        ZonePartitionId zonePartitionId = new ZonePartitionId(zoneId, partitionId);
        ZonePartitionResources resources = partitionReplicaLifecycleManager.zonePartitionResourcesOrNull(zonePartitionId);
        if (resources == null) {
            // Already stopped/destroyed, ignore.
            return;
        }

        TableTxRwOperationTracker txRwOperationTracker = txRwOperationTracker(zonePartitionId, tableId, resources, indexDescriptor);
        if (txRwOperationTracker == null) {
            return;
        }

        MvPartitionStorage mvPartition = mvPartitionStorage(mvTableStorage, zoneId, tableId, partitionId);

        IndexStorage indexStorage = indexStorage(mvTableStorage, partitionId, indexDescriptor);

        indexBuilder.scheduleBuildIndex(
                zoneId,
                tableId,
                partitionId,
                indexDescriptor.id(),
                indexStorage,
                mvPartition,
                txRwOperationTracker,
                resources.safeTimeTracker(),
                localNode(),
                enlistmentConsistencyToken,
                initialOperationTimestamp
        );
    }

    /** Shortcut to schedule {@link CatalogIndexStatus#AVAILABLE available} index building after disaster recovery. */
    private void scheduleBuildIndexAfterDisasterRecovery(
            int zoneId,
            int tableId,
            int partitionId,
            CatalogIndexDescriptor indexDescriptor,
            MvTableStorage mvTableStorage,
            long enlistmentConsistencyToken
    ) {
        ZonePartitionId zonePartitionId = new ZonePartitionId(zoneId, partitionId);
        ZonePartitionResources resources = partitionReplicaLifecycleManager.zonePartitionResourcesOrNull(zonePartitionId);
        if (resources == null) {
            // Already stopped/destroyed, ignore.
            return;
        }

        TableTxRwOperationTracker txRwOperationTracker = txRwOperationTracker(zonePartitionId, tableId, resources, indexDescriptor);
        if (txRwOperationTracker == null) {
            return;
        }

        MvPartitionStorage mvPartition = mvPartitionStorage(mvTableStorage, zoneId, tableId, partitionId);

        IndexStorage indexStorage = indexStorage(mvTableStorage, partitionId, indexDescriptor);

        indexBuilder.scheduleBuildIndexAfterDisasterRecovery(
                zoneId,
                tableId,
                partitionId,
                indexDescriptor.id(),
                indexStorage,
                mvPartition,
                txRwOperationTracker,
                resources.safeTimeTracker(),
                localNode(),
                enlistmentConsistencyToken,
                clockService.current()
        );
    }

    private static @Nullable TableTxRwOperationTracker txRwOperationTracker(
            ZonePartitionId zonePartitionId,
            int tableId,
            ZonePartitionResources resources,
            CatalogIndexDescriptor indexDescriptor
    ) {
        CompletableFuture<ZonePartitionReplicaListener> replicaListenerFuture = resources.replicaListenerFuture();
        assert replicaListenerFuture.isDone() : "Replica listener future is not done for [zonePartitionId=" + zonePartitionId + "].";

        ZonePartitionReplicaListener replicaListener = replicaListenerFuture.join();
        @Nullable TableTxRwOperationTracker txRwOperationTracker = replicaListener.txRwOperationTracker(tableId);

        if (txRwOperationTracker == null) {
            // Null means that the table has been removed due to table destruction.
            LOG.info(
                    "Tracker is null, skipping index build scheduling "
                            + "[zoneId={}, tableId={}, partitionId={}, indexId={}]",
                    zonePartitionId.zoneId(),
                    tableId,
                    zonePartitionId.partitionId(),
                    indexDescriptor.id()
            );
        }

        return txRwOperationTracker;
    }

    private InternalClusterNode localNode() {
        return IndexManagementUtils.localNode(clusterService);
    }

    private boolean isLeaseExpired(ReplicaMeta replicaMeta, HybridTimestamp buildAttemptTimestamp) {
        return !isPrimaryReplica(replicaMeta, localNode(), buildAttemptTimestamp);
    }

    private static long enlistmentConsistencyToken(ReplicaMeta replicaMeta) {
        return replicaMeta.getStartTime().longValue();
    }

    private static MvPartitionStorage mvPartitionStorage(
            MvTableStorage mvTableStorage,
            int zoneId,
            int tableId,
            int partitionId
    ) {
        MvPartitionStorage mvPartition = mvTableStorage.getMvPartition(partitionId);

        assert mvPartition != null : "Partition storage is missing [zoneId=" + zoneId
                + ", tableId=" + tableId + ", partitionId=" + partitionId + "].";

        return mvPartition;
    }

    private static IndexStorage indexStorage(
            MvTableStorage mvTableStorage,
            int partitionId,
            CatalogIndexDescriptor indexDescriptor
    ) {
        IndexStorage indexStorage = mvTableStorage.getIndex(partitionId, indexDescriptor.id());

        assert indexStorage != null : "Index storage is missing [partitionId=" + partitionId + ", indexId=" + indexDescriptor.id() + "].";

        return indexStorage;
    }
}
