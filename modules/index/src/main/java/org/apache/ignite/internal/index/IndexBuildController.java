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
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongFunction;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CreateIndexEventParameters;
import org.apache.ignite.internal.catalog.events.DropIndexEventParameters;
import org.apache.ignite.internal.causality.IncrementalVersionedValue;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
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

/** No doc. */
// TODO: IGNITE-20330 код, тесты и документация
public class IndexBuildController implements IgniteComponent {
    private static final long AWAIT_PRIMARY_REPLICA_TIMEOUT = 10;

    private final IndexBuilder indexBuilder;

    private final IndexManager indexManager;

    private final MetaStorageManager metaStorageManager;

    private final CatalogService catalogService;

    private final ClusterService clusterService;

    private final PlacementDriver placementDriver;

    private final HybridClock clock;

    /** Versioned value used only at the start of the component. */
    private final IncrementalVersionedValue<Void> startVv;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean closeGuard = new AtomicBoolean();

    private final Set<TablePartitionId> primaryReplicaIds = ConcurrentHashMap.newKeySet();

    /** Constructor. */
    public IndexBuildController(
            IndexBuilder indexBuilder,
            IndexManager indexManager,
            MetaStorageManager metaStorageManager,
            CatalogService catalogService,
            ClusterService clusterService,
            PlacementDriver placementDriver,
            HybridClock clock,
            Consumer<LongFunction<CompletableFuture<?>>> registry
    ) {
        this.indexBuilder = indexBuilder;
        this.indexManager = indexManager;
        this.metaStorageManager = metaStorageManager;
        this.catalogService = catalogService;
        this.clusterService = clusterService;
        this.placementDriver = placementDriver;
        this.clock = clock;

        startVv = new IncrementalVersionedValue<>(registry);
    }

    @Override
    public void start() {
        inBusyLock(busyLock, () -> {
            CompletableFuture<Long> recoveryFinishedFuture = metaStorageManager.recoveryFinishedFuture();

            assert recoveryFinishedFuture.isDone();

            int catalogVersion = catalogService.latestCatalogVersion();
            long causalityToken = recoveryFinishedFuture.join();

            CompletableFuture<Void> startBuildIndexesFuture = startBuildIndexesBusyAsync(causalityToken, catalogVersion);

            // Forces to wait until recovery is complete before the metastore watches are deployed to avoid races with other components.
            startVv.update(causalityToken, (unused, throwable) -> {
                if (throwable != null) {
                    return failedFuture(throwable);
                }

                return startBuildIndexesFuture;
            });

            addListeners();
        });
    }

    @Override
    public void stop() {
        if (!closeGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        indexBuilder.close();
    }

    private CompletableFuture<Void> startBuildIndexesBusyAsync(long causalityToken, int catalogVersion) {
        List<CompletableFuture<?>> startBuildIndexFutures = new ArrayList<>();

        // TODO: IGNITE-20530 We only need to get write-only indexes
        for (CatalogIndexDescriptor indexDescriptor : catalogService.indexes(catalogVersion)) {
            int partitions = partitions(indexDescriptor.tableId(), catalogVersion);

            for (int partitionId = 0; partitionId < partitions; partitionId++) {
                TablePartitionId replicaId = new TablePartitionId(indexDescriptor.tableId(), partitionId);

                CompletableFuture<?> startBuildIndexFuture = getMvTableStorageFuture(causalityToken, replicaId)
                        .thenCompose(mvTableStorage -> getPrimaryReplicaForNow(replicaId)
                                .thenAccept(replicaMeta -> inBusyLock(busyLock, () -> {
                                    if (replicaMeta == null || isLeaseExpire(replicaMeta)) {
                                        return;
                                    }

                                    primaryReplicaIds.add(replicaId);

                                    startBuildIndex(replicaId, indexDescriptor, mvTableStorage);
                                })));

                startBuildIndexFutures.add(startBuildIndexFuture);
            }
        }

        return allOf(startBuildIndexFutures.toArray(CompletableFuture[]::new));
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
                            .thenCompose(mvTableStorage -> awaitPrimaryReplicaFowNow(primaryReplicaId)
                                    .thenAccept(replicaMeta -> tryStartBuildIndex(
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
                        .thenCompose(mvTableStorage -> awaitPrimaryReplicaFowNow(primaryReplicaId)
                                .thenAccept(replicaMeta -> tryStartBuildIndexesForNewPrimaryReplica(
                                        catalogVersion,
                                        primaryReplicaId,
                                        mvTableStorage,
                                        replicaMeta
                                ))
                        );
            } else {
                stopBuildingIndexesIfPrimacyLost(primaryReplicaId);

                return completedFuture(null);
            }
        });
    }

    private void tryStartBuildIndexesForNewPrimaryReplica(
            int catalogVersion,
            TablePartitionId primaryReplicaId,
            MvTableStorage mvTableStorage,
            ReplicaMeta replicaMeta
    ) {
        inBusyLock(busyLock, () -> {
            if (isLeaseExpire(replicaMeta)) {
                stopBuildingIndexesIfPrimacyLost(primaryReplicaId);

                return;
            }

            // TODO: IGNITE-20530 We only need to get write-only indexes
            for (CatalogIndexDescriptor indexDescriptor : catalogService.indexes(catalogVersion)) {
                if (primaryReplicaId.tableId() == indexDescriptor.tableId()) {
                    startBuildIndex(primaryReplicaId, indexDescriptor, mvTableStorage);
                }
            }
        });
    }

    private void tryStartBuildIndex(
            TablePartitionId primaryReplicaId,
            CatalogIndexDescriptor indexDescriptor,
            MvTableStorage mvTableStorage,
            ReplicaMeta replicaMeta
    ) {
        inBusyLock(busyLock, () -> {
            if (isLeaseExpire(replicaMeta)) {
                stopBuildingIndexesIfPrimacyLost(primaryReplicaId);

                return;
            }

            startBuildIndex(primaryReplicaId, indexDescriptor, mvTableStorage);
        });
    }

    private void stopBuildingIndexesIfPrimacyLost(TablePartitionId replicaId) {
        if (primaryReplicaIds.remove(replicaId)) {
            // Primary replica is no longer current, we need to stop building indexes for it.
            indexBuilder.stopBuildingIndexes(replicaId.tableId(), replicaId.partitionId());
        }
    }

    private CompletableFuture<MvTableStorage> getMvTableStorageFuture(long causalityToken, TablePartitionId replicaId) {
        return indexManager.getMvTableStorage(causalityToken, replicaId.tableId());
    }

    private CompletableFuture<ReplicaMeta> awaitPrimaryReplicaFowNow(TablePartitionId replicaId) {
        return placementDriver
                .awaitPrimaryReplica(replicaId, clock.now(), AWAIT_PRIMARY_REPLICA_TIMEOUT, SECONDS)
                .handle((replicaMeta, throwable) -> {
                    if (throwable != null) {
                        Throwable unwrapThrowable = ExceptionUtils.unwrapCause(throwable);

                        if (unwrapThrowable instanceof PrimaryReplicaAwaitTimeoutException) {
                            return awaitPrimaryReplicaFowNow(replicaId);
                        } else {
                            throw new CompletionException(unwrapThrowable);
                        }
                    }

                    return completedFuture(replicaMeta);
                }).thenCompose(Function.identity());
    }

    private CompletableFuture<ReplicaMeta> getPrimaryReplicaForNow(TablePartitionId replicaId) {
        return placementDriver.getPrimaryReplica(replicaId, clock.now());
    }

    private void startBuildIndex(TablePartitionId replicaId, CatalogIndexDescriptor indexDescriptor, MvTableStorage mvTableStorage) {
        int partitionId = replicaId.partitionId();

        MvPartitionStorage mvPartition = mvTableStorage.getMvPartition(partitionId);

        assert mvPartition != null : replicaId;

        int indexId = indexDescriptor.id();

        IndexStorage indexStorage = mvTableStorage.getIndex(partitionId, indexId);

        assert indexStorage != null : "replicaId=" + replicaId + ", indexId=" + indexId;

        indexBuilder.startBuildIndex(replicaId.tableId(), partitionId, indexId, indexStorage, mvPartition, localNode());
    }

    private boolean isLocalNode(String nodeConsistentId) {
        return nodeConsistentId.equals(localNode().name());
    }

    private ClusterNode localNode() {
        return clusterService.topologyService().localMember();
    }

    private boolean isLeaseExpire(ReplicaMeta replicaMeta) {
        return !isLocalNode(replicaMeta.getLeaseholder()) || clock.now().after(replicaMeta.getExpirationTime());
    }

    private int partitions(int tableId, int catalogVersion) {
        CatalogTableDescriptor tableDescriptor = catalogService.table(tableId, catalogVersion);

        assert tableDescriptor != null : tableId;

        CatalogZoneDescriptor zoneDescriptor = catalogService.zone(tableDescriptor.zoneId(), catalogVersion);

        assert zoneDescriptor != null : "zoneId=" + tableDescriptor.zoneId() + ", tableId=" + tableId;

        return zoneDescriptor.partitions();
    }
}
