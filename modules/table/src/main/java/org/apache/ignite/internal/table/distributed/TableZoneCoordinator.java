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

package org.apache.ignite.internal.table.distributed;

import static java.util.Collections.emptySet;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.partition.replicator.LocalPartitionReplicaEvent.AFTER_REPLICA_DESTROYED;
import static org.apache.ignite.internal.partition.replicator.LocalPartitionReplicaEvent.AFTER_REPLICA_STOPPED;
import static org.apache.ignite.internal.partition.replicator.LocalPartitionReplicaEvent.BEFORE_REPLICA_STARTED;
import static org.apache.ignite.internal.table.distributed.index.IndexUtils.registerIndexesToTable;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;
import static org.apache.ignite.internal.util.ExceptionUtils.sneakyThrow;
import static org.apache.ignite.internal.util.IgniteUtils.closeAllManually;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.LongSupplier;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.causality.IncrementalVersionedValue;
import org.apache.ignite.internal.causality.OutdatedTokenException;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.partition.replicator.LocalBeforeReplicaStartEventParameters;
import org.apache.ignite.internal.partition.replicator.LocalPartitionReplicaEventParameters;
import org.apache.ignite.internal.partition.replicator.NaiveAsyncReadWriteLock;
import org.apache.ignite.internal.partition.replicator.PartitionReplicaLifecycleManager;
import org.apache.ignite.internal.partition.replicator.PartitionReplicaLifecycleManager.TablePartitionReplicaProcessorFactory;
import org.apache.ignite.internal.partition.replicator.ZoneResourcesManager.ZonePartitionResources;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionDataStorage;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionKey;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionMvStorageAccess;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageClosedException;
import org.apache.ignite.internal.storage.StorageDestroyedException;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.gc.MvGc;
import org.apache.ignite.internal.table.distributed.raft.MinimumRequiredTimeCollectorService;
import org.apache.ignite.internal.table.distributed.raft.TablePartitionProcessor;
import org.apache.ignite.internal.table.distributed.raft.snapshot.FullStateTransferIndexChooser;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.jetbrains.annotations.Nullable;

/**
 * Coordinates table partition lifecycle within distribution zones.
 *
 * <p>Ensures that when zone replicas start/stop/destroy, the correct table partition resources
 * are created, loaded, stopped, and destroyed. Maintains the zone-to-table mapping that enables this.
 */
class TableZoneCoordinator {
    private static final IgniteLogger LOG = Loggers.forClass(TableZoneCoordinator.class);

    // TODO https://issues.apache.org/jira/browse/IGNITE-25347 Clean up entries on zone drop to avoid memory leak.
    /** Mapping zone identifier to a collection of tables related to the zone. */
    private final Map<Integer, Set<TableViewInternal>> tablesPerZone = new ConcurrentHashMap<>();

    /** Locks to synchronize access to the {@link #tablesPerZone}. */
    private final Map<Integer, NaiveAsyncReadWriteLock> tablesPerZoneLocks = new ConcurrentHashMap<>();

    private final Map<TablePartitionId, PartitionTableStatsMetricSource> partModCounterMetricSources = new ConcurrentHashMap<>();
    private final Map<TablePartitionId, LongSupplier> pendingWriteIntentsSuppliers = new ConcurrentHashMap<>();

    private final MvGc mvGc;
    private final FullStateTransferIndexChooser fullStateTransferIndexChooser;

    private final PartitionReplicaLifecycleManager partitionReplicaLifecycleManager;
    private final TablePartitionResourcesFactory partitionResourcesFactory;
    private final MinimumRequiredTimeCollectorService minTimeCollectorService;
    private final MetricManager metricManager;
    private final CatalogService catalogService;
    private final LowWatermark lowWatermark;
    private final ExecutorService ioExecutor;
    private final IgniteSpinBusyLock busyLock;

    // Shared mutable state from TableManager.
    private final Map<Integer, TableViewInternal> tables;
    private final Map<Integer, TableViewInternal> startedTables;
    private final Map<Integer, PartitionSet> localPartsByTableId;
    private final IncrementalVersionedValue<Void> tablesVv;
    private final IncrementalVersionedValue<Void> localPartitionsVv;
    private final IncrementalVersionedValue<Void> assignmentsUpdatedVv;
    private final CompletableFuture<Void> readyToProcessReplicaStarts;

    private final EventListener<LocalBeforeReplicaStartEventParameters> onBeforeZoneReplicaStartedListener =
            this::beforeZoneReplicaStarted;
    private final EventListener<LocalPartitionReplicaEventParameters> onZoneReplicaStoppedListener =
            this::onZoneReplicaStopped;
    private final EventListener<LocalPartitionReplicaEventParameters> onZoneReplicaDestroyedListener =
            this::onZoneReplicaDestroyed;

    /**
     * Constructor.
     *
     * @param mvGc MV garbage collector.
     * @param fullStateTransferIndexChooser Index chooser for full state transfer.
     * @param partitionReplicaLifecycleManager Partition replica lifecycle manager.
     * @param partitionResourcesFactory Factory for creating table partition resources.
     * @param minTimeCollectorService Minimum required time collector service.
     * @param metricManager Metric manager.
     * @param catalogService Catalog service.
     * @param lowWatermark Low watermark.
     * @param ioExecutor Executor for IO operations.
     * @param busyLock Busy lock shared with TableManager.
     * @param tables Registered tables map shared with TableManager.
     * @param startedTables Started tables map shared with TableManager.
     * @param localPartsByTableId Local partitions map shared with TableManager.
     * @param tablesVv Versioned value for linearizing table changing events.
     * @param localPartitionsVv Versioned value for linearizing table partitions changing events.
     * @param assignmentsUpdatedVv Versioned value for tracking assignments updates.
     * @param readyToProcessReplicaStarts Future that completes after table recovery.
     */
    TableZoneCoordinator(
            MvGc mvGc,
            FullStateTransferIndexChooser fullStateTransferIndexChooser,
            PartitionReplicaLifecycleManager partitionReplicaLifecycleManager,
            TablePartitionResourcesFactory partitionResourcesFactory,
            MinimumRequiredTimeCollectorService minTimeCollectorService,
            MetricManager metricManager,
            CatalogService catalogService,
            LowWatermark lowWatermark,
            ExecutorService ioExecutor,
            IgniteSpinBusyLock busyLock,
            Map<Integer, TableViewInternal> tables,
            Map<Integer, TableViewInternal> startedTables,
            Map<Integer, PartitionSet> localPartsByTableId,
            IncrementalVersionedValue<Void> tablesVv,
            IncrementalVersionedValue<Void> localPartitionsVv,
            IncrementalVersionedValue<Void> assignmentsUpdatedVv,
            CompletableFuture<Void> readyToProcessReplicaStarts
    ) {
        this.mvGc = mvGc;
        this.fullStateTransferIndexChooser = fullStateTransferIndexChooser;
        this.partitionReplicaLifecycleManager = partitionReplicaLifecycleManager;
        this.partitionResourcesFactory = partitionResourcesFactory;
        this.minTimeCollectorService = minTimeCollectorService;
        this.metricManager = metricManager;
        this.catalogService = catalogService;
        this.lowWatermark = lowWatermark;
        this.ioExecutor = ioExecutor;
        this.busyLock = busyLock;
        this.tables = tables;
        this.startedTables = startedTables;
        this.localPartsByTableId = localPartsByTableId;
        this.tablesVv = tablesVv;
        this.localPartitionsVv = localPartitionsVv;
        this.assignmentsUpdatedVv = assignmentsUpdatedVv;
        this.readyToProcessReplicaStarts = readyToProcessReplicaStarts;

        // Register event listeners in the constructor to avoid races with "partitionReplicaLifecycleManager"'s recovery.
        // We rely on the "readyToProcessReplicaStarts" future to block event handling until table recovery is completed.
        partitionReplicaLifecycleManager.listen(BEFORE_REPLICA_STARTED, onBeforeZoneReplicaStartedListener);
        partitionReplicaLifecycleManager.listen(AFTER_REPLICA_STOPPED, onZoneReplicaStoppedListener);
        partitionReplicaLifecycleManager.listen(AFTER_REPLICA_DESTROYED, onZoneReplicaDestroyedListener);
    }

    /**
     * Starts the coordinator: starts MvGc and FullStateTransferIndexChooser.
     */
    void start() {
        mvGc.start();
        fullStateTransferIndexChooser.start();
    }

    /**
     * Stops the coordinator: removes replica listeners, closes MvGc and FullStateTransferIndexChooser.
     *
     * @throws Exception If an error occurs during close.
     */
    void stop() throws Exception {
        partitionReplicaLifecycleManager.removeListener(AFTER_REPLICA_DESTROYED, onZoneReplicaDestroyedListener);
        partitionReplicaLifecycleManager.removeListener(AFTER_REPLICA_STOPPED, onZoneReplicaStoppedListener);
        partitionReplicaLifecycleManager.removeListener(BEFORE_REPLICA_STARTED, onBeforeZoneReplicaStartedListener);

        closeAllManually(mvGc, fullStateTransferIndexChooser);
    }

    /**
     * Returns total pending write intents count across all partitions.
     */
    long totalPendingWriteIntents() {
        long sum = 0;

        for (LongSupplier supplier : pendingWriteIntentsSuppliers.values()) {
            sum += supplier.getAsLong();
        }

        return sum;
    }

    /**
     * Returns a copy of tables that belong to the specified zone.
     *
     * @param zoneId Zone identifier.
     * @return Set of tables.
     * @throws IgniteInternalException If failed to acquire a read lock for the zone or current thread was interrupted while waiting.
     */
    Set<TableViewInternal> zoneTables(int zoneId) throws IgniteInternalException {
        NaiveAsyncReadWriteLock zoneLock = tablesPerZoneLocks.computeIfAbsent(zoneId, id -> new NaiveAsyncReadWriteLock());

        CompletableFuture<Long> readLockAcquisitionFuture = zoneLock.readLock();

        try {
            return readLockAcquisitionFuture.thenApply(stamp -> {
                Set<TableViewInternal> res = Set.copyOf(zoneTablesRawSet(zoneId));

                zoneLock.unlockRead(stamp);

                return res;
            }).get();
        } catch (Throwable t) {
            readLockAcquisitionFuture.thenAccept(zoneLock::unlockRead);

            if (t instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }

            throw new IgniteInternalException(INTERNAL_ERR, "Failed to acquire a read lock for zone [zoneId=" + zoneId + ']', t);
        }
    }

    /**
     * Adds a table to the specified zone.
     *
     * @param zoneId Zone identifier.
     * @param table Table to add.
     * @throws IgniteInternalException If failed to acquire a write lock for the zone or current thread was interrupted while waiting.
     */
    void addTableToZone(int zoneId, TableImpl table) throws IgniteInternalException {
        NaiveAsyncReadWriteLock zoneLock = tablesPerZoneLocks.computeIfAbsent(zoneId, id -> new NaiveAsyncReadWriteLock());

        CompletableFuture<Long> writeLockAcquisitionFuture = zoneLock.writeLock();

        try {
            writeLockAcquisitionFuture.thenAccept(stamp -> {
                tablesPerZone.compute(zoneId, (id, zoneTbls) -> {
                    if (zoneTbls == null) {
                        zoneTbls = new HashSet<>();
                    }

                    zoneTbls.add(table);

                    return zoneTbls;
                });

                zoneLock.unlockWrite(stamp);
            }).get();
        } catch (Throwable t) {
            writeLockAcquisitionFuture.thenAccept(zoneLock::unlockWrite);

            if (t instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }

            throw new IgniteInternalException(INTERNAL_ERR, "Failed to acquire a write lock for zone [zoneId=" + zoneId + ']', t);
        }
    }

    /**
     * Loads a newly created table into the zone by creating partition storages and preparing partition resources for local zone replicas.
     *
     * @param readLockAcquisitionFuture Future for read lock on the zone in PartitionReplicaLifecycleManager.
     * @param causalityToken Causality token.
     * @param zoneDescriptor Zone descriptor.
     * @param tableId Table ID.
     * @return Completion future.
     */
    CompletableFuture<Void> loadTableToZone(
            CompletableFuture<Long> readLockAcquisitionFuture,
            long causalityToken,
            CatalogZoneDescriptor zoneDescriptor,
            int tableId
    ) {
        CompletableFuture<?> tablesByIdFuture = tablesVv.get(causalityToken);
        CompletableFuture<Void> readLockFutureEx = allOf(readLockAcquisitionFuture, tablesByIdFuture);

        // NB: all vv.update() calls must be made from the synchronous part of the method (not in thenCompose()/etc!).
        CompletableFuture<?> localPartsUpdateFuture = localPartitionsVv.update(causalityToken,
                (ignore, throwable) -> inBusyLock(busyLock, () -> readLockFutureEx.thenComposeAsync(unused -> {
                    PartitionSet parts = new BitSetPartitionSet();

                    for (int i = 0; i < zoneDescriptor.partitions(); i++) {
                        if (partitionReplicaLifecycleManager.hasLocalPartition(new ZonePartitionId(zoneDescriptor.id(), i))) {
                            parts.set(i);
                        }
                    }

                    var table = (TableImpl) tables.get(tableId);

                    return createPartitionStoragesIfAbsent(table, parts).thenRun(() -> localPartsByTableId.put(tableId, parts));
                }, ioExecutor))
                // If the table is already closed, it's not a problem (probably the node is stopping).
                .exceptionally(ignoreTableClosedException())
        );

        CompletableFuture<?> createPartsFut = assignmentsUpdatedVv.update(causalityToken, (token, e) -> {
            if (e != null) {
                return failedFuture(e);
            }

            return localPartsUpdateFuture.thenRunAsync(() -> inBusyLock(busyLock, () -> {
                var table = (TableImpl) tables.get(tableId);

                for (int i = 0; i < zoneDescriptor.partitions(); i++) {
                    var zonePartitionId = new ZonePartitionId(zoneDescriptor.id(), i);

                    if (partitionReplicaLifecycleManager.hasLocalPartition(zonePartitionId)) {
                        preparePartitionResourcesAndLoadToZoneReplicaBusy(
                                table,
                                zonePartitionId,
                                partitionReplicaLifecycleManager.zonePartitionResources(zonePartitionId),
                                false
                        );
                    }
                }
            }), ioExecutor);
        });

        // TODO: https://issues.apache.org/jira/browse/IGNITE-19913 Possible performance degradation.
        return createPartsFut.thenAccept(ignore -> {
            var table = (TableImpl) tables.get(tableId);

            startedTables.put(tableId, table);

            addTableToZone(zoneDescriptor.id(), table);
        });
    }

    /**
     * Stops and destroys table processors for all partitions of a table.
     *
     * @param table Table to stop and destroy processors for.
     * @return Completion future.
     */
    CompletableFuture<Void> stopAndDestroyTableProcessors(TableViewInternal table) {
        InternalTable internalTable = table.internalTable();

        int partitions = internalTable.partitions();

        NaiveAsyncReadWriteLock zoneLock = tablesPerZoneLocks.computeIfAbsent(
                internalTable.zoneId(),
                id -> new NaiveAsyncReadWriteLock());

        CompletableFuture<Long> writeLockAcquisitionFuture = zoneLock.writeLock();

        try {
            return writeLockAcquisitionFuture.thenCompose(stamp -> {
                CompletableFuture<?>[] stopReplicaAndDestroyFutures = new CompletableFuture<?>[partitions];

                for (int partitionId = 0; partitionId < partitions; partitionId++) {
                    CompletableFuture<Void> resourcesUnloadFuture;

                    resourcesUnloadFuture = partitionReplicaLifecycleManager.unloadTableResourcesFromZoneReplica(
                            new ZonePartitionId(internalTable.zoneId(), partitionId),
                            internalTable.tableId()
                    );

                    var tablePartitionId = new TablePartitionId(internalTable.tableId(), partitionId);

                    stopReplicaAndDestroyFutures[partitionId] = resourcesUnloadFuture
                            .thenCompose(v -> stopAndDestroyTablePartition(tablePartitionId, table));
                }

                return allOf(stopReplicaAndDestroyFutures).whenComplete((res, th) -> {
                    tablesPerZone.getOrDefault(internalTable.zoneId(), emptySet()).remove(table);
                });
            }).whenComplete((unused, t) -> writeLockAcquisitionFuture.thenAccept(zoneLock::unlockWrite));
        } catch (Throwable t) {
            writeLockAcquisitionFuture.thenAccept(zoneLock::unlockWrite);

            throw t;
        }
    }

    private CompletableFuture<Boolean> beforeZoneReplicaStarted(LocalBeforeReplicaStartEventParameters parameters) {
        return inBusyLockAsync(busyLock, () -> readyToProcessReplicaStarts
                .thenCompose(v -> beforeZoneReplicaStartedImpl(parameters))
                .thenApply(unused -> false)
        );
    }

    private CompletableFuture<Void> beforeZoneReplicaStartedImpl(LocalBeforeReplicaStartEventParameters parameters) {
        return inBusyLockAsync(busyLock, () -> {
            ZonePartitionId zonePartitionId = parameters.zonePartitionId();

            NaiveAsyncReadWriteLock zoneLock = tablesPerZoneLocks.computeIfAbsent(
                    zonePartitionId.zoneId(),
                    id -> new NaiveAsyncReadWriteLock());

            CompletableFuture<Long> readLockAcquisitionFuture = zoneLock.readLock();

            try {
                return readLockAcquisitionFuture.thenCompose(stamp -> {
                    Set<TableViewInternal> zoneTables = zoneTablesRawSet(zonePartitionId.zoneId());

                    return createPartitionsAndLoadResourcesToZoneReplica(zonePartitionId, zoneTables, parameters);
                }).whenComplete((unused, t) -> readLockAcquisitionFuture.thenAccept(zoneLock::unlockRead));
            } catch (Throwable t) {
                readLockAcquisitionFuture.thenAccept(zoneLock::unlockRead);

                return failedFuture(t);
            }
        });
    }

    private CompletableFuture<Void> createPartitionsAndLoadResourcesToZoneReplica(
            ZonePartitionId zonePartitionId,
            Set<TableViewInternal> zoneTables,
            LocalBeforeReplicaStartEventParameters event
    ) {
        int partitionIndex = zonePartitionId.partitionId();

        PartitionSet singlePartitionIdSet = PartitionSet.of(partitionIndex);

        List<CompletableFuture<?>> storageCreationFutures = zoneTables.stream()
                .map(tbl -> inBusyLockAsync(busyLock, () -> {
                    return createPartitionStoragesIfAbsent(tbl, singlePartitionIdSet)
                            // If the table is already closed, it's not a problem (probably the node is stopping).
                            .exceptionally(ignoreTableClosedException());
                }))
                .collect(toList());

        return CompletableFutures.allOf(storageCreationFutures)
                .thenRunAsync(() -> scheduleMvPartitionsCleanupIfNeeded(zoneTables, partitionIndex, event), ioExecutor)
                // If a table is already closed, it's not a problem (probably the node is stopping).
                .exceptionally(ignoreTableClosedException())
                .thenCompose(unused -> {
                    CompletableFuture<?>[] futures = zoneTables.stream()
                            .map(tbl -> inBusyLockAsync(busyLock, () -> {
                                return runAsync(() -> inBusyLock(busyLock, () -> {
                                    localPartsByTableId.compute(
                                            tbl.tableId(),
                                            (tableId, oldPartitionSet) -> extendPartitionSet(oldPartitionSet, partitionIndex)
                                    );

                                    lowWatermark.getLowWatermarkSafe(lwm ->
                                            registerIndexesToTable(
                                                    tbl,
                                                    catalogService,
                                                    singlePartitionIdSet,
                                                    tbl.schemaView(),
                                                    lwm
                                            )
                                    );

                                    preparePartitionResourcesAndLoadToZoneReplicaBusy(
                                            tbl,
                                            zonePartitionId,
                                            event.resources(),
                                            event.onRecovery()
                                    );
                                }), ioExecutor)
                                // If the table is already closed, it's not a problem (probably the node is stopping).
                                .exceptionally(ignoreTableClosedException());
                            }))
                            .toArray(CompletableFuture[]::new);

                    return allOf(futures);
                });
    }

    private static void scheduleMvPartitionsCleanupIfNeeded(
            Set<TableViewInternal> zoneTables,
            int partitionIndex,
            LocalBeforeReplicaStartEventParameters event
    ) {
        boolean anyMvPartitionStorageIsInRebalanceState = zoneTables.stream()
                .map(table -> table.internalTable().storage().getMvPartition(partitionIndex))
                .filter(Objects::nonNull)
                .anyMatch(partitionStorage -> partitionStorage.lastAppliedIndex() == MvPartitionStorage.REBALANCE_IN_PROGRESS);

        if (anyMvPartitionStorageIsInRebalanceState) {
            event.registerStorageInRebalanceState();
        }

        // Adding the cleanup action even if no MV partition storage is in rebalance state as it might be that the TX state storage is.
        event.addCleanupAction(() -> {
            CompletableFuture<?>[] clearFutures = zoneTables.stream()
                    .map(table -> table.internalTable().storage().clearPartition(partitionIndex))
                    .toArray(CompletableFuture[]::new);
            return allOf(clearFutures);
        });
    }

    private CompletableFuture<Boolean> onZoneReplicaStopped(LocalPartitionReplicaEventParameters parameters) {
        ZonePartitionId zonePartitionId = parameters.zonePartitionId();

        NaiveAsyncReadWriteLock zoneLock = tablesPerZoneLocks.computeIfAbsent(
                zonePartitionId.zoneId(),
                id -> new NaiveAsyncReadWriteLock());

        CompletableFuture<Long> readLockAcquisitionFuture = zoneLock.readLock();

        try {
            return readLockAcquisitionFuture.thenCompose(stamp -> {
                CompletableFuture<?>[] futures = zoneTablesRawSet(zonePartitionId.zoneId()).stream()
                        .map(this::stopTablePartitions)
                        .toArray(CompletableFuture[]::new);

                return allOf(futures);
            }).whenComplete((v, t) -> readLockAcquisitionFuture.thenAccept(zoneLock::unlockRead)).thenApply(v -> false);
        } catch (Throwable t) {
            readLockAcquisitionFuture.thenAccept(zoneLock::unlockRead);

            return failedFuture(t);
        }
    }

    private CompletableFuture<Boolean> onZoneReplicaDestroyed(LocalPartitionReplicaEventParameters parameters) {
        ZonePartitionId zonePartitionId = parameters.zonePartitionId();

        NaiveAsyncReadWriteLock zoneLock = tablesPerZoneLocks.computeIfAbsent(
                zonePartitionId.zoneId(),
                id -> new NaiveAsyncReadWriteLock());

        CompletableFuture<Long> readLockAcquisitionFuture = zoneLock.readLock();

        try {
            return readLockAcquisitionFuture.thenCompose(stamp -> {
                return inBusyLockAsync(busyLock, () -> {
                    CompletableFuture<?>[] futures = zoneTablesRawSet(zonePartitionId.zoneId()).stream()
                            .map(table -> supplyAsync(
                                    () -> inBusyLockAsync(
                                            busyLock,
                                            () -> stopAndDestroyTablePartitionByToken(
                                                    new TablePartitionId(table.tableId(), zonePartitionId.partitionId()),
                                                    parameters.causalityToken()
                                            )
                                    ),
                                    ioExecutor).thenCompose(identity()))
                            .toArray(CompletableFuture[]::new);

                    return allOf(futures);
                });
            }).whenComplete((v, t) -> readLockAcquisitionFuture.thenAccept(zoneLock::unlockRead)).thenApply(unused -> false);
        } catch (Throwable t) {
            readLockAcquisitionFuture.thenAccept(zoneLock::unlockRead);

            return failedFuture(t);
        }
    }

    private void preparePartitionResourcesAndLoadToZoneReplicaBusy(
            TableViewInternal table,
            ZonePartitionId zonePartitionId,
            ZonePartitionResources resources,
            boolean onNodeRecovery
    ) {
        int partId = zonePartitionId.partitionId();

        int tableId = table.tableId();

        var tablePartitionId = new TablePartitionId(tableId, partId);

        MvPartitionStorage mvPartitionStorage;
        try {
            mvPartitionStorage = getMvPartitionStorage(table, partId);
        } catch (TableClosedException e) {
            // The node is probably stopping while we start the table, let's just skip it.
            return;
        }

        PartitionDataStorage partitionDataStorage = partitionResourcesFactory.createPartitionDataStorage(
                new PartitionKey(zonePartitionId.zoneId(), partId),
                tableId,
                mvPartitionStorage
        );

        PartitionResources partitionResources = partitionResourcesFactory.createPartitionResources(
                partId,
                partitionDataStorage,
                table,
                resources.safeTimeTracker()
        );

        partitionResources.storageUpdateHandler.start(onNodeRecovery);

        registerPartitionTableStatsMetrics(table, partId, partitionResources);

        mvGc.addStorage(tablePartitionId, partitionResources.gcUpdateHandler);

        minTimeCollectorService.addPartition(tablePartitionId);

        TablePartitionReplicaProcessorFactory createListener = (raftClient, transactionStateResolver) ->
                partitionResourcesFactory.createReplicaListener(
                        zonePartitionId,
                        table,
                        resources.safeTimeTracker(),
                        mvPartitionStorage,
                        partitionResources,
                        raftClient,
                        transactionStateResolver
                );

        TablePartitionProcessor tablePartitionProcessor = partitionResourcesFactory.createTablePartitionProcessor(
                zonePartitionId, table, partitionDataStorage, partitionResources);

        PartitionMvStorageAccess partitionStorageAccess = partitionResourcesFactory.createPartitionMvStorageAccess(
                partId, table, partitionResources);

        partitionReplicaLifecycleManager.loadTableListenerToZoneReplica(
                zonePartitionId,
                tableId,
                createListener,
                tablePartitionProcessor,
                partitionStorageAccess,
                onNodeRecovery
        );
    }

    private CompletableFuture<Void> stopTablePartitions(TableViewInternal table) {
        return supplyAsync(() -> {
            InternalTable internalTable = table.internalTable();

            var stopReplicaFutures = new CompletableFuture<?>[internalTable.partitions()];

            for (int p = 0; p < internalTable.partitions(); p++) {
                TablePartitionId replicationGroupId = new TablePartitionId(table.tableId(), p);

                stopReplicaFutures[p] = stopTablePartition(replicationGroupId, table);
            }

            return allOf(stopReplicaFutures);
        }, ioExecutor).thenCompose(identity());
    }

    private CompletableFuture<Void> stopAndDestroyTablePartitionByToken(TablePartitionId tablePartitionId, long causalityToken) {
        CompletableFuture<?> tokenFuture;

        try {
            tokenFuture = tablesVv.get(causalityToken);
        } catch (OutdatedTokenException e) {
            // Here we need only to ensure that the token has been seen.
            // TODO https://issues.apache.org/jira/browse/IGNITE-25742
            tokenFuture = nullCompletedFuture();
        }

        return tokenFuture
                .thenCompose(ignore -> {
                    TableViewInternal table = tables.get(tablePartitionId.tableId());
                    assert table != null : tablePartitionId;

                    return stopAndDestroyTablePartition(tablePartitionId, table);
                });
    }

    private CompletableFuture<Void> stopAndDestroyTablePartition(
            TablePartitionId tablePartitionId,
            TableViewInternal table
    ) {
        return stopTablePartition(tablePartitionId, table)
                .thenComposeAsync(v -> destroyPartitionStorages(tablePartitionId, table), ioExecutor);
    }

    private CompletableFuture<Void> stopTablePartition(TablePartitionId tablePartitionId, TableViewInternal table) {
        // In case of colocation there shouldn't be any table replica and thus it shouldn't be stopped.
        minTimeCollectorService.removePartition(tablePartitionId);

        unregisterPartitionMetrics(tablePartitionId, table.name());

        return mvGc.removeStorage(tablePartitionId);
    }

    private void registerPartitionTableStatsMetrics(
            TableViewInternal table,
            int partitionId,
            PartitionResources partitionResources
    ) {
        var tablePartitionId = new TablePartitionId(table.tableId(), partitionId);

        PartitionTableStatsMetricSource metricSource =
                new PartitionTableStatsMetricSource(table.tableId(), partitionId, partitionResources.modificationCounter);

        try {
            // Only register this Metrics Source and do not enable it by default
            // as it is intended for online troubleshooting purposes only (see IGNITE-27813).
            metricManager.registerSource(metricSource);

            partModCounterMetricSources.put(tablePartitionId, metricSource);
        } catch (Exception e) {
            LOG.warn("Failed to register metrics source for table [name={}, partitionId={}].", e, table.name(), partitionId);
        }

        pendingWriteIntentsSuppliers.put(
                tablePartitionId,
                partitionResources.storageUpdateHandler::getPendingRowCount
        );
    }

    private void unregisterPartitionMetrics(TablePartitionId tablePartitionId, String tableName) {
        PartitionTableStatsMetricSource metricSource = partModCounterMetricSources.remove(tablePartitionId);
        pendingWriteIntentsSuppliers.remove(tablePartitionId);
        if (metricSource != null) {
            try {
                metricManager.unregisterSource(metricSource);
            } catch (Exception e) {
                String message = "Failed to unregister metrics source for table [name={}, partitionId={}].";
                LOG.warn(message, e, tableName, tablePartitionId.partitionId());
            }
        }
    }

    private CompletableFuture<Void> destroyPartitionStorages(
            TablePartitionId tablePartitionId,
            TableViewInternal table
    ) {
        InternalTable internalTable = table.internalTable();

        int partitionId = tablePartitionId.partitionId();

        List<CompletableFuture<?>> destroyFutures = new ArrayList<>();

        try {
            if (internalTable.storage().getMvPartition(partitionId) != null) {
                destroyFutures.add(internalTable.storage().destroyPartition(partitionId));
            }
        } catch (StorageDestroyedException ignored) {
            // Ignore as the storage is already destroyed, no need to destroy it again.
        } catch (StorageClosedException ignored) {
            // The storage is closed, so the node is being stopped. We'll destroy the partition on node recovery.
        }

        // TODO: IGNITE-24926 - reduce set in localPartsByTableId after storages destruction.
        return allOf(destroyFutures.toArray(new CompletableFuture[]{}));
    }

    // TODO: https://issues.apache.org/jira/browse/IGNITE-19739 Create storages only once.
    private CompletableFuture<Void> createPartitionStoragesIfAbsent(TableViewInternal table, PartitionSet partitions) {
        InternalTable internalTable = table.internalTable();

        List<CompletableFuture<MvPartitionStorage>> storageFuts = partitions.stream().mapToObj(partitionId -> {
            MvPartitionStorage mvPartition;
            try {
                mvPartition = internalTable.storage().getMvPartition(partitionId);
            } catch (StorageClosedException e) {
                return CompletableFuture.<MvPartitionStorage>failedFuture(new TableClosedException(table.tableId(), e));
            }

            return mvPartition != null
                    ? CompletableFuture.completedFuture(mvPartition)
                    : internalTable.storage().createMvPartition(partitionId);
        }).collect(toList());

        return CompletableFutures.allOf(storageFuts);
    }

    private static MvPartitionStorage getMvPartitionStorage(TableViewInternal table, int partitionId) {
        InternalTable internalTable = table.internalTable();

        MvPartitionStorage mvPartition;
        try {
            mvPartition = internalTable.storage().getMvPartition(partitionId);
        } catch (StorageClosedException e) {
            throw new TableClosedException(table.tableId(), e);
        }

        assert mvPartition != null : "tableId=" + table.tableId() + ", partitionId=" + partitionId;

        return mvPartition;
    }

    private Set<TableViewInternal> zoneTablesRawSet(int zoneId) {
        return tablesPerZone.getOrDefault(zoneId, Set.of());
    }

    private static PartitionSet extendPartitionSet(@Nullable PartitionSet oldPartitionSet, int partitionId) {
        PartitionSet newPartitionSet = Objects.requireNonNullElseGet(oldPartitionSet, BitSetPartitionSet::new);
        newPartitionSet.set(partitionId);
        return newPartitionSet;
    }

    private static <T> Function<Throwable, T> ignoreTableClosedException() {
        return ex -> {
            if (hasCause(ex, TableClosedException.class)) {
                return null;
            }
            throw sneakyThrow(ex);
        };
    }

    private static class TableClosedException extends IgniteInternalException {
        private static final long serialVersionUID = 1L;

        TableClosedException(int tableId, @Nullable Throwable cause) {
            super(INTERNAL_ERR, "Table is closed [tableId=" + tableId + "]", cause);
        }
    }
}
