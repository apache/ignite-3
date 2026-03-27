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

import static java.util.Collections.unmodifiableMap;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.anyOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.function.Function.identity;
import static org.apache.ignite.internal.causality.IncrementalVersionedValue.dependingOn;
import static org.apache.ignite.internal.event.EventListener.fromConsumer;
import static org.apache.ignite.internal.table.distributed.TableUtils.aliveTables;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_READ;
import static org.apache.ignite.internal.util.CollectionUtils.difference;
import static org.apache.ignite.internal.util.CompletableFutures.allOfToList;
import static org.apache.ignite.internal.util.CompletableFutures.copyStateTo;
import static org.apache.ignite.internal.util.CompletableFutures.emptyListCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;
import static org.apache.ignite.internal.util.IgniteUtils.closeAllManually;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.AlterTablePropertiesEventParameters;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.CreateTableEventParameters;
import org.apache.ignite.internal.catalog.events.DropTableEventParameters;
import org.apache.ignite.internal.catalog.events.RenameTableEventParameters;
import org.apache.ignite.internal.causality.CompletionListener;
import org.apache.ignite.internal.causality.IncrementalVersionedValue;
import org.apache.ignite.internal.causality.RevisionListenerRegistry;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.utils.SystemDistributedConfigurationPropertyHolder;
import org.apache.ignite.internal.distributionzones.DistributionZonesUtil;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.lowwatermark.event.ChangeLowWatermarkEventParameters;
import org.apache.ignite.internal.lowwatermark.event.LowWatermarkEvent;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.Revisions;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.partition.replicator.PartitionReplicaLifecycleManager;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.partition.replicator.schema.ExecutorInclinedSchemaSyncService;
import org.apache.ignite.internal.partition.replicator.schema.ValidationSchemasSource;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.wrappers.ExecutorInclinedPlacementDriver;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.schema.configuration.GcConfiguration;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.metrics.StorageEngineTablesMetricSource;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.StreamerReceiverRunner;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.gc.MvGc;
import org.apache.ignite.internal.table.distributed.index.IndexMetaStorage;
import org.apache.ignite.internal.table.distributed.raft.MinimumRequiredTimeCollectorService;
import org.apache.ignite.internal.table.distributed.raft.snapshot.FullStateTransferIndexChooser;
import org.apache.ignite.internal.table.distributed.schema.SchemaVersions;
import org.apache.ignite.internal.table.distributed.schema.SchemaVersionsImpl;
import org.apache.ignite.internal.table.metrics.TableMetricSource;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.RemotelyTriggeredResourceRegistry;
import org.apache.ignite.internal.tx.impl.TransactionInflights;
import org.apache.ignite.internal.tx.metrics.TransactionMetricsSource;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.LongPriorityQueue;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.QualifiedName;
import org.apache.ignite.table.QualifiedNameHelper;
import org.apache.ignite.table.Table;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Table manager.
 */
public class TableManager implements IgniteTablesInternal, IgniteComponent {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(TableManager.class);

    /** Transaction manager. */
    private final TxManager txManager;

    /** Meta storage manager. */
    private final MetaStorageManager metaStorageMgr;

    /** Data storage manager. */
    private final DataStorageManager dataStorageMgr;

    /**
     * Versioned value for linearizing table changing events.
     *
     * @see #localPartitionsVv
     * @see #assignmentsUpdatedVv
     */
    private final IncrementalVersionedValue<Void> tablesVv;

    /**
     * Versioned value for linearizing table partitions changing events.
     *
     * <p>Completed strictly after {@link #tablesVv} and strictly before {@link #assignmentsUpdatedVv}.
     */
    private final IncrementalVersionedValue<Void> localPartitionsVv;

    /**
     * Versioned value for tracking RAFT groups initialization and starting completion.
     *
     * <p>Completed strictly after {@link #localPartitionsVv}.
     */
    private final IncrementalVersionedValue<Void> assignmentsUpdatedVv;

    private final TableRegistry tableRegistry = new TableRegistry();

    /** A queue for deferred table destruction events. */
    private final LongPriorityQueue<DestroyTableEvent> destructionEventsQueue =
            new LongPriorityQueue<>(DestroyTableEvent::catalogVersion);

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping the component. */
    private final AtomicBoolean beforeStopGuard = new AtomicBoolean();

    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Schema manager. */
    private final SchemaManager schemaManager;

    /** Scan request executor. */
    private final ExecutorService scanRequestExecutor;

    /**
     * Separate executor for IO operations like partition storage initialization or partition raft group meta data persisting.
     */
    private final ExecutorService ioExecutor;

    private final ClockService clockService;

    private final SchemaSyncService executorInclinedSchemaSyncService;

    private final CatalogService catalogService;

    private final FailureProcessor failureProcessor;

    private final LowWatermark lowWatermark;

    private final PartitionReplicatorNodeRecovery partitionReplicatorNodeRecovery;

    /** Ends at the {@link IgniteComponent#stopAsync(ComponentContext)} with an {@link NodeStoppingException}. */
    private final CompletableFuture<Void> stopManagerFuture = new CompletableFuture<>();

    private final PartitionReplicaLifecycleManager partitionReplicaLifecycleManager;

    private final CompletableFuture<Void> readyToProcessReplicaStarts = new CompletableFuture<>();

    /** Configuration of rebalance retries delay. */
    private final SystemDistributedConfigurationPropertyHolder<Integer> rebalanceRetryDelayConfiguration;

    private final EventListener<CreateTableEventParameters> onTableCreateListener = this::loadTableToZoneOnTableCreate;
    private final EventListener<DropTableEventParameters> onTableDropListener = fromConsumer(this::onTableDrop);
    private final EventListener<CatalogEventParameters> onTableAlterListener = this::onTableAlter;

    private final EventListener<ChangeLowWatermarkEventParameters> onLowWatermarkChangedListener = this::onLwmChanged;

    private final MetricManager metricManager;

    private final StreamerFlushExecutorFactory streamerFlushExecutorFactory;

    private final TableImplFactory tableImplFactory;

    private final TablePartitionResourcesFactory partitionResourcesFactory;

    private final TableZoneCoordinator zoneCoordinator;

    /**
     * Creates a new table manager.
     *
     * @param nodeName Node name.
     * @param registry Registry for versioned values.
     * @param gcConfig Garbage collector configuration.
     * @param replicationConfiguration Replication configuration.
     * @param lockMgr Lock manager.
     * @param replicaSvc Replica service.
     * @param txManager Transaction manager.
     * @param dataStorageMgr Data storage manager.
     * @param schemaManager Schema manager.
     * @param ioExecutor Separate executor for IO operations like partition storage initialization or partition raft group meta data
     *         persisting.
     * @param partitionOperationsExecutor Striped executor on which partition operations (potentially requiring I/O with storages)
     *         will be executed.
     * @param clockService hybrid logical clock service.
     * @param placementDriver Placement driver.
     * @param sql A supplier function that returns {@link IgniteSql}.
     * @param lowWatermark Low watermark.
     * @param transactionInflights Transaction inflights.
     * @param indexMetaStorage Index meta storage.
     * @param partitionReplicaLifecycleManager Partition replica lifecycle manager.
     * @param minTimeCollectorService Collects minimum required timestamp for each partition.
     * @param systemDistributedConfiguration System distributed configuration.
     * @param metricManager Metric manager.
     * @param mvTableStorageFactory Factory for creating MV table storages.
     */
    public TableManager(
            String nodeName,
            RevisionListenerRegistry registry,
            GcConfiguration gcConfig,
            ReplicationConfiguration replicationConfiguration,
            MessagingService messagingService,
            TopologyService topologyService,
            LockManager lockMgr,
            ReplicaService replicaSvc,
            TxManager txManager,
            DataStorageManager dataStorageMgr,
            MetaStorageManager metaStorageMgr,
            SchemaManager schemaManager,
            ValidationSchemasSource validationSchemasSource,
            ExecutorService ioExecutor,
            Executor partitionOperationsExecutor,
            ClockService clockService,
            OutgoingSnapshotsManager outgoingSnapshotsManager,
            SchemaSyncService schemaSyncService,
            CatalogService catalogService,
            FailureProcessor failureProcessor,
            HybridTimestampTracker observableTimestampTracker,
            PlacementDriver placementDriver,
            Supplier<IgniteSql> sql,
            RemotelyTriggeredResourceRegistry remotelyTriggeredResourceRegistry,
            LowWatermark lowWatermark,
            TransactionInflights transactionInflights,
            IndexMetaStorage indexMetaStorage,
            PartitionReplicaLifecycleManager partitionReplicaLifecycleManager,
            MinimumRequiredTimeCollectorService minTimeCollectorService,
            SystemDistributedConfiguration systemDistributedConfiguration,
            MetricManager metricManager,
            PartitionModificationCounterFactory partitionModificationCounterFactory,
            MvTableStorageFactory mvTableStorageFactory
    ) {
        this.txManager = txManager;
        this.dataStorageMgr = dataStorageMgr;
        this.metaStorageMgr = metaStorageMgr;
        this.schemaManager = schemaManager;
        this.ioExecutor = ioExecutor;
        this.clockService = clockService;
        this.catalogService = catalogService;
        this.failureProcessor = failureProcessor;
        this.lowWatermark = lowWatermark;
        this.partitionReplicaLifecycleManager = partitionReplicaLifecycleManager;
        this.metricManager = metricManager;

        this.executorInclinedSchemaSyncService = new ExecutorInclinedSchemaSyncService(schemaSyncService, partitionOperationsExecutor);
        PlacementDriver executorInclinedPlacementDriver = new ExecutorInclinedPlacementDriver(placementDriver, partitionOperationsExecutor);

        SchemaVersions schemaVersions = new SchemaVersionsImpl(executorInclinedSchemaSyncService, catalogService, clockService);

        tablesVv = new IncrementalVersionedValue<>("TableManager#tables", registry, 100, null);

        localPartitionsVv = new IncrementalVersionedValue<>("TableManager#localPartitions", dependingOn(tablesVv));

        assignmentsUpdatedVv = new IncrementalVersionedValue<>("TableManager#assignmentsUpdated", dependingOn(localPartitionsVv));

        scanRequestExecutor = Executors.newSingleThreadExecutor(
                IgniteThreadFactory.create(nodeName, "scan-query-executor", LOG, STORAGE_READ));

        MvGc mvGc = new MvGc(nodeName, gcConfig, lowWatermark, failureProcessor);

        partitionReplicatorNodeRecovery = new PartitionReplicatorNodeRecovery(
                messagingService,
                partitionOperationsExecutor,
                tableId -> tablesById().get(tableId)
        );

        FullStateTransferIndexChooser fullStateTransferIndexChooser =
                new FullStateTransferIndexChooser(catalogService, lowWatermark, indexMetaStorage);

        partitionResourcesFactory = new TablePartitionResourcesFactory(
                txManager,
                lockMgr,
                scanRequestExecutor,
                clockService,
                catalogService,
                partitionModificationCounterFactory,
                outgoingSnapshotsManager,
                lowWatermark,
                validationSchemasSource,
                this.executorInclinedSchemaSyncService,
                executorInclinedPlacementDriver,
                topologyService,
                remotelyTriggeredResourceRegistry,
                failureProcessor,
                schemaManager,
                replicationConfiguration,
                partitionOperationsExecutor,
                indexMetaStorage,
                minTimeCollectorService,
                mvGc,
                fullStateTransferIndexChooser
        );

        streamerFlushExecutorFactory = new StreamerFlushExecutorFactory(nodeName);

        tableImplFactory = new TableImplFactory(
                topologyService,
                lockMgr,
                replicaSvc,
                txManager,
                clockService,
                observableTimestampTracker,
                executorInclinedPlacementDriver,
                transactionInflights,
                schemaVersions,
                sql,
                failureProcessor,
                mvTableStorageFactory,
                catalogService,
                dataStorageMgr,
                metricManager,
                streamerFlushExecutorFactory
        );

        rebalanceRetryDelayConfiguration = new SystemDistributedConfigurationPropertyHolder<>(
                systemDistributedConfiguration,
                (v, r) -> {},
                DistributionZonesUtil.REBALANCE_RETRY_DELAY_MS,
                DistributionZonesUtil.REBALANCE_RETRY_DELAY_DEFAULT,
                Integer::parseInt
        );

        zoneCoordinator = new TableZoneCoordinator(
                mvGc,
                fullStateTransferIndexChooser,
                partitionReplicaLifecycleManager,
                partitionResourcesFactory,
                minTimeCollectorService,
                metricManager,
                catalogService,
                lowWatermark,
                ioExecutor,
                busyLock,
                tableRegistry,
                tablesVv,
                localPartitionsVv,
                assignmentsUpdatedVv,
                readyToProcessReplicaStarts
        );
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        return inBusyLockAsync(busyLock, () -> {
            TransactionMetricsSource transactionMetricsSource = txManager.transactionMetricsSource();
            transactionMetricsSource.setPendingWriteIntentsSupplier(zoneCoordinator::totalPendingWriteIntents);

            zoneCoordinator.start();

            rebalanceRetryDelayConfiguration.init();

            cleanUpResourcesForDroppedTablesOnRecoveryBusy();

            catalogService.listen(CatalogEvent.TABLE_CREATE, onTableCreateListener);
            catalogService.listen(CatalogEvent.TABLE_DROP, onTableDropListener);
            catalogService.listen(CatalogEvent.TABLE_ALTER, onTableAlterListener);

            lowWatermark.listen(LowWatermarkEvent.LOW_WATERMARK_CHANGED, onLowWatermarkChangedListener);

            partitionReplicatorNodeRecovery.start();

            CompletableFuture<Revisions> recoveryFinishFuture = metaStorageMgr.recoveryFinishedFuture();

            assert recoveryFinishFuture.isDone();

            long recoveryRevision = recoveryFinishFuture.join().revision();

            return recoverTables(recoveryRevision, lowWatermark.getLowWatermark());
        });
    }

    /**
     * During node recovery pre-populates required internal table structures before zone replicas are started.
     *
     * <p>The created resources will then be loaded during replica startup by {@link TableZoneCoordinator}.
     */
    private CompletableFuture<Void> prepareTableResourcesOnRecovery(
            long causalityToken,
            CatalogZoneDescriptor zoneDescriptor,
            CatalogTableDescriptor tableDescriptor,
            CatalogSchemaDescriptor schemaDescriptor
    ) {
        return inBusyLockAsync(busyLock, () -> {

            int tableId = tableDescriptor.id();

            return schemaManager.schemaRegistry(causalityToken, tableId)
                    .thenAccept(schemaRegistry -> inBusyLock(busyLock, () -> {
                        TableImpl table = createTableImpl(
                                causalityToken,
                                tableDescriptor,
                                zoneDescriptor,
                                schemaDescriptor,
                                schemaRegistry
                        );

                        tableRegistry.tables().put(tableId, table);

                        zoneCoordinator.addTableToZone(zoneDescriptor.id(), table);

                        tableRegistry.startedTables().put(tableId, table);
                    }));
        });
    }

    private CompletableFuture<Boolean> loadTableToZoneOnTableCreate(CreateTableEventParameters parameters) {
        long causalityToken = parameters.causalityToken();
        CatalogTableDescriptor tableDescriptor = parameters.tableDescriptor();
        CatalogZoneDescriptor zoneDescriptor = getZoneDescriptor(tableDescriptor, parameters.catalogVersion());
        CatalogSchemaDescriptor schemaDescriptor = getSchemaDescriptor(tableDescriptor, parameters.catalogVersion());

        return loadTableToZoneOnTableCreate(causalityToken, zoneDescriptor, tableDescriptor, schemaDescriptor)
                .thenApply(v -> false);
    }

    private CompletableFuture<Void> loadTableToZoneOnTableCreate(
            long causalityToken,
            CatalogZoneDescriptor zoneDescriptor,
            CatalogTableDescriptor tableDescriptor,
            CatalogSchemaDescriptor schemaDescriptor
    ) {
        int tableId = tableDescriptor.id();

        tablesVv.update(causalityToken, (ignore, e) -> inBusyLock(busyLock, () -> {
            if (e != null) {
                return failedFuture(e);
            }

            return schemaManager.schemaRegistry(causalityToken, tableId).thenAccept(schemaRegistry -> {
                TableImpl table = createTableImpl(causalityToken, tableDescriptor, zoneDescriptor, schemaDescriptor, schemaRegistry);

                tableRegistry.tables().put(tableId, table);
            });
        }));

        // Obtain future, but don't chain on it yet because update() on VVs must be called in the same thread. The method we call
        // will call update() on VVs and inside those updates it will chain on the lock acquisition future.
        CompletableFuture<Long> acquisitionFuture = partitionReplicaLifecycleManager.lockZoneForRead(zoneDescriptor.id());
        try {
            return zoneCoordinator.loadTableToZone(acquisitionFuture, causalityToken, zoneDescriptor, tableId)
                    .whenComplete((res, ex) -> unlockZoneForRead(zoneDescriptor, acquisitionFuture));
        } catch (Throwable e) {
            unlockZoneForRead(zoneDescriptor, acquisitionFuture);

            return failedFuture(e);
        }
    }

    private void unlockZoneForRead(CatalogZoneDescriptor zoneDescriptor, CompletableFuture<Long> readLockAcquiryFuture) {
        readLockAcquiryFuture.thenAccept(stamp -> {
            partitionReplicaLifecycleManager.unlockZoneForRead(zoneDescriptor.id(), stamp);
        });
    }

    private void onTableDrop(DropTableEventParameters parameters) {
        inBusyLock(busyLock, () -> {
            unregisterMetricsSource(tableRegistry.startedTables().get(parameters.tableId()));

            destructionEventsQueue.enqueue(new DestroyTableEvent(parameters.catalogVersion(), parameters.tableId()));
        });
    }

    private CompletableFuture<Boolean> onTableAlter(CatalogEventParameters parameters) {
        if (parameters instanceof RenameTableEventParameters) {
            return onTableRename((RenameTableEventParameters) parameters).thenApply(unused -> false);
        } else if (parameters instanceof AlterTablePropertiesEventParameters) {
            return onTablePropertiesChanged((AlterTablePropertiesEventParameters) parameters).thenApply(unused -> false);
        } else {
            return falseCompletedFuture();
        }
    }

    // TODO https://issues.apache.org/jira/browse/IGNITE-27468 Not "thread-safe" in case of concurrent disaster recovery or rebalances.
    private CompletableFuture<Boolean> onLwmChanged(ChangeLowWatermarkEventParameters parameters) {
        if (!busyLock.enterBusy()) {
            return falseCompletedFuture();
        }

        try {
            int newEarliestCatalogVersion = catalogService.activeCatalogVersion(parameters.newLowWatermark().longValue());

            // Run table destruction fully asynchronously.
            destructionEventsQueue.drainUpTo(newEarliestCatalogVersion)
                    .forEach(event -> destroyTableLocally(event.tableId()));

            return falseCompletedFuture();
        } catch (Throwable t) {
            return failedFuture(t);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private CompletableFuture<?> onTablePropertiesChanged(AlterTablePropertiesEventParameters parameters) {
        return inBusyLockAsync(busyLock, () -> tablesVv.update(
                parameters.causalityToken(),
                (ignore, e) -> {
                    if (e != null) {
                        return failedFuture(e);
                    }

                    TableViewInternal table = tableRegistry.tables().get(parameters.tableId());

                    table.updateStalenessConfiguration(parameters.staleRowsFraction(), parameters.minStaleRowsCount());

                    return nullCompletedFuture();
                })
        );
    }

    private CompletableFuture<?> onTableRename(RenameTableEventParameters parameters) {
        return inBusyLockAsync(busyLock, () -> tablesVv.update(
                parameters.causalityToken(),
                (ignore, e) -> {
                    if (e != null) {
                        return failedFuture(e);
                    }

                    TableViewInternal table = tableRegistry.tables().get(parameters.tableId());

                    // TODO: revisit this approach, see https://issues.apache.org/jira/browse/IGNITE-21235.
                    ((TableImpl) table).name(parameters.newTableName());

                    return nullCompletedFuture();
                })
        );
    }

    @Override
    public void beforeNodeStop() {
        if (!beforeStopGuard.compareAndSet(false, true)) {
            return;
        }

        stopManagerFuture.completeExceptionally(new NodeStoppingException());

        streamerFlushExecutorFactory.beforeStop();
        busyLock.block();

        lowWatermark.removeListener(LowWatermarkEvent.LOW_WATERMARK_CHANGED, onLowWatermarkChangedListener);

        catalogService.removeListener(CatalogEvent.TABLE_CREATE, onTableCreateListener);
        catalogService.removeListener(CatalogEvent.TABLE_DROP, onTableDropListener);
        catalogService.removeListener(CatalogEvent.TABLE_ALTER, onTableAlterListener);
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        // NB: busy lock had already gotten in {@link beforeNodeStop}
        assert beforeStopGuard.get() : "'stop' called before 'beforeNodeStop'";

        if (!stopGuard.compareAndSet(false, true)) {
            return nullCompletedFuture();
        }

        TransactionMetricsSource transactionMetricsSource = txManager.transactionMetricsSource();
        if (transactionMetricsSource != null) {
            transactionMetricsSource.setPendingWriteIntentsSupplier(null);
        }

        int shutdownTimeoutSeconds = 10;

        try {
            closeAllManually(
                    zoneCoordinator::stop,
                    () -> closeAllManually(tableRegistry.tables().values().stream().map(table -> () -> closeTable(table))),
                    () -> shutdownAndAwaitTermination(scanRequestExecutor, shutdownTimeoutSeconds, TimeUnit.SECONDS),
                    () -> streamerFlushExecutorFactory.stop(shutdownTimeoutSeconds)
            );
        } catch (Exception e) {
            return failedFuture(e);
        }

        return nullCompletedFuture();
    }

    private static void closeTable(TableViewInternal table) throws Exception {
        InternalTable internalTable = table.internalTable();

        closeAllManually(
                internalTable.storage(),
                internalTable
        );
    }

    /**
     * Create table storages and internal instance.
     *
     * @param causalityToken Causality token.
     * @param tableDescriptor Catalog table descriptor.
     * @param zoneDescriptor Catalog distribution zone descriptor.
     * @param schemaDescriptor Catalog schema descriptor.
     * @param schemaRegistry Schema registry.
     * @return Table instance.
     */
    private TableImpl createTableImpl(
            long causalityToken,
            CatalogTableDescriptor tableDescriptor,
            CatalogZoneDescriptor zoneDescriptor,
            CatalogSchemaDescriptor schemaDescriptor,
            SchemaRegistry schemaRegistry
    ) {
        QualifiedName tableName = QualifiedNameHelper.fromNormalized(schemaDescriptor.name(), tableDescriptor.name());

        LOG.trace("Creating local table: name={}, id={}, token={}", tableName.toCanonicalForm(), tableDescriptor.id(), causalityToken);

        return tableImplFactory.createTableImpl(
                tableName,
                tableDescriptor,
                zoneDescriptor,
                schemaRegistry
        );
    }

    /**
     * Drops local structures for a table.
     *
     * @param tableId Table id to destroy.
     */
    private CompletableFuture<Void> destroyTableLocally(int tableId) {
        TableViewInternal table = tableRegistry.startedTables().remove(tableId);

        tableRegistry.localPartsByTableId().remove(tableId);

        assert table != null : tableId;

        InternalTable internalTable = table.internalTable();

        return zoneCoordinator.stopAndDestroyTableProcessors(table)
                .thenComposeAsync(unused -> inBusyLockAsync(busyLock, () -> internalTable.storage().destroy()), ioExecutor)
                .thenAccept(unused -> inBusyLock(busyLock, () -> {
                    tableRegistry.tables().remove(tableId);
                    schemaManager.dropRegistry(tableId);
                }))
                .whenComplete((v, e) -> {
                    if (e != null && !hasCause(e, NodeStoppingException.class)) {
                        LOG.error("Unable to destroy table [name={}, tableId={}]", e, table.name(), tableId);
                    }
                });
    }

    @Override
    public List<Table> tables() {
        return sync(tablesAsync());
    }

    @Override
    public CompletableFuture<List<Table>> tablesAsync() {
        return inBusyLockAsync(busyLock, this::tablesAsyncInternalBusy);
    }

    private CompletableFuture<List<Table>> tablesAsyncInternalBusy() {
        HybridTimestamp now = clockService.now();

        return orStopManagerFuture(executorInclinedSchemaSyncService.waitForMetadataCompleteness(now))
                .thenCompose(unused -> inBusyLockAsync(busyLock, () -> {
                    Catalog catalog = catalogService.activeCatalog(now.longValue());

                    Collection<CatalogTableDescriptor> tableDescriptors = catalog.tables();

                    if (tableDescriptors.isEmpty()) {
                        return emptyListCompletedFuture();
                    }

                    @SuppressWarnings("unchecked")
                    CompletableFuture<Table>[] tableImplFutures = tableDescriptors.stream()
                            .map(tableDescriptor -> tableAsyncInternalBusy(tableDescriptor.id()))
                            .toArray(CompletableFuture[]::new);

                    return allOfToList(tableImplFutures);
                }));
    }

    /**
     * Returns the tables by ID future for the given causality token.
     *
     * <p>The future will only be completed when corresponding assignments update completes.
     *
     * @param causalityToken Causality token.
     * @return The future with tables map.
     * @see #assignmentsUpdatedVv
     */
    private CompletableFuture<Map<Integer, TableViewInternal>> tablesById(long causalityToken) {
        return assignmentsUpdatedVv.get(causalityToken).thenApply(v -> unmodifiableMap(tableRegistry.startedTables()));
    }

    /**
     * Returns an internal map, which contains all managed tables by their ID.
     */
    private Map<Integer, TableViewInternal> tablesById() {
        return unmodifiableMap(tableRegistry.tables());
    }

    /**
     * Returns a map with started tables.
     */
    @TestOnly
    public Map<Integer, TableViewInternal> startedTables() {
        return unmodifiableMap(tableRegistry.startedTables());
    }

    @Override
    public Table table(QualifiedName name) {
        return sync(tableAsync(name));
    }

    @Override
    public TableViewInternal table(int id) throws NodeStoppingException {
        return sync(tableAsync(id));
    }

    @Override
    public CompletableFuture<Table> tableAsync(QualifiedName name) {
        return tableAsyncInternal(name)
                .thenApply(identity());
    }

    /**
     * Asynchronously gets the table using causality token.
     *
     * @param causalityToken Causality token.
     * @param id Table id.
     * @return Future.
     */
    public CompletableFuture<TableViewInternal> tableAsync(long causalityToken, int id) {
        return inBusyLockAsync(busyLock, () -> tablesById(causalityToken).thenApply(tablesById -> tablesById.get(id)));
    }

    @Override
    public CompletableFuture<TableViewInternal> tableAsync(int tableId) {
        return inBusyLockAsync(busyLock, () -> {
            HybridTimestamp now = clockService.now();

            return orStopManagerFuture(executorInclinedSchemaSyncService.waitForMetadataCompleteness(now))
                    .thenCompose(unused -> inBusyLockAsync(busyLock, () -> {
                        Catalog catalog = catalogService.activeCatalog(now.longValue());

                        // Check if the table has been deleted.
                        if (catalog.table(tableId) == null) {
                            return nullCompletedFuture();
                        }

                        return tableAsyncInternalBusy(tableId);
                    }));
        });
    }

    /**
     * Asynchronously gets the local partitions set of a table using causality token.
     *
     * @param causalityToken Causality token.
     * @return Future.
     */
    public CompletableFuture<PartitionSet> localPartitionSetAsync(long causalityToken, int tableId) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            return localPartitionsVv.get(causalityToken)
                    .thenApply(unused -> tableRegistry.localPartsByTableId().getOrDefault(tableId, PartitionSet.EMPTY_SET));
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public TableViewInternal tableView(QualifiedName name) {
        return sync(tableViewAsync(name));
    }

    @Override
    public CompletableFuture<TableViewInternal> tableViewAsync(QualifiedName name) {
        return tableAsyncInternal(name);
    }

    /**
     * Gets a table by name, if it was created before. Doesn't parse canonical name.
     *
     * @param name Table name.
     * @return Future representing pending completion of the {@code TableManager#tableAsyncInternal} operation.
     */
    private CompletableFuture<TableViewInternal> tableAsyncInternal(QualifiedName name) {
        return inBusyLockAsync(busyLock, () -> {
            HybridTimestamp now = clockService.now();

            return orStopManagerFuture(executorInclinedSchemaSyncService.waitForMetadataCompleteness(now))
                    .thenCompose(unused -> inBusyLockAsync(busyLock, () -> {
                        Catalog catalog = catalogService.activeCatalog(now.longValue());
                        CatalogTableDescriptor tableDescriptor = catalog.table(name.schemaName(), name.objectName());

                        // Check if the table has been deleted.
                        if (tableDescriptor == null) {
                            return nullCompletedFuture();
                        }

                        return tableAsyncInternalBusy(tableDescriptor.id());
                    }));
        });
    }

    private CompletableFuture<TableViewInternal> tableAsyncInternalBusy(int tableId) {
        TableViewInternal tableImpl = tableRegistry.startedTables().get(tableId);

        if (tableImpl != null) {
            return completedFuture(tableImpl);
        }

        CompletableFuture<TableViewInternal> getLatestTableFuture = new CompletableFuture<>();

        CompletionListener<Void> tablesListener = (token, v, th) -> {
            if (th == null) {
                CompletableFuture<?> tablesFuture = tablesVv.get(token);

                tablesFuture.whenComplete((tbls, e) -> {
                    if (e != null) {
                        getLatestTableFuture.completeExceptionally(e);
                    } else {
                        getLatestTableFuture.complete(tableRegistry.startedTables().get(tableId));
                    }
                });
            } else {
                getLatestTableFuture.completeExceptionally(th);
            }

            return nullCompletedFuture();
        };

        assignmentsUpdatedVv.whenComplete(tablesListener);

        // This check is needed for the case when we have registered tablesListener,
        // but tablesVv has already been completed, so listener would be triggered only for the next versioned value update.
        tableImpl = tableRegistry.startedTables().get(tableId);

        if (tableImpl != null) {
            assignmentsUpdatedVv.removeWhenComplete(tablesListener);

            return completedFuture(tableImpl);
        }

        return orStopManagerFuture(getLatestTableFuture)
                .whenComplete((unused, throwable) -> assignmentsUpdatedVv.removeWhenComplete(tablesListener));
    }

    private static <T> T sync(CompletableFuture<T> future) {
        return IgniteUtils.getInterruptibly(future);
    }

    /**
     * Returns a cached table instance if it exists, {@code null} otherwise. Can return a table that is being stopped.
     *
     * @param tableId Table id.
     */
    @Override
    public @Nullable TableViewInternal cachedTable(int tableId) {
        return tableRegistry.tables().get(tableId);
    }

    /**
     * Returns a cached table instance if it exists, {@code null} otherwise. Can return a table that is being stopped.
     *
     * @param name Table name.
     */
    @TestOnly
    public @Nullable TableViewInternal cachedTable(String name) {
        return findTableImplByName(tableRegistry.tables().values(), name);
    }

    private CatalogZoneDescriptor getZoneDescriptor(CatalogTableDescriptor tableDescriptor, int catalogVersion) {
        return getZoneDescriptor(tableDescriptor, catalogService.catalog(catalogVersion));
    }

    private static CatalogZoneDescriptor getZoneDescriptor(CatalogTableDescriptor tableDescriptor, Catalog catalog) {
        CatalogZoneDescriptor zoneDescriptor = catalog.zone(tableDescriptor.zoneId());

        assert zoneDescriptor != null :
                "tableId=" + tableDescriptor.id() + ", zoneId=" + tableDescriptor.zoneId() + ", catalogVersion=" + catalog.version();

        return zoneDescriptor;
    }

    private CatalogSchemaDescriptor getSchemaDescriptor(CatalogTableDescriptor tableDescriptor, int catalogVersion) {
        CatalogSchemaDescriptor schemaDescriptor = catalogService.catalog(catalogVersion).schema(tableDescriptor.schemaId());

        assert schemaDescriptor != null :
                "tableId=" + tableDescriptor.id() + ", schemaId=" + tableDescriptor.schemaId() + ", catalogVersion=" + catalogVersion;

        return schemaDescriptor;
    }

    private static @Nullable TableViewInternal findTableImplByName(Collection<TableViewInternal> tables, String name) {
        return tables.stream().filter(table -> table.qualifiedName().equals(QualifiedName.fromSimple(name))).findAny().orElse(null);
    }

    private CompletableFuture<Void> recoverTables(long recoveryRevision, @Nullable HybridTimestamp lwm) {
        int earliestCatalogVersion = lwm == null
                ? catalogService.earliestCatalogVersion()
                : catalogService.activeCatalogVersion(lwm.longValue());

        int latestCatalogVersion = catalogService.latestCatalogVersion();

        var startedTables = new IntOpenHashSet();
        var startTableFutures = new ArrayList<CompletableFuture<?>>();

        Catalog nextCatalog = null;

        for (int ver = latestCatalogVersion; ver >= earliestCatalogVersion; ver--) {
            Catalog catalog = catalogService.catalog(ver);

            for (CatalogTableDescriptor tableDescriptor : catalog.tables()) {
                // Handle missed table drop event.
                int tableId = tableDescriptor.id();

                if (nextCatalog != null && nextCatalog.table(tableId) == null) {
                    destructionEventsQueue.enqueue(new DestroyTableEvent(nextCatalog.version(), tableId));
                }

                if (!startedTables.add(tableId)) {
                    continue;
                }

                CatalogZoneDescriptor zoneDescriptor = getZoneDescriptor(tableDescriptor, ver);
                CatalogSchemaDescriptor schemaDescriptor = getSchemaDescriptor(tableDescriptor, ver);

                CompletableFuture<?> startTableFuture = prepareTableResourcesOnRecovery(
                        recoveryRevision,
                        zoneDescriptor,
                        tableDescriptor,
                        schemaDescriptor
                );

                startTableFutures.add(startTableFuture);
            }

            nextCatalog = catalog;
        }

        return allOf(startTableFutures.toArray(CompletableFuture[]::new))
                // Only now do we complete the future allowing replica starts being processed. This is why on node recovery
                // PartitionReplicaLifecycleManager does not acquire write locks (as mutual exclusion of replica starts and table additions
                // is guaranteed bu completing the future here, in TableManager).
                .whenComplete(copyStateTo(readyToProcessReplicaStarts))
                .whenComplete((unused, throwable) -> {
                    if (throwable != null) {
                        failureProcessor.process(new FailureContext(throwable, "Error starting tables"));
                    } else {
                        LOG.info("Tables started successfully [count={}]", startTableFutures.size());
                    }
                });
    }

    /**
     * Returns the future that will complete when, either the future from the argument or {@link #stopManagerFuture} will complete,
     * successfully or exceptionally. Allows to protect from getting stuck at {@link IgniteComponent#stopAsync(ComponentContext)} when
     * someone is blocked (by using {@link #busyLock}) for a long time.
     *
     * @param future Future.
     */
    private <T> CompletableFuture<T> orStopManagerFuture(CompletableFuture<T> future) {
        if (future.isDone()) {
            return future;
        }

        return anyOf(future, stopManagerFuture).thenApply(o -> (T) o);
    }

    /** Internal event. */
    private static class DestroyTableEvent {
        final int catalogVersion;
        final int tableId;

        DestroyTableEvent(int catalogVersion, int tableId) {
            this.catalogVersion = catalogVersion;
            this.tableId = tableId;
        }

        public int catalogVersion() {
            return catalogVersion;
        }

        public int tableId() {
            return tableId;
        }
    }

    private void cleanUpResourcesForDroppedTablesOnRecoveryBusy() {
        // TODO: IGNITE-20384 Clean up abandoned resources for dropped tables from vault and metastore

        Set<Integer> aliveTableIds = aliveTables(catalogService, lowWatermark.getLowWatermark());

        destroyMvStoragesForTablesNotIn(aliveTableIds);
    }

    private void destroyMvStoragesForTablesNotIn(Set<Integer> aliveTableIds) {
        for (StorageEngine storageEngine : dataStorageMgr.allStorageEngines()) {
            Set<Integer> tableIdsOnDisk = storageEngine.tableIdsOnDisk();

            for (int tableId : difference(tableIdsOnDisk, aliveTableIds)) {
                storageEngine.destroyMvTable(tableId);
                LOG.info("Destroyed table MV storage for table {} in storage engine '{}'", tableId, storageEngine.name());
            }
        }
    }

    @Override
    public void setStreamerReceiverRunner(StreamerReceiverRunner runner) {
        tableImplFactory.setStreamerReceiverRunner(runner);
    }

    /**
     * Returns a copy of tables that belong to the specified zone.
     *
     * @param zoneId Zone identifier.
     * @return Set of tables.
     * @throws IgniteInternalException If failed to acquire a read lock for the zone or current thread was interrupted while waiting.
     */
    public Set<TableViewInternal> zoneTables(int zoneId) throws IgniteInternalException {
        return zoneCoordinator.zoneTables(zoneId);
    }

    private void unregisterMetricsSource(TableViewInternal table) {
        if (table == null) {
            return;
        }

        QualifiedName tableName = table.qualifiedName();

        try {
            metricManager.unregisterSource(TableMetricSource.sourceName(tableName));
        } catch (Exception e) {
            LOG.warn("Failed to unregister metrics source for table [id={}, name={}].", e, table.tableId(), tableName);
        }

        String storageProfile = table.internalTable().storage().getTableDescriptor().getStorageProfile();
        StorageEngine engine = dataStorageMgr.engineByStorageProfile(storageProfile);

        // Engine can be null sometimes, see "TableManager.createTableStorage".
        if (engine != null) {
            try {
                metricManager.unregisterSource(StorageEngineTablesMetricSource.sourceName(engine.name(), tableName));
            } catch (Exception e) {
                LOG.warn("Failed to unregister storage engine metrics source for table [id={}, name={}].", e, table.tableId(), tableName);
            }
        }
    }

}
