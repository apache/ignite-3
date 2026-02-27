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
import static java.util.Collections.unmodifiableMap;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.anyOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.causality.IncrementalVersionedValue.dependingOn;
import static org.apache.ignite.internal.event.EventListener.fromConsumer;
import static org.apache.ignite.internal.partition.replicator.LocalPartitionReplicaEvent.AFTER_REPLICA_DESTROYED;
import static org.apache.ignite.internal.partition.replicator.LocalPartitionReplicaEvent.AFTER_REPLICA_STOPPED;
import static org.apache.ignite.internal.partition.replicator.LocalPartitionReplicaEvent.BEFORE_REPLICA_STARTED;
import static org.apache.ignite.internal.table.distributed.TableUtils.aliveTables;
import static org.apache.ignite.internal.table.distributed.index.IndexUtils.registerIndexesToTable;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_READ;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_WRITE;
import static org.apache.ignite.internal.util.CollectionUtils.difference;
import static org.apache.ignite.internal.util.CompletableFutures.allOfToList;
import static org.apache.ignite.internal.util.CompletableFutures.copyStateTo;
import static org.apache.ignite.internal.util.CompletableFutures.emptyListCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;
import static org.apache.ignite.internal.util.ExceptionUtils.sneakyThrow;
import static org.apache.ignite.internal.util.IgniteUtils.closeAllManually;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableProperties;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.AlterTablePropertiesEventParameters;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.CreateTableEventParameters;
import org.apache.ignite.internal.catalog.events.DropTableEventParameters;
import org.apache.ignite.internal.catalog.events.RenameTableEventParameters;
import org.apache.ignite.internal.causality.CompletionListener;
import org.apache.ignite.internal.causality.IncrementalVersionedValue;
import org.apache.ignite.internal.causality.OutdatedTokenException;
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
import org.apache.ignite.internal.marshaller.ReflectionMarshallersProvider;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.Revisions;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.partition.replicator.LocalBeforeReplicaStartEventParameters;
import org.apache.ignite.internal.partition.replicator.LocalPartitionReplicaEventParameters;
import org.apache.ignite.internal.partition.replicator.NaiveAsyncReadWriteLock;
import org.apache.ignite.internal.partition.replicator.PartitionReplicaLifecycleManager;
import org.apache.ignite.internal.partition.replicator.PartitionReplicaLifecycleManager.TablePartitionReplicaProcessorFactory;
import org.apache.ignite.internal.partition.replicator.ZoneResourcesManager.ZonePartitionResources;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionDataStorage;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionKey;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.partition.replicator.schema.ExecutorInclinedSchemaSyncService;
import org.apache.ignite.internal.partition.replicator.schema.ValidationSchemasSource;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.wrappers.ExecutorInclinedPlacementDriver;
import org.apache.ignite.internal.raft.ExecutorInclinedRaftCommandRunner;
import org.apache.ignite.internal.raft.service.RaftCommandRunner;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.schema.configuration.GcConfiguration;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageClosedException;
import org.apache.ignite.internal.storage.StorageDestroyedException;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.metrics.StorageEngineTablesMetricSource;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.StreamerReceiverRunner;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.PartitionModificationCounterFactory.SizeSupplier;
import org.apache.ignite.internal.table.distributed.gc.GcUpdateHandler;
import org.apache.ignite.internal.table.distributed.gc.MvGc;
import org.apache.ignite.internal.table.distributed.index.IndexMetaStorage;
import org.apache.ignite.internal.table.distributed.index.IndexUpdateHandler;
import org.apache.ignite.internal.table.distributed.raft.MinimumRequiredTimeCollectorService;
import org.apache.ignite.internal.table.distributed.raft.TablePartitionProcessor;
import org.apache.ignite.internal.table.distributed.raft.snapshot.FullStateTransferIndexChooser;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionMvStorageAccessImpl;
import org.apache.ignite.internal.table.distributed.raft.snapshot.SnapshotAwarePartitionDataStorage;
import org.apache.ignite.internal.table.distributed.replicator.PartitionReplicaListener;
import org.apache.ignite.internal.table.distributed.schema.SchemaVersions;
import org.apache.ignite.internal.table.distributed.schema.SchemaVersionsImpl;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.table.distributed.storage.NullStorageEngine;
import org.apache.ignite.internal.table.metrics.ReadWriteMetricSource;
import org.apache.ignite.internal.table.metrics.TableMetricSource;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.RemotelyTriggeredResourceRegistry;
import org.apache.ignite.internal.tx.impl.TransactionInflights;
import org.apache.ignite.internal.tx.impl.TransactionStateResolver;
import org.apache.ignite.internal.tx.metrics.TransactionMetricsSource;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.Lazy;
import org.apache.ignite.internal.util.LongPriorityQueue;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
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

    private final TopologyService topologyService;

    /** Lock manager. */
    private final LockManager lockMgr;

    /** Replica service. */
    private final ReplicaService replicaSvc;

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

    /** Registered tables. */
    private final Map<Integer, TableViewInternal> tables = new ConcurrentHashMap<>();

    /** Started tables. */
    private final Map<Integer, TableViewInternal> startedTables = new ConcurrentHashMap<>();

    /** A queue for deferred table destruction events. */
    private final LongPriorityQueue<DestroyTableEvent> destructionEventsQueue =
            new LongPriorityQueue<>(DestroyTableEvent::catalogVersion);

    /** Local partitions. */
    private final Map<Integer, PartitionSet> localPartsByTableId = new ConcurrentHashMap<>();

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

    private final OutgoingSnapshotsManager outgoingSnapshotsManager;

    private final SchemaSyncService executorInclinedSchemaSyncService;

    private final CatalogService catalogService;

    private final FailureProcessor failureProcessor;

    /** Incoming RAFT snapshots executor. */
    private final ThreadPoolExecutor incomingSnapshotsExecutor;

    private final MvGc mvGc;

    private final LowWatermark lowWatermark;

    private final HybridTimestampTracker observableTimestampTracker;

    /** Placement driver. */
    private final PlacementDriver executorInclinedPlacementDriver;

    /** A supplier function that returns {@link IgniteSql}. */
    private final Supplier<IgniteSql> sql;

    private final SchemaVersions schemaVersions;

    private final ValidationSchemasSource validationSchemasSource;

    private final PartitionReplicatorNodeRecovery partitionReplicatorNodeRecovery;

    /** Ends at the {@link IgniteComponent#stopAsync(ComponentContext)} with an {@link NodeStoppingException}. */
    private final CompletableFuture<Void> stopManagerFuture = new CompletableFuture<>();

    private final ReplicationConfiguration replicationConfiguration;

    /**
     * Executes partition operations (that might cause I/O and/or be blocked on locks).
     */
    private final Executor partitionOperationsExecutor;

    /** Marshallers provider. */
    private final ReflectionMarshallersProvider marshallers = new ReflectionMarshallersProvider();

    /** Index chooser for full state transfer. */
    private final FullStateTransferIndexChooser fullStateTransferIndexChooser;

    private final RemotelyTriggeredResourceRegistry remotelyTriggeredResourceRegistry;

    private final TransactionInflights transactionInflights;

    private final String nodeName;

    private final PartitionReplicaLifecycleManager partitionReplicaLifecycleManager;

    @Nullable
    private ScheduledExecutorService streamerFlushExecutor;

    private final IndexMetaStorage indexMetaStorage;

    private final MinimumRequiredTimeCollectorService minTimeCollectorService;

    @Nullable
    private StreamerReceiverRunner streamerReceiverRunner;

    private final CompletableFuture<Void> readyToProcessReplicaStarts = new CompletableFuture<>();

    // TODO https://issues.apache.org/jira/browse/IGNITE-25347
    /** Mapping zone identifier to a collection of tables related to the zone. */
    private final Map<Integer, Set<TableViewInternal>> tablesPerZone = new HashMap<>();
    /** Locks to synchronize an access to the {@link #tablesPerZone}. */
    private final Map<Integer, NaiveAsyncReadWriteLock> tablesPerZoneLocks = new ConcurrentHashMap<>();

    /** Configuration of rebalance retries delay. */
    private final SystemDistributedConfigurationPropertyHolder<Integer> rebalanceRetryDelayConfiguration;

    private final EventListener<LocalBeforeReplicaStartEventParameters> onBeforeZoneReplicaStartedListener = this::beforeZoneReplicaStarted;
    private final EventListener<LocalPartitionReplicaEventParameters> onZoneReplicaStoppedListener = this::onZoneReplicaStopped;
    private final EventListener<LocalPartitionReplicaEventParameters> onZoneReplicaDestroyedListener = this::onZoneReplicaDestroyed;

    private final EventListener<CreateTableEventParameters> onTableCreateListener = this::loadTableToZoneOnTableCreate;
    private final EventListener<DropTableEventParameters> onTableDropListener = fromConsumer(this::onTableDrop);
    private final EventListener<CatalogEventParameters> onTableAlterListener = this::onTableAlter;

    private final EventListener<ChangeLowWatermarkEventParameters> onLowWatermarkChangedListener = this::onLwmChanged;

    private final MetricManager metricManager;

    private final PartitionModificationCounterFactory partitionModificationCounterFactory;
    private final Map<TablePartitionId, PartitionTableStatsMetricSource> partModCounterMetricSources = new ConcurrentHashMap<>();
    private final Map<TablePartitionId, LongSupplier> pendingWriteIntentsSuppliers = new ConcurrentHashMap<>();

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
            PartitionModificationCounterFactory partitionModificationCounterFactory
    ) {
        this.topologyService = topologyService;
        this.lockMgr = lockMgr;
        this.replicaSvc = replicaSvc;
        this.txManager = txManager;
        this.dataStorageMgr = dataStorageMgr;
        this.metaStorageMgr = metaStorageMgr;
        this.schemaManager = schemaManager;
        this.validationSchemasSource = validationSchemasSource;
        this.ioExecutor = ioExecutor;
        this.partitionOperationsExecutor = partitionOperationsExecutor;
        this.clockService = clockService;
        this.outgoingSnapshotsManager = outgoingSnapshotsManager;
        this.catalogService = catalogService;
        this.failureProcessor = failureProcessor;
        this.observableTimestampTracker = observableTimestampTracker;
        this.sql = sql;
        this.replicationConfiguration = replicationConfiguration;
        this.remotelyTriggeredResourceRegistry = remotelyTriggeredResourceRegistry;
        this.lowWatermark = lowWatermark;
        this.transactionInflights = transactionInflights;
        this.nodeName = nodeName;
        this.indexMetaStorage = indexMetaStorage;
        this.partitionReplicaLifecycleManager = partitionReplicaLifecycleManager;
        this.minTimeCollectorService = minTimeCollectorService;
        this.metricManager = metricManager;
        this.partitionModificationCounterFactory = partitionModificationCounterFactory;

        this.executorInclinedSchemaSyncService = new ExecutorInclinedSchemaSyncService(schemaSyncService, partitionOperationsExecutor);
        this.executorInclinedPlacementDriver = new ExecutorInclinedPlacementDriver(placementDriver, partitionOperationsExecutor);

        schemaVersions = new SchemaVersionsImpl(executorInclinedSchemaSyncService, catalogService, clockService);

        tablesVv = new IncrementalVersionedValue<>("TableManager#tables", registry, 100, null);

        localPartitionsVv = new IncrementalVersionedValue<>("TableManager#localPartitions", dependingOn(tablesVv));

        assignmentsUpdatedVv = new IncrementalVersionedValue<>("TableManager#assignmentsUpdated", dependingOn(localPartitionsVv));

        scanRequestExecutor = Executors.newSingleThreadExecutor(
                IgniteThreadFactory.create(nodeName, "scan-query-executor", LOG, STORAGE_READ));

        int cpus = Runtime.getRuntime().availableProcessors();

        incomingSnapshotsExecutor = new ThreadPoolExecutor(
                cpus,
                cpus,
                30,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                IgniteThreadFactory.create(nodeName, "incoming-raft-snapshot", LOG, STORAGE_READ, STORAGE_WRITE)
        );
        incomingSnapshotsExecutor.allowCoreThreadTimeOut(true);

        mvGc = new MvGc(nodeName, gcConfig, lowWatermark, failureProcessor);

        partitionReplicatorNodeRecovery = new PartitionReplicatorNodeRecovery(
                messagingService,
                partitionOperationsExecutor,
                tableId -> tablesById().get(tableId)
        );

        fullStateTransferIndexChooser = new FullStateTransferIndexChooser(catalogService, lowWatermark, indexMetaStorage);

        rebalanceRetryDelayConfiguration = new SystemDistributedConfigurationPropertyHolder<>(
                systemDistributedConfiguration,
                (v, r) -> {},
                DistributionZonesUtil.REBALANCE_RETRY_DELAY_MS,
                DistributionZonesUtil.REBALANCE_RETRY_DELAY_DEFAULT,
                Integer::parseInt
        );

        // Register event listeners in the constructor to avoid races with "partitionReplicaLifecycleManager"'s recovery.
        // We rely on the "readyToProcessReplicaStarts" future to block event handling until "startAsync" is completed.
        partitionReplicaLifecycleManager.listen(BEFORE_REPLICA_STARTED, onBeforeZoneReplicaStartedListener);
        partitionReplicaLifecycleManager.listen(AFTER_REPLICA_STOPPED, onZoneReplicaStoppedListener);
        partitionReplicaLifecycleManager.listen(AFTER_REPLICA_DESTROYED, onZoneReplicaDestroyedListener);
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        return inBusyLockAsync(busyLock, () -> {
            TransactionMetricsSource transactionMetricsSource = txManager.transactionMetricsSource();
            transactionMetricsSource.setPendingWriteIntentsSupplier(this::totalPendingWriteIntents);

            mvGc.start();

            fullStateTransferIndexChooser.start();

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

    private static <T> Function<Throwable, T> ignoreTableClosedException() {
        return ex -> {
            if (hasCause(ex, TableClosedException.class)) {
                return null;
            }
            throw sneakyThrow(ex);
        };
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
                                            () -> stopAndDestroyTablePartition(
                                                    new TablePartitionId(table.tableId(), zonePartitionId.partitionId()),
                                                    parameters.causalityToken()
                                            )
                                    ),
                                    ioExecutor).thenCompose(identity()))
                            .toArray(CompletableFuture[]::new);

                    return allOf(futures);
                });
            }).whenComplete((v, t) -> readLockAcquisitionFuture.thenAccept(zoneLock::unlockRead)).thenApply((unused) -> false);
        } catch (Throwable t) {
            readLockAcquisitionFuture.thenAccept(zoneLock::unlockRead);

            return failedFuture(t);
        }
    }

    /**
     * During node recovery pre-populates required internal table structures before zone replicas are started.
     *
     * <p>The created resources will then be loaded during replica startup in {@link #beforeZoneReplicaStarted}.
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

                        tables.put(tableId, table);

                        addTableToZone(zoneDescriptor.id(), table);

                        startedTables.put(tableId, table);
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

                tables.put(tableId, table);
            });
        }));

        // Obtain future, but don't chain on it yet because update() on VVs must be called in the same thread. The method we call
        // will call update() on VVs and inside those updates it will chain on the lock acquisition future.
        CompletableFuture<Long> acquisitionFuture = partitionReplicaLifecycleManager.lockZoneForRead(zoneDescriptor.id());
        try {
            return loadTableToZoneOnTableCreateHavingZoneReadLock(acquisitionFuture, causalityToken, zoneDescriptor, tableId)
                    .whenComplete((res, ex) -> unlockZoneForRead(zoneDescriptor, acquisitionFuture));
        } catch (Throwable e) {
            unlockZoneForRead(zoneDescriptor, acquisitionFuture);

            return failedFuture(e);
        }
    }

    private CompletableFuture<Void> loadTableToZoneOnTableCreateHavingZoneReadLock(
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

    private void unlockZoneForRead(CatalogZoneDescriptor zoneDescriptor, CompletableFuture<Long> readLockAcquiryFuture) {
        readLockAcquiryFuture.thenAccept(stamp -> {
            partitionReplicaLifecycleManager.unlockZoneForRead(zoneDescriptor.id(), stamp);
        });
    }

    /**
     * Prepare the table partition resources and load it to the zone-based replica.
     *
     * @param table Table.
     * @param zonePartitionId Zone Partition ID.
     * @param resources Replica resources.
     */
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

        PartitionDataStorage partitionDataStorage = partitionDataStorage(
                new PartitionKey(zonePartitionId.zoneId(), partId),
                tableId,
                mvPartitionStorage
        );

        PartitionUpdateHandlers partitionUpdateHandlers = createPartitionUpdateHandlers(
                partId,
                partitionDataStorage,
                table,
                resources.safeTimeTracker(),
                replicationConfiguration,
                onNodeRecovery
        );

        mvGc.addStorage(tablePartitionId, partitionUpdateHandlers.gcUpdateHandler);

        minTimeCollectorService.addPartition(new TablePartitionId(tableId, partId));

        TablePartitionReplicaProcessorFactory createListener = (raftClient, transactionStateResolver) -> createReplicaListener(
                zonePartitionId,
                table,
                resources.safeTimeTracker(),
                mvPartitionStorage,
                partitionUpdateHandlers,
                raftClient,
                transactionStateResolver
        );

        var tablePartitionRaftListener = new TablePartitionProcessor(
                txManager,
                partitionDataStorage,
                partitionUpdateHandlers.storageUpdateHandler,
                catalogService,
                table.schemaView(),
                indexMetaStorage,
                topologyService.localMember().id(),
                minTimeCollectorService,
                partitionOperationsExecutor,
                executorInclinedPlacementDriver,
                clockService,
                zonePartitionId
        );

        var partitionStorageAccess = new PartitionMvStorageAccessImpl(
                partId,
                table.internalTable().storage(),
                mvGc,
                partitionUpdateHandlers.indexUpdateHandler,
                partitionUpdateHandlers.gcUpdateHandler,
                fullStateTransferIndexChooser,
                schemaManager.schemaRegistry(tableId),
                lowWatermark
        );

        partitionReplicaLifecycleManager.loadTableListenerToZoneReplica(
                zonePartitionId,
                tableId,
                createListener,
                tablePartitionRaftListener,
                partitionStorageAccess,
                onNodeRecovery
        );
    }

    private void onTableDrop(DropTableEventParameters parameters) {
        inBusyLock(busyLock, () -> {
            unregisterMetricsSource(startedTables.get(parameters.tableId()));

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

                    TableViewInternal table = tables.get(parameters.tableId());

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

                    TableViewInternal table = tables.get(parameters.tableId());

                    // TODO: revisit this approach, see https://issues.apache.org/jira/browse/IGNITE-21235.
                    ((TableImpl) table).name(parameters.newTableName());

                    return nullCompletedFuture();
                })
        );
    }

    private PartitionReplicaListener createReplicaListener(
            ZonePartitionId replicationGroupId,
            TableViewInternal table,
            PendingComparableValuesTracker<HybridTimestamp, Void> safeTimeTracker,
            MvPartitionStorage mvPartitionStorage,
            PartitionUpdateHandlers partitionUpdateHandlers,
            RaftCommandRunner raftClient,
            TransactionStateResolver transactionStateResolver
    ) {
        int partitionIndex = replicationGroupId.partitionId();

        return new PartitionReplicaListener(
                mvPartitionStorage,
                new ExecutorInclinedRaftCommandRunner(raftClient, partitionOperationsExecutor),
                txManager,
                lockMgr,
                scanRequestExecutor,
                replicationGroupId,
                table.tableId(),
                table.indexesLockers(partitionIndex),
                new Lazy<>(() -> table.indexStorageAdapters(partitionIndex).get().get(table.pkId())),
                () -> table.indexStorageAdapters(partitionIndex).get(),
                clockService,
                safeTimeTracker,
                transactionStateResolver,
                partitionUpdateHandlers.storageUpdateHandler,
                validationSchemasSource,
                localNode(),
                executorInclinedSchemaSyncService,
                catalogService,
                executorInclinedPlacementDriver,
                topologyService,
                remotelyTriggeredResourceRegistry,
                schemaManager.schemaRegistry(table.tableId()),
                indexMetaStorage,
                lowWatermark,
                failureProcessor,
                table.metrics()
        );
    }

    private PartitionDataStorage partitionDataStorage(PartitionKey partitionKey, int tableId, MvPartitionStorage partitionStorage) {
        return new SnapshotAwarePartitionDataStorage(
                tableId,
                partitionStorage,
                outgoingSnapshotsManager,
                partitionKey
        );
    }

    @Override
    public void beforeNodeStop() {
        if (!beforeStopGuard.compareAndSet(false, true)) {
            return;
        }

        stopManagerFuture.completeExceptionally(new NodeStoppingException());

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

        partitionReplicaLifecycleManager.removeListener(AFTER_REPLICA_DESTROYED, onZoneReplicaDestroyedListener);
        partitionReplicaLifecycleManager.removeListener(AFTER_REPLICA_STOPPED, onZoneReplicaStoppedListener);
        partitionReplicaLifecycleManager.removeListener(BEFORE_REPLICA_STARTED, onBeforeZoneReplicaStartedListener);

        int shutdownTimeoutSeconds = 10;

        try {
            closeAllManually(
                    () -> closeAllManually(tables.values().stream().map(table -> () -> closeTable(table))),
                    mvGc,
                    fullStateTransferIndexChooser,
                    () -> shutdownAndAwaitTermination(scanRequestExecutor, shutdownTimeoutSeconds, TimeUnit.SECONDS),
                    () -> shutdownAndAwaitTermination(incomingSnapshotsExecutor, shutdownTimeoutSeconds, TimeUnit.SECONDS),
                    () -> {
                        ScheduledExecutorService streamerFlushExecutor;

                        synchronized (this) {
                            streamerFlushExecutor = this.streamerFlushExecutor;
                        }

                        shutdownAndAwaitTermination(streamerFlushExecutor, shutdownTimeoutSeconds, TimeUnit.SECONDS);
                    }
            );
        } catch (Exception e) {
            return failedFuture(e);
        }

        return nullCompletedFuture();
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

        MvTableStorage tableStorage = createTableStorage(tableDescriptor, zoneDescriptor);

        int partitions = zoneDescriptor.partitions();

        InternalTableImpl internalTable = new InternalTableImpl(
                tableName,
                zoneDescriptor.id(),
                tableDescriptor.id(),
                partitions,
                topologyService,
                txManager,
                tableStorage,
                replicaSvc,
                clockService,
                observableTimestampTracker,
                executorInclinedPlacementDriver,
                transactionInflights,
                this::streamerFlushExecutor,
                Objects.requireNonNull(streamerReceiverRunner),
                createAndRegisterMetricsSource(tableStorage, tableName)
        );

        CatalogTableProperties descProps = tableDescriptor.properties();

        return new TableImpl(
                internalTable,
                lockMgr,
                schemaVersions,
                marshallers,
                sql.get(),
                failureProcessor,
                tableDescriptor.primaryKeyIndexId(),
                new TableStatsStalenessConfiguration(descProps.staleRowsFraction(), descProps.minStaleRowsCount()),
                schemaRegistry
        );
    }

    /**
     * Creates data storage for the provided table.
     *
     * @param tableDescriptor Catalog table descriptor.
     * @param zoneDescriptor Catalog distributed zone descriptor.
     */
    protected MvTableStorage createTableStorage(CatalogTableDescriptor tableDescriptor, CatalogZoneDescriptor zoneDescriptor) {
        StorageEngine engine = dataStorageMgr.engineByStorageProfile(tableDescriptor.storageProfile());

        if (engine == null) {
            // Create a placeholder to allow Table object being created.
            engine = new NullStorageEngine();
        }

        return engine.createMvTable(
                new StorageTableDescriptor(tableDescriptor.id(), zoneDescriptor.partitions(), tableDescriptor.storageProfile()),
                new CatalogStorageIndexDescriptorSupplier(catalogService, lowWatermark)
        );
    }

    /**
     * Drops local structures for a table.
     *
     * @param tableId Table id to destroy.
     */
    private CompletableFuture<Void> destroyTableLocally(int tableId) {
        TableViewInternal table = startedTables.remove(tableId);

        localPartsByTableId.remove(tableId);

        assert table != null : tableId;

        InternalTable internalTable = table.internalTable();

        return stopAndDestroyTableProcessors(table)
                .thenComposeAsync(unused -> inBusyLockAsync(busyLock, () -> internalTable.storage().destroy()), ioExecutor)
                .thenAccept(unused -> inBusyLock(busyLock, () -> {
                    tables.remove(tableId);
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
        return assignmentsUpdatedVv.get(causalityToken).thenApply(v -> unmodifiableMap(startedTables));
    }

    /**
     * Returns an internal map, which contains all managed tables by their ID.
     */
    private Map<Integer, TableViewInternal> tablesById() {
        return unmodifiableMap(tables);
    }

    /**
     * Returns a map with started tables.
     */
    @TestOnly
    public Map<Integer, TableViewInternal> startedTables() {
        return unmodifiableMap(startedTables);
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
                    .thenApply(unused -> localPartsByTableId.getOrDefault(tableId, PartitionSet.EMPTY_SET));
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
        TableViewInternal tableImpl = startedTables.get(tableId);

        if (tableImpl != null) {
            return completedFuture(tableImpl);
        }

        CompletableFuture<TableViewInternal> getLatestTableFuture = new CompletableFuture<>();

        CompletionListener<Void> tablesListener = (token, v, th) -> {
            if (th == null) {
                CompletableFuture<?> tablesFuture = tablesVv.get(token);

                tablesFuture.whenComplete((tables, e) -> {
                    if (e != null) {
                        getLatestTableFuture.completeExceptionally(e);
                    } else {
                        getLatestTableFuture.complete(startedTables.get(tableId));
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
        tableImpl = startedTables.get(tableId);

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

    private static PartitionSet extendPartitionSet(@Nullable PartitionSet oldPartitionSet, int partitionId) {
        PartitionSet newPartitionSet = Objects.requireNonNullElseGet(oldPartitionSet, BitSetPartitionSet::new);
        newPartitionSet.set(partitionId);
        return newPartitionSet;
    }

    /**
     * Gets MV partition storage.
     *
     * @param table Table.
     * @param partitionId Partition ID.
     * @return MvPartitionStorage.
     */
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

            return mvPartition != null ? completedFuture(mvPartition) : internalTable.storage().createMvPartition(partitionId);
        }).collect(toList());

        return CompletableFutures.allOf(storageFuts);
    }

    private CompletableFuture<Void> stopAndDestroyTableProcessors(TableViewInternal table) {
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

    private CompletableFuture<Void> stopAndDestroyTablePartition(TablePartitionId tablePartitionId, long causalityToken) {
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

    /**
     * Stops all resources associated with a given partition, like replicas and partition trackers.
     *
     * @param tablePartitionId Partition ID.
     * @param table Table which this partition belongs to.
     * @return Future that will be completed after all resources have been closed.
     */
    private CompletableFuture<Void> stopTablePartition(TablePartitionId tablePartitionId, TableViewInternal table) {
        // In case of colocation there shouldn't be any table replica and thus it shouldn't be stopped.
        minTimeCollectorService.removePartition(tablePartitionId);

        PartitionTableStatsMetricSource metricSource = partModCounterMetricSources.remove(tablePartitionId);
        pendingWriteIntentsSuppliers.remove(tablePartitionId);
        if (metricSource != null) {
            try {
                metricManager.unregisterSource(metricSource);
            } catch (Exception e) {
                String message = "Failed to unregister metrics source for table [name={}, partitionId={}].";
                LOG.warn(message, e, table.name(), tablePartitionId.partitionId());
            }
        }

        return mvGc.removeStorage(tablePartitionId);
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

    private InternalClusterNode localNode() {
        return topologyService.localMember();
    }

    private PartitionUpdateHandlers createPartitionUpdateHandlers(
            int partitionId,
            PartitionDataStorage partitionDataStorage,
            TableViewInternal table,
            PendingComparableValuesTracker<HybridTimestamp, Void> safeTimeTracker,
            ReplicationConfiguration replicationConfiguration,
            boolean onNodeRecovery
    ) {
        TableIndexStoragesSupplier indexes = table.indexStorageAdapters(partitionId);

        IndexUpdateHandler indexUpdateHandler = new IndexUpdateHandler(indexes);

        GcUpdateHandler gcUpdateHandler = new GcUpdateHandler(partitionDataStorage, safeTimeTracker, indexUpdateHandler);

        SizeSupplier partSizeSupplier = () -> partitionDataStorage.getStorage().estimatedSize();

        PartitionModificationCounter modificationCounter =
                partitionModificationCounterFactory.create(partSizeSupplier, table::stalenessConfiguration, table.tableId(), partitionId);

        StorageUpdateHandler storageUpdateHandler = new StorageUpdateHandler(
                partitionId,
                partitionDataStorage,
                indexUpdateHandler,
                replicationConfiguration,
                modificationCounter,
                txManager
        );

        storageUpdateHandler.start(onNodeRecovery);

        registerPartitionTableStatsMetrics(table, partitionId, modificationCounter);

        pendingWriteIntentsSuppliers.put(new TablePartitionId(table.tableId(), partitionId), storageUpdateHandler::getPendingRowCount);

        return new PartitionUpdateHandlers(storageUpdateHandler, indexUpdateHandler, gcUpdateHandler);
    }

    private void registerPartitionTableStatsMetrics(
            TableViewInternal table,
            int partitionId,
            PartitionModificationCounter counter
    ) {
        PartitionTableStatsMetricSource metricSource =
                new PartitionTableStatsMetricSource(table.tableId(), partitionId, counter);

        try {
            metricManager.registerSource(metricSource);
            // Do not enable this Metrics Source by default since it's purpose only for live troubleshooting

            TablePartitionId tablePartitionId = new TablePartitionId(table.tableId(), partitionId);

            partModCounterMetricSources.put(tablePartitionId, metricSource);
        } catch (Exception e) {
            LOG.warn("Failed to register metrics source for table [name={}, partitionId={}].", e, table.name(), partitionId);
        }
    }

    private long totalPendingWriteIntents() {
        long sum = 0;

        for (LongSupplier supplier : pendingWriteIntentsSuppliers.values()) {
            sum += supplier.getAsLong();
        }

        return sum;
    }

    /**
     * Returns a cached table instance if it exists, {@code null} otherwise. Can return a table that is being stopped.
     *
     * @param tableId Table id.
     */
    @Override
    public @Nullable TableViewInternal cachedTable(int tableId) {
        return tables.get(tableId);
    }

    /**
     * Returns a cached table instance if it exists, {@code null} otherwise. Can return a table that is being stopped.
     *
     * @param name Table name.
     */
    @TestOnly
    public @Nullable TableViewInternal cachedTable(String name) {
        return findTableImplByName(tables.values(), name);
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

    private synchronized ScheduledExecutorService streamerFlushExecutor() {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            if (streamerFlushExecutor == null) {
                streamerFlushExecutor = Executors.newSingleThreadScheduledExecutor(
                        IgniteThreadFactory.create(nodeName, "streamer-flush-executor", LOG, STORAGE_WRITE));
            }

            return streamerFlushExecutor;
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public void setStreamerReceiverRunner(StreamerReceiverRunner runner) {
        this.streamerReceiverRunner = runner;
    }

    /**
     * Returns a copy of tables that belong to the specified zone.
     *
     * @param zoneId Zone identifier.
     * @return Set of tables.
     * @throws IgniteInternalException If failed to acquire a read lock for the zone or current thread was interrupted while waiting.
     */
    public Set<TableViewInternal> zoneTables(int zoneId) throws IgniteInternalException {
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
     * Returns a set of tables that belong to the specified zone.
     * Note that this method call should be properly synchronized using {@link #tablesPerZoneLocks}.
     *
     * @param zoneId Zone identifier.
     * @return Set of tables.
     */
    private Set<TableViewInternal> zoneTablesRawSet(int zoneId) {
        return tablesPerZone.getOrDefault(zoneId, Set.of());
    }

    /**
     * Adds a table to the specified zone.
     *
     * @param zoneId Zone identifier.
     * @param table Table to add.
     * @throws IgniteInternalException If failed to acquire a write lock for the zone or current thread was interrupted while waiting.
     */
    private void addTableToZone(int zoneId, TableImpl table) throws IgniteInternalException {
        NaiveAsyncReadWriteLock zoneLock = tablesPerZoneLocks.computeIfAbsent(zoneId, id -> new NaiveAsyncReadWriteLock());

        CompletableFuture<Long> writeLockAcquisitionFuture = zoneLock.writeLock();

        try {
            writeLockAcquisitionFuture.thenAccept(stamp -> {
                tablesPerZone.compute(zoneId, (id, tables) -> {
                    if (tables == null) {
                        tables = new HashSet<>();
                    }

                    tables.add(table);

                    return tables;
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

    private ReadWriteMetricSource createAndRegisterMetricsSource(MvTableStorage tableStorage, QualifiedName tableName) {
        StorageTableDescriptor tableDescriptor = tableStorage.getTableDescriptor();

        CatalogTableDescriptor catalogTableDescriptor = catalogService.latestCatalog().table(tableDescriptor.getId());

        // The table might be created during the recovery phase.
        // In that case, we should only register the metric source for the actual tables that exist in the latest catalog.
        boolean registrationNeeded = catalogTableDescriptor != null;

        StorageEngine engine = dataStorageMgr.engineByStorageProfile(tableDescriptor.getStorageProfile());

        // Engine can be null sometimes, see "TableManager.createTableStorage".
        if (engine != null && registrationNeeded) {
            StorageEngineTablesMetricSource engineMetricSource = new StorageEngineTablesMetricSource(engine.name(), tableName);

            engine.addTableMetrics(tableDescriptor, engineMetricSource);

            try {
                metricManager.registerSource(engineMetricSource);
                metricManager.enable(engineMetricSource);
            } catch (Exception e) {
                String message = "Failed to register storage engine metrics source for table [id={}, name={}].";
                LOG.warn(message, e, tableDescriptor.getId(), tableName);
            }
        }

        ReadWriteMetricSource source = new TableMetricSource(tableName);

        if (registrationNeeded) {
            try {
                metricManager.registerSource(source);
                metricManager.enable(source);
            } catch (Exception e) {
                LOG.warn("Failed to register metrics source for table [id={}, name={}].", e, tableDescriptor.getId(), tableName);
            }
        }

        return source;
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

    private static class TableClosedException extends IgniteInternalException {
        private static final long serialVersionUID = 1L;

        private TableClosedException(int tableId, @Nullable Throwable cause) {
            super(INTERNAL_ERR, "Table is closed [tableId=" + tableId + "]", cause);
        }
    }
}
