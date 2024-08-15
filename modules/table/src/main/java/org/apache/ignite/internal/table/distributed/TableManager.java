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

import static java.nio.charset.StandardCharsets.UTF_8;
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
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.causality.IncrementalVersionedValue.dependingOn;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.ASSIGNMENTS_SWITCH_REDUCE_PREFIX;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.PENDING_ASSIGNMENTS_PREFIX;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.STABLE_ASSIGNMENTS_PREFIX;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.extractPartitionNumber;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.extractTableId;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.partitionAssignmentsGetLocally;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.pendingPartAssignmentsKey;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.stablePartAssignmentsKey;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.subtract;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.tableAssignmentsGetLocally;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.union;
import static org.apache.ignite.internal.event.EventListener.fromConsumer;
import static org.apache.ignite.internal.hlc.HybridTimestamp.LOGICAL_TIME_BITS_SIZE;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestampToLong;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.raft.PeersAndLearners.fromAssignments;
import static org.apache.ignite.internal.table.distributed.TableUtils.droppedTables;
import static org.apache.ignite.internal.table.distributed.index.IndexUtils.registerIndexesToTable;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_READ;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_WRITE;
import static org.apache.ignite.internal.util.ByteUtils.toByteArray;
import static org.apache.ignite.internal.util.CompletableFutures.allOfToList;
import static org.apache.ignite.internal.util.CompletableFutures.emptyListCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.trueCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.LongFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.affinity.AffinityUtils;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.affinity.Assignments;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CreateTableEventParameters;
import org.apache.ignite.internal.catalog.events.DropTableEventParameters;
import org.apache.ignite.internal.catalog.events.RenameTableEventParameters;
import org.apache.ignite.internal.causality.CompletionListener;
import org.apache.ignite.internal.causality.IncrementalVersionedValue;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.rebalance.PartitionMover;
import org.apache.ignite.internal.distributionzones.rebalance.RebalanceRaftGroupEventsListener;
import org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.lowwatermark.event.ChangeLowWatermarkEventParameters;
import org.apache.ignite.internal.lowwatermark.event.LowWatermarkEvent;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.marshaller.ReflectionMarshallersProvider;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.internal.partition.replicator.PartitionReplicaLifecycleManager;
import org.apache.ignite.internal.partition.replicator.ZonePartitionReplicaListener;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.raft.ExecutorInclinedRaftCommandRunner;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftGroupEventsListener;
import org.apache.ignite.internal.raft.service.LeaderWithTerm;
import org.apache.ignite.internal.raft.service.RaftCommandRunner;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.raft.storage.SnapshotStorageFactory;
import org.apache.ignite.internal.replicator.Replica;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ReplicaManager.WeakReplicaStopReason;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.schema.configuration.GcConfiguration;
import org.apache.ignite.internal.schema.configuration.StorageUpdateConfiguration;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.LongPriorityQueue;
import org.apache.ignite.internal.table.StreamerReceiverRunner;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.gc.GcUpdateHandler;
import org.apache.ignite.internal.table.distributed.gc.MvGc;
import org.apache.ignite.internal.table.distributed.index.IndexMetaStorage;
import org.apache.ignite.internal.table.distributed.index.IndexUpdateHandler;
import org.apache.ignite.internal.table.distributed.raft.PartitionDataStorage;
import org.apache.ignite.internal.table.distributed.raft.PartitionListener;
import org.apache.ignite.internal.table.distributed.raft.snapshot.FullStateTransferIndexChooser;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionAccessImpl;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionKey;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionSnapshotStorageFactory;
import org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing.SnapshotAwarePartitionDataStorage;
import org.apache.ignite.internal.table.distributed.replicator.PartitionReplicaListener;
import org.apache.ignite.internal.table.distributed.replicator.TransactionStateResolver;
import org.apache.ignite.internal.table.distributed.schema.CatalogValidationSchemasSource;
import org.apache.ignite.internal.table.distributed.schema.ExecutorInclinedSchemaSyncService;
import org.apache.ignite.internal.table.distributed.schema.SchemaVersions;
import org.apache.ignite.internal.table.distributed.schema.SchemaVersionsImpl;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.table.distributed.storage.PartitionStorages;
import org.apache.ignite.internal.table.distributed.wrappers.ExecutorInclinedPlacementDriver;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.tx.impl.RemotelyTriggeredResourceRegistry;
import org.apache.ignite.internal.tx.impl.TransactionInflights;
import org.apache.ignite.internal.tx.impl.TxMessageSender;
import org.apache.ignite.internal.tx.storage.state.ThreadAssertingTxStateTableStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateTableStorage;
import org.apache.ignite.internal.tx.storage.state.rocksdb.TxStateRocksDbSharedStorage;
import org.apache.ignite.internal.tx.storage.state.rocksdb.TxStateRocksDbTableStorage;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.Lazy;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.internal.utils.RebalanceUtilEx;
import org.apache.ignite.internal.worker.ThreadAssertions;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.util.IgniteNameUtils;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.Table;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Table manager.
 */
public class TableManager implements IgniteTablesInternal, IgniteComponent {

    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(TableManager.class);

    /** Name of a transaction state directory. */
    private static final String TX_STATE_DIR = "tx-state";

    /** Transaction storage flush delay. */
    private static final int TX_STATE_STORAGE_FLUSH_DELAY = 100;
    private static final IntSupplier TX_STATE_STORAGE_FLUSH_DELAY_SUPPLIER = () -> TX_STATE_STORAGE_FLUSH_DELAY;

    private final TopologyService topologyService;

    /** Replica manager. */
    private final ReplicaManager replicaMgr;

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

    /** Transaction state resolver. */
    private final TransactionStateResolver transactionStateResolver;

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
     * <p>Only explicitly updated in {@link #startLocalPartitionsAndClients(CompletableFuture, TableImpl, int, boolean, HybridTimestamp)}.
     *
     * <p>Completed strictly after {@link #localPartitionsVv}.
     */
    private final IncrementalVersionedValue<Void> assignmentsUpdatedVv;

    /** Registered tables. */
    private final Map<Integer, TableImpl> tables = new ConcurrentHashMap<>();

    /** Started tables. */
    private final Map<Integer, TableImpl> startedTables = new ConcurrentHashMap<>();

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

    /** Transaction state storage scheduled pool. */
    private final ScheduledExecutorService txStateStorageScheduledPool;

    /** Transaction state storage pool. */
    private final ExecutorService txStateStoragePool;

    private final TxStateRocksDbSharedStorage sharedTxStateStorage;

    /** Scan request executor. */
    private final ExecutorService scanRequestExecutor;

    /**
     * Separate executor for IO operations like partition storage initialization or partition raft group meta data persisting.
     */
    private final ExecutorService ioExecutor;

    private final HybridClock clock;

    private final ClockService clockService;

    private final OutgoingSnapshotsManager outgoingSnapshotsManager;

    private final DistributionZoneManager distributionZoneManager;

    private final SchemaSyncService executorInclinedSchemaSyncService;

    private final CatalogService catalogService;

    /** Incoming RAFT snapshots executor. */
    private final ExecutorService incomingSnapshotsExecutor;

    /** Meta storage listener for pending assignments. */
    private final WatchListener pendingAssignmentsRebalanceListener;

    /** Meta storage listener for stable assignments. */
    private final WatchListener stableAssignmentsRebalanceListener;

    /** Meta storage listener for switch reduce assignments. */
    private final WatchListener assignmentsSwitchRebalanceListener;

    private final MvGc mvGc;

    private final LowWatermark lowWatermark;

    private final HybridTimestampTracker observableTimestampTracker;

    /** Placement driver. */
    private final PlacementDriver executorInclinedPlacementDriver;

    /** A supplier function that returns {@link IgniteSql}. */
    private final Supplier<IgniteSql> sql;

    private final SchemaVersions schemaVersions;

    private final PartitionReplicatorNodeRecovery partitionReplicatorNodeRecovery;

    /** Versioned value used only at manager startup to correctly fire table creation events. */
    private final IncrementalVersionedValue<Void> startVv;

    /** Ends at the {@link IgniteComponent#stopAsync(ComponentContext)} with an {@link NodeStoppingException}. */
    private final CompletableFuture<Void> stopManagerFuture = new CompletableFuture<>();

    /** Configuration for {@link StorageUpdateHandler}. */
    private final StorageUpdateConfiguration storageUpdateConfig;

    /**
     * Executes partition operations (that might cause I/O and/or be blocked on locks).
     */
    private final Executor partitionOperationsExecutor;

    /** Executor for scheduling rebalance routine. */
    private final ScheduledExecutorService rebalanceScheduler;

    /** Marshallers provider. */
    private final ReflectionMarshallersProvider marshallers = new ReflectionMarshallersProvider();

    /** Index chooser for full state transfer. */
    private final FullStateTransferIndexChooser fullStateTransferIndexChooser;

    private final RemotelyTriggeredResourceRegistry remotelyTriggeredResourceRegistry;

    private final TransactionInflights transactionInflights;

    private final TransactionConfiguration txCfg;

    private final String nodeName;

    private final PartitionReplicaLifecycleManager partitionReplicaLifecycleManager;

    private long implicitTransactionTimeout;

    private int attemptsObtainLock;

    @Nullable
    private ScheduledExecutorService streamerFlushExecutor;

    private final IndexMetaStorage indexMetaStorage;

    private final Predicate<Assignment> isLocalNodeAssignment = assignment -> assignment.consistentId().equals(localNode().name());

    @Nullable
    private StreamerReceiverRunner streamerReceiverRunner;

    /**
     * Creates a new table manager.
     *
     * @param nodeName Node name.
     * @param registry Registry for versioned values.
     * @param gcConfig Garbage collector configuration.
     * @param txCfg Transaction configuration.
     * @param storageUpdateConfig Storage update handler configuration.
     * @param replicaMgr Replica manager.
     * @param lockMgr Lock manager.
     * @param replicaSvc Replica service.
     * @param txManager Transaction manager.
     * @param dataStorageMgr Data storage manager.
     * @param schemaManager Schema manager.
     * @param ioExecutor Separate executor for IO operations like partition storage initialization or partition raft group meta data
     *     persisting.
     * @param partitionOperationsExecutor Striped executor on which partition operations (potentially requiring I/O with storages)
     *     will be executed.
     * @param rebalanceScheduler Executor for scheduling rebalance routine.
     * @param placementDriver Placement driver.
     * @param sql A supplier function that returns {@link IgniteSql}.
     * @param lowWatermark Low watermark.
     * @param transactionInflights Transaction inflights.
     * @param indexMetaStorage Index meta storage.
     * @param partitionReplicaLifecycleManager Partition replica lifecycle manager.
     */
    public TableManager(
            String nodeName,
            Consumer<LongFunction<CompletableFuture<?>>> registry,
            GcConfiguration gcConfig,
            TransactionConfiguration txCfg,
            StorageUpdateConfiguration storageUpdateConfig,
            MessagingService messagingService,
            TopologyService topologyService,
            MessageSerializationRegistry messageSerializationRegistry,
            ReplicaManager replicaMgr,
            LockManager lockMgr,
            ReplicaService replicaSvc,
            TxManager txManager,
            DataStorageManager dataStorageMgr,
            Path storagePath,
            MetaStorageManager metaStorageMgr,
            SchemaManager schemaManager,
            ExecutorService ioExecutor,
            Executor partitionOperationsExecutor,
            ScheduledExecutorService rebalanceScheduler,
            HybridClock clock,
            ClockService clockService,
            OutgoingSnapshotsManager outgoingSnapshotsManager,
            DistributionZoneManager distributionZoneManager,
            SchemaSyncService schemaSyncService,
            CatalogService catalogService,
            HybridTimestampTracker observableTimestampTracker,
            PlacementDriver placementDriver,
            Supplier<IgniteSql> sql,
            RemotelyTriggeredResourceRegistry remotelyTriggeredResourceRegistry,
            LowWatermark lowWatermark,
            TransactionInflights transactionInflights,
            IndexMetaStorage indexMetaStorage,
            LogSyncer logSyncer,
            PartitionReplicaLifecycleManager partitionReplicaLifecycleManager
    ) {
        this.topologyService = topologyService;
        this.replicaMgr = replicaMgr;
        this.lockMgr = lockMgr;
        this.replicaSvc = replicaSvc;
        this.txManager = txManager;
        this.dataStorageMgr = dataStorageMgr;
        this.metaStorageMgr = metaStorageMgr;
        this.schemaManager = schemaManager;
        this.ioExecutor = ioExecutor;
        this.partitionOperationsExecutor = partitionOperationsExecutor;
        this.rebalanceScheduler = rebalanceScheduler;
        this.clock = clock;
        this.clockService = clockService;
        this.outgoingSnapshotsManager = outgoingSnapshotsManager;
        this.distributionZoneManager = distributionZoneManager;
        this.catalogService = catalogService;
        this.observableTimestampTracker = observableTimestampTracker;
        this.sql = sql;
        this.storageUpdateConfig = storageUpdateConfig;
        this.remotelyTriggeredResourceRegistry = remotelyTriggeredResourceRegistry;
        this.lowWatermark = lowWatermark;
        this.transactionInflights = transactionInflights;
        this.txCfg = txCfg;
        this.nodeName = nodeName;
        this.indexMetaStorage = indexMetaStorage;
        this.partitionReplicaLifecycleManager = partitionReplicaLifecycleManager;

        this.executorInclinedSchemaSyncService = new ExecutorInclinedSchemaSyncService(schemaSyncService, partitionOperationsExecutor);
        this.executorInclinedPlacementDriver = new ExecutorInclinedPlacementDriver(placementDriver, partitionOperationsExecutor);

        TxMessageSender txMessageSender = new TxMessageSender(
                messagingService,
                replicaSvc,
                clockService,
                txCfg
        );

        transactionStateResolver = new TransactionStateResolver(
                txManager,
                clockService,
                topologyService,
                messagingService,
                executorInclinedPlacementDriver,
                txMessageSender
        );

        schemaVersions = new SchemaVersionsImpl(executorInclinedSchemaSyncService, catalogService, clockService);

        tablesVv = new IncrementalVersionedValue<>(registry);

        localPartitionsVv = new IncrementalVersionedValue<>(dependingOn(tablesVv));

        assignmentsUpdatedVv = new IncrementalVersionedValue<>(dependingOn(localPartitionsVv));

        txStateStorageScheduledPool = Executors.newSingleThreadScheduledExecutor(
                NamedThreadFactory.create(nodeName, "tx-state-storage-scheduled-pool", LOG));

        txStateStoragePool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(),
                NamedThreadFactory.create(nodeName, "tx-state-storage-pool", LOG));

        scanRequestExecutor = Executors.newSingleThreadExecutor(
                IgniteThreadFactory.create(nodeName, "scan-query-executor", LOG, STORAGE_READ));

        int cpus = Runtime.getRuntime().availableProcessors();

        incomingSnapshotsExecutor = new ThreadPoolExecutor(
                cpus,
                cpus,
                100,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                IgniteThreadFactory.create(nodeName, "incoming-raft-snapshot", LOG, STORAGE_READ, STORAGE_WRITE)
        );

        pendingAssignmentsRebalanceListener = createPendingAssignmentsRebalanceListener();

        stableAssignmentsRebalanceListener = createStableAssignmentsRebalanceListener();

        assignmentsSwitchRebalanceListener = createAssignmentsSwitchRebalanceListener();

        mvGc = new MvGc(nodeName, gcConfig, lowWatermark);

        partitionReplicatorNodeRecovery = new PartitionReplicatorNodeRecovery(
                metaStorageMgr,
                messagingService,
                topologyService,
                partitionOperationsExecutor,
                tableId -> tablesById().get(tableId)
        );

        startVv = new IncrementalVersionedValue<>(registry);

        sharedTxStateStorage = new TxStateRocksDbSharedStorage(
                storagePath.resolve(TX_STATE_DIR),
                txStateStorageScheduledPool,
                txStateStoragePool,
                logSyncer,
                TX_STATE_STORAGE_FLUSH_DELAY_SUPPLIER
        );

        fullStateTransferIndexChooser = new FullStateTransferIndexChooser(catalogService, lowWatermark, indexMetaStorage);
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        return inBusyLockAsync(busyLock, () -> {
            mvGc.start();

            transactionStateResolver.start();

            fullStateTransferIndexChooser.start();

            CompletableFuture<Long> recoveryFinishFuture = metaStorageMgr.recoveryFinishedFuture();

            assert recoveryFinishFuture.isDone();

            long recoveryRevision = recoveryFinishFuture.join();

            cleanUpResourcesForDroppedTablesOnRecoveryBusy();

            startTables(recoveryRevision, lowWatermark.getLowWatermark());

            processAssignmentsOnRecovery(recoveryRevision);

            metaStorageMgr.registerPrefixWatch(ByteArray.fromString(PENDING_ASSIGNMENTS_PREFIX), pendingAssignmentsRebalanceListener);
            metaStorageMgr.registerPrefixWatch(ByteArray.fromString(STABLE_ASSIGNMENTS_PREFIX), stableAssignmentsRebalanceListener);
            metaStorageMgr.registerPrefixWatch(ByteArray.fromString(ASSIGNMENTS_SWITCH_REDUCE_PREFIX), assignmentsSwitchRebalanceListener);

            catalogService.listen(CatalogEvent.TABLE_CREATE, parameters -> onTableCreate((CreateTableEventParameters) parameters));
            catalogService.listen(CatalogEvent.TABLE_CREATE, parameters ->
                    prepareTableResourcesAndLoadToZoneReplica((CreateTableEventParameters) parameters));
            catalogService.listen(CatalogEvent.TABLE_DROP, fromConsumer(this::onTableDrop));
            catalogService.listen(CatalogEvent.TABLE_ALTER, parameters -> {
                if (parameters instanceof RenameTableEventParameters) {
                    return onTableRename((RenameTableEventParameters) parameters).thenApply(unused -> false);
                } else {
                    return falseCompletedFuture();
                }
            });

            lowWatermark.listen(
                    LowWatermarkEvent.LOW_WATERMARK_CHANGED,
                    parameters -> onLwmChanged((ChangeLowWatermarkEventParameters) parameters)
            );

            partitionReplicatorNodeRecovery.start();

            implicitTransactionTimeout = txCfg.implicitTransactionTimeout().value();
            attemptsObtainLock = txCfg.attemptsObtainLock().value();

            executorInclinedPlacementDriver.listen(PrimaryReplicaEvent.PRIMARY_REPLICA_EXPIRED, this::onPrimaryReplicaExpired);

            return nullCompletedFuture();
        });
    }

    private CompletableFuture<Void> waitForMetadataCompleteness(HybridTimestamp ts) {
        return executorInclinedSchemaSyncService.waitForMetadataCompleteness(ts);
    }

    private CompletableFuture<Boolean> prepareTableResourcesAndLoadToZoneReplica(CreateTableEventParameters parameters) {
        if (!PartitionReplicaLifecycleManager.ENABLED) {
            return completedFuture(false);
        }

        long causalityToken = parameters.causalityToken();
        CatalogTableDescriptor tableDescriptor = parameters.tableDescriptor();
        CatalogZoneDescriptor zoneDescriptor = getZoneDescriptor(tableDescriptor, parameters.catalogVersion());

        TableImpl table =  createTableImpl(causalityToken, tableDescriptor, zoneDescriptor);

        tablesVv.update(causalityToken, (ignore, e) -> inBusyLock(busyLock, () -> {
            if (e != null) {
                return failedFuture(e);
            }

            return schemaManager.schemaRegistry(causalityToken, parameters.tableId()).thenAccept(table::schemaView);
        }));

        // NB: all vv.update() calls must be made from the synchronous part of the method (not in thenCompose()/etc!).
        CompletableFuture<?> localPartsUpdateFuture = localPartitionsVv.update(causalityToken,
                (ignore, throwable) -> inBusyLock(busyLock, () -> nullCompletedFuture().thenComposeAsync((ignored) -> {
                    PartitionSet parts = new BitSetPartitionSet();

                    for (int i = 0; i < zoneDescriptor.partitions(); i++) {
                        if (partitionReplicaLifecycleManager.hasLocalPartition(new ZonePartitionId(zoneDescriptor.id(), i))) {
                            parts.set(i);
                        }
                    }

                    return getOrCreatePartitionStorages(table, parts).thenAccept(u -> localPartsByTableId.put(parameters.tableId(), parts));
                }, ioExecutor)));

        CompletableFuture<?> tablesByIdFuture = tablesVv.get(causalityToken);

        CompletableFuture<?> createPartsFut = assignmentsUpdatedVv.update(causalityToken, (token, e) -> {
            if (e != null) {
                return failedFuture(e);
            }

            return allOf(localPartsUpdateFuture, tablesByIdFuture).thenComposeAsync(ignore -> inBusyLock(busyLock, () -> {
                        var startPartsFut = new ArrayList<CompletableFuture<?>>();

                        for (int i = 0; i < zoneDescriptor.partitions(); i++) {
                            if (partitionReplicaLifecycleManager.hasLocalPartition(new ZonePartitionId(zoneDescriptor.id(), i))) {
                                startPartsFut.add(preparePartitionResourcesAndLoadToZoneReplica(
                                        table,
                                        i,
                                        zoneDescriptor.id()));
                            }
                        }

                        return allOf(startPartsFut.toArray(CompletableFuture[]::new));
                    }
            ), ioExecutor);
        });

        tables.put(parameters.tableId(), table);

        // TODO: https://issues.apache.org/jira/browse/IGNITE-19913 Possible performance degradation.
        return createPartsFut.thenAccept(ignore -> startedTables.put(parameters.tableId(), table))
                .thenApply(unused -> false);

    }

    /**
     * Prepare the table partition resources and load it to the zone-based replica.
     *
     * @param table Table.
     * @param partId Partition id.
     * @param zoneId Zone id.
     * @return Future, which will complete when the table processor loaded to the zone replica.
     */
    private CompletableFuture<Void> preparePartitionResourcesAndLoadToZoneReplica(
            TableImpl table,
            int partId,
            int zoneId
    ) {
        int tableId = table.tableId();

        var internalTbl = (InternalTableImpl) table.internalTable();

        TablePartitionId replicaGrpId = new TablePartitionId(tableId, partId);

        return inBusyLockAsync(busyLock, () -> {
            var safeTimeTracker = new PendingComparableValuesTracker<HybridTimestamp, Void>(HybridTimestamp.MIN_VALUE);

            var storageIndexTracker = new PendingComparableValuesTracker<Long, Void>(0L);

            PartitionStorages partitionStorages = getPartitionStorages(table, partId);

            PartitionDataStorage partitionDataStorage = partitionDataStorage(partitionStorages.getMvPartitionStorage(),
                    internalTbl, partId);

            storageIndexTracker.update(partitionDataStorage.lastAppliedIndex(), null);

            PartitionUpdateHandlers partitionUpdateHandlers = createPartitionUpdateHandlers(
                    partId,
                    partitionDataStorage,
                    table,
                    safeTimeTracker,
                    storageUpdateConfig
            );

            internalTbl.updatePartitionTrackers(partId, safeTimeTracker, storageIndexTracker);

            mvGc.addStorage(replicaGrpId, partitionUpdateHandlers.gcUpdateHandler);

            Function<RaftCommandRunner, ReplicaListener> createListener = (raftClient) -> createReplicaListener(
                    replicaGrpId,
                    table,
                    safeTimeTracker,
                    partitionStorages.getMvPartitionStorage(),
                    partitionStorages.getTxStateStorage(),
                    partitionUpdateHandlers,
                    raftClient);

            return replicaMgr.replica(new ZonePartitionId(zoneId, partId))
                    .thenAcceptAsync(zoneReplica ->
                            ((ZonePartitionReplicaListener) zoneReplica.listener()).addTableReplicaListener(
                                    new TablePartitionId(tableId, partId), createListener
                            ), ioExecutor
                    );
        });
    }


    private CompletableFuture<Boolean> onPrimaryReplicaExpired(PrimaryReplicaEventParameters parameters) {
        if (topologyService.localMember().id().equals(parameters.leaseholderId())) {
            TablePartitionId groupId = (TablePartitionId) parameters.groupId();

            replicaMgr.weakStopReplica(
                    groupId,
                    WeakReplicaStopReason.PRIMARY_EXPIRED,
                    () -> stopAndDestroyPartition(groupId, tablesVv.latestCausalityToken())
            );
        }

        return falseCompletedFuture();
    }

    private void processAssignmentsOnRecovery(long recoveryRevision) {
        var stableAssignmentsPrefix = new ByteArray(STABLE_ASSIGNMENTS_PREFIX);
        var pendingAssignmentsPrefix = new ByteArray(PENDING_ASSIGNMENTS_PREFIX);

        startVv.update(recoveryRevision, (v, e) -> handleAssignmentsOnRecovery(
                stableAssignmentsPrefix,
                recoveryRevision,
                (entry, rev) ->  handleChangeStableAssignmentEvent(entry, rev, true),
                "stable"
        ));
        startVv.update(recoveryRevision, (v, e) -> handleAssignmentsOnRecovery(
                pendingAssignmentsPrefix,
                recoveryRevision,
                (entry, rev) -> handleChangePendingAssignmentEvent(entry, rev, true),
                "pending"
        ));
    }

    private CompletableFuture<Void> handleAssignmentsOnRecovery(
            ByteArray prefix,
            long revision,
            BiFunction<Entry, Long, CompletableFuture<Void>> assignmentsEventHandler,
            String assignmentsType
    ) {
        try (Cursor<Entry> cursor = metaStorageMgr.prefixLocally(prefix, revision)) {
            CompletableFuture<?>[] futures = cursor.stream()
                    .map(entry -> {
                        if (LOG.isInfoEnabled()) {
                            LOG.info(
                                    "Non handled {} assignments for key '{}' discovered, performing recovery",
                                    assignmentsType,
                                    new String(entry.key(), UTF_8)
                            );
                        }

                        // We use the Meta Storage recovery revision here instead of the entry revision, because
                        // 'handleChangePendingAssignmentEvent' accesses some Versioned Values that only store values starting with
                        // tokens equal to Meta Storage recovery revision. In other words, if the entry has a lower revision than the
                        // recovery revision, there will never be a Versioned Value corresponding to its revision.
                        return assignmentsEventHandler.apply(entry, revision);
                    })
                    .toArray(CompletableFuture[]::new);

            return allOf(futures)
                    // Simply log any errors, we don't want to block watch processing.
                    .exceptionally(e -> {
                        LOG.error("Error when performing assignments recovery", e);

                        return null;
                    });
        }
    }

    private CompletableFuture<Boolean> onTableCreate(CreateTableEventParameters parameters) {
        return createTableLocally(parameters.causalityToken(), parameters.catalogVersion(), parameters.tableDescriptor(), false)
                .thenApply(unused -> false);
    }

    /**
     * Writes the set of assignments to meta storage. If there are some assignments already, gets them from meta storage. Returns
     * the list of assignments that really are in meta storage.
     *
     * @param tableId  Table id.
     * @param assignmentsFuture Assignments future, to get the assignments that should be written.
     * @return Real list of assignments.
     */
    public CompletableFuture<List<Assignments>> writeTableAssignmentsToMetastore(
            int tableId,
            CompletableFuture<List<Assignments>> assignmentsFuture
    ) {
        return assignmentsFuture.thenCompose(newAssignments -> {
            assert !newAssignments.isEmpty();

            List<Operation> partitionAssignments = new ArrayList<>(newAssignments.size());

            for (int i = 0; i < newAssignments.size(); i++) {
                ByteArray stableAssignmentsKey = stablePartAssignmentsKey(new TablePartitionId(tableId, i));
                byte[] anAssignment = newAssignments.get(i).toBytes();
                Operation op = put(stableAssignmentsKey, anAssignment);
                partitionAssignments.add(op);
            }

            Condition condition = notExists(new ByteArray(toByteArray(partitionAssignments.get(0).key())));

            return metaStorageMgr
                    .invoke(condition, partitionAssignments, Collections.emptyList())
                    .handle((invokeResult, e) -> {
                        if (e != null) {
                            LOG.error(
                                    "Couldn't write assignments [assignmentsList={}] to metastore during invoke.",
                                    e,
                                    Assignments.assignmentListToString(newAssignments)
                            );

                            throw ExceptionUtils.sneakyThrow(e);
                        }

                        return invokeResult;
                    })
                    .thenCompose(invokeResult -> {
                        if (invokeResult) {
                            LOG.info(
                                    "Assignments calculated from data nodes are successfully written to meta storage"
                                            + " [tableId={}, assignments={}].",
                                    tableId,
                                    Assignments.assignmentListToString(newAssignments)
                            );

                            return completedFuture(newAssignments);
                        } else {
                            Set<ByteArray> partKeys = IntStream.range(0, newAssignments.size())
                                    .mapToObj(p -> stablePartAssignmentsKey(new TablePartitionId(tableId, p)))
                                    .collect(toSet());

                            CompletableFuture<Map<ByteArray, Entry>> resFuture = metaStorageMgr.getAll(partKeys);

                            return resFuture.thenApply(metaStorageAssignments -> {
                                List<Assignments> realAssignments = new ArrayList<>();

                                for (int p = 0; p < newAssignments.size(); p++) {
                                    var partId = new TablePartitionId(tableId, p);
                                    Entry assignmentsEntry = metaStorageAssignments.get(stablePartAssignmentsKey(partId));

                                    assert assignmentsEntry != null && !assignmentsEntry.empty() && !assignmentsEntry.tombstone()
                                            : "Unexpected assignments for partition [" + partId + ", entry=" + assignmentsEntry + "].";

                                    Assignments real = Assignments.fromBytes(assignmentsEntry.value());

                                    realAssignments.add(real);
                                }

                                LOG.info(
                                        "Assignments picked up from meta storage [tableId={}, assignments={}].",
                                        tableId,
                                        Assignments.assignmentListToString(realAssignments)
                                );

                                return realAssignments;
                            });
                        }
                    })
                    .handle((realAssignments, e) -> {
                        if (e != null) {
                            LOG.error("Couldn't get assignments from metastore for table [tableId={}].", e, tableId);

                            throw ExceptionUtils.sneakyThrow(e);
                        }

                        return realAssignments;
                    });
        });
    }

    private void onTableDrop(DropTableEventParameters parameters) {
        inBusyLock(busyLock, () -> {
            destructionEventsQueue.enqueue(new DestroyTableEvent(parameters.catalogVersion(), parameters.tableId()));
        });
    }

    private CompletableFuture<Boolean> onLwmChanged(ChangeLowWatermarkEventParameters parameters) {
        return inBusyLockAsync(busyLock, () -> {
            int newEarliestCatalogVersion = catalogService.activeCatalogVersion(parameters.newLowWatermark().longValue());

            List<CompletableFuture<Void>> futures = destructionEventsQueue.drainUpTo(newEarliestCatalogVersion).stream()
                    .map(event -> destroyTableLocally(event.tableId()))
                    .collect(toList());

            return allOf(futures.toArray(CompletableFuture[]::new)).thenApply(unused -> false);
        });
    }

    private CompletableFuture<?> onTableRename(RenameTableEventParameters parameters) {
        return inBusyLockAsync(busyLock, () -> tablesVv.update(
                parameters.causalityToken(),
                (ignore, e) -> {
                    if (e != null) {
                        return failedFuture(e);
                    }

                    TableImpl table = tables.get(parameters.tableId());

                    // TODO: revisit this approach, see https://issues.apache.org/jira/browse/IGNITE-21235.
                    table.name(parameters.newTableName());

                    return nullCompletedFuture();
                })
        );
    }

    /**
     * Updates or creates partition raft groups and storages.
     *
     * @param assignmentsFuture Table assignments.
     * @param table Initialized table entity.
     * @param zoneId Zone id.
     * @param isRecovery {@code true} if the node is being started up.
     * @return future, which will be completed when the partitions creations done.
     */
    private CompletableFuture<Void> startLocalPartitionsAndClients(
            CompletableFuture<List<Assignments>> assignmentsFuture,
            TableImpl table,
            int zoneId,
            boolean isRecovery,
            HybridTimestamp timestamp
    ) {
        int tableId = table.tableId();

        // Create new raft nodes according to new assignments.
        return assignmentsFuture.thenCompose(tableAssignments -> {
            // Empty assignments might be a valid case if tables are created from within cluster init HOCON
            // configuration, which is not supported now.
            assert tableAssignments != null : IgniteStringFormatter.format("Table [id={}] has empty assignments.", tableId);

            int partitions = tableAssignments.size();

            var futures = new CompletableFuture<?>[partitions];

            for (int i = 0; i < partitions; i++) {
                int partId = i;

                Assignments assignments = tableAssignments.get(i);

                CompletableFuture<?> future = startPartitionAndStartClient(
                        table,
                        partId,
                        localMemberAssignment(assignments),
                        assignments,
                        zoneId,
                        isRecovery,
                        timestamp
                )
                        .whenComplete((res, ex) -> {
                            if (ex != null) {
                                LOG.warn("Unable to update raft groups on the node [tableId={}, partitionId={}]", ex, tableId, partId);
                            }
                        });

                futures[i] = future;
            }

            return allOf(futures);
        });
    }

    private CompletableFuture<Void> startPartitionAndStartClient(
            TableImpl table,
            int partId,
            @Nullable Assignment localMemberAssignment,
            Assignments stableAssignments,
            int zoneId,
            boolean isRecovery,
            HybridTimestamp timestamp
    ) {
        if (localMemberAssignment == null) {
            // (0) in case if node not in the assignments
            return nullCompletedFuture();
        }

        int tableId = table.tableId();

        var internalTbl = (InternalTableImpl) table.internalTable();

        PeersAndLearners stablePeersAndLearners = fromAssignments(stableAssignments.nodes());

        TablePartitionId replicaGrpId = new TablePartitionId(tableId, partId);

        CompletableFuture<Boolean> shouldStartGroupFut = isRecovery
                ? partitionReplicatorNodeRecovery.initiateGroupReentryIfNeeded(
                        replicaGrpId,
                        internalTbl,
                        stablePeersAndLearners,
                        localMemberAssignment,
                        timestamp
                )
                : trueCompletedFuture();

        Assignments forcedAssignments = stableAssignments.force() ? stableAssignments : null;

        Supplier<CompletableFuture<Boolean>> startReplicaSupplier = () -> shouldStartGroupFut
                .thenComposeAsync(startGroup -> inBusyLock(busyLock, () -> {
                    // (1) if partitionReplicatorNodeRecovery#shouldStartGroup fails -> do start nothing
                    if (!startGroup) {
                        return falseCompletedFuture();
                    }

                    // (2) Otherwise let's start replica manually
                    var safeTimeTracker = new PendingComparableValuesTracker<HybridTimestamp, Void>(HybridTimestamp.MIN_VALUE);

                    var storageIndexTracker = new PendingComparableValuesTracker<Long, Void>(0L);

                    PartitionStorages partitionStorages = getPartitionStorages(table, partId);

                    PartitionDataStorage partitionDataStorage = partitionDataStorage(partitionStorages.getMvPartitionStorage(),
                            internalTbl, partId);

                    storageIndexTracker.update(partitionDataStorage.lastAppliedIndex(), null);

                    PartitionUpdateHandlers partitionUpdateHandlers = createPartitionUpdateHandlers(
                            partId,
                            partitionDataStorage,
                            table,
                            safeTimeTracker,
                            storageUpdateConfig
                    );

                    internalTbl.updatePartitionTrackers(partId, safeTimeTracker, storageIndexTracker);

                    mvGc.addStorage(replicaGrpId, partitionUpdateHandlers.gcUpdateHandler);

                    RaftGroupListener raftGroupListener = new PartitionListener(
                            txManager,
                            partitionDataStorage,
                            partitionUpdateHandlers.storageUpdateHandler,
                            partitionStorages.getTxStateStorage(),
                            safeTimeTracker,
                            storageIndexTracker,
                            catalogService,
                            table.schemaView(),
                            clockService,
                            indexMetaStorage
                    );

                    SnapshotStorageFactory snapshotStorageFactory = createSnapshotStorageFactory(replicaGrpId,
                            partitionUpdateHandlers, internalTbl);

                    Function<RaftGroupService, ReplicaListener> createListener = (raftClient) -> createReplicaListener(
                            replicaGrpId,
                            table,
                            safeTimeTracker,
                            partitionStorages.getMvPartitionStorage(),
                            partitionStorages.getTxStateStorage(),
                            partitionUpdateHandlers,
                            raftClient);

                    RaftGroupEventsListener raftGroupEventsListener = createRaftGroupEventsListener(replicaGrpId);

                    MvTableStorage mvTableStorage = internalTbl.storage();

                    try {
                        return replicaMgr.startReplica(
                                raftGroupEventsListener,
                                raftGroupListener,
                                mvTableStorage.isVolatile(),
                                snapshotStorageFactory,
                                createListener,
                                storageIndexTracker,
                                replicaGrpId,
                                stablePeersAndLearners).thenApply(ignored -> true);
                    } catch (NodeStoppingException e) {
                        throw new AssertionError("Loza was stopped before Table manager", e);
                    }
                }), ioExecutor);

        return replicaMgr.weakStartReplica(
                replicaGrpId,
                startReplicaSupplier,
                forcedAssignments
        ).handle((res, ex) -> {
            if (ex != null) {
                LOG.warn("Unable to update raft groups on the node [tableId={}, partitionId={}]", ex, tableId, partId);
            }
            return null;
        });
    }

    @Nullable
    private Assignment localMemberAssignment(Assignments assignments) {
        Assignment localMemberAssignment = Assignment.forPeer(localNode().name());

        return assignments.nodes().contains(localMemberAssignment) ? localMemberAssignment : null;
    }

    private PartitionMover createPartitionMover(TablePartitionId replicaGrpId) {
        return new PartitionMover(busyLock, () -> {
            CompletableFuture<Replica> replicaFut = replicaMgr.replica(replicaGrpId);
            if (replicaFut == null) {
                return failedFuture(new IgniteInternalException("No such replica for partition " + replicaGrpId.partitionId()
                        + " in table " + replicaGrpId.tableId()));
            }
            return replicaFut.thenApply(Replica::raftClient);
        });
    }

    private RaftGroupEventsListener createRaftGroupEventsListener(TablePartitionId replicaGrpId) {
        PartitionMover partitionMover = createPartitionMover(replicaGrpId);

        return new RebalanceRaftGroupEventsListener(
                metaStorageMgr,
                replicaGrpId,
                busyLock,
                partitionMover,
                this::calculateAssignments,
                rebalanceScheduler
        );
    }

    private PartitionReplicaListener createReplicaListener(
            TablePartitionId tablePartitionId,
            TableImpl table,
            PendingComparableValuesTracker<HybridTimestamp, Void> safeTimeTracker,
            MvPartitionStorage mvPartitionStorage,
            TxStateStorage txStatePartitionStorage,
            PartitionUpdateHandlers partitionUpdateHandlers,
            RaftCommandRunner raftClient
    ) {
        int tableId = tablePartitionId.tableId();
        int partId = tablePartitionId.partitionId();

        return new PartitionReplicaListener(
                mvPartitionStorage,
                new ExecutorInclinedRaftCommandRunner(raftClient, partitionOperationsExecutor),
                txManager,
                lockMgr,
                scanRequestExecutor,
                partId,
                tableId,
                table.indexesLockers(partId),
                new Lazy<>(() -> table.indexStorageAdapters(partId).get().get(table.pkId())),
                () -> table.indexStorageAdapters(partId).get(),
                clockService,
                safeTimeTracker,
                txStatePartitionStorage,
                transactionStateResolver,
                partitionUpdateHandlers.storageUpdateHandler,
                new CatalogValidationSchemasSource(catalogService, schemaManager),
                localNode(),
                executorInclinedSchemaSyncService,
                catalogService,
                executorInclinedPlacementDriver,
                topologyService,
                remotelyTriggeredResourceRegistry,
                schemaManager.schemaRegistry(tableId),
                indexMetaStorage
        );
    }

    private CompletableFuture<Set<Assignment>> calculateAssignments(
            TablePartitionId tablePartitionId,
            HybridTimestamp assignmentsTimestamp
    ) {
        return waitForMetadataCompleteness(assignmentsTimestamp).thenCompose(unused -> {
            int catalogVersion = catalogService.activeCatalogVersion(assignmentsTimestamp.longValue());

            CatalogTableDescriptor tableDescriptor = getTableDescriptor(tablePartitionId.tableId(), catalogVersion);

            CatalogZoneDescriptor zoneDescriptor = getZoneDescriptor(tableDescriptor, catalogVersion);

            return distributionZoneManager.dataNodes(
                    zoneDescriptor.updateToken(),
                    catalogVersion,
                    tableDescriptor.zoneId()
            ).thenApply(dataNodes ->
                    AffinityUtils.calculateAssignmentForPartition(
                            dataNodes,
                            tablePartitionId.partitionId(),
                            zoneDescriptor.replicas()
                    )
            );
        });
    }

    private boolean isLocalPeer(Peer peer) {
        return peer.consistentId().equals(localNode().name());
    }

    private boolean isLocalNodeInAssignments(Collection<Assignment> assignments) {
        return assignments.stream().anyMatch(isLocalNodeAssignment);
    }

    /**
     * Checks that the local node is primary or not.
     * <br>
     * Internally we use there {@link PlacementDriver#getPrimaryReplica} with a penultimate
     * safe time value, because metastore is waiting for pending or stable assignments events handling over and only then metastore will
     * increment the safe time. On the other hand placement driver internally is waiting the metastore for given safe time plus
     * {@link ClockService#maxClockSkewMillis}. So, if given time is just {@link ClockService#now}, then there is a dead lock: metastore
     * is waiting until assignments handling is over, but internally placement driver is waiting for a non-applied safe time.
     * <br>
     * To solve this issue we pass to {@link PlacementDriver#getPrimaryReplica} current time minus the skew, so placement driver could
     * successfully get primary replica for the time stamp before the handling has began. Also there a corner case for tests that are using
     * {@code WatchListenerInhibitor#metastorageEventsInhibitor} and it leads to current time equals {@link HybridTimestamp#MIN_VALUE} and
     * the skew's subtraction will lead to {@link IllegalArgumentException} from {@link HybridTimestamp}. Then, if we got the minimal
     * possible timestamp, then we also couldn't have any primary replica, then return {@code false}.
     *
     * @param replicationGroupId Replication group ID for that we check is the local node a primary.
     * @return {@code true} is the local node is primary and {@code false} otherwise.
     */
    private CompletableFuture<Boolean> isLocalNodeIsPrimary(ReplicationGroupId replicationGroupId) {
        HybridTimestamp currentSafeTime = metaStorageMgr.clusterTime().currentSafeTime();

        if (HybridTimestamp.MIN_VALUE.equals(currentSafeTime)) {
            return falseCompletedFuture();
        }

        long skewMs = clockService.maxClockSkewMillis();

        try {
            HybridTimestamp previousMetastoreSafeTime = currentSafeTime.subtractPhysicalTime(skewMs);

            return executorInclinedPlacementDriver.getPrimaryReplica(replicationGroupId, previousMetastoreSafeTime)
                    .thenApply(replicaMeta -> replicaMeta != null
                            && replicaMeta.getLeaseholderId() != null
                            && replicaMeta.getLeaseholderId().equals(localNode().id()));
        } catch (IllegalArgumentException e) {
            long currentSafeTimeMs = currentSafeTime.longValue();

            throw new AssertionError("Got a negative time [currentSafeTime=" + currentSafeTime
                    + ", currentSafeTimeMs=" + currentSafeTimeMs
                    + ", skewMs=" + skewMs
                    + ", internal=" + (currentSafeTimeMs + ((-skewMs) << LOGICAL_TIME_BITS_SIZE)) + "]", e);
        }
    }

    private PartitionDataStorage partitionDataStorage(MvPartitionStorage partitionStorage, InternalTable internalTbl, int partId) {
        return new SnapshotAwarePartitionDataStorage(
                partitionStorage,
                outgoingSnapshotsManager,
                partitionKey(internalTbl, partId)
        );
    }

    private static PartitionKey partitionKey(InternalTable internalTbl, int partId) {
        return new PartitionKey(internalTbl.tableId(), partId);
    }

    @Override
    public void beforeNodeStop() {
        if (!beforeStopGuard.compareAndSet(false, true)) {
            return;
        }

        stopManagerFuture.completeExceptionally(new NodeStoppingException());

        busyLock.block();

        metaStorageMgr.unregisterWatch(pendingAssignmentsRebalanceListener);
        metaStorageMgr.unregisterWatch(stableAssignmentsRebalanceListener);
        metaStorageMgr.unregisterWatch(assignmentsSwitchRebalanceListener);

        cleanUpTablesResources(tables);
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        // NB: busy lock had already gotten in {@link beforeNodeStop}
        assert beforeStopGuard.get() : "'stop' called before 'beforeNodeStop'";

        if (!stopGuard.compareAndSet(false, true)) {
            return nullCompletedFuture();
        }

        int shutdownTimeoutSeconds = 10;

        try {
            IgniteUtils.closeAllManually(
                    mvGc,
                    fullStateTransferIndexChooser,
                    sharedTxStateStorage,
                    () -> shutdownAndAwaitTermination(txStateStoragePool, shutdownTimeoutSeconds, TimeUnit.SECONDS),
                    () -> shutdownAndAwaitTermination(txStateStorageScheduledPool, shutdownTimeoutSeconds, TimeUnit.SECONDS),
                    () -> shutdownAndAwaitTermination(scanRequestExecutor, shutdownTimeoutSeconds, TimeUnit.SECONDS),
                    () -> shutdownAndAwaitTermination(incomingSnapshotsExecutor, shutdownTimeoutSeconds, TimeUnit.SECONDS),
                    () -> shutdownAndAwaitTermination(rebalanceScheduler, shutdownTimeoutSeconds, TimeUnit.SECONDS),
                    () -> shutdownAndAwaitTermination(streamerFlushExecutor, shutdownTimeoutSeconds, TimeUnit.SECONDS)
            );
        } catch (Exception e) {
            return failedFuture(e);
        }

        return nullCompletedFuture();
    }

    /**
     * Stops resources that are related to provided tables.
     *
     * @param tables Tables to stop.
     */
    private void cleanUpTablesResources(Map<Integer, TableImpl> tables) {
        var futures = new ArrayList<CompletableFuture<Void>>(tables.size());

        for (TableImpl table : tables.values()) {
            futures.add(runAsync(() -> {
                Stream.Builder<ManuallyCloseable> stopping = Stream.builder();

                InternalTable internalTable = table.internalTable();

                stopping.add(() -> {
                    var stopReplicaFutures = new CompletableFuture<?>[internalTable.partitions()];

                    for (int p = 0; p < internalTable.partitions(); p++) {
                        TablePartitionId replicationGroupId = new TablePartitionId(table.tableId(), p);

                        stopReplicaFutures[p] = stopPartition(replicationGroupId, table);
                    }

                    allOf(stopReplicaFutures).get(10, TimeUnit.SECONDS);
                });

                stopping.add(internalTable.storage());
                stopping.add(internalTable.txStateStorage());
                stopping.add(internalTable);

                try {
                    IgniteUtils.closeAllManually(stopping.build());
                } catch (Throwable t) {
                    LOG.error("Unable to stop table [name={}, tableId={}]", t, table.name(), table.tableId());
                }
            }, ioExecutor));
        }

        try {
            allOf(futures.toArray(CompletableFuture[]::new)).get(30, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            LOG.error("Unable to clean table resources", e);
        }
    }

    /**
     * Create table storages and internal instance.
     *
     * @param causalityToken Causality token.
     * @param tableDescriptor Catalog table descriptor.
     * @param zoneDescriptor Catalog distribution zone descriptor.
     * @return Table instance.
     */
    private TableImpl createTableImpl(
            long causalityToken,
            CatalogTableDescriptor tableDescriptor,
            CatalogZoneDescriptor zoneDescriptor
    ) {
        String tableName = tableDescriptor.name();

        LOG.trace("Creating local table: name={}, id={}, token={}", tableDescriptor.name(), tableDescriptor.id(), causalityToken);

        MvTableStorage tableStorage = createTableStorage(tableDescriptor, zoneDescriptor);
        TxStateTableStorage txStateStorage = createTxStateTableStorage(tableDescriptor, zoneDescriptor);

        int partitions = zoneDescriptor.partitions();

        InternalTableImpl internalTable = new InternalTableImpl(
                tableName,
                tableDescriptor.id(),
                partitions,
                topologyService,
                txManager,
                tableStorage,
                txStateStorage,
                replicaSvc,
                clock,
                observableTimestampTracker,
                executorInclinedPlacementDriver,
                transactionInflights,
                implicitTransactionTimeout,
                attemptsObtainLock,
                this::streamerFlushExecutor,
                Objects.requireNonNull(streamerReceiverRunner)
        );

        return new TableImpl(
                internalTable,
                lockMgr,
                schemaVersions,
                marshallers,
                sql.get(),
                tableDescriptor.primaryKeyIndexId()
        );
    }


    /**
     * Creates local structures for a table.
     *
     * @param causalityToken Causality token.
     * @param catalogVersion Catalog version on which the table was created.
     * @param tableDescriptor Catalog table descriptor.
     * @param onNodeRecovery {@code true} when called during node recovery, {@code false} otherwise.
     * @return Future that will be completed when local changes related to the table creation are applied.
     */
    private CompletableFuture<?> createTableLocally(
            long causalityToken,
            int catalogVersion,
            CatalogTableDescriptor tableDescriptor,
            boolean onNodeRecovery
    ) {
        return inBusyLockAsync(busyLock, () -> {
            int tableId = tableDescriptor.id();

            // Retrieve descriptor during synchronous call, before the previous catalog version could be concurrently compacted.
            CatalogZoneDescriptor zoneDescriptor = getZoneDescriptor(tableDescriptor, catalogVersion);

            CompletableFuture<List<Assignments>> assignmentsFuture = getOrCreateAssignments(
                    tableDescriptor,
                    zoneDescriptor,
                    causalityToken,
                    catalogVersion
            );

            CompletableFuture<List<Assignments>> assignmentsFutureAfterInvoke =
                    writeTableAssignmentsToMetastore(tableId, assignmentsFuture);

            Catalog catalog = catalogService.catalog(catalogVersion);

            return createTableLocally(
                    causalityToken,
                    tableDescriptor,
                    zoneDescriptor,
                    assignmentsFutureAfterInvoke,
                    onNodeRecovery,
                    HybridTimestamp.hybridTimestamp(catalog.time())
            );
        });
    }

    /**
     * Creates local structures for a table.
     *
     * @param causalityToken Causality token.
     * @param tableDescriptor Catalog table descriptor.
     * @param zoneDescriptor Catalog distributed zone descriptor.
     * @param assignmentsFuture Future with assignments.
     * @param onNodeRecovery {@code true} when called during node recovery, {@code false} otherwise.
     * @return Future that will be completed when local changes related to the table creation are applied.
     */
    private CompletableFuture<Void> createTableLocally(
            long causalityToken,
            CatalogTableDescriptor tableDescriptor,
            CatalogZoneDescriptor zoneDescriptor,
            CompletableFuture<List<Assignments>> assignmentsFuture,
            boolean onNodeRecovery,
            HybridTimestamp timestamp
    ) {
        TableImpl table =  createTableImpl(causalityToken, tableDescriptor, zoneDescriptor);

        int tableId = tableDescriptor.id();

        tablesVv.update(causalityToken, (ignore, e) -> inBusyLock(busyLock, () -> {
            if (e != null) {
                return failedFuture(e);
            }

            return schemaManager.schemaRegistry(causalityToken, tableId).thenAccept(table::schemaView);
        }));

        // NB: all vv.update() calls must be made from the synchronous part of the method (not in thenCompose()/etc!).
        CompletableFuture<?> localPartsUpdateFuture = localPartitionsVv.update(causalityToken,
                (ignore, throwable) -> inBusyLock(busyLock, () -> assignmentsFuture.thenComposeAsync(newAssignments -> {
                    PartitionSet parts = new BitSetPartitionSet();

                    // TODO: https://issues.apache.org/jira/browse/IGNITE-19713 Process assignments and set partitions only for
                    // TODO assigned partitions.
                    for (int i = 0; i < newAssignments.size(); i++) {
                        parts.set(i);
                    }

                    return getOrCreatePartitionStorages(table, parts).thenAccept(u -> localPartsByTableId.put(tableId, parts));
                }, ioExecutor)));

        CompletableFuture<?> tablesByIdFuture = tablesVv.get(causalityToken);

        // TODO https://issues.apache.org/jira/browse/IGNITE-19170 Partitions should be started only on the assignments change
        //  event triggered by zone create or alter.
        CompletableFuture<?> createPartsFut = assignmentsUpdatedVv.update(causalityToken, (token, e) -> {
            if (e != null) {
                return failedFuture(e);
            }

            return allOf(localPartsUpdateFuture, tablesByIdFuture).thenComposeAsync(ignore -> inBusyLock(busyLock, () -> {
                        if (onNodeRecovery) {
                            SchemaRegistry schemaRegistry = table.schemaView();
                            PartitionSet partitionSet = localPartsByTableId.get(tableId);
                            // LWM starts updating only after the node is restored.
                            HybridTimestamp lwm = lowWatermark.getLowWatermark();

                            registerIndexesToTable(table, catalogService, partitionSet, schemaRegistry, lwm);
                        }
                        return startLocalPartitionsAndClients(assignmentsFuture, table, zoneDescriptor.id(), onNodeRecovery, timestamp);
                    }
            ), ioExecutor);
        });

        tables.put(tableId, table);

        // TODO should be reworked in IGNITE-16763

        // TODO: https://issues.apache.org/jira/browse/IGNITE-19913 Possible performance degradation.
        return createPartsFut.thenAccept(ignore -> startedTables.put(tableId, table));
    }

    /**
     * Check if the table already has assignments in the meta storage locally.
     * So, it means, that it is a recovery process and we should use the meta storage local assignments instead of calculation
     * of the new ones.
     */
    private CompletableFuture<List<Assignments>> getOrCreateAssignments(
            CatalogTableDescriptor tableDescriptor,
            CatalogZoneDescriptor zoneDescriptor,
            long causalityToken,
            int catalogVersion
    ) {
        int tableId = tableDescriptor.id();
        CompletableFuture<List<Assignments>> assignmentsFuture;

        if (partitionAssignmentsGetLocally(metaStorageMgr, tableId, 0, causalityToken) != null) {
            assignmentsFuture = completedFuture(
                    tableAssignmentsGetLocally(metaStorageMgr, tableId, zoneDescriptor.partitions(), causalityToken));
        } else {

            Catalog catalog = catalogService.catalog(catalogVersion);

            HybridTimestamp timestamp = HybridTimestamp.hybridTimestamp(catalog.time());

            assignmentsFuture = distributionZoneManager.dataNodes(causalityToken, catalogVersion, zoneDescriptor.id())
                    .thenApply(dataNodes -> AffinityUtils.calculateAssignments(
                            dataNodes,
                            zoneDescriptor.partitions(),
                            zoneDescriptor.replicas()
                    ).stream().map(assignments -> Assignments.of(assignments, timestamp)).collect(toList()));

            assignmentsFuture.thenAccept(assignmentsList -> LOG.info(
                    "Assignments calculated from data nodes [table={}, tableId={}, assignments={}, revision={}]",
                    tableDescriptor.name(),
                    tableId,
                    Assignments.assignmentListToString(assignmentsList),
                    causalityToken
            ));
        }

        return assignmentsFuture;
    }

    /**
     * Creates data storage for the provided table.
     *
     * @param tableDescriptor Catalog table descriptor.
     * @param zoneDescriptor Catalog distributed zone descriptor.
     */
    protected MvTableStorage createTableStorage(CatalogTableDescriptor tableDescriptor, CatalogZoneDescriptor zoneDescriptor) {
        StorageEngine engine = dataStorageMgr.engineByStorageProfile(tableDescriptor.storageProfile());

        assert engine != null : "tableId=" + tableDescriptor.id() + ", storageProfile=" + tableDescriptor.storageProfile();

        return engine.createMvTable(
                new StorageTableDescriptor(tableDescriptor.id(), zoneDescriptor.partitions(), tableDescriptor.storageProfile()),
                new CatalogStorageIndexDescriptorSupplier(catalogService, lowWatermark)
        );
    }

    /**
     * Creates transaction state storage for the provided table.
     *
     * @param tableDescriptor Catalog table descriptor.
     * @param zoneDescriptor Catalog distributed zone descriptor.
     */
    protected TxStateTableStorage createTxStateTableStorage(CatalogTableDescriptor tableDescriptor, CatalogZoneDescriptor zoneDescriptor) {
        int tableId = tableDescriptor.id();

        TxStateTableStorage txStateTableStorage = new TxStateRocksDbTableStorage(
                tableId,
                zoneDescriptor.partitions(),
                sharedTxStateStorage
        );
        if (ThreadAssertions.enabled()) {
            txStateTableStorage = new ThreadAssertingTxStateTableStorage(txStateTableStorage);
        }

        txStateTableStorage.start();

        return txStateTableStorage;
    }

    /**
     * Drops local structures for a table.
     *
     * @param tableId Table id to destroy.
     */
    private CompletableFuture<Void> destroyTableLocally(int tableId) {
        TableImpl table = startedTables.remove(tableId);
        localPartsByTableId.remove(tableId);

        assert table != null : tableId;

        InternalTable internalTable = table.internalTable();
        int partitions = internalTable.partitions();

        // TODO https://issues.apache.org/jira/browse/IGNITE-18991 Move assigment manipulations to Distribution zones.
        Set<ByteArray> assignmentKeys = IntStream.range(0, partitions)
                .mapToObj(p -> stablePartAssignmentsKey(new TablePartitionId(tableId, p)))
                .collect(toSet());
        metaStorageMgr.removeAll(assignmentKeys);

        CompletableFuture<?>[] stopReplicaFutures = new CompletableFuture<?>[partitions];

        // TODO https://issues.apache.org/jira/browse/IGNITE-19170 Partitions should be stopped on the assignments change
        //  event triggered by zone drop or alter. Stop replica asynchronously, out of metastorage event pipeline.
        for (int partitionId = 0; partitionId < partitions; partitionId++) {
            var replicationGroupId = new TablePartitionId(tableId, partitionId);

            stopReplicaFutures[partitionId] = stopPartition(replicationGroupId, table);
        }

        // TODO: IGNITE-18703 Destroy raft log and meta
        return allOf(stopReplicaFutures)
                .thenComposeAsync(
                        unused -> inBusyLockAsync(busyLock, () -> allOf(
                                internalTable.storage().destroy(),
                                runAsync(() -> inBusyLock(busyLock, () -> internalTable.txStateStorage().destroy()), ioExecutor)
                        )),
                        ioExecutor)
                .thenAccept(ignore0 -> tables.remove(tableId))
                .thenAcceptAsync(ignore0 -> schemaManager.dropRegistryAsync(tableId), ioExecutor);
    }

    @Override
    public List<Table> tables() {
        return join(tablesAsync());
    }

    @Override
    public CompletableFuture<List<Table>> tablesAsync() {
        return inBusyLockAsync(busyLock, this::tablesAsyncInternalBusy);
    }

    private CompletableFuture<List<Table>> tablesAsyncInternalBusy() {
        HybridTimestamp now = clockService.now();

        return orStopManagerFuture(executorInclinedSchemaSyncService.waitForMetadataCompleteness(now))
                .thenCompose(unused -> inBusyLockAsync(busyLock, () -> {
                    int catalogVersion = catalogService.activeCatalogVersion(now.longValue());

                    Collection<CatalogTableDescriptor> tableDescriptors = catalogService.tables(catalogVersion);

                    if (tableDescriptors.isEmpty()) {
                        return emptyListCompletedFuture();
                    }

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
    private CompletableFuture<Map<Integer, TableImpl>> tablesById(long causalityToken) {
        return assignmentsUpdatedVv.get(causalityToken).thenApply(v -> unmodifiableMap(startedTables));
    }

    /**
     * Returns an internal map, which contains all managed tables by their ID.
     */
    private Map<Integer, TableImpl> tablesById() {
        return unmodifiableMap(tables);
    }

    /**
     * Returns a map with started tables.
     */
    @TestOnly
    public Map<Integer, TableImpl> startedTables() {
        return unmodifiableMap(startedTables);
    }

    @Override
    public Table table(String name) {
        return join(tableAsync(name));
    }

    @Override
    public TableViewInternal table(int id) throws NodeStoppingException {
        return join(tableAsync(id));
    }

    @Override
    public CompletableFuture<Table> tableAsync(String name) {
        return tableAsyncInternal(IgniteNameUtils.parseSimpleName(name))
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
                        int catalogVersion = catalogService.activeCatalogVersion(now.longValue());

                        // Check if the table has been deleted.
                        if (catalogService.table(tableId, catalogVersion) == null) {
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
            throw new IgniteException(new NodeStoppingException());
        }

        try {
            return localPartitionsVv.get(causalityToken).thenApply(unused -> localPartsByTableId.get(tableId));
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public TableViewInternal tableView(String name) {
        return join(tableViewAsync(name));
    }

    @Override
    public CompletableFuture<TableViewInternal> tableViewAsync(String name) {
        return tableAsyncInternal(IgniteNameUtils.parseSimpleName(name));
    }

    /**
     * Gets a table by name, if it was created before. Doesn't parse canonical name.
     *
     * @param name Table name.
     * @return Future representing pending completion of the {@code TableManager#tableAsyncInternal} operation.
     */
    private CompletableFuture<TableViewInternal> tableAsyncInternal(String name) {
        return inBusyLockAsync(busyLock, () -> {
            HybridTimestamp now = clockService.now();

            return orStopManagerFuture(executorInclinedSchemaSyncService.waitForMetadataCompleteness(now))
                    .thenCompose(unused -> inBusyLockAsync(busyLock, () -> {
                        CatalogTableDescriptor tableDescriptor = catalogService.table(name, now.longValue());

                        // Check if the table has been deleted.
                        if (tableDescriptor == null) {
                            return nullCompletedFuture();
                        }

                        return tableAsyncInternalBusy(tableDescriptor.id());
                    }));
        });
    }

    private CompletableFuture<TableViewInternal> tableAsyncInternalBusy(int tableId) {
        TableImpl tableImpl = startedTables.get(tableId);

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

    /**
     * Waits for future result and return, or unwraps {@link CompletionException} to {@link IgniteException} if failed.
     *
     * @param future Completable future.
     * @return Future result.
     */
    private static <T> T join(CompletableFuture<T> future) {
        try {
            return future.join();
        } catch (CompletionException ex) {
            throw convertThrowable(ex.getCause());
        }
    }

    /**
     * Convert to public throwable.
     *
     * @param th Throwable.
     * @return Public throwable.
     */
    private static RuntimeException convertThrowable(Throwable th) {
        if (th instanceof RuntimeException) {
            return (RuntimeException) th;
        }

        return new IgniteException(th);
    }

    /**
     * Creates meta storage listener for pending assignments updates.
     *
     * @return The watch listener.
     */
    private WatchListener createPendingAssignmentsRebalanceListener() {
        return new WatchListener() {
            @Override
            public CompletableFuture<Void> onUpdate(WatchEvent evt) {
                if (!busyLock.enterBusy()) {
                    return failedFuture(new NodeStoppingException());
                }

                try {
                    Entry newEntry = evt.entryEvent().newEntry();

                    return handleChangePendingAssignmentEvent(newEntry, evt.revision(), false);
                } finally {
                    busyLock.leaveBusy();
                }
            }

            @Override
            public void onError(Throwable e) {
                LOG.warn("Unable to process pending assignments event", e);
            }
        };
    }

    private CompletableFuture<Void> handleChangePendingAssignmentEvent(
            Entry pendingAssignmentsEntry,
            long revision,
            boolean isRecovery
    ) {
        if (pendingAssignmentsEntry.value() == null) {
            return nullCompletedFuture();
        }

        int partId = extractPartitionNumber(pendingAssignmentsEntry.key());
        int tblId = extractTableId(pendingAssignmentsEntry.key(), PENDING_ASSIGNMENTS_PREFIX);

        var replicaGrpId = new TablePartitionId(tblId, partId);

        // Stable assignments from the meta store, which revision is bounded by the current pending event.
        Assignments stableAssignments = stableAssignments(replicaGrpId, revision);

        Assignments pendingAssignments = Assignments.fromBytes(pendingAssignmentsEntry.value());

        return tablesVv.get(revision)
                .thenApply(ignore -> {
                    if (!busyLock.enterBusy()) {
                        return CompletableFuture.<Void>failedFuture(new NodeStoppingException());
                    }

                    try {
                        TableImpl table = tables.get(tblId);

                        // Table can be null only recovery, because we use a revision from the future. See comment inside
                        // performRebalanceOnRecovery.
                        if (table == null) {
                            if (LOG.isInfoEnabled()) {
                                LOG.info("Skipping Pending Assignments update, because table {} does not exist", tblId);
                            }

                            return CompletableFutures.<Void>nullCompletedFuture();
                        }

                        if (LOG.isInfoEnabled()) {
                            var stringKey = new String(pendingAssignmentsEntry.key(), UTF_8);

                            LOG.info("Received update on pending assignments. Check if new raft group should be started [key={}, "
                                            + "partition={}, table={}, localMemberAddress={}, pendingAssignments={}, revision={}]",
                                    stringKey, partId, table.name(), localNode().address(), pendingAssignments, revision);
                        }

                        HybridTimestamp activationTimestamp = pendingAssignments.timestamp();

                        return handleChangePendingAssignmentEvent(
                                replicaGrpId,
                                table,
                                stableAssignments,
                                pendingAssignments,
                                revision,
                                isRecovery,
                                activationTimestamp
                        ).thenCompose(v -> {
                            boolean isLocalNodeInStableOrPending = isNodeInReducedStableOrPendingAssignments(
                                    replicaGrpId,
                                    stableAssignments,
                                    pendingAssignments,
                                    revision
                            );

                            if (!isLocalNodeInStableOrPending) {
                                return nullCompletedFuture();
                            }

                            return changePeersOnRebalance(replicaGrpId, pendingAssignments.nodes(), revision);
                        });
                    } finally {
                        busyLock.leaveBusy();
                    }
                })
                .thenCompose(identity());
    }

    private CompletableFuture<Void> handleChangePendingAssignmentEvent(
            TablePartitionId replicaGrpId,
            TableImpl tbl,
            @Nullable Assignments stableAssignments,
            Assignments pendingAssignments,
            long revision,
            boolean isRecovery,
            HybridTimestamp activationTimestamp
    ) {
        boolean pendingAssignmentsAreForced = pendingAssignments.force();
        Set<Assignment> pendingAssignmentsNodes = pendingAssignments.nodes();

        // Start a new Raft node and Replica if this node has appeared in the new assignments.
        Assignment localMemberAssignment = localMemberAssignment(pendingAssignments);

        boolean shouldStartLocalGroupNode = localMemberAssignment != null
                && (stableAssignments == null || !stableAssignments.nodes().contains(localMemberAssignment));

        // This is a set of assignments for nodes that are not the part of stable assignments, i.e. unstable part of the distribution.
        // For regular pending assignments we use (old) stable set, so that none of new nodes would be able to propose itself as a leader.
        // For forced assignments, we should do the same thing, but only for the subset of stable set that is alive right now. Dead nodes
        // are excluded. It is calculated precisely as an intersection between forced assignments and (old) stable assignments.
        Assignments computedStableAssignments;

        // TODO: https://issues.apache.org/jira/browse/IGNITE-22600 remove the second condition
        //  when we will have a proper handling of empty stable assignments
        if (stableAssignments == null || stableAssignments.nodes().isEmpty()) {
            // This condition can only pass if all stable nodes are dead, and we start new raft group from scratch.
            // In this case new initial configuration must match new forced assignments.
            computedStableAssignments = Assignments.forced(pendingAssignmentsNodes, pendingAssignments.timestamp());
        } else if (pendingAssignmentsAreForced) {
            // In case of forced assignments we need to remove nodes that are present in the stable set but are missing from the
            // pending set. Such operation removes dead stable nodes from the resulting stable set, which guarantees that we will
            // have a live majority.
            computedStableAssignments = pendingAssignments;
        } else {
            computedStableAssignments = stableAssignments;
        }

        int partitionId = replicaGrpId.partitionId();

        CompletableFuture<Void> localServicesStartFuture;

        if (shouldStartLocalGroupNode) {
            PartitionSet singlePartitionIdSet = PartitionSet.of(partitionId);

            localServicesStartFuture = localPartitionsVv.get(revision)
                    // TODO https://issues.apache.org/jira/browse/IGNITE-20957 Revisit this code
                    .thenComposeAsync(
                            partitionSet -> inBusyLock(busyLock, () -> getOrCreatePartitionStorages(tbl, singlePartitionIdSet)),
                            ioExecutor
                    )
                    .thenComposeAsync(unused -> inBusyLock(busyLock, () -> {
                        if (!isRecovery) {
                            // We create index storages (and also register the necessary structures) for the rebalancing one partition
                            // before start the raft node, so that the updates that come when applying the replication log can safely
                            // update the indexes. On recovery node, we do not need to call this code, since during restoration we start
                            // all partitions and already register indexes there.
                            lowWatermark.getLowWatermarkSafe(lwm ->
                                    registerIndexesToTable(tbl, catalogService, singlePartitionIdSet, tbl.schemaView(), lwm)
                            );
                        }

                        return waitForMetadataCompleteness(activationTimestamp).thenCompose(ignored -> inBusyLock(busyLock, () -> {
                            int catalogVersion = catalogService.activeCatalogVersion(activationTimestamp.longValue());

                            int zoneId = getTableDescriptor(tbl.tableId(), catalogVersion).zoneId();

                            return startPartitionAndStartClient(
                                    tbl,
                                    replicaGrpId.partitionId(),
                                    localMemberAssignment,
                                    computedStableAssignments,
                                    zoneId,
                                    isRecovery,
                                    activationTimestamp
                            );
                        }));
                    }), ioExecutor);
        } else if (pendingAssignmentsAreForced && localMemberAssignment != null) {
            localServicesStartFuture = runAsync(() -> inBusyLock(busyLock, () -> {
                assert replicaMgr.isReplicaStarted(replicaGrpId) : "The local node is outside of the replication group: " + replicaGrpId;

                replicaMgr.resetPeers(replicaGrpId, fromAssignments(computedStableAssignments.nodes()));
            }), ioExecutor);
        } else {
            localServicesStartFuture = nullCompletedFuture();
        }

        return localServicesStartFuture
                .thenComposeAsync(v -> inBusyLock(busyLock, () -> isLocalNodeIsPrimary(replicaGrpId)), ioExecutor)
                .thenAcceptAsync(isLeaseholder -> inBusyLock(busyLock, () -> {
                    boolean isLocalNodeInStableOrPending = isNodeInReducedStableOrPendingAssignments(
                            replicaGrpId,
                            stableAssignments,
                            pendingAssignments,
                            revision
                    );

                    if (!isLocalNodeInStableOrPending && !isLeaseholder) {
                        return;
                    }

                    assert isLocalNodeInStableOrPending || isLeaseholder
                            : "The local node is outside of the replication group [inStableOrPending=" + isLocalNodeInStableOrPending
                            + ", isLeaseholder=" + isLeaseholder + "].";

                    // For forced assignments, we exclude dead stable nodes, and all alive stable nodes are already in pending assignments.
                    // Union is not required in such a case.
                    Set<Assignment> newAssignments = pendingAssignmentsAreForced || stableAssignments == null
                            ? pendingAssignmentsNodes
                            : union(pendingAssignmentsNodes, stableAssignments.nodes());

                    replicaMgr.replica(replicaGrpId)
                            .thenApply(Replica::raftClient)
                            .thenAccept(raftClient -> raftClient.updateConfiguration(fromAssignments(newAssignments)));
                }), ioExecutor);
    }

    private boolean isNodeInReducedStableOrPendingAssignments(
            TablePartitionId replicaGrpId,
            @Nullable Assignments stableAssignments,
            Assignments pendingAssignments,
            long revision
    ) {
        Entry reduceEntry  = metaStorageMgr.getLocally(RebalanceUtil.switchReduceKey(replicaGrpId), revision);

        Assignments reduceAssignments = reduceEntry != null
                ? Assignments.fromBytes(reduceEntry.value())
                : null;

        Set<Assignment> reducedStableAssignments = reduceAssignments != null
                ? subtract(stableAssignments.nodes(), reduceAssignments.nodes())
                : stableAssignments.nodes();

        if (!isLocalNodeInAssignments(union(reducedStableAssignments, pendingAssignments.nodes()))) {
            return false;
        }

        assert replicaMgr.isReplicaStarted(replicaGrpId) : "The local node is outside of the replication group ["
                + ", stable=" + stableAssignments
                + ", pending=" + pendingAssignments
                + ", reduce=" + reduceAssignments
                + ", localName=" + localNode().name() + "].";

        return true;
    }

    private CompletableFuture<Void> changePeersOnRebalance(
            TablePartitionId replicaGrpId,
            Set<Assignment> pendingAssignments,
            long revision
    ) {
        return replicaMgr.replica(replicaGrpId)
                .thenApply(Replica::raftClient)
                .thenCompose(raftClient -> raftClient.refreshAndGetLeaderWithTerm()
                        .exceptionally(throwable -> {
                            throwable = unwrapCause(throwable);

                            if (throwable instanceof TimeoutException) {
                                LOG.info(
                                        "Node couldn't get the leader within timeout so the changing peers is skipped [grp={}].",
                                        replicaGrpId
                                );

                                return LeaderWithTerm.NO_LEADER;
                            }

                            throw new IgniteInternalException(
                                    INTERNAL_ERR,
                                    "Failed to get a leader for the RAFT replication group [get=" + replicaGrpId + "].",
                                    throwable
                            );
                        })
                        .thenCompose(leaderWithTerm -> {
                            if (leaderWithTerm.isEmpty() || !isLocalPeer(leaderWithTerm.leader())) {
                                return nullCompletedFuture();
                            }

                            // run update of raft configuration if this node is a leader
                            LOG.info("Current node={} is the leader of partition raft group={}. "
                                            + "Initiate rebalance process for partition={}, table={}",
                                    leaderWithTerm.leader(),
                                    replicaGrpId,
                                    replicaGrpId.partitionId(),
                                    tables.get(replicaGrpId.tableId()).name()
                            );

                            return metaStorageMgr.get(pendingPartAssignmentsKey(replicaGrpId))
                                    .thenCompose(latestPendingAssignmentsEntry -> {
                                        // Do not change peers of the raft group if this is a stale event.
                                        // Note that we start raft node before for the sake of the consistency in a
                                        // starting and stopping raft nodes.
                                        if (revision < latestPendingAssignmentsEntry.revision()) {
                                            return nullCompletedFuture();
                                        }

                                        PeersAndLearners newConfiguration = fromAssignments(pendingAssignments);

                                        return raftClient.changePeersAndLearnersAsync(newConfiguration, leaderWithTerm.term());
                                    });
                        })
                );
    }

    private SnapshotStorageFactory createSnapshotStorageFactory(
            TablePartitionId replicaGrpId,
            PartitionUpdateHandlers partitionUpdateHandlers,
            InternalTable internalTable
    ) {
        PartitionKey partitionKey = partitionKey(internalTable, replicaGrpId.partitionId());

        return new PartitionSnapshotStorageFactory(
                topologyService,
                outgoingSnapshotsManager,
                new PartitionAccessImpl(
                        partitionKey,
                        internalTable.storage(),
                        internalTable.txStateStorage(),
                        mvGc,
                        partitionUpdateHandlers.indexUpdateHandler,
                        partitionUpdateHandlers.gcUpdateHandler,
                        fullStateTransferIndexChooser,
                        schemaManager.schemaRegistry(partitionKey.tableId()),
                        lowWatermark
                ),
                catalogService,
                incomingSnapshotsExecutor
        );
    }

    /**
     * Creates Meta storage listener for stable assignments updates.
     *
     * @return The watch listener.
     */
    private WatchListener createStableAssignmentsRebalanceListener() {
        return new WatchListener() {
            @Override
            public CompletableFuture<Void> onUpdate(WatchEvent evt) {
                if (!busyLock.enterBusy()) {
                    return failedFuture(new NodeStoppingException());
                }

                try {
                    return handleChangeStableAssignmentEvent(evt);
                } finally {
                    busyLock.leaveBusy();
                }
            }

            @Override
            public void onError(Throwable e) {
                LOG.warn("Unable to process stable assignments event", e);
            }
        };
    }

    /** Creates Meta storage listener for switch reduce assignments updates. */
    private WatchListener createAssignmentsSwitchRebalanceListener() {
        return new WatchListener() {
            @Override
            public CompletableFuture<Void> onUpdate(WatchEvent evt) {
                return inBusyLockAsync(busyLock, () -> {
                    byte[] key = evt.entryEvent().newEntry().key();

                    int partitionId = extractPartitionNumber(key);
                    int tableId = extractTableId(key, ASSIGNMENTS_SWITCH_REDUCE_PREFIX);

                    TablePartitionId replicaGrpId = new TablePartitionId(tableId, partitionId);

                    return tablesById(evt.revision())
                            .thenCompose(tables -> inBusyLockAsync(busyLock, () -> {
                                Assignments assignments = Assignments.fromBytes(evt.entryEvent().newEntry().value());

                                HybridTimestamp timestamp = assignments.timestamp();

                                return waitForMetadataCompleteness(timestamp).thenCompose(unused -> inBusyLockAsync(busyLock, () -> {
                                    int catalogVersion = catalogService.activeCatalogVersion(timestamp.longValue());

                                    CatalogTableDescriptor tableDescriptor = getTableDescriptor(tableId, catalogVersion);

                                    CatalogZoneDescriptor zoneDescriptor = getZoneDescriptor(tableDescriptor, catalogVersion);

                                    long causalityToken = zoneDescriptor.updateToken();

                                    return distributionZoneManager.dataNodes(causalityToken, catalogVersion,
                                                    tableDescriptor.zoneId())
                                            .thenCompose(dataNodes -> RebalanceUtilEx.handleReduceChanged(
                                                    metaStorageMgr,
                                                    dataNodes,
                                                    zoneDescriptor.replicas(),
                                                    replicaGrpId,
                                                    evt,
                                                    timestamp
                                            ));
                                }));
                            }));
                });
            }

            @Override
            public void onError(Throwable e) {
                LOG.warn("Unable to process switch reduce event", e);
            }
        };
    }

    /**
     * Gets partition stores.
     *
     * @param table Table.
     * @param partitionId Partition ID.
     * @return PartitionStorages.
     */
    private static PartitionStorages getPartitionStorages(TableImpl table, int partitionId) {
        InternalTable internalTable = table.internalTable();

        MvPartitionStorage mvPartition = internalTable.storage().getMvPartition(partitionId);

        assert mvPartition != null : "tableId=" + table.tableId() + ", partitionId=" + partitionId;

        TxStateStorage txStateStorage = internalTable.txStateStorage().getTxStateStorage(partitionId);

        assert txStateStorage != null : "tableId=" + table.tableId() + ", partitionId=" + partitionId;

        return new PartitionStorages(mvPartition, txStateStorage);
    }

    // TODO: https://issues.apache.org/jira/browse/IGNITE-19739 Create storages only once.
    private CompletableFuture<Void> getOrCreatePartitionStorages(TableImpl table, PartitionSet partitions) {
        InternalTable internalTable = table.internalTable();

        CompletableFuture<?>[] storageFuts = partitions.stream().mapToObj(partitionId -> {
            MvPartitionStorage mvPartition = internalTable.storage().getMvPartition(partitionId);

            return (mvPartition != null ? completedFuture(mvPartition) : internalTable.storage().createMvPartition(partitionId))
                    .thenComposeAsync(mvPartitionStorage -> {
                        TxStateStorage txStateStorage = internalTable.txStateStorage().getOrCreateTxStateStorage(partitionId);

                        if (mvPartitionStorage.lastAppliedIndex() == MvPartitionStorage.REBALANCE_IN_PROGRESS
                                || txStateStorage.lastAppliedIndex() == TxStateStorage.REBALANCE_IN_PROGRESS) {
                            return allOf(
                                    internalTable.storage().clearPartition(partitionId),
                                    txStateStorage.clear()
                            );
                        } else {
                            return nullCompletedFuture();
                        }
                    }, ioExecutor);
        }).toArray(CompletableFuture[]::new);

        return allOf(storageFuts);
    }

    /**
     * Handles the {@link RebalanceUtil#STABLE_ASSIGNMENTS_PREFIX} update event.
     *
     * @param evt Event.
     */
    protected CompletableFuture<Void> handleChangeStableAssignmentEvent(WatchEvent evt) {
        if (evt.entryEvents().stream().allMatch(e -> e.oldEntry().value() == null)) {
            // It's the initial write to table stable assignments on table create event.
            return nullCompletedFuture();
        }

        if (!evt.single()) {
            // If there is not a single entry, then all entries must be tombstones (this happens after table drop).
            assert evt.entryEvents().stream().allMatch(entryEvent -> entryEvent.newEntry().tombstone()) : evt;

            return nullCompletedFuture();
        }

        // here we can receive only update from the rebalance logic
        // these updates always processing only 1 partition, so, only 1 stable partition key.
        assert evt.single() : evt;

        if (evt.entryEvent().oldEntry() == null) {
            // This means it's an event on table creation.
            return nullCompletedFuture();
        }

        Entry stableAssignmentsWatchEvent = evt.entryEvent().newEntry();

        long revision = evt.revision();

        assert stableAssignmentsWatchEvent.revision() == revision : stableAssignmentsWatchEvent;

        if (stableAssignmentsWatchEvent.value() == null) {
            return nullCompletedFuture();
        }

        return handleChangeStableAssignmentEvent(stableAssignmentsWatchEvent, evt.revision(), false);
    }

    protected CompletableFuture<Void> handleChangeStableAssignmentEvent(
            Entry stableAssignmentsWatchEvent,
            long revision,
            boolean isRecovery
    ) {
        int partitionId = extractPartitionNumber(stableAssignmentsWatchEvent.key());
        int tableId = extractTableId(stableAssignmentsWatchEvent.key(), STABLE_ASSIGNMENTS_PREFIX);

        TablePartitionId tablePartitionId = new TablePartitionId(tableId, partitionId);

        Set<Assignment> stableAssignments = stableAssignmentsWatchEvent.value() == null
                ? emptySet()
                : Assignments.fromBytes(stableAssignmentsWatchEvent.value()).nodes();

        return supplyAsync(() -> {
            Entry pendingAssignmentsEntry = metaStorageMgr.getLocally(pendingPartAssignmentsKey(tablePartitionId), revision);

            byte[] pendingAssignmentsFromMetaStorage = pendingAssignmentsEntry.value();

            Assignments pendingAssignments = pendingAssignmentsFromMetaStorage == null
                    ? Assignments.EMPTY
                    : Assignments.fromBytes(pendingAssignmentsFromMetaStorage);

            if (LOG.isInfoEnabled()) {
                var stringKey = new String(stableAssignmentsWatchEvent.key(), UTF_8);

                LOG.info("Received update on stable assignments [key={}, partition={}, localMemberAddress={}, "
                                + "stableAssignments={}, pendingAssignments={}, revision={}]", stringKey, tablePartitionId,
                        localNode().address(), stableAssignments, pendingAssignments, revision);
            }

            return stopAndDestroyPartitionAndUpdateClients(
                    tablePartitionId,
                    stableAssignments,
                    pendingAssignments,
                    isRecovery,
                    revision
            );
        }, ioExecutor).thenCompose(identity());
    }

    private CompletableFuture<Void> updatePartitionClients(
            TablePartitionId tablePartitionId,
            Set<Assignment> stableAssignments
    ) {
        return isLocalNodeIsPrimary(tablePartitionId).thenCompose(isLeaseholder -> inBusyLock(busyLock, () -> {
            boolean isLocalInStable = isLocalNodeInAssignments(stableAssignments);

            if (!isLocalInStable && !isLeaseholder) {
                return nullCompletedFuture();
            }

            assert replicaMgr.isReplicaStarted(tablePartitionId)
                    : "The local node is outside of the replication group [stable=" + stableAssignments
                            + ", isLeaseholder=" + isLeaseholder + "].";

            // Update raft client peers and learners according to the actual assignments.
            return replicaMgr.replica(tablePartitionId)
                    .thenApply(Replica::raftClient)
                    .thenAccept(raftClient -> raftClient.updateConfiguration(fromAssignments(stableAssignments)));
        }));
    }

    private CompletableFuture<Void> stopAndDestroyPartitionAndUpdateClients(
            TablePartitionId tablePartitionId,
            Set<Assignment> stableAssignments,
            Assignments pendingAssignments,
            boolean isRecovery,
            long revision
    ) {
        CompletableFuture<Void> clientUpdateFuture = isRecovery
                // Updating clients is not needed on recovery.
                ? nullCompletedFuture()
                : updatePartitionClients(tablePartitionId, stableAssignments);

        boolean shouldStopLocalServices = (pendingAssignments.force()
                        ? pendingAssignments.nodes().stream()
                        : Stream.concat(stableAssignments.stream(), pendingAssignments.nodes().stream())
                )
                .noneMatch(isLocalNodeAssignment);

        if (shouldStopLocalServices) {
            return allOf(
                    clientUpdateFuture,
                    weakStopAndDestroyPartition(tablePartitionId, revision)
            );
        } else {
            return clientUpdateFuture;
        }
    }

    private CompletableFuture<Void> weakStopAndDestroyPartition(TablePartitionId tablePartitionId, long causalityToken) {
        return replicaMgr.weakStopReplica(
                tablePartitionId,
                WeakReplicaStopReason.EXCLUDED_FROM_ASSIGNMENTS,
                () -> stopAndDestroyPartition(tablePartitionId, causalityToken)
        );
    }

    private CompletableFuture<Void> stopAndDestroyPartition(TablePartitionId tablePartitionId, long causalityToken) {
        return tablesVv
                .get(causalityToken)
                .thenCompose(ignore -> {
                    TableImpl table = tables.get(tablePartitionId.tableId());

                    return stopPartition(tablePartitionId, table)
                            .thenComposeAsync(v -> destroyPartitionStorages(tablePartitionId, table), ioExecutor);
                });
    }

    /**
     * Stops all resources associated with a given partition, like replicas and partition trackers. Calls
     * {@link ReplicaManager#weakStopReplica} in order to change the replica state.
     *
     * @param tablePartitionId Partition ID.
     * @param table Table which this partition belongs to.
     * @return Future that will be completed after all resources have been closed.
     */
    private CompletableFuture<Void> stopPartitionForRestart(TablePartitionId tablePartitionId, TableImpl table) {
        return replicaMgr.weakStopReplica(tablePartitionId, WeakReplicaStopReason.RESTART, () -> stopPartition(tablePartitionId, table));
    }

    /**
     * Stops all resources associated with a given partition, like replicas and partition trackers.
     *
     * @param tablePartitionId Partition ID.
     * @param table Table which this partition belongs to.
     * @return Future that will be completed after all resources have been closed.
     */
    private CompletableFuture<Void> stopPartition(TablePartitionId tablePartitionId, TableImpl table) {
        if (table != null) {
            closePartitionTrackers(table.internalTable(), tablePartitionId.partitionId());
        }

        CompletableFuture<Boolean> stopReplicaFuture;

        try {
            stopReplicaFuture = replicaMgr.stopReplica(tablePartitionId);
        } catch (NodeStoppingException e) {
            // No-op.
            stopReplicaFuture = falseCompletedFuture();
        }

        return stopReplicaFuture
                .thenCompose(v -> mvGc.removeStorage(tablePartitionId));
    }

    private CompletableFuture<Void> destroyPartitionStorages(TablePartitionId tablePartitionId, TableImpl table) {
        // TODO: IGNITE-18703 Destroy raft log and meta
        if (table == null) {
            return nullCompletedFuture();
        }

        InternalTable internalTable = table.internalTable();

        int partitionId = tablePartitionId.partitionId();

        List<CompletableFuture<?>> destroyFutures = new ArrayList<>();

        if (internalTable.storage().getMvPartition(partitionId) != null) {
            destroyFutures.add(internalTable.storage().destroyPartition(partitionId));
        }

        if (internalTable.txStateStorage().getTxStateStorage(partitionId) != null) {
            destroyFutures.add(runAsync(() -> internalTable.txStateStorage().destroyTxStateStorage(partitionId), ioExecutor));
        }

        return allOf(destroyFutures.toArray(new CompletableFuture[]{}));
    }

    private static void closePartitionTrackers(InternalTable internalTable, int partitionId) {
        closeTracker(internalTable.getPartitionSafeTimeTracker(partitionId));

        closeTracker(internalTable.getPartitionStorageIndexTracker(partitionId));
    }

    private static void closeTracker(@Nullable PendingComparableValuesTracker<?, Void> tracker) {
        if (tracker != null) {
            tracker.close();
        }
    }

    private ClusterNode localNode() {
        return topologyService.localMember();
    }

    private static PartitionUpdateHandlers createPartitionUpdateHandlers(
            int partitionId,
            PartitionDataStorage partitionDataStorage,
            TableImpl table,
            PendingComparableValuesTracker<HybridTimestamp, Void> safeTimeTracker,
            StorageUpdateConfiguration storageUpdateConfig
    ) {
        TableIndexStoragesSupplier indexes = table.indexStorageAdapters(partitionId);

        IndexUpdateHandler indexUpdateHandler = new IndexUpdateHandler(indexes);

        GcUpdateHandler gcUpdateHandler = new GcUpdateHandler(partitionDataStorage, safeTimeTracker, indexUpdateHandler);

        StorageUpdateHandler storageUpdateHandler = new StorageUpdateHandler(
                partitionId,
                partitionDataStorage,
                indexUpdateHandler,
                storageUpdateConfig
        );

        return new PartitionUpdateHandlers(storageUpdateHandler, indexUpdateHandler, gcUpdateHandler);
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

    private CatalogTableDescriptor getTableDescriptor(int tableId, int catalogVersion) {
        CatalogTableDescriptor tableDescriptor = catalogService.table(tableId, catalogVersion);

        assert tableDescriptor != null : "tableId=" + tableId + ", catalogVersion=" + catalogVersion;

        return tableDescriptor;
    }

    private CatalogZoneDescriptor getZoneDescriptor(CatalogTableDescriptor tableDescriptor, int catalogVersion) {
        CatalogZoneDescriptor zoneDescriptor = catalogService.zone(tableDescriptor.zoneId(), catalogVersion);

        assert zoneDescriptor != null :
                "tableId=" + tableDescriptor.id() + ", zoneId=" + tableDescriptor.zoneId() + ", catalogVersion=" + catalogVersion;

        return zoneDescriptor;
    }

    private static @Nullable TableImpl findTableImplByName(Collection<TableImpl> tables, String name) {
        return tables.stream().filter(table -> table.name().equals(name)).findAny().orElse(null);
    }

    private void startTables(long recoveryRevision, @Nullable HybridTimestamp lwm) {
        sharedTxStateStorage.start();

        int earliestCatalogVersion = catalogService.activeCatalogVersion(hybridTimestampToLong(lwm));
        int latestCatalogVersion = catalogService.latestCatalogVersion();

        var startedTables = new IntOpenHashSet();
        var startTableFutures = new ArrayList<CompletableFuture<?>>();

        for (int ver = latestCatalogVersion; ver >= earliestCatalogVersion; ver--) {
            int ver0 = ver;
            catalogService.tables(ver).stream()
                    .filter(tbl -> startedTables.add(tbl.id()))
                    .forEach(tableDescriptor -> startTableFutures.add(createTableLocally(recoveryRevision, ver0, tableDescriptor, true)));
        }

        // Forces you to wait until recovery is complete before the metastore watches is deployed to avoid races with catalog listeners.
        startVv.update(recoveryRevision, (unused, throwable) -> allOf(startTableFutures.toArray(CompletableFuture[]::new)))
                .whenComplete((unused, throwable) -> {
                    if (throwable != null) {
                        LOG.error("Error starting tables", throwable);
                    } else {
                        LOG.debug("Tables started successfully");
                    }
                });
    }

    /**
     * Returns the future that will complete when, either the future from the argument or {@link #stopManagerFuture} will complete,
     * successfully or exceptionally. Allows to protect from getting stuck at {@link IgniteComponent#stopAsync(ComponentContext)}
     * when someone is blocked (by using {@link #busyLock}) for a long time.
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
        // TODO: IGNITE-20384 Clean up abandoned resources for dropped zones from vault and metastore
        for (DroppedTableInfo droppedTableInfo : droppedTables(catalogService, lowWatermark.getLowWatermark())) {
            int catalogVersion = droppedTableInfo.tableRemovalCatalogVersion() - 1;

            CatalogTableDescriptor tableDescriptor = catalogService.table(droppedTableInfo.tableId(), catalogVersion);

            assert tableDescriptor != null : "tableId=" + droppedTableInfo.tableId() + ", catalogVersion=" + catalogVersion;

            destroyTableStorageOnRecoveryBusy(tableDescriptor);
        }
    }

    private void destroyTableStorageOnRecoveryBusy(CatalogTableDescriptor tableDescriptor) {
        StorageEngine engine = dataStorageMgr.engineByStorageProfile(tableDescriptor.storageProfile());

        assert engine != null : "tableId=" + tableDescriptor.id() + ", storageProfile=" + tableDescriptor.storageProfile();

        engine.dropMvTable(tableDescriptor.id());
    }

    private synchronized ScheduledExecutorService streamerFlushExecutor() {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
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

    /**
     * Restarts the table partition including the replica and raft node.
     *
     * @param tablePartitionId Table partition that needs to be restarted.
     * @param revision Metastore revision.
     * @return Operation future.
     */
    public CompletableFuture<Void> restartPartition(TablePartitionId tablePartitionId, long revision, HybridTimestamp timestamp) {
        return inBusyLockAsync(busyLock, () -> tablesVv.get(revision).thenComposeAsync(unused -> inBusyLockAsync(busyLock, () -> {
            TableImpl table = tables.get(tablePartitionId.tableId());

            return stopPartitionForRestart(tablePartitionId, table).thenComposeAsync(unused1 -> {
                Assignments stableAssignments = stableAssignments(tablePartitionId, revision);

                assert stableAssignments != null : "tablePartitionId=" + tablePartitionId + ", revision=" + revision;

                return waitForMetadataCompleteness(timestamp).thenCompose(unused2 -> inBusyLockAsync(busyLock, () -> {
                    int catalogVersion = catalogService.activeCatalogVersion(timestamp.longValue());

                    int zoneId = getTableDescriptor(tablePartitionId.tableId(), catalogVersion).zoneId();

                    return startPartitionAndStartClient(
                            table,
                            tablePartitionId.partitionId(),
                            localMemberAssignment(stableAssignments),
                            stableAssignments,
                            zoneId,
                            false,
                            timestamp
                    );
                }));
            }, ioExecutor);
        }), ioExecutor));
    }

    private @Nullable Assignments stableAssignments(TablePartitionId tablePartitionId, long revision) {
        Entry entry = metaStorageMgr.getLocally(stablePartAssignmentsKey(tablePartitionId), revision);

        return Assignments.fromBytes(entry.value());
    }

    @Override
    public void setStreamerReceiverRunner(StreamerReceiverRunner runner) {
        this.streamerReceiverRunner = runner;
    }
}
