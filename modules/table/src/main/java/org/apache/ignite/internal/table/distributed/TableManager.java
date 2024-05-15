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
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.findTablesByZoneId;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.ASSIGNMENTS_SWITCH_REDUCE_PREFIX;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.PENDING_ASSIGNMENTS_PREFIX;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.STABLE_ASSIGNMENTS_PREFIX;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.extractPartitionNumber;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.extractZoneId;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.intersect;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.partitionAssignmentsGetLocally;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.pendingPartAssignmentsKey;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.stablePartAssignmentsKey;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.tableAssignmentsGetLocally;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.tablesCounterKey;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.union;
import static org.apache.ignite.internal.event.EventListener.fromConsumer;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestampToLong;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.or;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.revision;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.value;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.metastorage.dsl.Statements.iif;
import static org.apache.ignite.internal.table.distributed.TableUtils.droppedTables;
import static org.apache.ignite.internal.table.distributed.index.IndexUtils.registerIndexesToTable;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_READ;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_WRITE;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.apache.ignite.internal.util.CompletableFutures.emptyListCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.trueCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
import java.util.function.IntSupplier;
import java.util.function.LongFunction;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.affinity.AffinityUtils;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.affinity.Assignments;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CreateTableEventParameters;
import org.apache.ignite.internal.catalog.events.DropTableEventParameters;
import org.apache.ignite.internal.catalog.events.RenameTableEventParameters;
import org.apache.ignite.internal.causality.CompletionListener;
import org.apache.ignite.internal.causality.IncrementalVersionedValue;
import org.apache.ignite.internal.close.ManuallyCloseable;
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
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.marshaller.ReflectionMarshallersProvider;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.SimpleCondition;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.raft.ExecutorInclinedRaftCommandRunner;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Marshaller;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftGroupEventsListener;
import org.apache.ignite.internal.raft.RaftManager;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupService;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.raft.service.LeaderWithTerm;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.raft.storage.impl.LogStorageFactoryCreator;
import org.apache.ignite.internal.replicator.ReplicaAwareLeaseTracker;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.SchemaRegistry;
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
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.TableRaftService;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.gc.GcUpdateHandler;
import org.apache.ignite.internal.table.distributed.gc.MvGc;
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
import org.apache.ignite.internal.table.distributed.schema.SchemaSyncService;
import org.apache.ignite.internal.table.distributed.schema.SchemaVersions;
import org.apache.ignite.internal.table.distributed.schema.SchemaVersionsImpl;
import org.apache.ignite.internal.table.distributed.schema.ThreadLocalPartitionCommandsMarshaller;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.table.distributed.storage.PartitionStorages;
import org.apache.ignite.internal.table.distributed.storage.TableRaftServiceImpl;
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
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.raft.jraft.storage.impl.VolatileRaftMetaStorage;
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

    /** Raft manager. */
    private final RaftManager raftMgr;

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
     * <p>Only explicitly updated in {@link #startLocalPartitionsAndClients(CompletableFuture, TableImpl, int)}.
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

    private final LogStorageFactoryCreator volatileLogStorageFactoryCreator;

    /** Executor for scheduling rebalance routine. */
    private final ScheduledExecutorService rebalanceScheduler;

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

    private final TopologyAwareRaftGroupServiceFactory raftGroupServiceFactory;

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

    private final Marshaller raftCommandsMarshaller;

    private final HybridTimestampTracker observableTimestampTracker;

    /** Placement driver. */
    private final PlacementDriver executorInclinedPlacementDriver;

    /** A supplier function that returns {@link IgniteSql}. */
    private final Supplier<IgniteSql> sql;

    private final SchemaVersions schemaVersions;

    private final PartitionReplicatorNodeRecovery partitionReplicatorNodeRecovery;

    /** Versioned value used only at manager startup to correctly fire table creation events. */
    private final IncrementalVersionedValue<Void> startVv;

    /** Ends at the {@link #stop()} with an {@link NodeStoppingException}. */
    private final CompletableFuture<Void> stopManagerFuture = new CompletableFuture<>();

    /** Configuration for {@link StorageUpdateHandler}. */
    private final StorageUpdateConfiguration storageUpdateConfig;

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

    private final TransactionConfiguration txCfg;

    private final String nodeName;

    private long implicitTransactionTimeout;

    private int attemptsObtainLock;

    @Nullable
    private ScheduledExecutorService streamerFlushExecutor;

    /**
     * Creates a new table manager.
     *
     * @param nodeName Node name.
     * @param registry Registry for versioned values.
     * @param gcConfig Garbage collector configuration.
     * @param txCfg Transaction configuration.
     * @param storageUpdateConfig Storage update handler configuration.
     * @param raftMgr Raft manager.
     * @param replicaMgr Replica manager.
     * @param lockMgr Lock manager.
     * @param replicaSvc Replica service.
     * @param txManager Transaction manager.
     * @param dataStorageMgr Data storage manager.
     * @param schemaManager Schema manager.
     * @param volatileLogStorageFactoryCreator Creator for {@link org.apache.ignite.internal.raft.storage.LogStorageFactory} for
     *         volatile tables.
     * @param ioExecutor Separate executor for IO operations like partition storage initialization or partition raft group meta data
     *     persisting.
     * @param partitionOperationsExecutor Striped executor on which partition operations (potentially requiring I/O with storages)
     *     will be executed.
     * @param raftGroupServiceFactory Factory that is used for creation of raft group services for replication groups.
     * @param placementDriver Placement driver.
     * @param sql A supplier function that returns {@link IgniteSql}.
     * @param rebalanceScheduler Executor for scheduling rebalance routine.
     * @param lowWatermark Low watermark.
     * @param transactionInflights Transaction inflights.
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
            RaftManager raftMgr,
            ReplicaManager replicaMgr,
            LockManager lockMgr,
            ReplicaService replicaSvc,
            TxManager txManager,
            DataStorageManager dataStorageMgr,
            Path storagePath,
            MetaStorageManager metaStorageMgr,
            SchemaManager schemaManager,
            LogStorageFactoryCreator volatileLogStorageFactoryCreator,
            ExecutorService ioExecutor,
            Executor partitionOperationsExecutor,
            HybridClock clock,
            ClockService clockService,
            OutgoingSnapshotsManager outgoingSnapshotsManager,
            TopologyAwareRaftGroupServiceFactory raftGroupServiceFactory,
            DistributionZoneManager distributionZoneManager,
            SchemaSyncService schemaSyncService,
            CatalogService catalogService,
            HybridTimestampTracker observableTimestampTracker,
            PlacementDriver placementDriver,
            Supplier<IgniteSql> sql,
            RemotelyTriggeredResourceRegistry remotelyTriggeredResourceRegistry,
            ScheduledExecutorService rebalanceScheduler,
            LowWatermark lowWatermark,
            TransactionInflights transactionInflights
    ) {
        this.topologyService = topologyService;
        this.raftMgr = raftMgr;
        this.replicaMgr = replicaMgr;
        this.lockMgr = lockMgr;
        this.replicaSvc = replicaSvc;
        this.txManager = txManager;
        this.dataStorageMgr = dataStorageMgr;
        this.metaStorageMgr = metaStorageMgr;
        this.schemaManager = schemaManager;
        this.volatileLogStorageFactoryCreator = volatileLogStorageFactoryCreator;
        this.ioExecutor = ioExecutor;
        this.partitionOperationsExecutor = partitionOperationsExecutor;
        this.clock = clock;
        this.clockService = clockService;
        this.outgoingSnapshotsManager = outgoingSnapshotsManager;
        this.raftGroupServiceFactory = raftGroupServiceFactory;
        this.distributionZoneManager = distributionZoneManager;
        this.catalogService = catalogService;
        this.observableTimestampTracker = observableTimestampTracker;
        this.sql = sql;
        this.storageUpdateConfig = storageUpdateConfig;
        this.remotelyTriggeredResourceRegistry = remotelyTriggeredResourceRegistry;
        this.rebalanceScheduler = rebalanceScheduler;
        this.lowWatermark = lowWatermark;
        this.transactionInflights = transactionInflights;
        this.txCfg = txCfg;
        this.nodeName = nodeName;

        this.executorInclinedSchemaSyncService = new ExecutorInclinedSchemaSyncService(schemaSyncService, partitionOperationsExecutor);
        this.executorInclinedPlacementDriver = new ExecutorInclinedPlacementDriver(
                new ReplicaAwareLeaseTracker(placementDriver, replicaSvc, topologyService),
                partitionOperationsExecutor
        );

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

        raftCommandsMarshaller = new ThreadLocalPartitionCommandsMarshaller(messageSerializationRegistry);

        partitionReplicatorNodeRecovery = new PartitionReplicatorNodeRecovery(
                metaStorageMgr,
                messagingService,
                topologyService,
                tableId -> tablesById().get(tableId)
        );

        startVv = new IncrementalVersionedValue<>(registry);

        sharedTxStateStorage = new TxStateRocksDbSharedStorage(
                storagePath.resolve(TX_STATE_DIR),
                txStateStorageScheduledPool,
                txStateStoragePool,
                raftMgr.getLogSyncer(),
                TX_STATE_STORAGE_FLUSH_DELAY_SUPPLIER
        );

        fullStateTransferIndexChooser = new FullStateTransferIndexChooser(catalogService, lowWatermark);
    }

    @Override
    public CompletableFuture<Void> startAsync() {
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

            return nullCompletedFuture();
        });
    }

    private void processAssignmentsOnRecovery(long recoveryRevision) {
        var stableAssignmentsPrefix = new ByteArray(STABLE_ASSIGNMENTS_PREFIX);
        var pendingAssignmentsPrefix = new ByteArray(PENDING_ASSIGNMENTS_PREFIX);

        startVv.update(recoveryRevision, (v, e) -> handleAssignmentsOnRecovery(
                stableAssignmentsPrefix,
                recoveryRevision,
                (entry, rev) -> handleChangeStableAssignmentEvent(entry, rev, true),
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
                                    "Missed {} assignments for key '{}' discovered, performing recovery",
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
     * @param zoneId  Zone id.
     * @param assignmentsFuture Assignments future, to get the assignments that should be written.
     * @return Real list of assignments.
     */
    public CompletableFuture<List<Assignments>> writeZoneAssignmentsToMetastore(
            int zoneId,
            CompletableFuture<List<Assignments>> assignmentsFuture
    ) {
        return assignmentsFuture.thenCompose(newAssignments -> {
            assert !newAssignments.isEmpty();

            List<Operation> partitionAssignments = new ArrayList<>(newAssignments.size());

            for (int i = 0; i < newAssignments.size(); i++) {
                ByteArray stableAssignmentsKey = stablePartAssignmentsKey(new ZonePartitionId(zoneId, i));
                byte[] anAssignment = newAssignments.get(i).toBytes();
                Operation op = put(stableAssignmentsKey, anAssignment);
                partitionAssignments.add(op);
            }

            Condition condition = notExists(new ByteArray(partitionAssignments.get(0).key()));

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
                                            + " [zoneId={}, assignments={}].",
                                    zoneId,
                                    Assignments.assignmentListToString(newAssignments)
                            );

                            return completedFuture(newAssignments);
                        } else {
                            Set<ByteArray> partKeys = IntStream.range(0, newAssignments.size())
                                    .mapToObj(p -> stablePartAssignmentsKey(new ZonePartitionId(zoneId, p)))
                                    .collect(toSet());

                            CompletableFuture<Map<ByteArray, Entry>> resFuture = metaStorageMgr.getAll(partKeys);

                            return resFuture.thenApply(metaStorageAssignments -> {
                                List<Assignments> realAssignments = new ArrayList<>();

                                for (int p = 0; p < newAssignments.size(); p++) {
                                    var partId = new ZonePartitionId(zoneId, p);
                                    Entry assignmentsEntry = metaStorageAssignments.get(stablePartAssignmentsKey(partId));

                                    assert assignmentsEntry != null && !assignmentsEntry.empty() && !assignmentsEntry.tombstone()
                                            : "Unexpected assignments for partition [" + partId + ", entry=" + assignmentsEntry + "].";

                                    Assignments real = Assignments.fromBytes(assignmentsEntry.value());

                                    realAssignments.add(real);
                                }

                                LOG.info(
                                        "Assignments picked up from meta storage [zoneId={}, assignments={}].",
                                        zoneId,
                                        Assignments.assignmentListToString(realAssignments)
                                );

                                return realAssignments;
                            });
                        }
                    })
                    .handle((realAssignments, e) -> {
                        if (e != null) {
                            LOG.error("Couldn't get assignments from metastore for table [zoneId={}].", e, zoneId);

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
     * @return future, which will be completed when the partitions creations done.
     */
    private CompletableFuture<Void> startLocalPartitionsAndClients(
            CompletableFuture<List<Assignments>> assignmentsFuture,
            TableImpl table,
            int zoneId
    ) {
        int tableId = table.tableId();

        // Create new raft nodes according to new assignments.
        return assignmentsFuture.thenCompose(assignments -> {
            // Empty assignments might be a valid case if tables are created from within cluster init HOCON
            // configuration, which is not supported now.
            assert assignments != null : IgniteStringFormatter.format("Table [id={}] has empty assignments.", tableId);

            int partitions = assignments.size();

            List<CompletableFuture<?>> futures = new ArrayList<>();

            for (int i = 0; i < partitions; i++) {
                int partId = i;

                LOG.info("Start partition " + new TablePartitionId(tableId, i));

                CompletableFuture<?> future = startPartitionAndStartClient(
                                table,
                                partId,
                                assignments.get(partId),
                                null,
                                zoneId,
                                false
                        )
                        .whenComplete((res, ex) -> {
                            if (ex != null) {
                                LOG.warn("Unable to update raft groups on the node [tableId={}, partitionId={}]", ex, tableId, partId);
                            }
                        });

                futures.add(future);
            }

            return allOf(futures.toArray(new CompletableFuture<?>[0]));
        });
    }

    private CompletableFuture<Void> startPartitionAndStartClient(
            TableImpl table,
            int partId,
            Assignments assignments,
            @Nullable Assignments nonStableNodeAssignments,
            int zoneId,
            boolean isRecovery
    ) {
        CompletableFuture<Void> resultFuture = new CompletableFuture<>();

        int tableId = table.tableId();

        InternalTable internalTbl = table.internalTable();

        Assignment localMemberAssignment = assignments.nodes().stream()
                .filter(a -> a.consistentId().equals(localNode().name()))
                .findAny()
                .orElse(null);

        PeersAndLearners realConfiguration = configurationFromAssignments(assignments.nodes());
        PeersAndLearners newConfiguration = nonStableNodeAssignments == null
                ? realConfiguration : configurationFromAssignments(nonStableNodeAssignments.nodes());

        TablePartitionId replicaGrpId = new TablePartitionId(tableId, partId);

        var safeTimeTracker = new PendingComparableValuesTracker<HybridTimestamp, Void>(
                new HybridTimestamp(1, 0)
        );
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

        Peer serverPeer = realConfiguration.peer(localNode().name());

        var raftNodeId = localMemberAssignment == null ? null : new RaftNodeId(replicaGrpId, serverPeer);

        boolean shouldStartRaftListeners = localMemberAssignment != null && !((Loza) raftMgr).isStarted(raftNodeId);

        if (shouldStartRaftListeners) {
            ((InternalTableImpl) internalTbl).updatePartitionTrackers(partId, safeTimeTracker, storageIndexTracker);

            mvGc.addStorage(replicaGrpId, partitionUpdateHandlers.gcUpdateHandler);
        }

        CompletableFuture<Boolean> startGroupFut;

        // TODO: revisit for in-memory
        ZonePartitionId zonePartitionId = new ZonePartitionId(zoneId, partId);

        if (localMemberAssignment != null) {
            CompletableFuture<Boolean> shouldStartGroupFut = isRecovery
                    ? partitionReplicatorNodeRecovery.shouldStartGroup(
                            zonePartitionId,
                            internalTbl,
                            newConfiguration,
                            localMemberAssignment
                    )
                    : trueCompletedFuture();

            startGroupFut = shouldStartGroupFut.thenApplyAsync(startGroup -> inBusyLock(busyLock, () -> {
                if (!startGroup) {
                    return false;
                }

                if (((Loza) raftMgr).isStarted(raftNodeId)) {
                    if (nonStableNodeAssignments != null && nonStableNodeAssignments.force()) {
                        ((Loza) raftMgr).resetPeers(raftNodeId, configurationFromAssignments(nonStableNodeAssignments.nodes()));
                    }

                    return true;
                }

                try {
                    startPartitionRaftGroupNode(
                            zonePartitionId,
                            raftNodeId,
                            newConfiguration,
                            safeTimeTracker,
                            storageIndexTracker,
                            table,
                            partitionStorages.getTxStateStorage(),
                            partitionDataStorage,
                            partitionUpdateHandlers
                    );

                    return true;
                } catch (NodeStoppingException ex) {
                    throw new CompletionException(ex);
                }
            }), ioExecutor);
        } else {
            startGroupFut = falseCompletedFuture();
        }

        startGroupFut
                .thenComposeAsync(v -> inBusyLock(busyLock, () -> {
                    TableRaftService tableRaftService = table.internalTable().tableRaftService();

                    try {
                        // Return existing service if it's already started.
                        return completedFuture(
                                (TopologyAwareRaftGroupService) tableRaftService.partitionRaftGroupService(replicaGrpId.partitionId())
                        );
                    } catch (IgniteInternalException e) {
                        // We use "IgniteInternalException" in accordance with the javadoc of "partitionRaftGroupService" method.
                        try {
                            // TODO IGNITE-19614 This procedure takes 10 seconds if there's no majority online.
                            return raftMgr
                                    .startRaftGroupService(replicaGrpId, newConfiguration, raftGroupServiceFactory, raftCommandsMarshaller);
                        } catch (NodeStoppingException ex) {
                            return failedFuture(ex);
                        }
                    }
                }), ioExecutor)
                .thenAcceptAsync(updatedRaftGroupService -> inBusyLock(busyLock, () -> {
                    ((InternalTableImpl) internalTbl).tableRaftService()
                            .updateInternalTableRaftGroupService(partId, updatedRaftGroupService);

                    boolean startedRaftNode = startGroupFut.join();
                    if (localMemberAssignment == null || !startedRaftNode || replicaMgr.isReplicaStarted(replicaGrpId)) {
                        return;
                    }

                    try {
                        startReplicaWithNewListener(
                                replicaGrpId,
                                zonePartitionId,
                                table,
                                safeTimeTracker,
                                storageIndexTracker,
                                partitionStorages.getMvPartitionStorage(),
                                partitionStorages.getTxStateStorage(),
                                partitionUpdateHandlers,
                                updatedRaftGroupService
                        );
                    } catch (NodeStoppingException ex) {
                        throw new AssertionError("Loza was stopped before Table manager", ex);
                    }
                }), ioExecutor)
                .whenComplete((res, ex) -> {
                    if (ex != null) {
                        LOG.warn("Unable to update raft groups on the node [tableId={}, partitionId={}]", ex, tableId, partId);

                        resultFuture.completeExceptionally(ex);
                    } else {
                        resultFuture.complete(null);
                    }
                });

        return resultFuture;
    }

    private void startReplicaWithNewListener(
            TablePartitionId replicaGrpId,
            ZonePartitionId zonePartitionId,
            TableImpl table,
            PendingComparableValuesTracker<HybridTimestamp, Void> safeTimeTracker,
            PendingComparableValuesTracker<Long, Void> storageIndexTracker,
            MvPartitionStorage mvPartitionStorage,
            TxStateStorage txStatePartitionStorage,
            PartitionUpdateHandlers partitionUpdateHandlers,
            TopologyAwareRaftGroupService raftGroupService
    ) throws NodeStoppingException {
        PartitionReplicaListener listener = createReplicaListener(
                replicaGrpId.partitionId(),
                table,
                safeTimeTracker,
                mvPartitionStorage,
                txStatePartitionStorage,
                partitionUpdateHandlers,
                raftGroupService
        );

        replicaMgr.startReplica(
                replicaGrpId,
                zonePartitionId,
                listener,
                raftGroupService,
                storageIndexTracker
        );
    }

    private PartitionReplicaListener createReplicaListener(
            int partId,
            TableImpl table,
            PendingComparableValuesTracker<HybridTimestamp, Void> safeTimeTracker,
            MvPartitionStorage mvPartitionStorage,
            TxStateStorage txStatePartitionStorage,
            PartitionUpdateHandlers partitionUpdateHandlers,
            RaftGroupService raftClient
    ) {
        int tableId = table.tableId();

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
                schemaManager.schemaRegistry(tableId)
        );
    }

    private boolean isLocalPeer(Peer peer) {
        return peer.consistentId().equals(localNode().name());
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

    private RaftGroupOptions groupOptionsForPartition(
            MvTableStorage mvTableStorage,
            TxStateTableStorage txStateTableStorage,
            PartitionKey partitionKey,
            PartitionUpdateHandlers partitionUpdateHandlers
    ) {
        RaftGroupOptions raftGroupOptions;

        if (mvTableStorage.isVolatile()) {
            raftGroupOptions = RaftGroupOptions.forVolatileStores()
                    // TODO: use RaftManager interface, see https://issues.apache.org/jira/browse/IGNITE-18273
                    .setLogStorageFactory(volatileLogStorageFactoryCreator.factory(((Loza) raftMgr).volatileRaft().logStorage().value()))
                    .raftMetaStorageFactory((groupId, raftOptions) -> new VolatileRaftMetaStorage());
        } else {
            raftGroupOptions = RaftGroupOptions.forPersistentStores();
        }

        raftGroupOptions.snapshotStorageFactory(new PartitionSnapshotStorageFactory(
                topologyService,
                outgoingSnapshotsManager,
                new PartitionAccessImpl(
                        partitionKey,
                        mvTableStorage,
                        txStateTableStorage,
                        mvGc,
                        partitionUpdateHandlers.indexUpdateHandler,
                        partitionUpdateHandlers.gcUpdateHandler,
                        fullStateTransferIndexChooser,
                        schemaManager.schemaRegistry(partitionKey.tableId()),
                        lowWatermark
                ),
                catalogService,
                incomingSnapshotsExecutor
        ));

        raftGroupOptions.commandsMarshaller(raftCommandsMarshaller);

        return raftGroupOptions;
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
    public CompletableFuture<Void> stopAsync() {
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
                    () -> shutdownAndAwaitTermination(rebalanceScheduler, shutdownTimeoutSeconds, TimeUnit.SECONDS),
                    () -> shutdownAndAwaitTermination(txStateStoragePool, shutdownTimeoutSeconds, TimeUnit.SECONDS),
                    () -> shutdownAndAwaitTermination(txStateStorageScheduledPool, shutdownTimeoutSeconds, TimeUnit.SECONDS),
                    () -> shutdownAndAwaitTermination(scanRequestExecutor, shutdownTimeoutSeconds, TimeUnit.SECONDS),
                    () -> shutdownAndAwaitTermination(incomingSnapshotsExecutor, shutdownTimeoutSeconds, TimeUnit.SECONDS),
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
            int zoneId = tableDescriptor.zoneId();

            // Retrieve descriptor during synchronous call, before the previous catalog version could be concurrently compacted.
            CatalogZoneDescriptor zoneDescriptor = getZoneDescriptor(tableDescriptor, catalogVersion);

            CompletableFuture<List<Assignments>> assignmentsFuture = getOrCreateAssignments(
                    tableDescriptor,
                    zoneDescriptor,
                    causalityToken,
                    catalogVersion
            );

            CompletableFuture<List<Assignments>> assignmentsFutureAfterInvoke =
                    writeZoneAssignmentsToMetastore(zoneId, assignmentsFuture);

            return createTableLocally(
                    causalityToken,
                    tableDescriptor,
                    zoneDescriptor,
                    assignmentsFutureAfterInvoke,
                    onNodeRecovery
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
            boolean onNodeRecovery
    ) {
        String tableName = tableDescriptor.name();
        int tableId = tableDescriptor.id();

        LOG.trace("Creating local table: name={}, id={}, token={}", tableDescriptor.name(), tableDescriptor.id(), causalityToken);

        MvTableStorage tableStorage = createTableStorage(tableDescriptor, zoneDescriptor);
        TxStateTableStorage txStateStorage = createTxStateTableStorage(tableDescriptor, zoneDescriptor);

        int partitions = zoneDescriptor.partitions();

        TableRaftServiceImpl tableRaftService = new TableRaftServiceImpl(
                tableName,
                partitions,
                new Int2ObjectOpenHashMap<>(partitions),
                topologyService
        );

        InternalTableImpl internalTable = new InternalTableImpl(
                tableName,
                tableId,
                zoneDescriptor.id(),
                partitions,
                topologyService,
                txManager,
                tableStorage,
                txStateStorage,
                replicaSvc,
                clock,
                observableTimestampTracker,
                executorInclinedPlacementDriver,
                tableRaftService,
                transactionInflights,
                implicitTransactionTimeout,
                attemptsObtainLock,
                this::streamerFlushExecutor
        );

        var table = new TableImpl(
                internalTable,
                lockMgr,
                schemaVersions,
                marshallers,
                sql.get(),
                tableDescriptor.primaryKeyIndexId()
        );

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
                        return startLocalPartitionsAndClients(assignmentsFuture, table, zoneDescriptor.id());
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
        int zoneId = tableDescriptor.zoneId();
        CompletableFuture<List<Assignments>> assignmentsFuture;

        if (partitionAssignmentsGetLocally(metaStorageMgr, zoneId, 0, causalityToken) != null) {
            assignmentsFuture = completedFuture(
                    tableAssignmentsGetLocally(metaStorageMgr, zoneId, zoneDescriptor.partitions(), causalityToken));
        } else {
            assignmentsFuture = distributionZoneManager.dataNodes(causalityToken, catalogVersion, zoneDescriptor.id())
                    .thenApply(dataNodes -> AffinityUtils.calculateAssignments(
                            dataNodes,
                            zoneDescriptor.partitions(),
                            zoneDescriptor.replicas()
                    ).stream().map(Assignments::of).collect(toList()));

            assignmentsFuture.thenAccept(assignmentsList -> LOG.info(
                    "Assignments calculated from data nodes [table={}, zoneId={}, assignments={}, revision={}]",
                    tableDescriptor.name(),
                    zoneId,
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

        assert engine != null : "tableId=" + tableDescriptor.id() + ", engine=" + engine.name();

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

                    return CompletableFutures.allOf(tableImplFutures);
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
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }
        try {
            return tablesById(causalityToken).thenApply(tablesById -> tablesById.get(id));
        } finally {
            busyLock.leaveBusy();
        }
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
        int zoneId = extractZoneId(pendingAssignmentsEntry.key(), PENDING_ASSIGNMENTS_PREFIX);

        var zonePartitionId = new ZonePartitionId(zoneId, partId);

        // Stable assignments from the meta store, which revision is bounded by the current pending event.
        Assignments stableAssignments = stableAssignments(zonePartitionId, revision);

        Assignments pendingAssignments = Assignments.fromBytes(pendingAssignmentsEntry.value());

        HybridTimestamp msSafeTime = metaStorageMgr.timestampByRevision(revision);

        int catalogVersion = catalogService.activeCatalogVersion(msSafeTime.longValue());

        Set<Integer> tablesInZone = findTablesByZoneId(zoneId, catalogVersion, catalogService).stream()
                .map(CatalogObjectDescriptor::id)
                .collect(toSet());

        return tablesVv.get(revision)
                .thenApply(ignore -> {
                    if (!busyLock.enterBusy()) {
                        return CompletableFuture.<Void>failedFuture(new NodeStoppingException());
                    }

                    try {
                        return setTablesPartitionCountersForRebalance(zonePartitionId, revision, pendingAssignments.force())
                                .thenCompose(r -> {
                                    List<CompletableFuture<?>> tableFutures = new ArrayList<>(tables.size());

                                    for (int tableId : tablesInZone) {
                                        TableImpl table = tables.get(tableId);

                                        if (LOG.isInfoEnabled()) {
                                            var stringKey = new String(pendingAssignmentsEntry.key(), UTF_8);

                                            LOG.info("Received update on pending assignments. Check if new raft group should be started"
                                                            + " [key={}, partition={}, table={}, "
                                                            + "localMemberAddress={}, pendingAssignments={}]",
                                                    stringKey, partId, table.name(), localNode().address(), pendingAssignments);
                                        }

                                        tableFutures.add(
                                                handleChangePendingAssignmentEventForTable(
                                                        zonePartitionId,
                                                        table,
                                                        pendingAssignments,
                                                        stableAssignments == null ? emptySet() : stableAssignments.nodes(),
                                                        revision,
                                                        isRecovery
                                                ).thenCompose(
                                                        v -> changePeersOnRebalance(
                                                                table,
                                                                zonePartitionId,
                                                                pendingAssignments.nodes(),
                                                                revision
                                                        )
                                                )
                                        );
                                    }

                                    return allOf(tableFutures.toArray(CompletableFuture[]::new));
                                });
                    } finally {
                        busyLock.leaveBusy();
                    }
                })
                .thenCompose(identity());
    }

    private CompletableFuture<Void> handleChangePendingAssignmentEventForTable(
            ZonePartitionId zonePartitionId,
            TableImpl tbl,
            Assignments pendingAssignments,
            Set<Assignment> stableAssignments,
            long revision,
            boolean isRecovery
    ) {
        ClusterNode localMember = localNode();
        RaftNodeId raftNodeId = new RaftNodeId(
                new TablePartitionId(tbl.tableId(), zonePartitionId.partitionId()),
                new Peer(localNode().name())
        );

        boolean pendingAssignmentsAreForced = pendingAssignments.force();
        Set<Assignment> pendingAssignmentsNodes = pendingAssignments.nodes();

        // Start a new Raft node and Replica if this node has appeared in the new assignments.
        boolean shouldStartLocalGroupNode = pendingAssignmentsNodes.stream()
                .filter(assignment -> localMember.name().equals(assignment.consistentId()))
                .anyMatch(assignment -> !stableAssignments.contains(assignment));

        CompletableFuture<Void> localServicesStartFuture;

        int zoneId = zonePartitionId.zoneId();

        // This is a set of assignments for nodes that are not the part of stable assignments, i.e. unstable part of the distribution.
        // For regular pending assignments we use (old) stable set, so that none of new nodes would be able to propose itself as a leader.
        // For forced assignments, we should do the same thing, but only for the subset of stable set that is alive right now. Dead nodes
        // are excluded. It is calculated precisely as an intersection between forced assignments and (old) stable assignments.
        Assignments nonStableNodeAssignments = pendingAssignmentsAreForced
                ? Assignments.forced(intersect(stableAssignments, pendingAssignmentsNodes))
                : Assignments.of(stableAssignments);

        // This condition can only pass if all stable nodes are dead, and we start new raft group from scratch.
        // In this case new initial configuration must match new forced assignments.
        // TODO https://issues.apache.org/jira/browse/IGNITE-21661 Something might not work, extensive testing is required.
        if (nonStableNodeAssignments.nodes().isEmpty()) {
            nonStableNodeAssignments = Assignments.forced(pendingAssignmentsNodes);
        }

        Assignments nonStableNodeAssignmentsFinal = nonStableNodeAssignments;

        int partitionId = zonePartitionId.partitionId();

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

                        return startPartitionAndStartClient(
                                tbl,
                                zonePartitionId.partitionId(),
                                pendingAssignments,
                                nonStableNodeAssignmentsFinal,
                                zoneId,
                                isRecovery
                        );
                    }), ioExecutor);
        } else {
            localServicesStartFuture = runAsync(() -> {
                if (pendingAssignmentsAreForced && ((Loza) raftMgr).isStarted(raftNodeId)) {
                    ((Loza) raftMgr).resetPeers(raftNodeId, configurationFromAssignments(nonStableNodeAssignmentsFinal.nodes()));
                }
            }, ioExecutor);
        }

        return localServicesStartFuture.thenRunAsync(() -> {
            // For forced assignments, we exclude dead stable nodes, and all alive stable nodes are already in pending assignments.
            // Union is not required in such a case.
            Set<Assignment> cfg = pendingAssignmentsAreForced
                    ? pendingAssignmentsNodes
                    : union(pendingAssignmentsNodes, stableAssignments);

            tbl.internalTable()
                    .tableRaftService()
                    .partitionRaftGroupService(zonePartitionId.partitionId())
                    .updateConfiguration(configurationFromAssignments(cfg));
        }, ioExecutor);
    }

    private CompletableFuture<Void> setTablesPartitionCountersForRebalance(ZonePartitionId zonePartitionId, long revision, boolean force) {
        int catalogVersion = catalogService.latestCatalogVersion();

        int zoneId = zonePartitionId.zoneId();

        int partId = zonePartitionId.partitionId();

        SimpleCondition revisionMatches = revision(tablesCounterKey(zoneId, partId)).lt(revision);
        SimpleCondition counterIsEmpty = value(tablesCounterKey(zoneId, partId)).eq(toBytes(Set.of()));

        Condition condition = or(
                notExists(tablesCounterKey(zoneId, partId)),
                force ? revisionMatches : revisionMatches.and(counterIsEmpty)
        );

        Set<Integer> tablesInZone = findTablesByZoneId(zoneId, catalogVersion, catalogService).stream()
                .map(CatalogObjectDescriptor::id)
                .collect(toSet());

        byte[] countersValue = toBytes(tablesInZone);

        return metaStorageMgr.invoke(iif(
                condition,
                ops(put(tablesCounterKey(zoneId, partId), countersValue)).yield(true),
                ops().yield(false)
        )).whenComplete((res, e) -> {
            if (e != null) {
                LOG.error("Failed to update counter for the zone [zoneId = {}]", zoneId);
            } else if (res.getAsBoolean()) {
                LOG.info(
                        "Rebalance counter for the zone is updated [zoneId = {}, partId = {}, counter = {}, revision = {}]",
                        zoneId,
                        partId,
                        tablesInZone,
                        revision
                );
            } else {
                LOG.debug(
                        "Rebalance counter for the zone is not updated [zoneId = {}, partId = {}, revision = {}]",
                        zoneId,
                        partId,
                        revision
                );
            }
        }).thenCompose((ignored) -> nullCompletedFuture());
    }

    private CompletableFuture<Void> changePeersOnRebalance(
            TableImpl table,
            ZonePartitionId replicaGrpId,
            Set<Assignment> pendingAssignments,
            long revision
    ) {
        int partId = replicaGrpId.partitionId();

        RaftGroupService partGrpSvc = table.internalTable().tableRaftService().partitionRaftGroupService(partId);

        return partGrpSvc.refreshAndGetLeaderWithTerm()
                .exceptionally(throwable -> {
                    throwable = unwrapCause(throwable);

                    if (throwable instanceof TimeoutException) {
                        LOG.info("Node couldn't get the leader within timeout so the changing peers is skipped [grp={}].", replicaGrpId);

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
                            leaderWithTerm.leader(), replicaGrpId, partId, table.name());

                    return metaStorageMgr.get(pendingPartAssignmentsKey(replicaGrpId))
                            .thenCompose(latestPendingAssignmentsEntry -> {
                                // Do not change peers of the raft group if this is a stale event.
                                // Note that we start raft node before for the sake of the consistency in a
                                // starting and stopping raft nodes.
                                if (revision < latestPendingAssignmentsEntry.revision()) {
                                    return nullCompletedFuture();
                                }

                                PeersAndLearners newConfiguration =
                                        configurationFromAssignments(pendingAssignments);

                                return partGrpSvc.changePeersAsync(newConfiguration, leaderWithTerm.term());
                            });
                });
    }

    private void startPartitionRaftGroupNode(
            ZonePartitionId zonePartitionId,
            RaftNodeId raftNodeId,
            PeersAndLearners stableConfiguration,
            PendingComparableValuesTracker<HybridTimestamp, Void> safeTimeTracker,
            PendingComparableValuesTracker<Long, Void> storageIndexTracker,
            TableImpl table,
            TxStateStorage txStatePartitionStorage,
            PartitionDataStorage partitionDataStorage,
            PartitionUpdateHandlers partitionUpdateHandlers
    ) throws NodeStoppingException {
        InternalTable internalTable = table.internalTable();

        RaftGroupOptions groupOptions = groupOptionsForPartition(
                internalTable.storage(),
                internalTable.txStateStorage(),
                partitionKey(internalTable, zonePartitionId.partitionId()),
                partitionUpdateHandlers
        );

        RaftGroupListener raftGrpLsnr = new PartitionListener(
                txManager,
                partitionDataStorage,
                partitionUpdateHandlers.storageUpdateHandler,
                txStatePartitionStorage,
                safeTimeTracker,
                storageIndexTracker,
                catalogService,
                table.schemaView(),
                clockService
        );

        RaftGroupEventsListener raftGrpEvtsLsnr = new RebalanceRaftGroupEventsListener(
                metaStorageMgr,
                zonePartitionId,
                busyLock,
                createPartitionMover(internalTable, zonePartitionId.partitionId()),
                rebalanceScheduler,
                table.tableId()
        );

        // TODO: use RaftManager interface, see https://issues.apache.org/jira/browse/IGNITE-18273
        ((Loza) raftMgr).startRaftGroupNodeWithoutService(
                raftNodeId,
                stableConfiguration,
                raftGrpLsnr,
                raftGrpEvtsLsnr,
                groupOptions
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
                    int zoneId = extractZoneId(key, ASSIGNMENTS_SWITCH_REDUCE_PREFIX);

                    ZonePartitionId replicaGrpId = new ZonePartitionId(zoneId, partitionId);

                    // It is safe to get the latest version of the catalog as we are in the metastore thread.
                    int catalogVersion = catalogService.latestCatalogVersion();

                    return tablesById(evt.revision())
                            .thenCompose(tables -> inBusyLockAsync(busyLock, () -> {
                                CatalogZoneDescriptor zoneDescriptor = catalogService.zone(zoneId, catalogVersion);

                                long causalityToken = zoneDescriptor.updateToken();

                                return distributionZoneManager.dataNodes(causalityToken, catalogVersion, zoneId)
                                        .thenCompose(dataNodes -> RebalanceUtilEx.handleReduceChanged(
                                                metaStorageMgr,
                                                dataNodes,
                                                zoneDescriptor.replicas(),
                                                replicaGrpId,
                                                evt
                                        ));
                            }));
                });
            }

            @Override
            public void onError(Throwable e) {
                LOG.warn("Unable to process switch reduce event", e);
            }
        };
    }

    private PartitionMover createPartitionMover(InternalTable internalTable, int partId) {
        return new PartitionMover(busyLock, () -> internalTable.tableRaftService().partitionRaftGroupService(partId));
    }

    private static PeersAndLearners configurationFromAssignments(Collection<Assignment> assignments) {
        var peers = new HashSet<String>();
        var learners = new HashSet<String>();

        for (Assignment assignment : assignments) {
            if (assignment.isPeer()) {
                peers.add(assignment.consistentId());
            } else {
                learners.add(assignment.consistentId());
            }
        }

        return PeersAndLearners.fromConsistentIds(peers, learners);
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
        int zoneId = extractZoneId(stableAssignmentsWatchEvent.key(), STABLE_ASSIGNMENTS_PREFIX);

        ZonePartitionId zonePartitionId = new ZonePartitionId(zoneId, partitionId);

        Set<Assignment> stableAssignments = stableAssignmentsWatchEvent.value() == null
                ? emptySet()
                : Assignments.fromBytes(stableAssignmentsWatchEvent.value()).nodes();

        return supplyAsync(() -> {
            Entry pendingAssignmentsEntry = metaStorageMgr.getLocally(pendingPartAssignmentsKey(zonePartitionId), revision);

            byte[] pendingAssignmentsFromMetaStorage = pendingAssignmentsEntry.value();

            Assignments pendingAssignments = pendingAssignmentsFromMetaStorage == null
                    ? Assignments.EMPTY
                    : Assignments.fromBytes(pendingAssignmentsFromMetaStorage);

            HybridTimestamp msSafeTime = metaStorageMgr.timestampByRevision(revision);

            int catalogVersion = catalogService.activeCatalogVersion(msSafeTime.longValue());

            Set<Integer> tablesInZone = findTablesByZoneId(zoneId, catalogVersion, catalogService).stream()
                    .map(CatalogObjectDescriptor::id)
                    .collect(toSet());

            List<CompletableFuture<?>> tableFutures = new ArrayList<>(tablesInZone.size());

            for (Integer tableId : tablesInZone) {
                tableFutures.add(stopAndDestroyPartitionAndUpdateClients(
                        new TablePartitionId(tableId, partitionId),
                        stableAssignments,
                        pendingAssignments,
                        isRecovery,
                        revision
                ));
            }

            return allOf(tableFutures.toArray(CompletableFuture[]::new));
        }, ioExecutor).thenCompose(identity());
    }

    private CompletableFuture<Void> updatePartitionClients(
            TablePartitionId tablePartitionId,
            Set<Assignment> stableAssignments,
            long revision
    ) {
        // Update raft client peers and learners according to the actual assignments.
        return tablesById(revision).thenAccept(t -> {
            t.get(tablePartitionId.tableId()).internalTable()
                    .tableRaftService()
                    .partitionRaftGroupService(tablePartitionId.partitionId())
                    .updateConfiguration(configurationFromAssignments(stableAssignments));
        });
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
                : updatePartitionClients(tablePartitionId, stableAssignments, revision);

        boolean shouldStopLocalServices = (pendingAssignments.force()
                        ? pendingAssignments.nodes().stream()
                        : Stream.concat(stableAssignments.stream(), pendingAssignments.nodes().stream())
                )
                .noneMatch(assignment -> assignment.consistentId().equals(localNode().name()));

        if (shouldStopLocalServices) {
            return allOf(
                    clientUpdateFuture,
                    stopAndDestroyPartition(tablePartitionId, revision)
            );
        } else {
            return clientUpdateFuture;
        }
    }

    private CompletableFuture<Void> stopAndDestroyPartition(TablePartitionId tablePartitionId, long causalityToken) {
        return tablesVv.get(causalityToken)
                .thenCompose(ignore -> {
                    TableImpl table = tables.get(tablePartitionId.tableId());

                    return stopPartition(tablePartitionId, table)
                            .thenComposeAsync(v -> destroyPartitionStorages(tablePartitionId.partitionId(), table), ioExecutor);
                });
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
                .thenCompose(v -> {
                    try {
                        raftMgr.stopRaftNodes(tablePartitionId);
                    } catch (NodeStoppingException ignored) {
                        // No-op.
                    }

                    return mvGc.removeStorage(tablePartitionId);
                });
    }

    private CompletableFuture<Void> destroyPartitionStorages(int partitionId, TableImpl table) {
        // TODO: IGNITE-18703 Destroy raft log and meta
        if (table == null) {
            return nullCompletedFuture();
        }

        InternalTable internalTable = table.internalTable();

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
     * successfully or exceptionally. Allows to protect from getting stuck at {@link #stop()} when someone is blocked (by using
     * {@link #busyLock}) for a long time.
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
    public CompletableFuture<Void> restartPartition(TablePartitionId tablePartitionId, long revision) {
        return inBusyLockAsync(busyLock, () -> tablesVv.get(revision).thenComposeAsync(unused -> inBusyLockAsync(busyLock, () -> {
            TableImpl table = tables.get(tablePartitionId.tableId());
            ZonePartitionId zonePartitionId = new ZonePartitionId(table.internalTable().zoneId(), tablePartitionId.partitionId());

            return stopPartition(tablePartitionId, table).thenComposeAsync(unused1 -> {
                Assignments stableAssignments = stableAssignments(zonePartitionId, revision);

                assert stableAssignments != null : "tablePartitionId=" + tablePartitionId + ", revision=" + revision;

                int zoneId = zonePartitionId.zoneId();

                return startPartitionAndStartClient(table, tablePartitionId.partitionId(), stableAssignments, null, zoneId, false);
            }, ioExecutor);
        }), ioExecutor));
    }

    private @Nullable Assignments stableAssignments(ZonePartitionId zonePartitionId, long revision) {
        Entry entry = metaStorageMgr.getLocally(stablePartAssignmentsKey(zonePartitionId), revision);

        return Assignments.fromBytes(entry.value());
    }
}
