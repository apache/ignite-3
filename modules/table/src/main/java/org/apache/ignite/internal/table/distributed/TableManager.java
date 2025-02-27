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
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.ASSIGNMENTS_SWITCH_REDUCE_PREFIX_BYTES;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.PENDING_ASSIGNMENTS_PREFIX_BYTES;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.STABLE_ASSIGNMENTS_PREFIX_BYTES;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.assignmentsChainGetLocally;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.assignmentsChainKey;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.extractTablePartitionId;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.partitionAssignmentsGetLocally;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.pendingPartAssignmentsKey;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.stableAssignmentsGetLocally;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.stablePartAssignmentsKey;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.subtract;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.tableAssignmentsChainGetLocally;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.tableAssignmentsGetLocally;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.tablePendingAssignmentsGetLocally;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.union;
import static org.apache.ignite.internal.event.EventListener.fromConsumer;
import static org.apache.ignite.internal.hlc.HybridTimestamp.LOGICAL_TIME_BITS_SIZE;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.enabledColocation;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.partitiondistribution.PartitionDistributionUtils.calculateAssignmentForPartition;
import static org.apache.ignite.internal.raft.PeersAndLearners.fromAssignments;
import static org.apache.ignite.internal.raft.RaftGroupConfiguration.UNKNOWN_INDEX;
import static org.apache.ignite.internal.raft.RaftGroupConfiguration.UNKNOWN_TERM;
import static org.apache.ignite.internal.table.distributed.TableUtils.droppedTables;
import static org.apache.ignite.internal.table.distributed.index.IndexUtils.registerIndexesToTable;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_READ;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_WRITE;
import static org.apache.ignite.internal.util.ByteUtils.toByteArray;
import static org.apache.ignite.internal.util.CompletableFutures.allOfToList;
import static org.apache.ignite.internal.util.CompletableFutures.copyStateTo;
import static org.apache.ignite.internal.util.CompletableFutures.emptyListCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.trueCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
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
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CreateTableEventParameters;
import org.apache.ignite.internal.catalog.events.DropTableEventParameters;
import org.apache.ignite.internal.catalog.events.RenameTableEventParameters;
import org.apache.ignite.internal.causality.CompletionListener;
import org.apache.ignite.internal.causality.IncrementalVersionedValue;
import org.apache.ignite.internal.causality.RevisionListenerRegistry;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.utils.SystemDistributedConfigurationPropertyHolder;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.DistributionZonesUtil;
import org.apache.ignite.internal.distributionzones.rebalance.PartitionMover;
import org.apache.ignite.internal.distributionzones.rebalance.RebalanceRaftGroupEventsListener;
import org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
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
import org.apache.ignite.internal.metastorage.Revisions;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.internal.partition.replicator.LocalPartitionReplicaEvent;
import org.apache.ignite.internal.partition.replicator.LocalPartitionReplicaEventParameters;
import org.apache.ignite.internal.partition.replicator.PartitionReplicaLifecycleManager;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.replication.ChangePeersAndLearnersAsyncReplicaRequest;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionDataStorage;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionKey;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionSnapshotStorageFactory;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionTxStateAccessImpl;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.ZonePartitionKey;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.partition.replicator.schema.CatalogValidationSchemasSource;
import org.apache.ignite.internal.partition.replicator.schema.ExecutorInclinedSchemaSyncService;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.partitiondistribution.AssignmentsChain;
import org.apache.ignite.internal.partitiondistribution.PartitionDistributionUtils;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.raft.ExecutorInclinedRaftCommandRunner;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftGroupEventsListener;
import org.apache.ignite.internal.raft.service.RaftCommandRunner;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.raft.storage.SnapshotStorageFactory;
import org.apache.ignite.internal.replicator.PartitionGroupId;
import org.apache.ignite.internal.replicator.Replica;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ReplicaManager.WeakReplicaStopReason;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.ReplicaMessageUtils;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.TablePartitionIdMessage;
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
import org.apache.ignite.internal.table.distributed.raft.MinimumRequiredTimeCollectorService;
import org.apache.ignite.internal.table.distributed.raft.PartitionListener;
import org.apache.ignite.internal.table.distributed.raft.snapshot.FullStateTransferIndexChooser;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionMvStorageAccessImpl;
import org.apache.ignite.internal.table.distributed.raft.snapshot.SnapshotAwarePartitionDataStorage;
import org.apache.ignite.internal.table.distributed.raft.snapshot.TablePartitionKey;
import org.apache.ignite.internal.table.distributed.replicator.PartitionReplicaListener;
import org.apache.ignite.internal.table.distributed.replicator.TransactionStateResolver;
import org.apache.ignite.internal.table.distributed.schema.SchemaVersions;
import org.apache.ignite.internal.table.distributed.schema.SchemaVersionsImpl;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.table.distributed.storage.NullStorageEngine;
import org.apache.ignite.internal.table.distributed.storage.PartitionStorages;
import org.apache.ignite.internal.table.distributed.wrappers.ExecutorInclinedPlacementDriver;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.tx.impl.RemotelyTriggeredResourceRegistry;
import org.apache.ignite.internal.tx.impl.TransactionInflights;
import org.apache.ignite.internal.tx.impl.TxMessageSender;
import org.apache.ignite.internal.tx.storage.state.ThreadAssertingTxStateStorage;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.rocksdb.TxStateRocksDbSharedStorage;
import org.apache.ignite.internal.tx.storage.state.rocksdb.TxStateRocksDbStorage;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.Lazy;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.internal.util.SafeTimeValuesTracker;
import org.apache.ignite.internal.utils.RebalanceUtilEx;
import org.apache.ignite.internal.worker.ThreadAssertions;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.ClusterNode;
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

    /** Replica messages factory. */
    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    /** Table messages factory. */
    private static final PartitionReplicationMessagesFactory TABLE_MESSAGES_FACTORY = new PartitionReplicationMessagesFactory();

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
     * <p>Only explicitly updated in
     * {@link #startLocalPartitionsAndClients(CompletableFuture, List, List, TableImpl, boolean, long)}.
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

    private final TxStateRocksDbSharedStorage sharedTxStateStorage;

    /** Scan request executor. */
    private final ExecutorService scanRequestExecutor;

    /**
     * Separate executor for IO operations like partition storage initialization or partition raft group meta data persisting.
     */
    private final ExecutorService ioExecutor;

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

    private int attemptsObtainLock;

    @Nullable
    private ScheduledExecutorService streamerFlushExecutor;

    private final IndexMetaStorage indexMetaStorage;

    private final Predicate<Assignment> isLocalNodeAssignment = assignment -> assignment.consistentId().equals(localNode().name());

    private final MinimumRequiredTimeCollectorService minTimeCollectorService;

    @Nullable
    private StreamerReceiverRunner streamerReceiverRunner;

    private final CompletableFuture<Void> readyToProcessReplicaStarts = new CompletableFuture<>();

    private final Map<Integer, Set<TableImpl>> tablesPerZone = new ConcurrentHashMap<>();

    private final CompletableFuture<Void> recoveryFuture = new CompletableFuture<>();

    /** Configuration of rebalance retries delay. */
    private final SystemDistributedConfigurationPropertyHolder<Integer> rebalanceRetryDelayConfiguration;

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
     * @param commonScheduler Common Scheduled executor. Needed only for asynchronous start of scheduled operations without performing
     *      blocking, long or IO operations.
     * @param clockService hybrid logical clock service.
     * @param placementDriver Placement driver.
     * @param sql A supplier function that returns {@link IgniteSql}.
     * @param lowWatermark Low watermark.
     * @param transactionInflights Transaction inflights.
     * @param indexMetaStorage Index meta storage.
     * @param partitionReplicaLifecycleManager Partition replica lifecycle manager.
     * @param minTimeCollectorService Collects minimum required timestamp for each partition.
     * @param systemDistributedConfiguration System distributed configuration.
     */
    public TableManager(
            String nodeName,
            RevisionListenerRegistry registry,
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
            TxStateRocksDbSharedStorage txStateRocksDbSharedStorage,
            MetaStorageManager metaStorageMgr,
            SchemaManager schemaManager,
            ExecutorService ioExecutor,
            Executor partitionOperationsExecutor,
            ScheduledExecutorService rebalanceScheduler,
            ScheduledExecutorService commonScheduler,
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
            PartitionReplicaLifecycleManager partitionReplicaLifecycleManager,
            MinimumRequiredTimeCollectorService minTimeCollectorService,
            SystemDistributedConfiguration systemDistributedConfiguration
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
        this.minTimeCollectorService = minTimeCollectorService;

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

        this.sharedTxStateStorage = txStateRocksDbSharedStorage;

        fullStateTransferIndexChooser = new FullStateTransferIndexChooser(catalogService, lowWatermark, indexMetaStorage);

        partitionReplicaLifecycleManager.listen(
                LocalPartitionReplicaEvent.BEFORE_REPLICA_STARTED,
                this::beforeZoneReplicaStarted
        );

        partitionReplicaLifecycleManager.listen(
                LocalPartitionReplicaEvent.AFTER_REPLICA_DESTROYED,
                this::onZoneReplicaDestroyed
        );

        rebalanceRetryDelayConfiguration = new SystemDistributedConfigurationPropertyHolder<>(
                systemDistributedConfiguration,
                (v, r) -> {},
                DistributionZonesUtil.REBALANCE_RETRY_DELAY_MS,
                DistributionZonesUtil.REBALANCE_RETRY_DELAY_DEFAULT,
                Integer::parseInt
        );
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        return inBusyLockAsync(busyLock, () -> {
            mvGc.start();

            transactionStateResolver.start();

            fullStateTransferIndexChooser.start();

            CompletableFuture<Revisions> recoveryFinishFuture = metaStorageMgr.recoveryFinishedFuture();

            assert recoveryFinishFuture.isDone();

            rebalanceRetryDelayConfiguration.init();

            long recoveryRevision = recoveryFinishFuture.join().revision();

            cleanUpResourcesForDroppedTablesOnRecoveryBusy();

            startTables(recoveryRevision, lowWatermark.getLowWatermark());

            processAssignmentsOnRecovery(recoveryRevision);

            if (!enabledColocation()) {
                metaStorageMgr.registerPrefixWatch(new ByteArray(PENDING_ASSIGNMENTS_PREFIX_BYTES), pendingAssignmentsRebalanceListener);
                metaStorageMgr.registerPrefixWatch(new ByteArray(STABLE_ASSIGNMENTS_PREFIX_BYTES), stableAssignmentsRebalanceListener);
                metaStorageMgr.registerPrefixWatch(
                        new ByteArray(ASSIGNMENTS_SWITCH_REDUCE_PREFIX_BYTES),
                        assignmentsSwitchRebalanceListener
                );
            }

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

            attemptsObtainLock = txCfg.attemptsObtainLock().value();

            if (!enabledColocation()) {
                executorInclinedPlacementDriver.listen(PrimaryReplicaEvent.PRIMARY_REPLICA_EXPIRED, this::onPrimaryReplicaExpired);
            }
            return nullCompletedFuture();
        });
    }

    @TestOnly
    public CompletableFuture<Void> recoveryFuture() {
        return recoveryFuture;
    }

    private CompletableFuture<Void> waitForMetadataCompleteness(long ts) {
        return executorInclinedSchemaSyncService.waitForMetadataCompleteness(HybridTimestamp.hybridTimestamp(ts));
    }

    private CompletableFuture<Boolean> beforeZoneReplicaStarted(LocalPartitionReplicaEventParameters parameters) {
        if (!enabledColocation()) {
            return falseCompletedFuture();
        }

        return inBusyLockAsync(busyLock, () -> readyToProcessReplicaStarts
                .thenCompose(v -> {
                    ZonePartitionId zonePartitionId = parameters.zonePartitionId();

                    Set<TableImpl> zoneTables = zoneTables(zonePartitionId.zoneId());

                    PartitionSet singlePartitionIdSet = PartitionSet.of(zonePartitionId.partitionId());

                    CompletableFuture<?>[] futures = zoneTables.stream()
                            .map(tbl -> inBusyLockAsync(busyLock, () -> {
                                return getOrCreatePartitionStorages(tbl, singlePartitionIdSet)
                                        .thenRunAsync(() -> inBusyLock(busyLock, () -> {
                                            lowWatermark.getLowWatermarkSafe(lwm ->
                                                    registerIndexesToTable(tbl, catalogService, singlePartitionIdSet, tbl.schemaView(), lwm)
                                            );

                                            preparePartitionResourcesAndLoadToZoneReplica(tbl, zonePartitionId);
                                        }), ioExecutor);
                            }))
                            .toArray(CompletableFuture[]::new);

                    return allOf(futures);
                })
                .thenApply(unused -> false)
        );
    }

    private CompletableFuture<Boolean> onZoneReplicaDestroyed(LocalPartitionReplicaEventParameters parameters) {
        if (!enabledColocation()) {
            return falseCompletedFuture();
        }

        return inBusyLockAsync(busyLock, () -> {
            Set<TableImpl> zoneTables = zoneTables(parameters.zonePartitionId().zoneId());

            CompletableFuture<?>[] futures = zoneTables.stream()
                    .map(table -> {
                        closePartitionTrackers(table.internalTable(), parameters.zonePartitionId().partitionId());

                        TablePartitionId tablePartitionId = new TablePartitionId(
                                table.tableId(),
                                parameters.zonePartitionId().partitionId()
                        );

                        return mvGc.removeStorage(tablePartitionId)
                                .thenComposeAsync(
                                        v -> inBusyLockAsync(busyLock, () -> weakStopAndDestroyPartition(
                                                tablePartitionId,
                                                parameters.causalityToken())
                                        ),
                                        ioExecutor
                                );
                    })
                    .toArray(CompletableFuture[]::new);

            return allOf(futures);
        }).thenApply((unused) -> false);
    }

    private CompletableFuture<Boolean> prepareTableResourcesAndLoadToZoneReplica(CreateTableEventParameters parameters) {
        if (!enabledColocation()) {
            return falseCompletedFuture();
        }

        long causalityToken = parameters.causalityToken();
        CatalogTableDescriptor tableDescriptor = parameters.tableDescriptor();
        CatalogZoneDescriptor zoneDescriptor = getZoneDescriptor(tableDescriptor, parameters.catalogVersion());
        CatalogSchemaDescriptor schemaDescriptor = getSchemaDescriptor(tableDescriptor, parameters.catalogVersion());

        return prepareTableResourcesAndLoadToZoneReplica(causalityToken, zoneDescriptor, tableDescriptor, schemaDescriptor, false)
                .thenApply(v -> false);
    }

    private CompletableFuture<Void> prepareTableResourcesAndLoadToZoneReplica(
            long causalityToken,
            CatalogZoneDescriptor zoneDescriptor,
            CatalogTableDescriptor tableDescriptor,
            CatalogSchemaDescriptor schemaDescriptor,
            boolean onNodeRecovery
    ) {
        TableImpl table = createTableImpl(causalityToken, tableDescriptor, zoneDescriptor, schemaDescriptor);

        int tableId = tableDescriptor.id();

        tablesVv.update(causalityToken, (ignore, e) -> inBusyLock(busyLock, () -> {
            if (e != null) {
                return failedFuture(e);
            }

            return schemaManager.schemaRegistry(causalityToken, tableId).thenAccept(table::schemaView);
        }));

        long stamp = partitionReplicaLifecycleManager.lockZoneForRead(zoneDescriptor.id());

        // NB: all vv.update() calls must be made from the synchronous part of the method (not in thenCompose()/etc!).
        CompletableFuture<?> localPartsUpdateFuture = localPartitionsVv.update(causalityToken,
                (ignore, throwable) -> inBusyLock(busyLock, () -> supplyAsync(() -> {
                    PartitionSet parts = new BitSetPartitionSet();

                    for (int i = 0; i < zoneDescriptor.partitions(); i++) {
                        if (partitionReplicaLifecycleManager.hasLocalPartition(new ZonePartitionId(zoneDescriptor.id(), i))) {
                            parts.set(i);
                        }
                    }

                    return getOrCreatePartitionStorages(table, parts).thenRun(() -> localPartsByTableId.put(tableId, parts));
                }, ioExecutor).thenCompose(identity())));

        CompletableFuture<?> tablesByIdFuture = tablesVv.get(causalityToken);

        CompletableFuture<?> createPartsFut = assignmentsUpdatedVv.update(causalityToken, (token, e) -> {
            if (e != null) {
                return failedFuture(e);
            }

            return allOf(localPartsUpdateFuture, tablesByIdFuture).thenRunAsync(() -> inBusyLock(busyLock, () -> {
                        if (onNodeRecovery) {
                            SchemaRegistry schemaRegistry = table.schemaView();
                            PartitionSet partitionSet = localPartsByTableId.get(tableId);
                            // LWM starts updating only after the node is restored.
                            HybridTimestamp lwm = lowWatermark.getLowWatermark();

                            registerIndexesToTable(table, catalogService, partitionSet, schemaRegistry, lwm);
                        }

                        for (int i = 0; i < zoneDescriptor.partitions(); i++) {
                            var zonePartitionId = new ZonePartitionId(zoneDescriptor.id(), i);

                            if (partitionReplicaLifecycleManager.hasLocalPartition(zonePartitionId)) {
                                preparePartitionResourcesAndLoadToZoneReplica(table, zonePartitionId);
                            }
                        }
                    }

            ), ioExecutor);
        });

        tables.put(tableId, table);

        // TODO: https://issues.apache.org/jira/browse/IGNITE-19913 Possible performance degradation.
        return createPartsFut.thenAccept(ignore -> startedTables.put(tableId, table))
                .whenComplete((v, th) -> {
                    partitionReplicaLifecycleManager.unlockZoneForRead(zoneDescriptor.id(), stamp);

                    if (th == null) {
                        addTableToZone(zoneDescriptor.id(), table);
                    }
                });
    }

    /**
     * Prepare the table partition resources and load it to the zone-based replica.
     *
     * @param table Table.
     * @param zonePartitionId Zone Partition ID.
     */
    private void preparePartitionResourcesAndLoadToZoneReplica(TableImpl table, ZonePartitionId zonePartitionId) {
        int partId = zonePartitionId.partitionId();

        int tableId = table.tableId();

        var internalTbl = (InternalTableImpl) table.internalTable();

        var tablePartitionId = new TablePartitionId(tableId, partId);

        inBusyLock(busyLock, () -> {
            var safeTimeTracker = new SafeTimeValuesTracker(HybridTimestamp.MIN_VALUE);

            // TODO https://issues.apache.org/jira/browse/IGNITE-22522 After switching to the colocation track, the storageIndexTracker
            //  will no longer need to be transferred to the table listeners.
            var storageIndexTracker = new PendingComparableValuesTracker<Long, Void>(0L) {
                @Override
                public void update(Long newValue, @Nullable Void futureResult) {
                    throw new UnsupportedOperationException("It's not expected that in case of enabled colocation table storageIndexTracker"
                            + " will be updated.");
                }

                @Override
                public CompletableFuture<Void> waitFor(Long valueToWait) {
                    throw new UnsupportedOperationException("It's not expected that in case of enabled colocation table storageIndexTracker"
                            + " will be updated.");
                }
            };

            PartitionStorages partitionStorages = getPartitionStorages(table, partId);

            PartitionDataStorage partitionDataStorage = partitionDataStorage(
                    new ZonePartitionKey(zonePartitionId.zoneId(), partId),
                    tableId,
                    partitionStorages.getMvPartitionStorage()
            );

            PartitionUpdateHandlers partitionUpdateHandlers = createPartitionUpdateHandlers(
                    partId,
                    partitionDataStorage,
                    table,
                    safeTimeTracker,
                    storageUpdateConfig
            );

            internalTbl.updatePartitionTrackers(partId, safeTimeTracker, storageIndexTracker);

            mvGc.addStorage(tablePartitionId, partitionUpdateHandlers.gcUpdateHandler);

            minTimeCollectorService.addPartition(new TablePartitionId(tableId, partId));

            Function<RaftCommandRunner, ReplicaListener> createListener = raftClient -> createReplicaListener(
                    zonePartitionId,
                    table,
                    safeTimeTracker,
                    partitionStorages.getMvPartitionStorage(),
                    partitionStorages.getTxStateStorage(),
                    partitionUpdateHandlers,
                    raftClient
            );

            var tablePartitionRaftListener = new PartitionListener(
                    txManager,
                    partitionDataStorage,
                    partitionUpdateHandlers.storageUpdateHandler,
                    partitionStorages.getTxStateStorage(),
                    safeTimeTracker,
                    storageIndexTracker,
                    catalogService,
                    table.schemaView(),
                    indexMetaStorage,
                    topologyService.localMember().id(),
                    minTimeCollectorService
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
                    tablePartitionId,
                    createListener,
                    tablePartitionRaftListener,
                    partitionStorageAccess
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
        var stableAssignmentsPrefix = new ByteArray(STABLE_ASSIGNMENTS_PREFIX_BYTES);
        var pendingAssignmentsPrefix = new ByteArray(PENDING_ASSIGNMENTS_PREFIX_BYTES);

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
        )).whenComplete((v, th) -> {
            if (th != null) {
                recoveryFuture.completeExceptionally(th);
            } else {
                recoveryFuture.complete(null);
            }
        });
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
            ConsistencyMode consistencyMode,
            CompletableFuture<List<Assignments>> assignmentsFuture
    ) {
        return assignmentsFuture.thenCompose(newAssignments -> {
            assert !newAssignments.isEmpty();

            boolean haMode = consistencyMode == ConsistencyMode.HIGH_AVAILABILITY;

            List<Operation> partitionAssignments = new ArrayList<>(newAssignments.size());

            for (int i = 0; i < newAssignments.size(); i++) {
                TablePartitionId tablePartitionId = new TablePartitionId(tableId, i);

                ByteArray stableAssignmentsKey = stablePartAssignmentsKey(tablePartitionId);
                byte[] anAssignment = newAssignments.get(i).toBytes();
                Operation op = put(stableAssignmentsKey, anAssignment);
                partitionAssignments.add(op);

                if (haMode) {
                    ByteArray assignmentsChainKey = assignmentsChainKey(tablePartitionId);
                    byte[] assignmentChain = AssignmentsChain.of(UNKNOWN_TERM, UNKNOWN_INDEX, newAssignments.get(i)).toBytes();
                    Operation chainOp = put(assignmentsChainKey, assignmentChain);
                    partitionAssignments.add(chainOp);
                }
            }

            Condition condition = notExists(new ByteArray(toByteArray(partitionAssignments.get(0).key())));

            return metaStorageMgr
                    .invoke(condition, partitionAssignments, Collections.emptyList())
                    .whenComplete((invokeResult, e) -> {
                        if (e != null) {
                            LOG.error(
                                    "Couldn't write assignments [assignmentsList={}] to metastore during invoke.",
                                    e,
                                    Assignments.assignmentListToString(newAssignments)
                            );
                        }
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
                    .whenComplete((realAssignments, e) -> {
                        if (e != null) {
                            LOG.error("Couldn't get assignments from metastore for table [tableId={}].", e, tableId);
                        }
                    });
        });
    }

    private void onTableDrop(DropTableEventParameters parameters) {
        inBusyLock(busyLock, () -> {
            destructionEventsQueue.enqueue(new DestroyTableEvent(parameters.catalogVersion(), parameters.tableId()));
        });
    }

    private CompletableFuture<Boolean> onLwmChanged(ChangeLowWatermarkEventParameters parameters) {
        if (!busyLock.enterBusy()) {
            return falseCompletedFuture();
        }

        try {
            int newEarliestCatalogVersion = catalogService.activeCatalogVersion(parameters.newLowWatermark().longValue());

            List<CompletableFuture<Void>> futures = destructionEventsQueue.drainUpTo(newEarliestCatalogVersion).stream()
                    .map(event -> destroyTableLocally(event.tableId()))
                    .collect(toList());

            return allOf(futures.toArray(CompletableFuture[]::new)).thenApply(unused -> false);
        } catch (Throwable t) {
            return failedFuture(t);
        } finally {
            busyLock.leaveBusy();
        }
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
     * @param stableAssignmentsFuture Table assignments.
     * @param table Initialized table entity.
     * @param isRecovery {@code true} if the node is being started up.
     * @param assignmentsTimestamp Assignments timestamp.
     * @return future, which will be completed when the partitions creations done.
     */
    private CompletableFuture<Void> startLocalPartitionsAndClients(
            CompletableFuture<List<Assignments>> stableAssignmentsFuture,
            List<@Nullable Assignments> pendingAssignmentsForPartitions,
            List<@Nullable AssignmentsChain> assignmentsChains,
            TableImpl table,
            boolean isRecovery,
            long assignmentsTimestamp
    ) {
        int tableId = table.tableId();

        // Create new raft nodes according to new assignments.
        return stableAssignmentsFuture.thenCompose(stableAssignmentsForPartitions -> {
            // Empty assignments might be a valid case if tables are created from within cluster init HOCON
            // configuration, which is not supported now.
            assert stableAssignmentsForPartitions != null
                    : IgniteStringFormatter.format("Table [id={}] has empty assignments.", tableId);

            int partitions = stableAssignmentsForPartitions.size();

            var futures = new CompletableFuture<?>[partitions];

            for (int i = 0; i < partitions; i++) {
                int partId = i;

                Assignments stableAssignments = stableAssignmentsForPartitions.get(i);

                Assignments pendingAssignments = pendingAssignmentsForPartitions.get(i);

                Assignment localMemberAssignmentInStable = localMemberAssignment(stableAssignments);

                boolean shouldStartPartition;

                if (isRecovery) {
                    AssignmentsChain assignmentsChain = assignmentsChains.get(i);

                    if (lastRebalanceWasGraceful(assignmentsChain)) {
                        // The condition to start the replica is
                        // `pending.contains(node) || (stable.contains(node) && !pending.isForce())`.
                        // However we check only the right part of this condition here
                        // since after `startTables` we have a call to `processAssignmentsOnRecovery`,
                        // which executes pending assignments update and will start required partitions there.
                        shouldStartPartition = localMemberAssignmentInStable != null
                                && (pendingAssignments == null || !pendingAssignments.force());
                    } else {
                        // TODO: Use logic from https://issues.apache.org/jira/browse/IGNITE-23874
                        LOG.warn("Recovery after a forced rebalance for table is not supported yet [tableId={}, partitionId={}].",
                                tableId, partId);
                        shouldStartPartition = localMemberAssignmentInStable != null
                                && (pendingAssignments == null || !pendingAssignments.force());
                    }
                } else {
                    shouldStartPartition = localMemberAssignmentInStable != null;
                }

                if (shouldStartPartition) {
                    futures[i] = startPartitionAndStartClient(
                            table,
                            partId,
                            localMemberAssignmentInStable,
                            stableAssignments,
                            isRecovery,
                            assignmentsTimestamp
                    ).whenComplete((res, ex) -> {
                        if (ex != null) {
                            LOG.warn("Unable to update raft groups on the node [tableId={}, partitionId={}]", ex, tableId, partId);
                        }
                    });
                } else {
                    futures[i] = nullCompletedFuture();
                }
            }

            return allOf(futures);
        });
    }

    private CompletableFuture<Void> startPartitionAndStartClient(
            TableImpl table,
            int partId,
            Assignment localMemberAssignment,
            Assignments stableAssignments,
            boolean isRecovery,
            long assignmentsTimestamp
    ) {
        if (enabledColocation()) {
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
                        assignmentsTimestamp
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
                    var safeTimeTracker = new SafeTimeValuesTracker(HybridTimestamp.MIN_VALUE);

                    var storageIndexTracker = new PendingComparableValuesTracker<Long, Void>(0L);

                    PartitionStorages partitionStorages = getPartitionStorages(table, partId);

                    var partitionKey = new TablePartitionKey(tableId, partId);

                    PartitionDataStorage partitionDataStorage = partitionDataStorage(
                            partitionKey,
                            internalTbl.tableId(),
                            partitionStorages.getMvPartitionStorage()
                    );

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
                            indexMetaStorage,
                            topologyService.localMember().id(),
                            minTimeCollectorService
                    );

                    minTimeCollectorService.addPartition(new TablePartitionId(tableId, partId));

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
    private Assignment localMemberAssignment(@Nullable Assignments assignments) {
        Assignment localMemberAssignment = Assignment.forPeer(localNode().name());

        return assignments != null && assignments.nodes().contains(localMemberAssignment) ? localMemberAssignment : null;
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
                rebalanceScheduler,
                rebalanceRetryDelayConfiguration
        );
    }

    private PartitionReplicaListener createReplicaListener(
            // TODO https://issues.apache.org/jira/browse/IGNITE-22522 Use ZonePartitionIdInstead.
            PartitionGroupId replicationGroupId,
            TableImpl table,
            PendingComparableValuesTracker<HybridTimestamp, Void> safeTimeTracker,
            MvPartitionStorage mvPartitionStorage,
            TxStatePartitionStorage txStatePartitionStorage,
            PartitionUpdateHandlers partitionUpdateHandlers,
            RaftCommandRunner raftClient
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
                schemaManager.schemaRegistry(table.tableId()),
                indexMetaStorage,
                lowWatermark
        );
    }

    private CompletableFuture<Set<Assignment>> calculateAssignments(
            TablePartitionId tablePartitionId,
            Long assignmentsTimestamp
    ) {
        return waitForMetadataCompleteness(assignmentsTimestamp).thenCompose(unused -> {
            int catalogVersion = catalogService.activeCatalogVersion(assignmentsTimestamp);

            CatalogTableDescriptor tableDescriptor = getTableDescriptor(tablePartitionId.tableId(), catalogVersion);

            CatalogZoneDescriptor zoneDescriptor = getZoneDescriptor(tableDescriptor, catalogVersion);

            return distributionZoneManager.dataNodes(
                    zoneDescriptor.updateToken(),
                    catalogVersion,
                    tableDescriptor.zoneId()
            ).thenApply(dataNodes ->
                    calculateAssignmentForPartition(
                            dataNodes,
                            tablePartitionId.partitionId(),
                            zoneDescriptor.partitions(),
                            zoneDescriptor.replicas()
                    )
            );
        });
    }

    private boolean isLocalNodeInAssignments(Collection<Assignment> assignments) {
        return assignments.stream().anyMatch(isLocalNodeAssignment);
    }

    private CompletableFuture<Boolean> isLocalNodeIsPrimary(ReplicationGroupId replicationGroupId) {
        return isLocalNodeIsPrimary(getPrimaryReplica(replicationGroupId));
    }

    private CompletableFuture<Boolean> isLocalNodeIsPrimary(CompletableFuture<ReplicaMeta> primaryReplicaFuture) {
        return primaryReplicaFuture.thenApply(primaryReplicaMeta -> primaryReplicaMeta != null
                    && primaryReplicaMeta.getLeaseholder() != null
                    && primaryReplicaMeta.getLeaseholder().equals(localNode().name())
        );
    }

    /**
     * Returns future with {@link ReplicaMeta} if primary is presented or null otherwise.
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
     * possible timestamp, then we also couldn't have any primary replica, then return {@code null} as a future's result.
     *
     * @param replicationGroupId Replication group ID for that we check is the local node a primary.
     * @return completed future with primary replica's meta or {@code null} otherwise.
     */
    private CompletableFuture<ReplicaMeta> getPrimaryReplica(ReplicationGroupId replicationGroupId) {
        HybridTimestamp currentSafeTime = metaStorageMgr.clusterTime().currentSafeTime();

        if (HybridTimestamp.MIN_VALUE.equals(currentSafeTime)) {
            return nullCompletedFuture();
        }

        long skewMs = clockService.maxClockSkewMillis();

        try {
            HybridTimestamp previousMetastoreSafeTime = currentSafeTime.subtractPhysicalTime(skewMs);

            return executorInclinedPlacementDriver.getPrimaryReplica(replicationGroupId, previousMetastoreSafeTime);
        } catch (IllegalArgumentException e) {
            long currentSafeTimeMs = currentSafeTime.longValue();

            throw new AssertionError("Got a negative time [currentSafeTime=" + currentSafeTime
                    + ", currentSafeTimeMs=" + currentSafeTimeMs
                    + ", skewMs=" + skewMs
                    + ", internal=" + (currentSafeTimeMs + ((-skewMs) << LOGICAL_TIME_BITS_SIZE)) + "]", e);
        }
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

        if (!enabledColocation()) {
            metaStorageMgr.unregisterWatch(pendingAssignmentsRebalanceListener);
            metaStorageMgr.unregisterWatch(stableAssignmentsRebalanceListener);
            metaStorageMgr.unregisterWatch(assignmentsSwitchRebalanceListener);
        }

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
            futures.add(
                    supplyAsync(() -> tableStopFuture(table), ioExecutor).thenCompose(identity())
            );
        }

        try {
            allOf(futures.toArray(CompletableFuture[]::new)).get(30, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            LOG.error("Unable to clean table resources", e);
        }
    }

    private CompletableFuture<Void> tableStopFuture(TableImpl table) {
        InternalTable internalTable = table.internalTable();

        var stopReplicaFutures = new CompletableFuture<?>[internalTable.partitions()];

        for (int p = 0; p < internalTable.partitions(); p++) {
            TablePartitionId replicationGroupId = new TablePartitionId(table.tableId(), p);

            stopReplicaFutures[p] = stopPartition(replicationGroupId, table);
        }

        CompletableFuture<Void> stopPartitionReplicasFuture = allOf(stopReplicaFutures).orTimeout(10, TimeUnit.SECONDS);

        return stopPartitionReplicasFuture
                .whenCompleteAsync((res, ex) -> {
                    Stream.Builder<ManuallyCloseable> stopping = Stream.builder();

                    stopping.add(internalTable.storage());
                    stopping.add(internalTable.txStateStorage());
                    stopping.add(internalTable);

                    try {
                        IgniteUtils.closeAllManually(stopping.build());
                    } catch (Throwable e) {
                        throw new CompletionException(e);
                    }
                }, ioExecutor)
                .whenComplete((res, ex) -> {
                    if (ex != null) {
                        LOG.error("Unable to stop table [name={}, tableId={}]", ex, table.name(), table.tableId());
                    }
                });
    }

    /**
     * Create table storages and internal instance.
     *
     * @param causalityToken Causality token.
     * @param tableDescriptor Catalog table descriptor.
     * @param zoneDescriptor Catalog distribution zone descriptor.
     * @param schemaDescriptor Catalog schema descriptor.
     * @return Table instance.
     */
    private TableImpl createTableImpl(
            long causalityToken,
            CatalogTableDescriptor tableDescriptor,
            CatalogZoneDescriptor zoneDescriptor,
            CatalogSchemaDescriptor schemaDescriptor
    ) {
        QualifiedName tableName = QualifiedNameHelper.fromNormalized(schemaDescriptor.name(), tableDescriptor.name());

        LOG.trace("Creating local table: name={}, id={}, token={}", tableName.toCanonicalForm(), tableDescriptor.id(), causalityToken);

        MvTableStorage tableStorage = createTableStorage(tableDescriptor, zoneDescriptor);
        TxStateStorage txStateStorage = createTxStateTableStorage(tableDescriptor, zoneDescriptor);

        int partitions = zoneDescriptor.partitions();

        InternalTableImpl internalTable = new InternalTableImpl(
                tableName,
                zoneDescriptor.id(),
                tableDescriptor.id(),
                partitions,
                topologyService,
                txManager,
                tableStorage,
                txStateStorage,
                replicaSvc,
                clockService,
                observableTimestampTracker,
                executorInclinedPlacementDriver,
                transactionInflights,
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
            CatalogSchemaDescriptor schemaDescriptor = getSchemaDescriptor(tableDescriptor, catalogVersion);

            // Returns stable assignments.
            CompletableFuture<List<Assignments>> stableAssignmentsFuture = getOrCreateAssignments(
                    tableDescriptor,
                    zoneDescriptor,
                    causalityToken,
                    catalogVersion
            );

            List<Assignments> pendingAssignments =
                    tablePendingAssignmentsGetLocally(metaStorageMgr, tableId, zoneDescriptor.partitions(), causalityToken);

            List<AssignmentsChain> assignmentsChains =
                    tableAssignmentsChainGetLocally(metaStorageMgr, tableId, zoneDescriptor.partitions(), causalityToken);

            CompletableFuture<List<Assignments>> stableAssignmentsFutureAfterInvoke =
                    writeTableAssignmentsToMetastore(tableId, zoneDescriptor.consistencyMode(), stableAssignmentsFuture);

            Catalog catalog = catalogService.catalog(catalogVersion);

            return createTableLocally(
                    causalityToken,
                    tableDescriptor,
                    zoneDescriptor,
                    schemaDescriptor,
                    stableAssignmentsFutureAfterInvoke,
                    pendingAssignments,
                    assignmentsChains,
                    onNodeRecovery,
                    catalog.time()
            );
        });
    }

    /**
     * Creates local structures for a table.
     *
     * @param causalityToken Causality token.
     * @param tableDescriptor Catalog table descriptor.
     * @param zoneDescriptor Catalog distributed zone descriptor.
     * @param schemaDescriptor Catalog schema descriptor.
     * @param stableAssignmentsFuture Future with assignments.
     * @param onNodeRecovery {@code true} when called during node recovery, {@code false} otherwise.
     * @return Future that will be completed when local changes related to the table creation are applied.
     */
    private CompletableFuture<Void> createTableLocally(
            long causalityToken,
            CatalogTableDescriptor tableDescriptor,
            CatalogZoneDescriptor zoneDescriptor,
            CatalogSchemaDescriptor schemaDescriptor,
            CompletableFuture<List<Assignments>> stableAssignmentsFuture,
            List<Assignments> pendingAssignments,
            List<AssignmentsChain> assignmentsChains,
            boolean onNodeRecovery,
            long assignmentsTimestamp
    ) {
        TableImpl table = createTableImpl(causalityToken, tableDescriptor, zoneDescriptor, schemaDescriptor);

        int tableId = tableDescriptor.id();

        tablesVv.update(causalityToken, (ignore, e) -> inBusyLock(busyLock, () -> {
            if (e != null) {
                return failedFuture(e);
            }

            return schemaManager.schemaRegistry(causalityToken, tableId).thenAccept(table::schemaView);
        }));

        // NB: all vv.update() calls must be made from the synchronous part of the method (not in thenCompose()/etc!).
        CompletableFuture<?> localPartsUpdateFuture = localPartitionsVv.update(causalityToken,
                (ignore, throwable) -> inBusyLock(busyLock, () -> stableAssignmentsFuture.thenComposeAsync(newAssignments -> {
                    PartitionSet parts = new BitSetPartitionSet();

                    for (int i = 0; i < newAssignments.size(); i++) {
                        Assignments partitionAssignments = newAssignments.get(i);
                        if (localMemberAssignment(partitionAssignments) != null) {
                            parts.set(i);
                        }
                    }

                    return getOrCreatePartitionStorages(table, parts).thenRun(() -> localPartsByTableId.put(tableId, parts));
                }, ioExecutor)));

        CompletableFuture<?> tablesByIdFuture = tablesVv.get(causalityToken);

        // TODO https://issues.apache.org/jira/browse/IGNITE-19170 Partitions should be started only on the assignments change
        //  event triggered by zone create or alter.
        CompletableFuture<?> createPartsFut = assignmentsUpdatedVv.update(causalityToken, (token, e) -> {
            if (e != null) {
                return failedFuture(e);
            }

            // `stableAssignmentsFuture` is already completed by this time (it's in chain of `localPartsUpdateFuture`).
            return allOf(localPartsUpdateFuture, tablesByIdFuture).thenComposeAsync(ignore -> inBusyLock(busyLock, () -> {
                        if (onNodeRecovery) {
                            SchemaRegistry schemaRegistry = table.schemaView();
                            PartitionSet partitionSet = localPartsByTableId.get(tableId);
                            // LWM starts updating only after the node is restored.
                            HybridTimestamp lwm = lowWatermark.getLowWatermark();

                            registerIndexesToTable(table, catalogService, partitionSet, schemaRegistry, lwm);
                        }
                        return startLocalPartitionsAndClients(
                                stableAssignmentsFuture,
                                pendingAssignments,
                                assignmentsChains,
                                table,
                                onNodeRecovery,
                                assignmentsTimestamp
                        );
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

            long assignmentsTimestamp = catalog.time();

            assignmentsFuture = distributionZoneManager.dataNodes(causalityToken, catalogVersion, zoneDescriptor.id())
                    .thenApply(dataNodes ->
                            PartitionDistributionUtils.calculateAssignments(
                                            dataNodes,
                                            zoneDescriptor.partitions(),
                                            zoneDescriptor.replicas()
                                    )
                                    .stream()
                                    .map(assignments -> Assignments.of(assignments, assignmentsTimestamp))
                                    .collect(toList())
                    );

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
     * Creates transaction state storage for the provided table.
     *
     * @param tableDescriptor Catalog table descriptor.
     * @param zoneDescriptor Catalog distributed zone descriptor.
     */
    protected TxStateStorage createTxStateTableStorage(CatalogTableDescriptor tableDescriptor, CatalogZoneDescriptor zoneDescriptor) {
        int tableId = tableDescriptor.id();

        TxStateStorage txStateStorage = new TxStateRocksDbStorage(
                tableId,
                zoneDescriptor.partitions(),
                sharedTxStateStorage
        );
        if (ThreadAssertions.enabled()) {
            txStateStorage = new ThreadAssertingTxStateStorage(txStateStorage);
        }

        txStateStorage.start();

        return txStateStorage;
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

        Set<ByteArray> assignmentKeys = IntStream.range(0, partitions)
                .mapToObj(p -> stablePartAssignmentsKey(new TablePartitionId(tableId, p)))
                .collect(toSet());
        metaStorageMgr.removeAll(assignmentKeys);

        CompletableFuture<?>[] stopReplicaAndDestroyFutures = new CompletableFuture<?>[partitions];

        // TODO https://issues.apache.org/jira/browse/IGNITE-19170 Partitions should be stopped on the assignments change
        //  event triggered by zone drop or alter. Stop replica asynchronously, out of metastorage event pipeline.
        for (int partitionId = 0; partitionId < partitions; partitionId++) {
            var replicationGroupId = new TablePartitionId(tableId, partitionId);

            stopReplicaAndDestroyFutures[partitionId] = stopAndDestroyPartition(replicationGroupId, table);
        }

        return allOf(stopReplicaAndDestroyFutures)
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
                    Catalog catalog = catalogService.activeCatalog(now.longValue());

                    Collection<CatalogTableDescriptor> tableDescriptors = catalog.tables();

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
    public Table table(QualifiedName name) {
        return join(tableAsync(name));
    }

    @Override
    public TableViewInternal table(int id) throws NodeStoppingException {
        return join(tableAsync(id));
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
            return localPartitionsVv.get(causalityToken).thenApply(unused -> localPartsByTableId.get(tableId));
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public TableViewInternal tableView(QualifiedName name) {
        return join(tableViewAsync(name));
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

        return new IgniteException(INTERNAL_ERR, th);
    }

    /**
     * Creates meta storage listener for pending assignments updates.
     *
     * @return The watch listener.
     */
    private WatchListener createPendingAssignmentsRebalanceListener() {
        return evt -> {
            if (!busyLock.enterBusy()) {
                return failedFuture(new NodeStoppingException());
            }

            try {
                Entry newEntry = evt.entryEvent().newEntry();

                return handleChangePendingAssignmentEvent(newEntry, evt.revision(), false);
            } finally {
                busyLock.leaveBusy();
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

        TablePartitionId replicaGrpId = extractTablePartitionId(pendingAssignmentsEntry.key(), PENDING_ASSIGNMENTS_PREFIX_BYTES);

        // Stable assignments from the meta store, which revision is bounded by the current pending event.
        Assignments stableAssignments = stableAssignmentsGetLocally(metaStorageMgr, replicaGrpId, revision);

        AssignmentsChain assignmentsChain = assignmentsChainGetLocally(metaStorageMgr, replicaGrpId, revision);

        Assignments pendingAssignments = Assignments.fromBytes(pendingAssignmentsEntry.value());

        return tablesVv.get(revision)
                .thenApply(ignore -> {
                    if (!busyLock.enterBusy()) {
                        return CompletableFuture.<Void>failedFuture(new NodeStoppingException());
                    }

                    try {
                        TableImpl table = tables.get(replicaGrpId.tableId());

                        // Table can be null only recovery, because we use a revision from the future. See comment inside
                        // performRebalanceOnRecovery.
                        if (table == null) {
                            if (LOG.isInfoEnabled()) {
                                LOG.info("Skipping Pending Assignments update, because table {} does not exist", replicaGrpId.tableId());
                            }

                            return CompletableFutures.<Void>nullCompletedFuture();
                        }

                        if (LOG.isInfoEnabled()) {
                            var stringKey = new String(pendingAssignmentsEntry.key(), UTF_8);

                            LOG.info(
                                    "Received update on pending assignments. Check if new raft group should be started [key={}, "
                                            + "partition={}, table={}, localMemberAddress={}, pendingAssignments={}, revision={}]",
                                    stringKey,
                                    replicaGrpId.partitionId(),
                                    table.name(),
                                    localNode().address(),
                                    pendingAssignments,
                                    revision
                            );
                        }

                        return handleChangePendingAssignmentEvent(
                                replicaGrpId,
                                table,
                                stableAssignments,
                                pendingAssignments,
                                assignmentsChain,
                                revision,
                                isRecovery
                        ).thenAccept(v -> executeIfLocalNodeIsPrimaryForGroup(
                                replicaGrpId,
                                replicaMeta -> sendChangePeersAndLearnersRequest(
                                        replicaMeta,
                                        replicaGrpId,
                                        pendingAssignments,
                                        revision
                                )
                        ));
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
            @Nullable AssignmentsChain assignmentsChain,
            long revision,
            boolean isRecovery
    ) {
        boolean pendingAssignmentsAreForced = pendingAssignments.force();
        Set<Assignment> pendingAssignmentsNodes = pendingAssignments.nodes();
        long assignmentsTimestamp = pendingAssignments.timestamp();

        // Start a new Raft node and Replica if this node has appeared in the new assignments.
        Assignment localMemberAssignmentInPending = localMemberAssignment(pendingAssignments);
        Assignment localMemberAssignmentInStable = localMemberAssignment(stableAssignments);

        boolean shouldStartLocalGroupNode;
        if (isRecovery) {
            // The condition to start the replica is
            // `pending.contains(node) || (stable.contains(node) && !pending.isForce())`.
            // This condition covers the left part of the OR expression.
            // The right part of it is covered in `startLocalPartitionsAndClients`.
            if (lastRebalanceWasGraceful(assignmentsChain)) {
                shouldStartLocalGroupNode = localMemberAssignmentInPending != null;
            } else {
                // TODO: Use logic from https://issues.apache.org/jira/browse/IGNITE-23874.
                LOG.warn("Recovery after a forced rebalance for table is not supported yet [tablePartitionId={}].",
                        replicaGrpId);
                shouldStartLocalGroupNode = localMemberAssignmentInPending != null;
            }
        } else {
            shouldStartLocalGroupNode = localMemberAssignmentInPending != null && localMemberAssignmentInStable == null;
        }

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
            computedStableAssignments = Assignments.forced(pendingAssignmentsNodes, assignmentsTimestamp);
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
                            unused -> inBusyLock(
                                    busyLock,
                                    () -> getOrCreatePartitionStorages(tbl, singlePartitionIdSet)
                                            .thenRun(() -> localPartsByTableId.compute(
                                                    replicaGrpId.tableId(),
                                                    (tableId, oldPartitionSet) -> extendPartitionSet(oldPartitionSet, partitionId)
                                            ))
                            ),
                            ioExecutor
                    )
                    .thenComposeAsync(unused -> inBusyLock(busyLock, () -> {
                        lowWatermark.getLowWatermarkSafe(lwm ->
                                registerIndexesToTable(tbl, catalogService, singlePartitionIdSet, tbl.schemaView(), lwm)
                        );

                        return waitForMetadataCompleteness(assignmentsTimestamp).thenCompose(ignored -> inBusyLock(busyLock, () -> {
                            assert localMemberAssignmentInPending != null : "Local member assignment";

                            return startPartitionAndStartClient(
                                    tbl,
                                    replicaGrpId.partitionId(),
                                    localMemberAssignmentInPending,
                                    computedStableAssignments,
                                    isRecovery,
                                    assignmentsTimestamp
                            );
                        }));
                    }), ioExecutor);
        } else if (pendingAssignmentsAreForced && localMemberAssignmentInPending != null) {
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

                    // A node might exist in stable yet we don't want to start the replica
                    // The case is: nodes A, B and C hold a replication group,
                    // nodes A and B die.
                    // Reset adds C into force pending and user writes data onto C.
                    // Then A and B go back online. In this case
                    // stable = [A, B, C], pending = [C, force] and only C should be started.
                    if (isRecovery && !replicaMgr.isReplicaStarted(replicaGrpId)) {
                        return;
                    }

                    assert replicaMgr.isReplicaStarted(replicaGrpId) : "The local node is outside of the replication group ["
                            + ", stable=" + stableAssignments
                            + ", pending=" + pendingAssignments
                            + ", localName=" + localNode().name() + "].";

                    // For forced assignments, we exclude dead stable nodes, and all alive stable nodes are already in pending assignments.
                    // Union is not required in such a case.
                    Set<Assignment> newAssignments = pendingAssignmentsAreForced || stableAssignments == null
                            ? pendingAssignmentsNodes
                            : union(pendingAssignmentsNodes, stableAssignments.nodes());

                    replicaMgr.replica(replicaGrpId)
                            .thenAccept(replica -> replica.updatePeersAndLearners(fromAssignments(newAssignments)));
                }), ioExecutor);
    }

    /**
     * For HA zones: Check that last rebalance was graceful (caused by common rebalance triggers, like data nodes change, replica factor
     * change, etc.) rather than forced (caused by a disaster recovery reset after losing the majority of nodes).
     */
    private static boolean lastRebalanceWasGraceful(@Nullable AssignmentsChain assignmentsChain) {
        // Assignments chain is either empty (when there have been no stable switch yet) or contains a single element in chain.
        return assignmentsChain == null || assignmentsChain.size() == 1;
    }

    private static PartitionSet extendPartitionSet(@Nullable PartitionSet oldPartitionSet, int partitionId) {
        PartitionSet newPartitionSet = Objects.requireNonNullElseGet(oldPartitionSet, BitSetPartitionSet::new);
        newPartitionSet.set(partitionId);
        return newPartitionSet;
    }

    private void executeIfLocalNodeIsPrimaryForGroup(
            ReplicationGroupId groupId,
            Consumer<ReplicaMeta> toExecute
    ) {
        CompletableFuture<ReplicaMeta> primaryReplicaFuture = getPrimaryReplica(groupId);

        isLocalNodeIsPrimary(primaryReplicaFuture).thenAccept(isPrimary -> {
            if (isPrimary) {
                primaryReplicaFuture.thenAccept(toExecute);
            }
        });
    }

    private void sendChangePeersAndLearnersRequest(
            ReplicaMeta replicaMeta,
            TablePartitionId replicationGroupId,
            Assignments pendingAssignments,
            long currentRevision
    ) {
        metaStorageMgr.get(pendingPartAssignmentsKey(replicationGroupId)).thenAccept(latestPendingAssignmentsEntry -> {
            // Do not change peers of the raft group if this is a stale event.
            // Note that we start raft node before for the sake of the consistency in a
            // starting and stopping raft nodes.
            if (currentRevision < latestPendingAssignmentsEntry.revision()) {
                return;
            }

            TablePartitionIdMessage partitionIdMessage = ReplicaMessageUtils
                    .toTablePartitionIdMessage(REPLICA_MESSAGES_FACTORY, replicationGroupId);

            ChangePeersAndLearnersAsyncReplicaRequest request = TABLE_MESSAGES_FACTORY.changePeersAndLearnersAsyncReplicaRequest()
                    .groupId(partitionIdMessage)
                    .pendingAssignments(pendingAssignments.toBytes())
                    .enlistmentConsistencyToken(replicaMeta.getStartTime().longValue())
                    .build();

            replicaSvc.invoke(localNode(), request);
        });
    }

    private boolean isNodeInReducedStableOrPendingAssignments(
            TablePartitionId replicaGrpId,
            @Nullable Assignments stableAssignments,
            Assignments pendingAssignments,
            long revision
    ) {
        Entry reduceEntry = metaStorageMgr.getLocally(RebalanceUtil.switchReduceKey(replicaGrpId), revision);

        Assignments reduceAssignments = reduceEntry != null
                ? Assignments.fromBytes(reduceEntry.value())
                : null;

        Set<Assignment> reducedStableAssignments = reduceAssignments != null
                ? subtract(stableAssignments.nodes(), reduceAssignments.nodes())
                : stableAssignments.nodes();

        return isLocalNodeInAssignments(union(reducedStableAssignments, pendingAssignments.nodes()));
    }

    private SnapshotStorageFactory createSnapshotStorageFactory(
            TablePartitionId replicaGrpId,
            PartitionUpdateHandlers partitionUpdateHandlers,
            InternalTable internalTable
    ) {
        int partitionId = replicaGrpId.partitionId();

        var partitionAccess = new PartitionMvStorageAccessImpl(
                partitionId,
                internalTable.storage(),
                mvGc,
                partitionUpdateHandlers.indexUpdateHandler,
                partitionUpdateHandlers.gcUpdateHandler,
                fullStateTransferIndexChooser,
                schemaManager.schemaRegistry(replicaGrpId.tableId()),
                lowWatermark
        );

        var txStateAccess = new PartitionTxStateAccessImpl(internalTable.txStateStorage().getPartitionStorage(partitionId));

        var factory = new PartitionSnapshotStorageFactory(
                new TablePartitionKey(replicaGrpId.tableId(), replicaGrpId.partitionId()),
                topologyService,
                outgoingSnapshotsManager,
                txStateAccess,
                catalogService,
                incomingSnapshotsExecutor
        );

        factory.addMvPartition(internalTable.tableId(), partitionAccess);

        return factory;
    }

    /**
     * Creates Meta storage listener for stable assignments updates.
     *
     * @return The watch listener.
     */
    private WatchListener createStableAssignmentsRebalanceListener() {
        return evt -> {
            if (!busyLock.enterBusy()) {
                return failedFuture(new NodeStoppingException());
            }

            try {
                return handleChangeStableAssignmentEvent(evt);
            } finally {
                busyLock.leaveBusy();
            }
        };
    }

    /** Creates Meta storage listener for switch reduce assignments updates. */
    private WatchListener createAssignmentsSwitchRebalanceListener() {
        return evt -> inBusyLockAsync(busyLock, () -> {
            byte[] key = evt.entryEvent().newEntry().key();

            TablePartitionId replicaGrpId = extractTablePartitionId(key, ASSIGNMENTS_SWITCH_REDUCE_PREFIX_BYTES);

            return tablesById(evt.revision())
                    .thenCompose(tables -> inBusyLockAsync(busyLock, () -> {
                        Assignments assignments = Assignments.fromBytes(evt.entryEvent().newEntry().value());

                        long assignmentsTimestamp = assignments.timestamp();

                        return waitForMetadataCompleteness(assignmentsTimestamp)
                                .thenCompose(unused -> inBusyLockAsync(busyLock, () -> {
                                    int catalogVersion = catalogService.activeCatalogVersion(assignmentsTimestamp);

                                    CatalogTableDescriptor tableDescriptor =
                                            getTableDescriptor(replicaGrpId.tableId(), catalogVersion);

                                    CatalogZoneDescriptor zoneDescriptor = getZoneDescriptor(tableDescriptor, catalogVersion);

                                    long causalityToken = zoneDescriptor.updateToken();

                                    return distributionZoneManager.dataNodes(causalityToken, catalogVersion,
                                                    tableDescriptor.zoneId())
                                            .thenCompose(dataNodes -> RebalanceUtilEx.handleReduceChanged(
                                                    metaStorageMgr,
                                                    dataNodes,
                                                    zoneDescriptor.partitions(),
                                                    zoneDescriptor.replicas(),
                                                    replicaGrpId,
                                                    evt,
                                                    assignmentsTimestamp
                                            ));
                                }));
                    }));
        });
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

        TxStatePartitionStorage txStatePartitionStorage = internalTable.txStateStorage().getPartitionStorage(partitionId);

        assert txStatePartitionStorage != null : "tableId=" + table.tableId() + ", partitionId=" + partitionId;

        return new PartitionStorages(mvPartition, txStatePartitionStorage);
    }

    // TODO: https://issues.apache.org/jira/browse/IGNITE-19739 Create storages only once.
    private CompletableFuture<Void> getOrCreatePartitionStorages(TableImpl table, PartitionSet partitions) {
        InternalTable internalTable = table.internalTable();

        CompletableFuture<?>[] storageFuts = partitions.stream().mapToObj(partitionId -> {
            MvPartitionStorage mvPartition = internalTable.storage().getMvPartition(partitionId);

            return (mvPartition != null ? completedFuture(mvPartition) : internalTable.storage().createMvPartition(partitionId))
                    .thenComposeAsync(mvPartitionStorage -> {
                        TxStatePartitionStorage txStatePartitionStorage = internalTable.txStateStorage()
                                .getOrCreatePartitionStorage(partitionId);

                        if (mvPartitionStorage.lastAppliedIndex() == MvPartitionStorage.REBALANCE_IN_PROGRESS
                                || txStatePartitionStorage.lastAppliedIndex() == TxStatePartitionStorage.REBALANCE_IN_PROGRESS) {
                            return allOf(
                                    internalTable.storage().clearPartition(partitionId),
                                    txStatePartitionStorage.clear()
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
        TablePartitionId tablePartitionId = extractTablePartitionId(stableAssignmentsWatchEvent.key(), STABLE_ASSIGNMENTS_PREFIX_BYTES);

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
                    .thenAccept(replica -> replica.updatePeersAndLearners(fromAssignments(stableAssignments)));
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

                    return stopAndDestroyPartition(tablePartitionId, table);
                });
    }

    private CompletableFuture<Void> stopAndDestroyPartition(TablePartitionId tablePartitionId, TableImpl table) {
        return stopPartition(tablePartitionId, table)
                .thenComposeAsync(v -> destroyPartitionStorages(tablePartitionId, table), ioExecutor);
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
                .thenCompose(v -> {
                    minTimeCollectorService.removePartition(tablePartitionId);
                    return mvGc.removeStorage(tablePartitionId);
                });
    }

    private CompletableFuture<Void> destroyPartitionStorages(TablePartitionId tablePartitionId, TableImpl table) {
        if (table == null) {
            return nullCompletedFuture();
        }

        InternalTable internalTable = table.internalTable();

        int partitionId = tablePartitionId.partitionId();

        List<CompletableFuture<?>> destroyFutures = new ArrayList<>();

        if (internalTable.storage().getMvPartition(partitionId) != null) {
            destroyFutures.add(internalTable.storage().destroyPartition(partitionId));
        }

        if (internalTable.txStateStorage().getPartitionStorage(partitionId) != null) {
            destroyFutures.add(runAsync(() -> internalTable.txStateStorage().destroyTxStateStorage(partitionId), ioExecutor));
        }

        destroyFutures.add(runAsync(() -> destroyReplicationProtocolStorages(tablePartitionId, table), ioExecutor));

        return allOf(destroyFutures.toArray(new CompletableFuture[]{}));
    }

    private void destroyReplicationProtocolStorages(TablePartitionId tablePartitionId, TableImpl table) {
        var internalTbl = (InternalTableImpl) table.internalTable();

        try {
            replicaMgr.destroyReplicationProtocolStorages(tablePartitionId, internalTbl.storage().isVolatile());
        } catch (NodeStoppingException e) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, e);
        }
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
        CatalogTableDescriptor tableDescriptor = catalogService.catalog(catalogVersion).table(tableId);

        assert tableDescriptor != null : "tableId=" + tableId + ", catalogVersion=" + catalogVersion;

        return tableDescriptor;
    }

    private CatalogZoneDescriptor getZoneDescriptor(CatalogTableDescriptor tableDescriptor, int catalogVersion) {
        CatalogZoneDescriptor zoneDescriptor = catalogService.catalog(catalogVersion).zone(tableDescriptor.zoneId());

        assert zoneDescriptor != null :
                "tableId=" + tableDescriptor.id() + ", zoneId=" + tableDescriptor.zoneId() + ", catalogVersion=" + catalogVersion;

        return zoneDescriptor;
    }

    private CatalogSchemaDescriptor getSchemaDescriptor(CatalogTableDescriptor tableDescriptor, int catalogVersion) {
        CatalogSchemaDescriptor schemaDescriptor = catalogService.catalog(catalogVersion).schema(tableDescriptor.schemaId());

        assert schemaDescriptor != null :
                "tableId=" + tableDescriptor.id() + ", schemaId=" + tableDescriptor.schemaId() + ", catalogVersion=" + catalogVersion;

        return schemaDescriptor;
    }

    private static @Nullable TableImpl findTableImplByName(Collection<TableImpl> tables, String name) {
        return tables.stream().filter(table -> table.qualifiedName().equals(QualifiedName.fromSimple(name))).findAny().orElse(null);
    }

    private void startTables(long recoveryRevision, @Nullable HybridTimestamp lwm) {
        int earliestCatalogVersion = lwm == null
                ? catalogService.earliestCatalogVersion()
                : catalogService.activeCatalogVersion(lwm.longValue());
        int latestCatalogVersion = catalogService.latestCatalogVersion();

        var startedTables = new IntOpenHashSet();
        var startTableFutures = new ArrayList<CompletableFuture<?>>();

        for (int ver = latestCatalogVersion; ver >= earliestCatalogVersion; ver--) {
            for (CatalogTableDescriptor tableDescriptor : catalogService.catalog(ver).tables()) {
                if (!startedTables.add(tableDescriptor.id())) {
                    continue;
                }

                CompletableFuture<?> startTableFuture;

                if (enabledColocation()) {
                    CatalogZoneDescriptor zoneDescriptor = getZoneDescriptor(tableDescriptor, ver);
                    CatalogSchemaDescriptor schemaDescriptor = getSchemaDescriptor(tableDescriptor, ver);

                    startTableFuture = prepareTableResourcesAndLoadToZoneReplica(
                            recoveryRevision,
                            zoneDescriptor,
                            tableDescriptor,
                            schemaDescriptor,
                            true
                    );
                } else {
                    startTableFuture = createTableLocally(recoveryRevision, ver, tableDescriptor, true);
                }

                startTableFutures.add(startTableFuture);
            }
        }

        CompletableFuture<Void> allTablesStartedFuture = allOf(startTableFutures.toArray(CompletableFuture[]::new))
                .whenComplete(copyStateTo(readyToProcessReplicaStarts))
                .whenComplete((unused, throwable) -> {
                    if (throwable != null) {
                        LOG.error("Error starting tables", throwable);
                    } else {
                        LOG.debug("Tables started successfully");
                    }
                });

        // Forces you to wait until recovery is complete before the metastore watches is deployed to avoid races with catalog listeners.
        startVv.update(recoveryRevision, (unused, throwable) -> allTablesStartedFuture);
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

            CatalogTableDescriptor tableDescriptor = catalogService.catalog(catalogVersion).table(droppedTableInfo.tableId());

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

    /**
     * Restarts the table partition including the replica and raft node.
     *
     * @param tablePartitionId Table partition that needs to be restarted.
     * @param revision Metastore revision.
     * @return Operation future.
     */
    public CompletableFuture<Void> restartPartition(TablePartitionId tablePartitionId, long revision, long assignmentsTimestamp) {
        return inBusyLockAsync(busyLock, () -> tablesVv.get(revision).thenComposeAsync(unused -> inBusyLockAsync(busyLock, () -> {
            TableImpl table = tables.get(tablePartitionId.tableId());

            return stopPartitionForRestart(tablePartitionId, table).thenComposeAsync(unused1 -> {
                Assignments stableAssignments = stableAssignmentsGetLocally(metaStorageMgr, tablePartitionId, revision);

                assert stableAssignments != null : "tablePartitionId=" + tablePartitionId + ", revision=" + revision;

                return waitForMetadataCompleteness(assignmentsTimestamp).thenCompose(unused2 -> inBusyLockAsync(busyLock, () -> {
                    Assignment localMemberAssignment = localMemberAssignment(stableAssignments);

                    if (localMemberAssignment == null) {
                        // (0) in case if node not in the assignments
                        return nullCompletedFuture();
                    }

                    return startPartitionAndStartClient(
                            table,
                            tablePartitionId.partitionId(),
                            localMemberAssignment,
                            stableAssignments,
                            false,
                            assignmentsTimestamp
                    );
                }));
            }, ioExecutor);
        }), ioExecutor));
    }

    @Override
    public void setStreamerReceiverRunner(StreamerReceiverRunner runner) {
        this.streamerReceiverRunner = runner;
    }

    private Set<TableImpl> zoneTables(int zoneId) {
        return tablesPerZone.computeIfAbsent(zoneId, id -> new HashSet<>());
    }

    private void addTableToZone(int zoneId, TableImpl table) {
        tablesPerZone.compute(zoneId, (id, tbls) -> {
            if (tbls == null) {
                tbls = new HashSet<>();
            }

            tbls.add(table);

            return tbls;
        });
    }
}
