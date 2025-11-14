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
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.causality.IncrementalVersionedValue.dependingOn;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.ASSIGNMENTS_SWITCH_REDUCE_PREFIX_BYTES;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.PENDING_ASSIGNMENTS_QUEUE_PREFIX_BYTES;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.STABLE_ASSIGNMENTS_PREFIX_BYTES;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.assignmentsChainGetLocally;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.extractTablePartitionId;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.pendingPartAssignmentsQueueKey;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.stableAssignmentsGetLocally;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.stablePartAssignmentsKey;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.subtract;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.tableAssignmentsChainGetLocally;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.tablePendingAssignmentsGetLocally;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.union;
import static org.apache.ignite.internal.event.EventListener.fromConsumer;
import static org.apache.ignite.internal.hlc.HybridTimestamp.LOGICAL_TIME_BITS_SIZE;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;
import static org.apache.ignite.internal.partition.replicator.LocalPartitionReplicaEvent.AFTER_REPLICA_DESTROYED;
import static org.apache.ignite.internal.partition.replicator.LocalPartitionReplicaEvent.AFTER_REPLICA_STOPPED;
import static org.apache.ignite.internal.partition.replicator.LocalPartitionReplicaEvent.BEFORE_REPLICA_STARTED;
import static org.apache.ignite.internal.partitiondistribution.PartitionDistributionUtils.calculateAssignmentForPartition;
import static org.apache.ignite.internal.raft.PeersAndLearners.fromAssignments;
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
import static org.apache.ignite.internal.util.CompletableFutures.trueCompletedFuture;
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
import java.util.UUID;
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
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.components.NodeProperties;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.utils.SystemDistributedConfigurationPropertyHolder;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.DistributionZonesUtil;
import org.apache.ignite.internal.distributionzones.rebalance.PartitionMover;
import org.apache.ignite.internal.distributionzones.rebalance.RebalanceRaftGroupEventsListener;
import org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
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
import org.apache.ignite.internal.metrics.LongGauge;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.internal.partition.replicator.LocalPartitionReplicaEventParameters;
import org.apache.ignite.internal.partition.replicator.NaiveAsyncReadWriteLock;
import org.apache.ignite.internal.partition.replicator.PartitionReplicaLifecycleManager;
import org.apache.ignite.internal.partition.replicator.ReliableCatalogVersions;
import org.apache.ignite.internal.partition.replicator.ReplicaTableProcessor;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.replication.ChangePeersAndLearnersAsyncReplicaRequest;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionDataStorage;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionKey;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionSnapshotStorage;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionSnapshotStorageFactory;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionTxStateAccessImpl;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.ZonePartitionKey;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.partition.replicator.schema.CatalogValidationSchemasSource;
import org.apache.ignite.internal.partition.replicator.schema.ExecutorInclinedSchemaSyncService;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.partitiondistribution.AssignmentsChain;
import org.apache.ignite.internal.partitiondistribution.AssignmentsQueue;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.placementdriver.wrappers.ExecutorInclinedPlacementDriver;
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
import org.apache.ignite.internal.replicator.TransientReplicaStartException;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.ReplicaMessageUtils;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.TablePartitionIdMessage;
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
import org.apache.ignite.internal.table.LongPriorityQueue;
import org.apache.ignite.internal.table.StreamerReceiverRunner;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.PartitionModificationCounterFactory.SizeSupplier;
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
import org.apache.ignite.internal.table.distributed.storage.BrokenTxStateStorage;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.table.distributed.storage.NullStorageEngine;
import org.apache.ignite.internal.table.distributed.storage.PartitionStorages;
import org.apache.ignite.internal.table.metrics.TableMetricSource;
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
     * {@link #startLocalPartitionsAndClients(CompletableFuture, List, List, TableImpl, boolean, long, long)}.
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

    private final FailureProcessor failureProcessor;

    /** Incoming RAFT snapshots executor. */
    private final ThreadPoolExecutor incomingSnapshotsExecutor;

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

    /** Ends at the {@link IgniteComponent#stopAsync(ComponentContext)} with an {@link NodeStoppingException}. */
    private final CompletableFuture<Void> stopManagerFuture = new CompletableFuture<>();

    private final ReplicationConfiguration replicationConfiguration;

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

    private final NodeProperties nodeProperties;

    @Nullable
    private ScheduledExecutorService streamerFlushExecutor;

    private final IndexMetaStorage indexMetaStorage;

    private final Predicate<Assignment> isLocalNodeAssignment = assignment -> assignment.consistentId().equals(localNode().name());

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

    private final EventListener<LocalPartitionReplicaEventParameters> onBeforeZoneReplicaStartedListener = this::beforeZoneReplicaStarted;
    private final EventListener<LocalPartitionReplicaEventParameters> onZoneReplicaStoppedListener = this::onZoneReplicaStopped;
    private final EventListener<LocalPartitionReplicaEventParameters> onZoneReplicaDestroyedListener = this::onZoneReplicaDestroyed;

    private final EventListener<CreateTableEventParameters> onTableCreateWithColocationListener = this::loadTableToZoneOnTableCreate;
    private final EventListener<CreateTableEventParameters> onTableCreateWithoutColocationListener = this::onTableCreate;
    private final EventListener<DropTableEventParameters> onTableDropListener = fromConsumer(this::onTableDrop);
    private final EventListener<CatalogEventParameters> onTableAlterListener = this::onTableAlter;

    private final EventListener<ChangeLowWatermarkEventParameters> onLowWatermarkChangedListener = this::onLwmChanged;
    private final EventListener<PrimaryReplicaEventParameters> onPrimaryReplicaExpiredListener = this::onTablePrimaryReplicaExpired;
    private final TableAssignmentsService assignmentsService;
    private final ReliableCatalogVersions reliableCatalogVersions;

    private final MetricManager metricManager;
    private final PartitionModificationCounterFactory partitionModificationCounterFactory;
    private final Map<TablePartitionId, PartitionModificationCounterMetricSource> partModCounterMetricSources = new ConcurrentHashMap<>();

    /**
     * Creates a new table manager.
     *
     * @param nodeName Node name.
     * @param registry Registry for versioned values.
     * @param gcConfig Garbage collector configuration.
     * @param txCfg Transaction configuration.
     * @param replicationConfiguration Replication configuration.
     * @param replicaMgr Replica manager.
     * @param lockMgr Lock manager.
     * @param replicaSvc Replica service.
     * @param txManager Transaction manager.
     * @param dataStorageMgr Data storage manager.
     * @param schemaManager Schema manager.
     * @param ioExecutor Separate executor for IO operations like partition storage initialization or partition raft group meta data
     *         persisting.
     * @param partitionOperationsExecutor Striped executor on which partition operations (potentially requiring I/O with storages)
     *         will be executed.
     * @param rebalanceScheduler Executor for scheduling rebalance routine.
     * @param commonScheduler Common Scheduled executor. Needed only for asynchronous start of scheduled operations without
     *         performing blocking, long or IO operations.
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
            TransactionConfiguration txCfg,
            ReplicationConfiguration replicationConfiguration,
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
            FailureProcessor failureProcessor,
            HybridTimestampTracker observableTimestampTracker,
            PlacementDriver placementDriver,
            Supplier<IgniteSql> sql,
            RemotelyTriggeredResourceRegistry remotelyTriggeredResourceRegistry,
            LowWatermark lowWatermark,
            TransactionInflights transactionInflights,
            IndexMetaStorage indexMetaStorage,
            LogSyncer logSyncer,
            PartitionReplicaLifecycleManager partitionReplicaLifecycleManager,
            NodeProperties nodeProperties,
            MinimumRequiredTimeCollectorService minTimeCollectorService,
            SystemDistributedConfiguration systemDistributedConfiguration,
            MetricManager metricManager,
            PartitionModificationCounterFactory partitionModificationCounterFactory
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
        this.failureProcessor = failureProcessor;
        this.observableTimestampTracker = observableTimestampTracker;
        this.sql = sql;
        this.replicationConfiguration = replicationConfiguration;
        this.remotelyTriggeredResourceRegistry = remotelyTriggeredResourceRegistry;
        this.lowWatermark = lowWatermark;
        this.transactionInflights = transactionInflights;
        this.txCfg = txCfg;
        this.nodeName = nodeName;
        this.indexMetaStorage = indexMetaStorage;
        this.partitionReplicaLifecycleManager = partitionReplicaLifecycleManager;
        this.nodeProperties = nodeProperties;
        this.minTimeCollectorService = minTimeCollectorService;
        this.metricManager = metricManager;
        this.partitionModificationCounterFactory = partitionModificationCounterFactory;

        this.executorInclinedSchemaSyncService = new ExecutorInclinedSchemaSyncService(schemaSyncService, partitionOperationsExecutor);
        this.executorInclinedPlacementDriver = new ExecutorInclinedPlacementDriver(placementDriver, partitionOperationsExecutor);
        this.reliableCatalogVersions = new ReliableCatalogVersions(schemaSyncService, catalogService);

        TxMessageSender txMessageSender = new TxMessageSender(
                messagingService,
                replicaSvc,
                clockService
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

        pendingAssignmentsRebalanceListener = createPendingAssignmentsRebalanceListener();

        stableAssignmentsRebalanceListener = createStableAssignmentsRebalanceListener();

        assignmentsSwitchRebalanceListener = createAssignmentsSwitchRebalanceListener();

        mvGc = new MvGc(nodeName, gcConfig, lowWatermark, failureProcessor);

        partitionReplicatorNodeRecovery = new PartitionReplicatorNodeRecovery(
                metaStorageMgr,
                messagingService,
                topologyService,
                partitionOperationsExecutor,
                tableId -> tablesById().get(tableId)
        );

        this.sharedTxStateStorage = txStateRocksDbSharedStorage;

        fullStateTransferIndexChooser = new FullStateTransferIndexChooser(catalogService, lowWatermark, indexMetaStorage);

        rebalanceRetryDelayConfiguration = new SystemDistributedConfigurationPropertyHolder<>(
                systemDistributedConfiguration,
                (v, r) -> {},
                DistributionZonesUtil.REBALANCE_RETRY_DELAY_MS,
                DistributionZonesUtil.REBALANCE_RETRY_DELAY_DEFAULT,
                Integer::parseInt
        );

        assignmentsService = new TableAssignmentsService(metaStorageMgr, catalogService, distributionZoneManager, failureProcessor);

        // Register event listeners in the constructor to avoid races with "partitionReplicaLifecycleManager"'s recovery.
        // We rely on the "readyToProcessReplicaStarts" future to block event handling until "startAsync" is completed.
        partitionReplicaLifecycleManager.listen(BEFORE_REPLICA_STARTED, onBeforeZoneReplicaStartedListener);
        partitionReplicaLifecycleManager.listen(AFTER_REPLICA_STOPPED, onZoneReplicaStoppedListener);
        partitionReplicaLifecycleManager.listen(AFTER_REPLICA_DESTROYED, onZoneReplicaDestroyedListener);
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        return inBusyLockAsync(busyLock, () -> {
            mvGc.start();

            transactionStateResolver.start();

            fullStateTransferIndexChooser.start();

            rebalanceRetryDelayConfiguration.init();

            cleanUpResourcesForDroppedTablesOnRecoveryBusy();

            if (!nodeProperties.colocationEnabled()) {
                metaStorageMgr.registerPrefixWatch(
                        new ByteArray(PENDING_ASSIGNMENTS_QUEUE_PREFIX_BYTES),
                        pendingAssignmentsRebalanceListener
                );
                metaStorageMgr.registerPrefixWatch(new ByteArray(STABLE_ASSIGNMENTS_PREFIX_BYTES), stableAssignmentsRebalanceListener);
                metaStorageMgr.registerPrefixWatch(
                        new ByteArray(ASSIGNMENTS_SWITCH_REDUCE_PREFIX_BYTES),
                        assignmentsSwitchRebalanceListener
                );
            }

            catalogService.listen(
                    CatalogEvent.TABLE_CREATE,
                    nodeProperties.colocationEnabled() ? onTableCreateWithColocationListener : onTableCreateWithoutColocationListener
            );
            catalogService.listen(CatalogEvent.TABLE_DROP, onTableDropListener);
            catalogService.listen(CatalogEvent.TABLE_ALTER, onTableAlterListener);

            lowWatermark.listen(LowWatermarkEvent.LOW_WATERMARK_CHANGED, onLowWatermarkChangedListener);

            partitionReplicatorNodeRecovery.start();

            if (!nodeProperties.colocationEnabled()) {
                executorInclinedPlacementDriver.listen(PrimaryReplicaEvent.PRIMARY_REPLICA_EXPIRED, onPrimaryReplicaExpiredListener);
            }

            CompletableFuture<Revisions> recoveryFinishFuture = metaStorageMgr.recoveryFinishedFuture();

            assert recoveryFinishFuture.isDone();

            long recoveryRevision = recoveryFinishFuture.join().revision();

            return recoverTables(recoveryRevision, lowWatermark.getLowWatermark())
                    .thenCompose(v -> processAssignmentsOnRecovery(recoveryRevision));
        });
    }

    private CompletableFuture<Void> waitForMetadataCompleteness(long ts) {
        return executorInclinedSchemaSyncService.waitForMetadataCompleteness(hybridTimestamp(ts));
    }

    private CompletableFuture<Boolean> beforeZoneReplicaStarted(LocalPartitionReplicaEventParameters parameters) {
        if (!nodeProperties.colocationEnabled()) {
            return falseCompletedFuture();
        }

        return inBusyLockAsync(busyLock, () -> readyToProcessReplicaStarts
                .thenCompose(v -> beforeZoneReplicaStartedImpl(parameters))
                .thenApply(unused -> false)
        );
    }

    private CompletableFuture<Void> beforeZoneReplicaStartedImpl(LocalPartitionReplicaEventParameters parameters) {
        return inBusyLockAsync(busyLock, () -> {
            ZonePartitionId zonePartitionId = parameters.zonePartitionId();

            NaiveAsyncReadWriteLock zoneLock = tablesPerZoneLocks.computeIfAbsent(
                    zonePartitionId.zoneId(),
                    id -> new NaiveAsyncReadWriteLock());

            CompletableFuture<Long> readLockAcquisitionFuture = zoneLock.readLock();

            try {
                return readLockAcquisitionFuture.thenCompose(stamp -> {
                    Set<TableViewInternal> zoneTables = zoneTablesRawSet(zonePartitionId.zoneId());

                    return createPartitionsAndLoadResourcesToZoneReplica(zonePartitionId, zoneTables, parameters.onRecovery());
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
            boolean onRecovery
    ) {
        int partitionIndex = zonePartitionId.partitionId();

        PartitionSet singlePartitionIdSet = PartitionSet.of(partitionIndex);

        CompletableFuture<?>[] futures = zoneTables.stream()
                .map(tbl -> inBusyLockAsync(busyLock, () -> {
                    return getOrCreatePartitionStorages(tbl, singlePartitionIdSet)
                            .thenRunAsync(() -> inBusyLock(busyLock, () -> {
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

                                preparePartitionResourcesAndLoadToZoneReplicaBusy(tbl, zonePartitionId, onRecovery);
                            }), ioExecutor)
                            // If the table is already closed, it's not a problem (probably the node is stopping).
                            .exceptionally(ignoreTableClosedException());
                }))
                .toArray(CompletableFuture[]::new);

        return allOf(futures);
    }

    private static Function<Throwable, Void> ignoreTableClosedException() {
        return ex -> {
            if (hasCause(ex, TableClosedException.class)) {
                return null;
            }
            throw sneakyThrow(ex);
        };
    }

    private CompletableFuture<Boolean> onZoneReplicaStopped(LocalPartitionReplicaEventParameters parameters) {
        if (!nodeProperties.colocationEnabled()) {
            return falseCompletedFuture();
        }

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
        if (!nodeProperties.colocationEnabled()) {
            return falseCompletedFuture();
        }

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
            TableImpl table = createTableImpl(causalityToken, tableDescriptor, zoneDescriptor, schemaDescriptor);

            int tableId = tableDescriptor.id();

            tables.put(tableId, table);

            return schemaManager.schemaRegistry(causalityToken, tableId)
                    .thenAccept(schemaRegistry -> inBusyLock(busyLock, () -> {
                        table.schemaView(schemaRegistry);

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
        TableImpl table = createTableImpl(causalityToken, tableDescriptor, zoneDescriptor, schemaDescriptor);

        int tableId = tableDescriptor.id();

        tablesVv.update(causalityToken, (ignore, e) -> inBusyLock(busyLock, () -> {
            if (e != null) {
                return failedFuture(e);
            }

            return schemaManager.schemaRegistry(causalityToken, tableId).thenAccept(table::schemaView);
        }));

        // Obtain future, but don't chain on it yet because update() on VVs must be called in the same thread. The method we call
        // will call update() on VVs and inside those updates it will chain on the lock acquisition future.
        CompletableFuture<Long> acquisitionFuture = partitionReplicaLifecycleManager.lockZoneForRead(zoneDescriptor.id());
        try {
            return loadTableToZoneOnTableCreateHavingZoneReadLock(acquisitionFuture, causalityToken, zoneDescriptor, table)
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
            TableImpl table
    ) {
        int tableId = table.tableId();

        // NB: all vv.update() calls must be made from the synchronous part of the method (not in thenCompose()/etc!).
        CompletableFuture<?> localPartsUpdateFuture = localPartitionsVv.update(causalityToken,
                (ignore, throwable) -> inBusyLock(busyLock, () -> readLockAcquisitionFuture.thenComposeAsync(unused -> {
                    PartitionSet parts = new BitSetPartitionSet();

                    for (int i = 0; i < zoneDescriptor.partitions(); i++) {
                        if (partitionReplicaLifecycleManager.hasLocalPartition(new ZonePartitionId(zoneDescriptor.id(), i))) {
                            parts.set(i);
                        }
                    }

                    return getOrCreatePartitionStorages(table, parts).thenRun(() -> localPartsByTableId.put(tableId, parts));
                }, ioExecutor))
                // If the table is already closed, it's not a problem (probably the node is stopping).
                .exceptionally(ignoreTableClosedException())
        );

        CompletableFuture<?> tablesByIdFuture = tablesVv.get(causalityToken);

        CompletableFuture<?> createPartsFut = assignmentsUpdatedVv.update(causalityToken, (token, e) -> {
            if (e != null) {
                return failedFuture(e);
            }

            return allOf(localPartsUpdateFuture, tablesByIdFuture).thenRunAsync(() -> inBusyLock(busyLock, () -> {
                for (int i = 0; i < zoneDescriptor.partitions(); i++) {
                    var zonePartitionId = new ZonePartitionId(zoneDescriptor.id(), i);

                    if (partitionReplicaLifecycleManager.hasLocalPartition(zonePartitionId)) {
                        preparePartitionResourcesAndLoadToZoneReplicaBusy(table, zonePartitionId, false);
                    }
                }
            }), ioExecutor);
        });

        tables.put(tableId, table);

        // TODO: https://issues.apache.org/jira/browse/IGNITE-19913 Possible performance degradation.
        return createPartsFut.thenAccept(ignore -> {
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
     */
    private void preparePartitionResourcesAndLoadToZoneReplicaBusy(
            TableViewInternal table, ZonePartitionId zonePartitionId, boolean onNodeRecovery
    ) {
        int partId = zonePartitionId.partitionId();

        int tableId = table.tableId();

        var internalTbl = (InternalTableImpl) table.internalTable();

        var tablePartitionId = new TablePartitionId(tableId, partId);

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

        PartitionStorages partitionStorages;
        try {
            partitionStorages = getPartitionStorages(table, partId);
        } catch (TableClosedException e) {
            // The node is probably stopping while we start the table, let's just skip it.
            return;
        }

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
                replicationConfiguration
        );

        internalTbl.updatePartitionTrackers(partId, safeTimeTracker, storageIndexTracker);

        mvGc.addStorage(tablePartitionId, partitionUpdateHandlers.gcUpdateHandler);

        minTimeCollectorService.addPartition(new TablePartitionId(tableId, partId));

        Function<RaftCommandRunner, ReplicaTableProcessor> createListener = raftClient -> createReplicaListener(
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
                minTimeCollectorService,
                partitionOperationsExecutor,
                executorInclinedPlacementDriver,
                clockService,
                nodeProperties,
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

    private CompletableFuture<Boolean> onTablePrimaryReplicaExpired(PrimaryReplicaEventParameters parameters) {
        if (thisNodeHoldsLease(parameters.leaseholderId())) {
            TablePartitionId groupId = (TablePartitionId) parameters.groupId();

            // We do not wait future in order not to block meta storage updates.
            replicaMgr.weakStopReplica(
                    groupId,
                    WeakReplicaStopReason.PRIMARY_EXPIRED,
                    () -> stopAndDestroyTablePartition(groupId, tablesVv.latestCausalityToken())
            );
        }

        return falseCompletedFuture();
    }

    private boolean thisNodeHoldsLease(@Nullable UUID leaseholderId) {
        return localNode().id().equals(leaseholderId);
    }

    private CompletableFuture<Void> processAssignmentsOnRecovery(long recoveryRevision) {
        return recoverStableAssignments(recoveryRevision).thenCompose(v -> recoverPendingAssignments(recoveryRevision));
    }

    private CompletableFuture<Void> recoverStableAssignments(long recoveryRevision) {
        return handleAssignmentsOnRecovery(
                new ByteArray(STABLE_ASSIGNMENTS_PREFIX_BYTES),
                recoveryRevision,
                (entry, rev) -> handleChangeStableAssignmentEvent(entry, rev, true),
                "stable"
        );
    }

    private CompletableFuture<Void> recoverPendingAssignments(long recoveryRevision) {
        return handleAssignmentsOnRecovery(
                new ByteArray(PENDING_ASSIGNMENTS_QUEUE_PREFIX_BYTES),
                recoveryRevision,
                (entry, rev) -> handleChangePendingAssignmentEvent(entry, rev, true),
                "pending"
        );
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
            long assignmentsTimestamp,
            long revision
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

                Assignment localAssignmentInStable = localAssignment(stableAssignments);

                boolean shouldStartPartition;

                if (isRecovery) {
                    AssignmentsChain assignmentsChain = assignmentsChains.get(i);

                    if (lastRebalanceWasGraceful(assignmentsChain)) {
                        // The condition to start the replica is
                        // `pending.contains(node) || (stable.contains(node) && !pending.isForce())`.
                        // However we check only the right part of this condition here
                        // since after `startTables` we have a call to `processAssignmentsOnRecovery`,
                        // which executes pending assignments update and will start required partitions there.
                        shouldStartPartition = localAssignmentInStable != null
                                && (pendingAssignments == null || !pendingAssignments.force());
                    } else {
                        // TODO: Use logic from https://issues.apache.org/jira/browse/IGNITE-23874
                        LOG.warn("Recovery after a forced rebalance for table is not supported yet [tableId={}, partitionId={}].",
                                tableId, partId);
                        shouldStartPartition = localAssignmentInStable != null
                                && (pendingAssignments == null || !pendingAssignments.force());
                    }
                } else {
                    shouldStartPartition = localAssignmentInStable != null;
                }

                if (shouldStartPartition) {
                    futures[i] = startPartitionAndStartClient(
                            table,
                            partId,
                            localAssignmentInStable,
                            stableAssignments,
                            isRecovery,
                            assignmentsTimestamp,
                            revision
                    ).whenComplete((res, ex) -> {
                        if (ex != null) {
                            String errorMessage = String.format(
                                    "Unable to update raft groups on the node [tableId=%s, partitionId=%s]",
                                    tableId,
                                    partId
                            );
                            failureProcessor.process(new FailureContext(ex, errorMessage));
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
            TableViewInternal table,
            int partId,
            Assignment localAssignment,
            Assignments stableAssignments,
            boolean isRecovery,
            long assignmentsTimestamp,
            long revision
    ) {
        if (nodeProperties.colocationEnabled()) {
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
                        localAssignment,
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

                    PartitionStorages partitionStorages;
                    try {
                        partitionStorages = getPartitionStorages(table, partId);
                    } catch (TableClosedException e) {
                        // The node is probably stopping while we start the table, let's just skip it.
                        return falseCompletedFuture();
                    }

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
                            replicationConfiguration
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
                            minTimeCollectorService,
                            partitionOperationsExecutor,
                            executorInclinedPlacementDriver,
                            clockService,
                            nodeProperties,
                            replicaGrpId
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
                forcedAssignments,
                revision
        ).handle((res, ex) -> {
            if (ex != null && !(hasCause(ex, NodeStoppingException.class, TransientReplicaStartException.class))) {
                String errorMessage = String.format(
                        "Unable to update raft groups on the node [tableId=%s, partitionId=%s]",
                        tableId,
                        partId
                );
                failureProcessor.process(new FailureContext(ex, errorMessage));
            }
            return null;
        });
    }

    @Nullable
    private Assignment localAssignment(@Nullable Assignments assignments) {
        if (assignments != null) {
            for (Assignment assignment : assignments.nodes()) {
                if (isLocalNodeAssignment.test(assignment)) {
                    return assignment;
                }
            }
        }
        return null;
    }

    private PartitionMover createPartitionMover(TablePartitionId replicaGrpId) {
        return new PartitionMover(busyLock, rebalanceScheduler, () -> {
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
                failureProcessor,
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
            TableViewInternal table,
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
                lowWatermark,
                failureProcessor,
                nodeProperties,
                table.metrics()
        );
    }

    private CompletableFuture<Set<Assignment>> calculateAssignments(
            TablePartitionId tablePartitionId,
            Long assignmentsTimestamp
    ) {
        CompletableFuture<Set<Assignment>> assignmentsFuture =
                reliableCatalogVersions.safeReliableCatalogFor(hybridTimestamp(assignmentsTimestamp))
                        .thenCompose(catalog -> calculateAssignments(tablePartitionId, catalog));

        return orStopManagerFuture(assignmentsFuture);
    }

    private CompletableFuture<Set<Assignment>> calculateAssignments(TablePartitionId tablePartitionId, Catalog catalog) {
        CatalogTableDescriptor tableDescriptor = getTableDescriptor(tablePartitionId.tableId(), catalog);
        CatalogZoneDescriptor zoneDescriptor = getZoneDescriptor(tableDescriptor, catalog);

        return distributionZoneManager.dataNodes(zoneDescriptor.updateTimestamp(), catalog.version(), tableDescriptor.zoneId())
                .thenApply(dataNodes -> calculateAssignmentForPartition(
                        dataNodes,
                        tablePartitionId.partitionId(),
                        zoneDescriptor.partitions(),
                        zoneDescriptor.replicas(),
                        zoneDescriptor.consensusGroupSize()
                ));
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
     * Internally we use there {@link PlacementDriver#getPrimaryReplica} with a penultimate safe time value, because metastore is waiting
     * for pending or stable assignments events handling over and only then metastore will increment the safe time. On the other hand
     * placement driver internally is waiting the metastore for given safe time plus {@link ClockService#maxClockSkewMillis}. So, if given
     * time is just {@link ClockService#now}, then there is a dead lock: metastore is waiting until assignments handling is over, but
     * internally placement driver is waiting for a non-applied safe time.
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

        if (!nodeProperties.colocationEnabled()) {
            executorInclinedPlacementDriver.removeListener(PrimaryReplicaEvent.PRIMARY_REPLICA_EXPIRED, onPrimaryReplicaExpiredListener);
        }

        lowWatermark.removeListener(LowWatermarkEvent.LOW_WATERMARK_CHANGED, onLowWatermarkChangedListener);

        catalogService.removeListener(CatalogEvent.TABLE_CREATE, onTableCreateWithColocationListener);
        catalogService.removeListener(CatalogEvent.TABLE_CREATE, onTableCreateWithoutColocationListener);
        catalogService.removeListener(CatalogEvent.TABLE_DROP, onTableDropListener);
        catalogService.removeListener(CatalogEvent.TABLE_ALTER, onTableAlterListener);

        if (!nodeProperties.colocationEnabled()) {
            metaStorageMgr.unregisterWatch(pendingAssignmentsRebalanceListener);
            metaStorageMgr.unregisterWatch(stableAssignmentsRebalanceListener);
            metaStorageMgr.unregisterWatch(assignmentsSwitchRebalanceListener);

            stopReplicasAndCloseTables(tables);
        }
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        // NB: busy lock had already gotten in {@link beforeNodeStop}
        assert beforeStopGuard.get() : "'stop' called before 'beforeNodeStop'";

        if (!stopGuard.compareAndSet(false, true)) {
            return nullCompletedFuture();
        }

        partitionReplicaLifecycleManager.removeListener(AFTER_REPLICA_DESTROYED, onZoneReplicaDestroyedListener);
        partitionReplicaLifecycleManager.removeListener(AFTER_REPLICA_STOPPED, onZoneReplicaStoppedListener);
        partitionReplicaLifecycleManager.removeListener(BEFORE_REPLICA_STARTED, onBeforeZoneReplicaStartedListener);

        int shutdownTimeoutSeconds = 10;

        try {
            closeAllManually(
                    () -> {
                        if (nodeProperties.colocationEnabled()) {
                            closeAllManually(tables.values().stream().map(table -> () -> closeTable(table)));
                        }
                    },
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

    /**
     * Stops resources that are related to provided tables.
     *
     * @param tables Tables to stop.
     */
    private void stopReplicasAndCloseTables(Map<Integer, TableViewInternal> tables) {
        var futures = new ArrayList<CompletableFuture<Void>>(tables.size());

        for (TableViewInternal table : tables.values()) {
            CompletableFuture<Void> stopFuture = stopTablePartitions(table)
                    .thenRun(() -> {
                        try {
                            closeTable(table);
                        } catch (Exception e) {
                            throw new CompletionException(e);
                        }
                    })
                    .whenComplete((res, ex) -> {
                        if (ex != null) {
                            String errorMessage = String.format("Unable to stop table [name=%s, tableId=%s]", table.name(),
                                    table.tableId());
                            failureProcessor.process(new FailureContext(ex, errorMessage));
                        }
                    });

            futures.add(stopFuture);
        }

        try {
            allOf(futures.toArray(CompletableFuture[]::new)).get(30, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            LOG.error("Unable to clean table resources", e);
        }
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
                internalTable.txStateStorage(),
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
                this::streamerFlushExecutor,
                Objects.requireNonNull(streamerReceiverRunner),
                () -> txCfg.value().readWriteTimeoutMillis(),
                () -> txCfg.value().readOnlyTimeoutMillis(),
                nodeProperties.colocationEnabled(),
                createAndRegisterMetricsSource(tableStorage.getTableDescriptor(), tableName)
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
                new TableStatsStalenessConfiguration(descProps.staleRowsFraction(), descProps.minStaleRowsCount())
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

            CompletableFuture<List<Assignments>> stableAssignmentsFutureAfterInvoke =
                    assignmentsService.createAndWriteTableAssignmentsToMetastorage(
                            tableId,
                            zoneDescriptor,
                            tableDescriptor,
                            causalityToken,
                            catalogVersion
                    );

            return createTableLocally(
                    causalityToken,
                    tableDescriptor,
                    zoneDescriptor,
                    schemaDescriptor,
                    stableAssignmentsFutureAfterInvoke,
                    tablePendingAssignmentsGetLocally(metaStorageMgr, tableId, zoneDescriptor.partitions(), causalityToken),
                    tableAssignmentsChainGetLocally(metaStorageMgr, tableId, zoneDescriptor.partitions(), causalityToken),
                    onNodeRecovery,
                    catalogService.catalog(catalogVersion).time()
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
                        if (localAssignment(partitionAssignments) != null) {
                            parts.set(i);
                        }
                    }

                    return getOrCreatePartitionStorages(table, parts)
                            .thenRun(() -> localPartsByTableId.put(tableId, parts))
                            // If the table is already closed, it's not a problem (probably the node is stopping).
                            .exceptionally(ignoreTableClosedException());
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
                        assignmentsTimestamp,
                        causalityToken
                );
            }), ioExecutor);
        });

        tables.put(tableId, table);

        // TODO should be reworked in IGNITE-16763

        // TODO: https://issues.apache.org/jira/browse/IGNITE-19913 Possible performance degradation.
        return createPartsFut.thenAccept(ignore -> startedTables.put(tableId, table));
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
        if (nodeProperties.colocationEnabled()) {
            return new BrokenTxStateStorage();
        }

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
        TableViewInternal table = startedTables.remove(tableId);

        localPartsByTableId.remove(tableId);

        assert table != null : tableId;

        InternalTable internalTable = table.internalTable();

        if (!nodeProperties.colocationEnabled()) {
            Set<ByteArray> assignmentKeys = IntStream.range(0, internalTable.partitions())
                    .mapToObj(p -> stablePartAssignmentsKey(new TablePartitionId(tableId, p)))
                    .collect(toSet());

            metaStorageMgr.removeAll(assignmentKeys)
                    .whenComplete((v, e) -> {
                        if (e != null) {
                            LOG.error("Failed to remove assignments from metastorage [tableId={}]", e, tableId);
                        }
                    });
        }

        return stopAndDestroyTablePartitions(table)
                .thenComposeAsync(unused -> inBusyLockAsync(busyLock, () -> {
                    CompletableFuture<Void> tableStorageDestroyFuture = internalTable.storage().destroy();

                    // TX state storage destruction is synchronous.
                    internalTable.txStateStorage().destroy();

                    return tableStorageDestroyFuture;
                }), ioExecutor)
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

        TablePartitionId replicaGrpId = extractTablePartitionId(pendingAssignmentsEntry.key(), PENDING_ASSIGNMENTS_QUEUE_PREFIX_BYTES);

        // Stable assignments from the meta store, which revision is bounded by the current pending event.
        Assignments stableAssignments = stableAssignmentsGetLocally(metaStorageMgr, replicaGrpId, revision);

        AssignmentsChain assignmentsChain = assignmentsChainGetLocally(metaStorageMgr, replicaGrpId, revision);

        return tablesVv.get(revision)
                .thenApply(ignore -> {
                    if (!busyLock.enterBusy()) {
                        return CompletableFuture.<Void>failedFuture(new NodeStoppingException());
                    }

                    try {
                        TableViewInternal table = tables.get(replicaGrpId.tableId());

                        // Table can be null only recovery, because we use a revision from the future. See comment inside
                        // performRebalanceOnRecovery.
                        if (table == null) {
                            if (LOG.isInfoEnabled()) {
                                LOG.info("Skipping Pending Assignments update, because table {} does not exist", replicaGrpId.tableId());
                            }

                            return CompletableFutures.<Void>nullCompletedFuture();
                        }

                        AssignmentsQueue pendingAssignmentsQueue = AssignmentsQueue.fromBytes(pendingAssignmentsEntry.value());

                        if (LOG.isInfoEnabled()) {
                            var stringKey = new String(pendingAssignmentsEntry.key(), UTF_8);

                            LOG.info(
                                    "Received update on pending assignments. Check if new raft group should be started [key={}, "
                                            + "partition={}, table={}, localMemberAddress={}, pendingAssignmentsQueue={}, revision={}]",
                                    stringKey,
                                    replicaGrpId.partitionId(),
                                    table.name(),
                                    localNode().address(),
                                    pendingAssignmentsQueue,
                                    revision
                            );
                        }

                        Assignments pendingAssignments =
                                pendingAssignmentsQueue == null ? Assignments.EMPTY : pendingAssignmentsQueue.poll();

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
            TableViewInternal tbl,
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
        Assignment localAssignmentInPending = localAssignment(pendingAssignments);
        Assignment localAssignmentInStable = localAssignment(stableAssignments);

        boolean shouldStartLocalGroupNode;
        if (isRecovery) {
            // The condition to start the replica is
            // `pending.contains(node) || (stable.contains(node) && !pending.isForce())`.
            // This condition covers the left part of the OR expression.
            // The right part of it is covered in `startLocalPartitionsAndClients`.
            if (lastRebalanceWasGraceful(assignmentsChain)) {
                shouldStartLocalGroupNode = localAssignmentInPending != null;
            } else {
                // TODO: Use logic from https://issues.apache.org/jira/browse/IGNITE-23874.
                LOG.warn("Recovery after a forced rebalance for table is not supported yet [tablePartitionId={}].",
                        replicaGrpId);
                shouldStartLocalGroupNode = localAssignmentInPending != null;
            }
        } else {
            shouldStartLocalGroupNode = localAssignmentInPending != null && localAssignmentInStable == null;
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

        CompletableFuture<Void> localServicesStartFuture;

        if (shouldStartLocalGroupNode) {
            localServicesStartFuture = localPartitionsVv.get(revision)
                    .thenComposeAsync(unused -> createPartitionAndStartClient(
                            replicaGrpId,
                            tbl,
                            revision,
                            isRecovery,
                            assignmentsTimestamp,
                            localAssignmentInPending,
                            computedStableAssignments
                    ), ioExecutor);
        } else if (pendingAssignmentsAreForced && localAssignmentInPending != null) {
            localServicesStartFuture = runAsync(() -> inBusyLock(busyLock, () -> {
                assert replicaMgr.isReplicaStarted(replicaGrpId) : "The local node is outside of the replication group: " + replicaGrpId;

                // Sequence token for data partitions is MS revision.
                replicaMgr.resetPeers(replicaGrpId, fromAssignments(computedStableAssignments.nodes()), revision);
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
                            + "stable=" + stableAssignments
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

    private CompletableFuture<Void> createPartitionAndStartClient(
            TablePartitionId replicaGrpId,
            TableViewInternal tbl,
            long revision,
            boolean isRecovery,
            long assignmentsTimestamp,
            Assignment localAssignmentInPending,
            Assignments computedStableAssignments
    ) {
        int partitionId = replicaGrpId.partitionId();

        PartitionSet singlePartitionIdSet = PartitionSet.of(partitionId);

        // TODO https://issues.apache.org/jira/browse/IGNITE-20957 Revisit this code
        return inBusyLock(
                busyLock,
                () -> getOrCreatePartitionStorages(tbl, singlePartitionIdSet)
                        .thenRun(() -> localPartsByTableId.compute(
                                replicaGrpId.tableId(),
                                (tableId, oldPartitionSet) -> extendPartitionSet(oldPartitionSet, partitionId)
                        ))
                        // If the table is already closed, it's not a problem (probably the node is stopping).
                        .exceptionally(ignoreTableClosedException())
        ).thenComposeAsync(unused -> inBusyLock(busyLock, () -> {
            lowWatermark.getLowWatermarkSafe(lwm ->
                    registerIndexesToTable(tbl, catalogService, singlePartitionIdSet, tbl.schemaView(), lwm)
            );

            return waitForMetadataCompleteness(assignmentsTimestamp).thenCompose(ignored -> inBusyLock(busyLock, () -> {
                assert localAssignmentInPending != null : "Local member assignment";

                return startPartitionAndStartClient(
                        tbl,
                        replicaGrpId.partitionId(),
                        localAssignmentInPending,
                        computedStableAssignments,
                        isRecovery,
                        assignmentsTimestamp,
                        revision
                );
            }));
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
        metaStorageMgr.get(pendingPartAssignmentsQueueKey(replicationGroupId)).thenAccept(latestPendingAssignmentsEntry -> {
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
                    .sequenceToken(currentRevision)
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

        var snapshotStorage = new PartitionSnapshotStorage(
                new TablePartitionKey(replicaGrpId.tableId(), replicaGrpId.partitionId()),
                topologyService,
                outgoingSnapshotsManager,
                txStateAccess,
                catalogService,
                failureProcessor,
                incomingSnapshotsExecutor
        );

        snapshotStorage.addMvPartition(internalTable.tableId(), partitionAccess);

        return new PartitionSnapshotStorageFactory(snapshotStorage);
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

                        return reliableCatalogVersions.safeReliableCatalogFor(hybridTimestamp(assignmentsTimestamp))
                                .thenCompose(catalog -> inBusyLockAsync(busyLock, () -> {
                                    CatalogTableDescriptor tableDescriptor = getTableDescriptor(replicaGrpId.tableId(), catalog);

                                    CatalogZoneDescriptor zoneDescriptor = getZoneDescriptor(tableDescriptor, catalog);

                                    return handleReduceChanged(evt, catalog.version(), zoneDescriptor, replicaGrpId, assignmentsTimestamp);
                                }));
                    }));
        });
    }

    private CompletableFuture<Void> handleReduceChanged(
            WatchEvent evt,
            int catalogVersion,
            CatalogZoneDescriptor zoneDescriptor,
            TablePartitionId replicaGrpId,
            long assignmentsTimestamp
    ) {
        return distributionZoneManager.dataNodes(zoneDescriptor.updateTimestamp(), catalogVersion, zoneDescriptor.id())
                .thenCompose(dataNodes -> RebalanceUtilEx.handleReduceChanged(
                        metaStorageMgr,
                        dataNodes,
                        zoneDescriptor.partitions(),
                        zoneDescriptor.replicas(),
                        zoneDescriptor.consensusGroupSize(),
                        replicaGrpId,
                        evt,
                        assignmentsTimestamp
                ));
    }

    /**
     * Gets partition stores.
     *
     * @param table Table.
     * @param partitionId Partition ID.
     * @return PartitionStorages.
     */
    private static PartitionStorages getPartitionStorages(TableViewInternal table, int partitionId) {
        InternalTable internalTable = table.internalTable();

        MvPartitionStorage mvPartition;
        try {
            mvPartition = internalTable.storage().getMvPartition(partitionId);
        } catch (StorageClosedException e) {
            throw new TableClosedException(table.tableId(), e);
        }

        assert mvPartition != null : "tableId=" + table.tableId() + ", partitionId=" + partitionId;

        TxStatePartitionStorage txStatePartitionStorage = internalTable.txStateStorage().getPartitionStorage(partitionId);

        assert txStatePartitionStorage != null : "tableId=" + table.tableId() + ", partitionId=" + partitionId;

        return new PartitionStorages(mvPartition, txStatePartitionStorage);
    }

    // TODO: https://issues.apache.org/jira/browse/IGNITE-19739 Create storages only once.
    private CompletableFuture<Void> getOrCreatePartitionStorages(TableViewInternal table, PartitionSet partitions) {
        InternalTable internalTable = table.internalTable();

        CompletableFuture<?>[] storageFuts = partitions.stream().mapToObj(partitionId -> {
            MvPartitionStorage mvPartition;
            try {
                mvPartition = internalTable.storage().getMvPartition(partitionId);
            } catch (StorageClosedException e) {
                return failedFuture(new TableClosedException(table.tableId(), e));
            }

            return (mvPartition != null ? completedFuture(mvPartition) : internalTable.storage().createMvPartition(partitionId))
                    .thenComposeAsync(mvPartitionStorage -> {
                        TxStatePartitionStorage txStatePartitionStorage = internalTable.txStateStorage()
                                .getOrCreatePartitionStorage(partitionId);

                        if (mvPartitionStorage.lastAppliedIndex() == MvPartitionStorage.REBALANCE_IN_PROGRESS
                                || !nodeProperties.colocationEnabled()
                                        && txStatePartitionStorage.lastAppliedIndex() == TxStatePartitionStorage.REBALANCE_IN_PROGRESS) {
                            if (nodeProperties.colocationEnabled()) {
                                return internalTable.storage().clearPartition(partitionId);
                            } else {
                                return allOf(
                                        internalTable.storage().clearPartition(partitionId),
                                        txStatePartitionStorage.clear()
                                );
                            }
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
            Entry pendingAssignmentsEntry = metaStorageMgr.getLocally(pendingPartAssignmentsQueueKey(tablePartitionId), revision);
            AssignmentsQueue pendingAssignmentsQueue = AssignmentsQueue.fromBytes(pendingAssignmentsEntry.value());

            if (LOG.isInfoEnabled()) {
                var stringKey = new String(stableAssignmentsWatchEvent.key(), UTF_8);

                LOG.info("Received update on stable assignments [key={}, partition={}, localMemberAddress={}, "
                                + "stableAssignments={}, pendingAssignmentsQueue={}, revision={}]",
                        stringKey,
                        tablePartitionId,
                        localNode().address(),
                        stableAssignments,
                        pendingAssignmentsQueue,
                        revision
                );
            }

            Assignments pendingAssignments = pendingAssignmentsQueue == null ? Assignments.EMPTY : pendingAssignmentsQueue.poll();

            return stopAndDestroyTablePartitionAndUpdateClients(
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
            Set<Assignment> stableAssignments,
            Set<Assignment> pendingAssignments
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
                    .thenAccept(replica -> replica.updatePeersAndLearners(fromAssignments(union(stableAssignments, pendingAssignments))));
        }));
    }

    private CompletableFuture<Void> stopAndDestroyTablePartitionAndUpdateClients(
            TablePartitionId tablePartitionId,
            Set<Assignment> stableAssignments,
            Assignments pendingAssignments,
            boolean isRecovery,
            long revision
    ) {
        CompletableFuture<Void> clientUpdateFuture = isRecovery
                // Updating clients is not needed on recovery.
                ? nullCompletedFuture()
                : updatePartitionClients(tablePartitionId, stableAssignments, pendingAssignments.nodes());

        boolean shouldStopLocalServices = (pendingAssignments.force()
                        ? pendingAssignments.nodes().stream()
                        : Stream.concat(stableAssignments.stream(), pendingAssignments.nodes().stream())
                )
                .noneMatch(isLocalNodeAssignment);

        if (shouldStopLocalServices) {
            return allOf(
                    clientUpdateFuture,
                    weakStopAndDestroyTablePartition(tablePartitionId, revision)
            );
        } else {
            return clientUpdateFuture;
        }
    }

    private CompletableFuture<Void> weakStopAndDestroyTablePartition(TablePartitionId tablePartitionId, long causalityToken) {
        return replicaMgr.weakStopReplica(
                tablePartitionId,
                WeakReplicaStopReason.EXCLUDED_FROM_ASSIGNMENTS,
                () -> stopAndDestroyTablePartition(tablePartitionId, causalityToken)
        );
    }

    private CompletableFuture<Void> stopAndDestroyTablePartitions(TableViewInternal table) {
        InternalTable internalTable = table.internalTable();

        int partitions = internalTable.partitions();

        NaiveAsyncReadWriteLock zoneLock = tablesPerZoneLocks.computeIfAbsent(
                internalTable.zoneId(),
                id -> new NaiveAsyncReadWriteLock());

        CompletableFuture<Long> writeLockAcquisitionFuture = zoneLock.writeLock();

        try {
            return writeLockAcquisitionFuture.thenCompose(stamp -> {
                CompletableFuture<?>[] stopReplicaAndDestroyFutures = new CompletableFuture<?>[partitions];

                // TODO https://issues.apache.org/jira/browse/IGNITE-19170 Partitions should be stopped on the assignments change
                //  event triggered by zone drop or alter. Stop replica asynchronously, out of metastorage event pipeline.
                for (int partitionId = 0; partitionId < partitions; partitionId++) {
                    CompletableFuture<Void> resourcesUnloadFuture;

                    if (nodeProperties.colocationEnabled()) {
                        resourcesUnloadFuture = partitionReplicaLifecycleManager.unloadTableResourcesFromZoneReplica(
                                new ZonePartitionId(internalTable.zoneId(), partitionId),
                                internalTable.tableId()
                        );
                    } else {
                        resourcesUnloadFuture = nullCompletedFuture();
                    }

                    var tablePartitionId = new TablePartitionId(internalTable.tableId(), partitionId);

                    stopReplicaAndDestroyFutures[partitionId] = resourcesUnloadFuture
                            .thenCompose(v -> stopAndDestroyTablePartition(tablePartitionId, table, true));
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

                    return stopAndDestroyTablePartition(tablePartitionId, table, false);
                });
    }

    private CompletableFuture<Void> stopAndDestroyTablePartition(
            TablePartitionId tablePartitionId,
            TableViewInternal table,
            boolean destroyingWholeTable
    ) {
        return stopTablePartition(tablePartitionId, table)
                .thenComposeAsync(v -> destroyPartitionStorages(tablePartitionId, table, destroyingWholeTable), ioExecutor);
    }

    /**
     * Stops all resources associated with a given partition, like replicas and partition trackers. Calls
     * {@link ReplicaManager#weakStopReplica} in order to change the replica state.
     *
     * @param tablePartitionId Partition ID.
     * @param table Table which this partition belongs to.
     * @return Future that will be completed after all resources have been closed.
     */
    private CompletableFuture<Void> stopPartitionForRestart(TablePartitionId tablePartitionId, TableViewInternal table) {
        return replicaMgr.weakStopReplica(
                tablePartitionId,
                WeakReplicaStopReason.RESTART,
                () -> stopTablePartition(tablePartitionId, table)
        );
    }

    /**
     * Stops all resources associated with a given partition, like replicas and partition trackers. Calls
     * {@link ReplicaManager#weakStopReplica} in order to change the replica state.
     *
     * @param tablePartitionId Partition ID.
     * @param table Table which this partition belongs to.
     * @return Future that will be completed after all resources have been closed.
     */
    private CompletableFuture<Void> stopPartitionAndDestroyForRestart(TablePartitionId tablePartitionId, TableViewInternal table) {
        return replicaMgr.weakStopReplica(
                tablePartitionId,
                WeakReplicaStopReason.RESTART,
                () -> stopAndDestroyTablePartition(tablePartitionId, table, false)
        );
    }

    /**
     * Stops all resources associated with a given partition, like replicas and partition trackers.
     *
     * @param tablePartitionId Partition ID.
     * @param table Table which this partition belongs to.
     * @return Future that will be completed after all resources have been closed.
     */
    private CompletableFuture<Void> stopTablePartition(TablePartitionId tablePartitionId, TableViewInternal table) {
        CompletableFuture<Boolean> stopReplicaFuture;

        try {
            // In case of colocation there shouldn't be any table replica and thus it shouldn't be stopped.
            stopReplicaFuture = nodeProperties.colocationEnabled()
                    ? trueCompletedFuture()
                    : replicaMgr.stopReplica(tablePartitionId);
        } catch (NodeStoppingException e) {
            // No-op.
            stopReplicaFuture = falseCompletedFuture();
        }

        return stopReplicaFuture
                .thenCompose(v -> {
                    closePartitionTrackers(table.internalTable(), tablePartitionId.partitionId());

                    minTimeCollectorService.removePartition(tablePartitionId);

                    PartitionModificationCounterMetricSource metricSource = partModCounterMetricSources.remove(tablePartitionId);
                    if (metricSource != null) {
                        try {
                            metricManager.unregisterSource(metricSource);
                        } catch (Exception e) {
                            String message = "Failed to register metrics source for table [name={}, partitionId={}].";
                            LOG.warn(message, e, table.name(), tablePartitionId.partitionId());
                        }
                    }

                    return mvGc.removeStorage(tablePartitionId);
                });
    }

    private CompletableFuture<Void> destroyPartitionStorages(
            TablePartitionId tablePartitionId,
            TableViewInternal table,
            boolean destroyingWholeTable
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

        if (!nodeProperties.colocationEnabled()) {
            if (internalTable.txStateStorage().getPartitionStorage(partitionId) != null) {
                destroyFutures.add(runAsync(() -> internalTable.txStateStorage().destroyPartitionStorage(partitionId), ioExecutor));
            }

            destroyFutures.add(
                    runAsync(() -> {
                        // No need for durability guarantees if destruction reliability is guaranteed by destroying table storages
                        // on startup.
                        destroyReplicationProtocolStorages(tablePartitionId, table, !destroyingWholeTable);
                    }, ioExecutor)
            );
        }

        // TODO: IGNITE-24926 - reduce set in localPartsByTableId after storages destruction.
        return allOf(destroyFutures.toArray(new CompletableFuture[]{}));
    }

    private void destroyReplicationProtocolStorages(TablePartitionId tablePartitionId, TableViewInternal table, boolean destroyDurably) {
        var internalTbl = (InternalTableImpl) table.internalTable();

        destroyReplicationProtocolStorages(tablePartitionId, internalTbl.storage().isVolatile(), destroyDurably);
    }

    private void destroyReplicationProtocolStorages(
            TablePartitionId tablePartitionId,
            boolean isVolatileStorage,
            boolean destroyDurably
    ) {
        try {
            if (destroyDurably) {
                replicaMgr.destroyReplicationProtocolStoragesDurably(tablePartitionId, isVolatileStorage);
            } else {
                replicaMgr.destroyReplicationProtocolStorages(tablePartitionId, isVolatileStorage);
            }
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

    private InternalClusterNode localNode() {
        return topologyService.localMember();
    }

    private PartitionUpdateHandlers createPartitionUpdateHandlers(
            int partitionId,
            PartitionDataStorage partitionDataStorage,
            TableViewInternal table,
            PendingComparableValuesTracker<HybridTimestamp, Void> safeTimeTracker,
            ReplicationConfiguration replicationConfiguration
    ) {
        TableIndexStoragesSupplier indexes = table.indexStorageAdapters(partitionId);

        IndexUpdateHandler indexUpdateHandler = new IndexUpdateHandler(indexes);

        GcUpdateHandler gcUpdateHandler = new GcUpdateHandler(partitionDataStorage, safeTimeTracker, indexUpdateHandler);

        SizeSupplier partSizeSupplier = () -> partitionDataStorage.getStorage().estimatedSize();
        PartitionModificationCounter modificationCounter = partitionModificationCounterFactory.create(
                partSizeSupplier, table::stalenessConfiguration
        );
        registerPartitionModificationCounterMetrics(table, partitionId, modificationCounter);

        StorageUpdateHandler storageUpdateHandler = new StorageUpdateHandler(
                partitionId,
                partitionDataStorage,
                indexUpdateHandler,
                replicationConfiguration,
                modificationCounter
        );

        return new PartitionUpdateHandlers(storageUpdateHandler, indexUpdateHandler, gcUpdateHandler);
    }

    private void registerPartitionModificationCounterMetrics(
            TableViewInternal table,
            int partitionId,
            PartitionModificationCounter counter
    ) {
        PartitionModificationCounterMetricSource metricSource =
                new PartitionModificationCounterMetricSource(table.tableId(), partitionId);

        metricSource.addMetric(new LongGauge(
                PartitionModificationCounterMetricSource.METRIC_COUNTER,
                "The value of the volatile counter of partition modifications. "
                        + "This value is used to determine staleness of the related SQL statistics.",
                counter::value
        ));

        metricSource.addMetric(new LongGauge(
                PartitionModificationCounterMetricSource.METRIC_NEXT_MILESTONE,
                "The value of the next milestone for the number of partition modifications. "
                        + "This value is used to determine staleness of the related SQL statistics.",
                counter::nextMilestone
        ));

        metricSource.addMetric(new LongGauge(
                PartitionModificationCounterMetricSource.METRIC_LAST_MILESTONE_TIMESTAMP,
                "The timestamp value representing the commit time of the last modification operation that "
                        + "reached the milestone. This value is used to determine staleness of the related SQL statistics.",
                () -> counter.lastMilestoneTimestamp().longValue()
        ));

        try {
            metricManager.registerSource(metricSource);
            metricManager.enable(metricSource);

            partModCounterMetricSources.put(new TablePartitionId(table.tableId(), partitionId), metricSource);
        } catch (Exception e) {
            LOG.warn("Failed to register metrics source for table [name={}, partitionId={}].", e, table.name(), partitionId);
        }
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

    private static CatalogTableDescriptor getTableDescriptor(int tableId, Catalog catalog) {
        CatalogTableDescriptor tableDescriptor = catalog.table(tableId);

        assert tableDescriptor != null : "tableId=" + tableId + ", catalogVersion=" + catalog.version();

        return tableDescriptor;
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

                boolean destroyTableEventFired = nextCatalog != null && nextCatalog.table(tableId) == null;
                if (destroyTableEventFired) {
                    destructionEventsQueue.enqueue(new DestroyTableEvent(nextCatalog.version(), tableId));
                }

                if (!startedTables.add(tableId)) {
                    continue;
                }

                CompletableFuture<?> startTableFuture;

                if (nodeProperties.colocationEnabled()) {
                    CatalogZoneDescriptor zoneDescriptor = getZoneDescriptor(tableDescriptor, ver);
                    CatalogSchemaDescriptor schemaDescriptor = getSchemaDescriptor(tableDescriptor, ver);

                    startTableFuture = prepareTableResourcesOnRecovery(
                            recoveryRevision,
                            zoneDescriptor,
                            tableDescriptor,
                            schemaDescriptor
                    );

                    if (destroyTableEventFired) {
                        // prepareTableResourcesOnRecovery registers a table metric source, so we need to unregister it here,
                        // just because the table is being dropped and there is no need to keep the metric source.
                        unregisterMetricsSource(tables.get(tableId));
                    }
                } else {
                    startTableFuture = createTableLocally(recoveryRevision, ver, tableDescriptor, true);
                }

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
        // TODO: IGNITE-20384 Clean up abandoned resources for dropped zones from vault and metastore

        Set<Integer> aliveTableIds = aliveTables(catalogService, lowWatermark.getLowWatermark());

        destroyMvStoragesForTablesNotIn(aliveTableIds);

        if (!nodeProperties.colocationEnabled()) {
            destroyTxStateStoragesForTablesNotIn(aliveTableIds);
            destroyReplicationProtocolStoragesForTablesNotIn(aliveTableIds);
        }
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

    private void destroyTxStateStoragesForTablesNotIn(Set<Integer> aliveTableIds) {
        Set<Integer> tableIdsOnDisk = sharedTxStateStorage.tableOrZoneIdsOnDisk();

        for (int tableId : difference(tableIdsOnDisk, aliveTableIds)) {
            sharedTxStateStorage.destroyStorage(tableId);
            LOG.info("Destroyed table TX state storage for table {}", tableId);
        }
    }

    private void destroyReplicationProtocolStoragesForTablesNotIn(Set<Integer> aliveTableIds) {
        Set<TablePartitionId> partitionIdsOnDisk;
        try {
            partitionIdsOnDisk = replicaMgr.replicationProtocolTablePartitionIdsOnDisk();
        } catch (NodeStoppingException e) {
            // We'll proceed on next start.
            return;
        }

        Map<Integer, List<TablePartitionId>> partitionIdsByTableId = partitionIdsOnDisk.stream()
                .collect(groupingBy(TablePartitionId::tableId));

        for (Map.Entry<Integer, List<TablePartitionId>> entry : partitionIdsByTableId.entrySet()) {
            int tableId = entry.getKey();
            List<TablePartitionId> partitionIds = entry.getValue();

            if (!aliveTableIds.contains(tableId)) {
                destroyReplicationProtocolStoragesOnRecovery(tableId, partitionIds);
            }
        }
    }

    private void destroyReplicationProtocolStoragesOnRecovery(int tableId, List<TablePartitionId> partitionIds) {
        for (TablePartitionId partitionId : partitionIds) {
            try {
                replicaMgr.destroyReplicationProtocolStoragesOnStartup(partitionId);
            } catch (NodeStoppingException e) {
                // No problem, we'll proceed on next start.
                break;
            }
        }

        List<Integer> partitionIndexes = partitionIds.stream()
                .map(TablePartitionId::partitionId)
                .collect(toList());
        LOG.info(
                "Destroyed replication protocol storages for table {} and partitions {}",
                tableId,
                partitionIndexes
        );
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
            TableViewInternal table = tables.get(tablePartitionId.tableId());
            assert table != null : tablePartitionId;

            return stopPartitionForRestart(tablePartitionId, table).thenComposeAsync(unused1 -> {
                Assignments stableAssignments = stableAssignmentsGetLocally(metaStorageMgr, tablePartitionId, revision);

                assert stableAssignments != null : "tablePartitionId=" + tablePartitionId + ", revision=" + revision;

                return waitForMetadataCompleteness(assignmentsTimestamp).thenCompose(unused2 -> inBusyLockAsync(busyLock, () -> {
                    Assignment localAssignment = localAssignment(stableAssignments);

                    if (localAssignment == null) {
                        // (0) in case if node not in the assignments
                        return nullCompletedFuture();
                    }

                    return startPartitionAndStartClient(
                            table,
                            tablePartitionId.partitionId(),
                            localAssignment,
                            stableAssignments,
                            false,
                            assignmentsTimestamp,
                            revision
                    );
                }));
            }, ioExecutor);
        }), ioExecutor));
    }

    /**
     * Restarts the table partition including the replica and raft node.
     *
     * @param tablePartitionId Table partition that needs to be restarted.
     * @param revision Metastore revision.
     * @param assignmentsTimestamp Assignments timestamp.
     * @return Operation future.
     */
    public CompletableFuture<Void> restartPartitionWithCleanUp(
            TablePartitionId tablePartitionId,
            long revision,
            long assignmentsTimestamp
    ) {
        return tableAsync(tablePartitionId.tableId()).thenComposeAsync(table -> inBusyLockAsync(busyLock, () -> {
            assert table != null : tablePartitionId;

            Assignments stableAssignments = stableAssignmentsGetLocally(metaStorageMgr, tablePartitionId, revision);

            Assignment localAssignment = localAssignment(stableAssignments);

            return stopPartitionAndDestroyForRestart(tablePartitionId, table).thenComposeAsync(unused1 ->
                            createPartitionAndStartClient(
                                    tablePartitionId,
                                    table,
                                    revision,
                                    false,
                                    assignmentsTimestamp,
                                    localAssignment,
                                    stableAssignments
                            ),
                    ioExecutor
            );
        }), ioExecutor);
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

    private TableMetricSource createAndRegisterMetricsSource(StorageTableDescriptor tableDescriptor, QualifiedName tableName) {
        StorageEngine engine = dataStorageMgr.engineByStorageProfile(tableDescriptor.getStorageProfile());

        // Engine can be null sometimes, see "TableManager.createTableStorage".
        if (engine != null) {
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

        TableMetricSource source = new TableMetricSource(tableName);

        try {
            metricManager.registerSource(source);
            metricManager.enable(source);
        } catch (Exception e) {
            LOG.warn("Failed to register metrics source for table [id={}, name={}].", e, tableDescriptor.getId(), tableName);
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
