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
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.dataNodes;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.extractZoneId;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesPrefix;
import static org.apache.ignite.internal.schema.SchemaManager.INITIAL_SCHEMA_VERSION;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.apache.ignite.internal.utils.RebalanceUtil.ASSIGNMENTS_SWITCH_REDUCE_PREFIX;
import static org.apache.ignite.internal.utils.RebalanceUtil.PENDING_ASSIGNMENTS_PREFIX;
import static org.apache.ignite.internal.utils.RebalanceUtil.STABLE_ASSIGNMENTS_PREFIX;
import static org.apache.ignite.internal.utils.RebalanceUtil.extractPartitionNumber;
import static org.apache.ignite.internal.utils.RebalanceUtil.extractTableId;
import static org.apache.ignite.internal.utils.RebalanceUtil.pendingPartAssignmentsKey;
import static org.apache.ignite.internal.utils.RebalanceUtil.stablePartAssignmentsKey;
import static org.apache.ignite.internal.utils.RebalanceUtil.updatePendingAssignmentsKeys;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.stream.Stream;
import org.apache.ignite.configuration.ConfigurationChangeException;
import org.apache.ignite.configuration.ConfigurationProperty;
import org.apache.ignite.configuration.NamedConfigurationTree;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.affinity.AffinityUtils;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.baseline.BaselineManager;
import org.apache.ignite.internal.causality.VersionedValue;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.EventListener;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.manager.Producer;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftGroupEventsListener;
import org.apache.ignite.internal.raft.RaftManager;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.raft.service.LeaderWithTerm;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.raft.storage.impl.LogStorageFactoryCreator;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.configuration.ExtendedTableChange;
import org.apache.ignite.internal.schema.configuration.ExtendedTableConfiguration;
import org.apache.ignite.internal.schema.configuration.ExtendedTableView;
import org.apache.ignite.internal.schema.configuration.TableChange;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.schema.configuration.TableView;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.schema.event.SchemaEvent;
import org.apache.ignite.internal.schema.event.SchemaEventParameters;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.gc.MvGc;
import org.apache.ignite.internal.table.distributed.message.HasDataRequest;
import org.apache.ignite.internal.table.distributed.message.HasDataResponse;
import org.apache.ignite.internal.table.distributed.raft.PartitionDataStorage;
import org.apache.ignite.internal.table.distributed.raft.PartitionListener;
import org.apache.ignite.internal.table.distributed.raft.RebalanceRaftGroupEventsListener;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionAccessImpl;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionKey;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionSnapshotStorageFactory;
import org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing.SnapshotAwarePartitionDataStorage;
import org.apache.ignite.internal.table.distributed.replicator.PartitionReplicaListener;
import org.apache.ignite.internal.table.distributed.replicator.PlacementDriver;
import org.apache.ignite.internal.table.distributed.replicator.TablePartitionId;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.table.distributed.storage.PartitionStorages;
import org.apache.ignite.internal.table.event.TableEvent;
import org.apache.ignite.internal.table.event.TableEventParameters;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateTableStorage;
import org.apache.ignite.internal.tx.storage.state.rocksdb.TxStateRocksDbTableStorage;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.IgniteNameUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.Lazy;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.internal.utils.RebalanceUtil;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.lang.IgniteSystemProperties;
import org.apache.ignite.lang.IgniteTriConsumer;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.lang.TableAlreadyExistsException;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.raft.jraft.storage.impl.VolatileRaftMetaStorage;
import org.apache.ignite.table.Table;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Table manager.
 */
public class TableManager extends Producer<TableEvent, TableEventParameters> implements IgniteTablesInternal, IgniteComponent {
    private static final String DEFAULT_SCHEMA_NAME = "PUBLIC";

    // TODO get rid of this in future? IGNITE-17307
    /** Timeout to complete the tablesByIdVv on revision update. */
    private static final long TABLES_COMPLETE_TIMEOUT = 120;

    private static final long QUERY_DATA_NODES_COUNT_TIMEOUT = TimeUnit.SECONDS.toMillis(3);

    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(TableManager.class);

    /** Name of a transaction state directory. */
    private static final String TX_STATE_DIR = "tx-state-";

    /** Transaction storage flush delay. */
    private static final int TX_STATE_STORAGE_FLUSH_DELAY = 1000;
    private static final IntSupplier TX_STATE_STORAGE_FLUSH_DELAY_SUPPLIER = () -> TX_STATE_STORAGE_FLUSH_DELAY;

    /**
     * If this property is set to {@code true} then an attempt to get the configuration property directly from Meta storage will be
     * skipped, and the local property will be returned.
     * TODO: IGNITE-16774 This property and overall approach, access configuration directly through Meta storage,
     * TODO: will be removed after fix of the issue.
     */
    private final boolean getMetadataLocallyOnly = IgniteSystemProperties.getBoolean("IGNITE_GET_METADATA_LOCALLY_ONLY");

    /** Tables configuration. */
    private final TablesConfiguration tablesCfg;

    private final ClusterService clusterService;

    /** Raft manager. */
    private final RaftManager raftMgr;

    /** Replica manager. */
    private final ReplicaManager replicaMgr;

    /** Lock manager. */
    private final LockManager lockMgr;

    /** Replica service. */
    private final ReplicaService replicaSvc;

    /** Baseline manager. */
    private final BaselineManager baselineMgr;

    /** Transaction manager. */
    private final TxManager txManager;

    /** Meta storage manager. */
    private final MetaStorageManager metaStorageMgr;

    /** Data storage manager. */
    private final DataStorageManager dataStorageMgr;

    /** Placement driver. */
    private final PlacementDriver placementDriver;

    /** Here a table future stores during creation (until the table can be provided to client). */
    private final Map<UUID, CompletableFuture<Table>> tableCreateFuts = new ConcurrentHashMap<>();

    /** Versioned store for tables by id. */
    private final VersionedValue<Map<UUID, TableImpl>> tablesByIdVv;

    /** Set of futures that should complete before completion of {@link #tablesByIdVv}, after completion this set is cleared. */
    private final Set<CompletableFuture<?>> beforeTablesVvComplete = ConcurrentHashMap.newKeySet();

    /**
     * {@link TableImpl} is created during update of tablesByIdVv, we store reference to it in case of updating of tablesByIdVv fails, so we
     * can stop resources associated with the table.
     */
    private final Map<UUID, TableImpl> tablesToStopInCaseOfError = new ConcurrentHashMap<>();

    /** Resolver that resolves a node consistent ID to cluster node. */
    private final Function<String, ClusterNode> clusterNodeResolver;

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Schema manager. */
    private final SchemaManager schemaManager;

    private final LogStorageFactoryCreator volatileLogStorageFactoryCreator;

    /** Executor for scheduling retries of a rebalance. */
    private final ScheduledExecutorService rebalanceScheduler;

    /** Transaction state storage scheduled pool. */
    private final ScheduledExecutorService txStateStorageScheduledPool;

    /** Transaction state storage pool. */
    private final ExecutorService txStateStoragePool;

    /** Scan request executor. */
    private final ExecutorService scanRequestExecutor;

    /**
     * Separate executor for IO operations like partition storage initialization or partition raft group meta data persisting.
     */
    private final ExecutorService ioExecutor;

    private final HybridClock clock;

    private final OutgoingSnapshotsManager outgoingSnapshotsManager;

    /** Partitions storage path. */
    private final Path storagePath;

    /** Assignment change event listeners. */
    private final CopyOnWriteArrayList<Consumer<IgniteTablesInternal>> assignmentsChangeListeners = new CopyOnWriteArrayList<>();

    /** Incoming RAFT snapshots executor. */
    private final ExecutorService incomingSnapshotsExecutor;

    /** Rebalance scheduler pool size. */
    private static final int REBALANCE_SCHEDULER_POOL_SIZE = Math.min(Runtime.getRuntime().availableProcessors() * 3, 20);

    private static final TableMessagesFactory TABLE_MESSAGES_FACTORY = new TableMessagesFactory();

    /** Meta storage listener for changes in the distribution zones data nodes. */
    private final WatchListener distributionZonesDataNodesListener;

    /** Meta storage listener for pending assignments. */
    private final WatchListener pendingAssignmentsRebalanceListener;

    /** Meta storage listener for stable assignments. */
    private final WatchListener stableAssignmentsRebalanceListener;

    /** Meta storage listener for switch reduce assignments. */
    private final WatchListener assignmentsSwitchRebalanceListener;

    private final MvGc mvGc;

    /**
     * Creates a new table manager.
     *
     * @param nodeName Node name.
     * @param registry Registry for versioned values.
     * @param tablesCfg Tables configuration.
     * @param raftMgr Raft manager.
     * @param replicaMgr Replica manager.
     * @param lockMgr Lock manager.
     * @param replicaSvc Replica service.
     * @param baselineMgr Baseline manager.
     * @param txManager Transaction manager.
     * @param dataStorageMgr Data storage manager.
     * @param schemaManager Schema manager.
     * @param volatileLogStorageFactoryCreator Creator for {@link org.apache.ignite.internal.raft.storage.LogStorageFactory} for volatile
     *                                         tables.
     */
    public TableManager(
            String nodeName,
            Consumer<Function<Long, CompletableFuture<?>>> registry,
            TablesConfiguration tablesCfg,
            ClusterService clusterService,
            RaftManager raftMgr,
            ReplicaManager replicaMgr,
            LockManager lockMgr,
            ReplicaService replicaSvc,
            BaselineManager baselineMgr,
            TopologyService topologyService,
            TxManager txManager,
            DataStorageManager dataStorageMgr,
            Path storagePath,
            MetaStorageManager metaStorageMgr,
            SchemaManager schemaManager,
            LogStorageFactoryCreator volatileLogStorageFactoryCreator,
            HybridClock clock,
            OutgoingSnapshotsManager outgoingSnapshotsManager
    ) {
        this.tablesCfg = tablesCfg;
        this.clusterService = clusterService;
        this.raftMgr = raftMgr;
        this.baselineMgr = baselineMgr;
        this.replicaMgr = replicaMgr;
        this.lockMgr = lockMgr;
        this.replicaSvc = replicaSvc;
        this.txManager = txManager;
        this.dataStorageMgr = dataStorageMgr;
        this.storagePath = storagePath;
        this.metaStorageMgr = metaStorageMgr;
        this.schemaManager = schemaManager;
        this.volatileLogStorageFactoryCreator = volatileLogStorageFactoryCreator;
        this.clock = clock;
        this.outgoingSnapshotsManager = outgoingSnapshotsManager;

        clusterNodeResolver = topologyService::getByConsistentId;

        placementDriver = new PlacementDriver(replicaSvc, clusterNodeResolver);

        tablesByIdVv = new VersionedValue<>(null, HashMap::new);

        registry.accept(token -> {
            List<CompletableFuture<?>> futures = new ArrayList<>(beforeTablesVvComplete);

            beforeTablesVvComplete.clear();

            return allOf(futures.toArray(new CompletableFuture[]{}))
                    .orTimeout(TABLES_COMPLETE_TIMEOUT, TimeUnit.SECONDS)
                    .whenComplete((v, e) -> {
                        if (!busyLock.enterBusy()) {
                            if (e != null) {
                                LOG.warn("Error occurred while updating tables and stopping components.", e);
                                // Stop of the components has been started, so we do nothing and resources of tablesByIdVv will be
                                // freed in the logic of TableManager stop. We cannot complete tablesByIdVv exceptionally because
                                // we will lose a context of tables.
                            }
                            return;
                        }
                        try {
                            if (e != null) {
                                LOG.warn("Error occurred while updating tables.", e);
                                if (e instanceof CompletionException) {
                                    Throwable th = e.getCause();
                                    // Case when stopping of the previous component has been started and related futures completed
                                    // exceptionally
                                    if (th instanceof NodeStoppingException || (th.getCause() != null
                                            && th.getCause() instanceof NodeStoppingException)) {
                                        // Stop of the components has been started so we do nothing and resources will be freed in the
                                        // logic of TableManager stop
                                        return;
                                    }
                                }
                                // TODO: https://issues.apache.org/jira/browse/IGNITE-17515
                                tablesByIdVv.completeExceptionally(token, e);
                            }

                            //Normal scenario, when all related futures for tablesByIdVv are completed and we can complete tablesByIdVv
                            tablesByIdVv.complete(token);

                            tablesToStopInCaseOfError.clear();
                        } finally {
                            busyLock.leaveBusy();
                        }
                    });
        });

        txStateStorageScheduledPool = Executors.newSingleThreadScheduledExecutor(
                NamedThreadFactory.create(nodeName, "tx-state-storage-scheduled-pool", LOG));

        txStateStoragePool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(),
                NamedThreadFactory.create(nodeName, "tx-state-storage-pool", LOG));

        scanRequestExecutor = Executors.newSingleThreadExecutor(
                NamedThreadFactory.create(nodeName, "scan-query-executor-", LOG));

        rebalanceScheduler = new ScheduledThreadPoolExecutor(REBALANCE_SCHEDULER_POOL_SIZE,
                NamedThreadFactory.create(nodeName, "rebalance-scheduler", LOG));

        int cpus = Runtime.getRuntime().availableProcessors();

        ioExecutor = new ThreadPoolExecutor(
                Math.min(cpus * 3, 25),
                Integer.MAX_VALUE,
                100,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                NamedThreadFactory.create(nodeName, "tableManager-io", LOG));

        incomingSnapshotsExecutor = new ThreadPoolExecutor(
                cpus,
                cpus,
                100,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                NamedThreadFactory.create(nodeName, "incoming-raft-snapshot", LOG)
        );

        distributionZonesDataNodesListener = createDistributionZonesDataNodesListener();

        pendingAssignmentsRebalanceListener = createPendingAssignmentsRebalanceListener();

        stableAssignmentsRebalanceListener = createStableAssignmentsRebalanceListener();

        assignmentsSwitchRebalanceListener = createAssignmentsSwitchRebalanceListener();

        mvGc = new MvGc(nodeName, tablesCfg);
    }

    @Override
    public void start() {
        mvGc.start();

        tablesCfg.tables().any().replicas().listen(this::onUpdateReplicas);

        // TODO: IGNITE-18694 - Recovery for the case when zones watch listener processed event but assignments were not updated.
        metaStorageMgr.registerPrefixWatch(zoneDataNodesPrefix(), distributionZonesDataNodesListener);

        metaStorageMgr.registerPrefixWatch(ByteArray.fromString(PENDING_ASSIGNMENTS_PREFIX), pendingAssignmentsRebalanceListener);
        metaStorageMgr.registerPrefixWatch(ByteArray.fromString(STABLE_ASSIGNMENTS_PREFIX), stableAssignmentsRebalanceListener);
        metaStorageMgr.registerPrefixWatch(ByteArray.fromString(ASSIGNMENTS_SWITCH_REDUCE_PREFIX), assignmentsSwitchRebalanceListener);

        ((ExtendedTableConfiguration) tablesCfg.tables().any()).assignments().listen(this::onUpdateAssignments);

        tablesCfg.tables().listenElements(new ConfigurationNamedListListener<>() {
            @Override
            public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<TableView> ctx) {
                return onTableCreate(ctx);
            }

            @Override
            public CompletableFuture<?> onRename(String oldName, String newName, ConfigurationNotificationEvent<TableView> ctx) {
                // TODO: IGNITE-15485 Support table rename operation.

                return completedFuture(null);
            }

            @Override
            public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<TableView> ctx) {
                return onTableDelete(ctx);
            }
        });

        schemaManager.listen(SchemaEvent.CREATE, new EventListener<>() {
            /** {@inheritDoc} */
            @Override
            public CompletableFuture<Boolean> notify(@NotNull SchemaEventParameters parameters, @Nullable Throwable exception) {
                if (tablesByIdVv.latest().get(parameters.tableId()) != null) {
                    fireEvent(
                            TableEvent.ALTER,
                            new TableEventParameters(parameters.causalityToken(), tablesByIdVv.latest().get(parameters.tableId())), null
                    );
                }

                return completedFuture(false);
            }
        });

        addMessageHandler(clusterService.messagingService());
    }

    /**
     * Adds a table manager message handler.
     *
     * @param messagingService Messaging service.
     */
    private void addMessageHandler(MessagingService messagingService) {
        messagingService.addMessageHandler(TableMessageGroup.class, (message, sender, correlationId) -> {
            if (message instanceof HasDataRequest) {
                // This message queries if a node has any data for a specific partition of a table
                assert correlationId != null;

                HasDataRequest msg = (HasDataRequest) message;

                UUID tableId = msg.tableId();
                int partitionId = msg.partitionId();

                boolean contains = false;

                TableImpl table = tablesByIdVv.latest().get(tableId);

                if (table != null) {
                    MvTableStorage storage = table.internalTable().storage();

                    MvPartitionStorage mvPartition = storage.getMvPartition(partitionId);

                    // If node's recovery process is incomplete (no partition storage), then we consider this node's
                    // partition storage empty.
                    if (mvPartition != null) {
                        // If applied index of a storage is greater than 0,
                        // then there is data.
                        contains = mvPartition.lastAppliedIndex() > 0;
                    }
                }

                messagingService.respond(sender, TABLE_MESSAGES_FACTORY.hasDataResponse().result(contains).build(), correlationId);
            }
        });
    }

    /**
     * Listener of table create configuration change.
     *
     * @param ctx Table configuration context.
     * @return A future.
     */
    private CompletableFuture<?> onTableCreate(ConfigurationNotificationEvent<TableView> ctx) {
        if (!busyLock.enterBusy()) {
            String tblName = ctx.newValue().name();
            UUID tblId = ((ExtendedTableView) ctx.newValue()).id();

            fireEvent(TableEvent.CREATE,
                    new TableEventParameters(ctx.storageRevision(), tblId, tblName),
                    new NodeStoppingException()
            );

            return failedFuture(new NodeStoppingException());
        }

        try {
            return createTableLocally(
                    ctx.storageRevision(),
                    ctx.newValue().name(),
                    ((ExtendedTableView) ctx.newValue()).id(),
                    ctx.newValue().partitions()
            );
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Listener of table drop configuration change.
     *
     * @param ctx Table configuration context.
     * @return A future.
     */
    private CompletableFuture<?> onTableDelete(ConfigurationNotificationEvent<TableView> ctx) {
        if (!busyLock.enterBusy()) {
            String tblName = ctx.oldValue().name();
            UUID tblId = ((ExtendedTableView) ctx.oldValue()).id();

            fireEvent(
                    TableEvent.DROP,
                    new TableEventParameters(ctx.storageRevision(), tblId, tblName),
                    new NodeStoppingException()
            );

            return failedFuture(new NodeStoppingException());
        }

        try {
            dropTableLocally(
                    ctx.storageRevision(),
                    ctx.oldValue().name(),
                    ((ExtendedTableView) ctx.oldValue()).id(),
                    ByteUtils.fromBytes(((ExtendedTableView) ctx.oldValue()).assignments())
            );
        } finally {
            busyLock.leaveBusy();
        }

        return completedFuture(null);
    }

    /**
     * Listener of replicas configuration changes.
     *
     * @param replicasCtx Replicas configuration event context.
     * @return A future, which will be completed, when event processed by listener.
     */
    private CompletableFuture<?> onUpdateReplicas(ConfigurationNotificationEvent<Integer> replicasCtx) {
        if (!busyLock.enterBusy()) {
            return completedFuture(new NodeStoppingException());
        }

        try {
            if (replicasCtx.oldValue() != null && replicasCtx.oldValue() > 0) {
                TableConfiguration tblCfg = replicasCtx.config(TableConfiguration.class);

                LOG.info("Received update for replicas number [table={}, oldNumber={}, newNumber={}]",
                        tblCfg.name().value(), replicasCtx.oldValue(), replicasCtx.newValue());

                int partCnt = tblCfg.partitions().value();

                int newReplicas = replicasCtx.newValue();

                CompletableFuture<?>[] futures = new CompletableFuture<?>[partCnt];

                byte[] assignmentsBytes = ((ExtendedTableConfiguration) tblCfg).assignments().value();

                List<Set<Assignment>> tableAssignments = ByteUtils.fromBytes(assignmentsBytes);

                for (int i = 0; i < partCnt; i++) {
                    TablePartitionId replicaGrpId = new TablePartitionId(((ExtendedTableConfiguration) tblCfg).id().value(), i);

                    futures[i] = updatePendingAssignmentsKeys(tblCfg.name().value(), replicaGrpId,
                            baselineMgr.nodes().stream().map(ClusterNode::name).collect(toList()), newReplicas,
                            replicasCtx.storageRevision(), metaStorageMgr, i, tableAssignments.get(i));
                }

                return allOf(futures);
            } else {
                return completedFuture(null);
            }
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Listener of assignment configuration changes.
     *
     * @param assignmentsCtx Assignment configuration context.
     * @return A future.
     */
    private CompletableFuture<?> onUpdateAssignments(ConfigurationNotificationEvent<byte[]> assignmentsCtx) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            updateAssignmentInternal(assignmentsCtx);
        } finally {
            busyLock.leaveBusy();
        }

        for (var listener : assignmentsChangeListeners) {
            listener.accept(this);
        }

        return completedFuture(null);
    }

    /**
     * Updates or creates partition raft groups.
     *
     * @param assignmentsCtx Change assignment event.
     */
    private void updateAssignmentInternal(ConfigurationNotificationEvent<byte[]> assignmentsCtx) {
        ExtendedTableConfiguration tblCfg = assignmentsCtx.config(ExtendedTableConfiguration.class);

        UUID tblId = tblCfg.id().value();

        long causalityToken = assignmentsCtx.storageRevision();

        List<Set<Assignment>> oldAssignments = assignmentsCtx.oldValue() == null ? null : ByteUtils.fromBytes(assignmentsCtx.oldValue());

        List<Set<Assignment>> newAssignments = ByteUtils.fromBytes(assignmentsCtx.newValue());

        // Empty assignments might be a valid case if tables are created from within cluster init HOCON
        // configuration, which is not supported now.
        assert newAssignments != null : IgniteStringFormatter.format("Table [id={}] has empty assignments.", tblId);

        int partitions = newAssignments.size();

        CompletableFuture<?>[] futures = new CompletableFuture<?>[partitions];
        for (int i = 0; i < futures.length; i++) {
            futures[i] = new CompletableFuture<>();
        }

        String localMemberName = clusterService.topologyService().localMember().name();

        // Create new raft nodes according to new assignments.
        tablesByIdVv.update(causalityToken, (tablesById, e) -> {
            if (e != null) {
                return failedFuture(e);
            }

            for (int i = 0; i < partitions; i++) {
                int partId = i;

                Set<Assignment> oldPartAssignment = oldAssignments == null ? Set.of() : oldAssignments.get(partId);

                Set<Assignment> newPartAssignment = newAssignments.get(partId);

                TableImpl table = tablesById.get(tblId);
                InternalTable internalTbl = table.internalTable();

                Assignment localMemberAssignment = newPartAssignment.stream()
                        .filter(a -> a.consistentId().equals(localMemberName))
                        .findAny()
                        .orElse(null);

                PeersAndLearners newConfiguration = configurationFromAssignments(newPartAssignment);

                TablePartitionId replicaGrpId = new TablePartitionId(tblId, partId);

                placementDriver.updateAssignment(replicaGrpId, newConfiguration.peers().stream().map(Peer::consistentId).collect(toList()));

                PendingComparableValuesTracker<HybridTimestamp> safeTime = new PendingComparableValuesTracker<>(new HybridTimestamp(1, 0));

                CompletableFuture<PartitionStorages> partitionStoragesFut = getOrCreatePartitionStorages(table, partId);

                CompletableFuture<PartitionDataStorage> partitionDataStorageFut = partitionStoragesFut
                        .thenApply(partitionStorages -> partitionDataStorage(partitionStorages.getMvPartitionStorage(),
                                internalTbl, partId));

                CompletableFuture<StorageUpdateHandler> storageUpdateHandlerFut = partitionDataStorageFut
                        .thenApply(storage -> {
                            StorageUpdateHandler storageUpdateHandler = new StorageUpdateHandler(
                                    partId,
                                    storage,
                                    table.indexStorageAdapters(partId),
                                    tblCfg.dataStorage()
                            );

                            mvGc.addStorage(replicaGrpId, storageUpdateHandler);

                            return storageUpdateHandler;
                        });

                CompletableFuture<Void> startGroupFut;

                // start new nodes, only if it is table creation, other cases will be covered by rebalance logic
                if (oldPartAssignment.isEmpty() && localMemberAssignment != null) {
                    startGroupFut = partitionStoragesFut.thenComposeAsync(partitionStorages -> {
                        MvPartitionStorage mvPartitionStorage = partitionStorages.getMvPartitionStorage();

                        boolean hasData = mvPartitionStorage.lastAppliedIndex() > 0;

                        CompletableFuture<Boolean> fut;

                        // If Raft is running in in-memory mode or the PDS has been cleared, we need to remove the current node
                        // from the Raft group in order to avoid the double vote problem.
                        // <MUTED> See https://issues.apache.org/jira/browse/IGNITE-16668 for details.
                        if (internalTbl.storage().isVolatile() || !hasData) {
                            fut = queryDataNodesCount(tblId, partId, newConfiguration.peers()).thenApply(dataNodesCount -> {
                                boolean fullPartitionRestart = dataNodesCount == 0;

                                if (fullPartitionRestart) {
                                    return true;
                                }

                                boolean majorityAvailable = dataNodesCount >= (newConfiguration.peers().size() / 2) + 1;

                                if (majorityAvailable) {
                                    RebalanceUtil.startPeerRemoval(replicaGrpId, localMemberAssignment, metaStorageMgr);

                                    return false;
                                } else {
                                    // No majority and not a full partition restart - need to restart nodes
                                    // with current partition.
                                    String msg = "Unable to start partition " + partId + ". Majority not available.";

                                    throw new IgniteInternalException(msg);
                                }
                            });
                        } else {
                            fut = completedFuture(true);
                        }

                        return fut.thenCompose(startGroup -> {
                            if (!startGroup) {
                                return completedFuture(null);
                            }

                            return partitionDataStorageFut
                                    .thenCompose(s -> storageUpdateHandlerFut)
                                    .thenAcceptAsync(storageUpdateHandler -> {
                                        TxStateStorage txStatePartitionStorage = partitionStorages.getTxStateStorage();

                                        RaftGroupOptions groupOptions = groupOptionsForPartition(
                                                internalTbl.storage(),
                                                internalTbl.txStateStorage(),
                                                partitionKey(internalTbl, partId),
                                                storageUpdateHandler
                                        );

                                        Peer serverPeer = newConfiguration.peer(localMemberName);

                                        var raftNodeId = new RaftNodeId(replicaGrpId, serverPeer);

                                        PartitionDataStorage partitionDataStorage = partitionDataStorageFut.join();

                                        try {
                                            // TODO: use RaftManager interface, see https://issues.apache.org/jira/browse/IGNITE-18273
                                            ((Loza) raftMgr).startRaftGroupNode(
                                                    raftNodeId,
                                                    newConfiguration,
                                                    new PartitionListener(
                                                            partitionDataStorage,
                                                            storageUpdateHandler,
                                                            txStatePartitionStorage,
                                                            safeTime
                                                    ),
                                                    new RebalanceRaftGroupEventsListener(
                                                            metaStorageMgr,
                                                            tablesCfg.tables().get(table.name()),
                                                            replicaGrpId,
                                                            partId,
                                                            busyLock,
                                                            createPartitionMover(internalTbl, partId),
                                                            this::calculateAssignments,
                                                            rebalanceScheduler
                                                    ),
                                                    groupOptions
                                            );
                                        } catch (NodeStoppingException ex) {
                                            throw new CompletionException(ex);
                                        }
                                    }, ioExecutor);
                        });
                    }, ioExecutor);
                } else {
                    startGroupFut = completedFuture(null);
                }

                startGroupFut
                        .thenCompose(v -> storageUpdateHandlerFut)
                        .thenComposeAsync(v -> {
                            try {
                                return raftMgr.startRaftGroupService(replicaGrpId, newConfiguration);
                            } catch (NodeStoppingException ex) {
                                return failedFuture(ex);
                            }
                        }, ioExecutor)
                        .thenCompose(updatedRaftGroupService -> {
                            ((InternalTableImpl) internalTbl).updateInternalTableRaftGroupService(partId, updatedRaftGroupService);

                            if (localMemberAssignment == null) {
                                return completedFuture(null);
                            }

                            StorageUpdateHandler storageUpdateHandler = storageUpdateHandlerFut.join();

                            return partitionStoragesFut.thenAccept(partitionStorages -> {
                                MvPartitionStorage partitionStorage = partitionStorages.getMvPartitionStorage();
                                TxStateStorage txStateStorage = partitionStorages.getTxStateStorage();

                                try {
                                    replicaMgr.startReplica(replicaGrpId,
                                            new PartitionReplicaListener(
                                                    partitionStorage,
                                                    updatedRaftGroupService,
                                                    txManager,
                                                    lockMgr,
                                                    scanRequestExecutor,
                                                    partId,
                                                    tblId,
                                                    table.indexesLockers(partId),
                                                    new Lazy<>(() -> table.indexStorageAdapters(partId).get().get(table.pkId())),
                                                    () -> table.indexStorageAdapters(partId).get(),
                                                    clock,
                                                    safeTime,
                                                    txStateStorage,
                                                    placementDriver,
                                                    storageUpdateHandler,
                                                    this::isLocalPeer,
                                                    schemaManager.schemaRegistry(causalityToken, tblId)
                                            )
                                    );
                                } catch (NodeStoppingException ex) {
                                    throw new AssertionError("Loza was stopped before Table manager", ex);
                                }
                            });
                        })
                        .whenComplete((res, ex) -> {
                            if (ex != null) {
                                LOG.warn("Unable to update raft groups on the node", ex);
                            }

                            futures[partId].complete(null);
                        });
            }

            return allOf(futures).thenApply(unused -> tablesById);
        }).join();
    }

    private boolean isLocalPeer(Peer peer) {
        return peer.consistentId().equals(clusterService.topologyService().localMember().name());
    }

    private PartitionDataStorage partitionDataStorage(MvPartitionStorage partitionStorage, InternalTable internalTbl, int partId) {
        return new SnapshotAwarePartitionDataStorage(
                partitionStorage,
                outgoingSnapshotsManager,
                partitionKey(internalTbl, partId)
        );
    }

    private PartitionKey partitionKey(InternalTable internalTbl, int partId) {
        return new PartitionKey(internalTbl.tableId(), partId);
    }

    /**
     * Calculates the quantity of the data nodes for the partition of the table.
     *
     * @param tblId Table id.
     * @param partId Partition id.
     * @param peers Raft peers.
     * @return A future that will hold the quantity of data nodes.
     */
    private CompletableFuture<Long> queryDataNodesCount(UUID tblId, int partId, Collection<Peer> peers) {
        HasDataRequest request = TABLE_MESSAGES_FACTORY.hasDataRequest().tableId(tblId).partitionId(partId).build();

        //noinspection unchecked
        CompletableFuture<Boolean>[] requestFutures = peers.stream()
                .map(Peer::consistentId)
                .map(clusterNodeResolver)
                .filter(Objects::nonNull)
                .map(node -> clusterService.messagingService()
                        .invoke(node, request, QUERY_DATA_NODES_COUNT_TIMEOUT)
                        .thenApply(response -> {
                            assert response instanceof HasDataResponse : response;

                            return ((HasDataResponse) response).result();
                        })
                        .exceptionally(unused -> false))
                .toArray(CompletableFuture[]::new);

        return allOf(requestFutures)
                .thenApply(unused -> Arrays.stream(requestFutures).filter(CompletableFuture::join).count());
    }

    private RaftGroupOptions groupOptionsForPartition(
            MvTableStorage mvTableStorage,
            TxStateTableStorage txStateTableStorage,
            PartitionKey partitionKey,
            StorageUpdateHandler storageUpdateHandler
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
                clusterService.topologyService(),
                outgoingSnapshotsManager,
                new PartitionAccessImpl(
                        partitionKey,
                        mvTableStorage,
                        txStateTableStorage,
                        storageUpdateHandler,
                        mvGc
                ),
                incomingSnapshotsExecutor
        ));

        return raftGroupOptions;
    }

    /** {@inheritDoc} */
    @Override
    public void stop() {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        metaStorageMgr.unregisterWatch(distributionZonesDataNodesListener);

        metaStorageMgr.unregisterWatch(pendingAssignmentsRebalanceListener);
        metaStorageMgr.unregisterWatch(stableAssignmentsRebalanceListener);
        metaStorageMgr.unregisterWatch(assignmentsSwitchRebalanceListener);

        Map<UUID, TableImpl> tables = tablesByIdVv.latest();

        cleanUpTablesResources(tables);

        cleanUpTablesResources(tablesToStopInCaseOfError);

        tablesToStopInCaseOfError.clear();

        shutdownAndAwaitTermination(rebalanceScheduler, 10, TimeUnit.SECONDS);
        shutdownAndAwaitTermination(ioExecutor, 10, TimeUnit.SECONDS);
        shutdownAndAwaitTermination(txStateStoragePool, 10, TimeUnit.SECONDS);
        shutdownAndAwaitTermination(txStateStorageScheduledPool, 10, TimeUnit.SECONDS);
        shutdownAndAwaitTermination(scanRequestExecutor, 10, TimeUnit.SECONDS);
        shutdownAndAwaitTermination(incomingSnapshotsExecutor, 10, TimeUnit.SECONDS);
    }

    /**
     * Stops resources that are related to provided tables.
     *
     * @param tables Tables to stop.
     */
    private void cleanUpTablesResources(Map<UUID, TableImpl> tables) {
        for (TableImpl table : tables.values()) {
            table.beforeClose();

            List<Runnable> stopping = new ArrayList<>();

            AtomicReference<Throwable> throwable = new AtomicReference<>();

            AtomicBoolean nodeStoppingEx = new AtomicBoolean();

            for (int p = 0; p < table.internalTable().partitions(); p++) {
                TablePartitionId replicationGroupId = new TablePartitionId(table.tableId(), p);

                stopping.add(() -> {
                    try {
                        raftMgr.stopRaftNodes(replicationGroupId);
                    } catch (Throwable t) {
                        handleExceptionOnCleanUpTablesResources(t, throwable, nodeStoppingEx);
                    }
                });

                stopping.add(() -> {
                    try {
                        replicaMgr.stopReplica(replicationGroupId);
                    } catch (Throwable t) {
                        handleExceptionOnCleanUpTablesResources(t, throwable, nodeStoppingEx);
                    }
                });

                CompletableFuture<Void> removeFromGcFuture = mvGc.removeStorage(replicationGroupId);

                stopping.add(() -> {
                    try {
                        // Should be done fairly quickly.
                        removeFromGcFuture.join();
                    } catch (Throwable t) {
                        handleExceptionOnCleanUpTablesResources(t, throwable, nodeStoppingEx);
                    }
                });
            }

            stopping.forEach(Runnable::run);

            try {
                IgniteUtils.closeAllManually(
                        table.internalTable().storage(),
                        table.internalTable().txStateStorage(),
                        table.internalTable()
                );
            } catch (Throwable t) {
                handleExceptionOnCleanUpTablesResources(t, throwable, nodeStoppingEx);
            }

            if (throwable.get() != null) {
                LOG.error("Unable to stop table [name={}, tableId={}]", throwable.get(), table.name(), table.tableId());
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public List<String> assignments(UUID tableId) throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }
        try {
            TableImpl table = table(tableId);

            if (table == null) {
                return null;
            }

            return table.internalTable().assignments();
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void addAssignmentsChangeListener(Consumer<IgniteTablesInternal> listener) {
        Objects.requireNonNull(listener);

        assignmentsChangeListeners.add(listener);
    }

    /** {@inheritDoc} */
    @Override
    public boolean removeAssignmentsChangeListener(Consumer<IgniteTablesInternal> listener) {
        Objects.requireNonNull(listener);

        return assignmentsChangeListeners.remove(listener);
    }

    /**
     * Creates local structures for a table.
     *
     * @param causalityToken Causality token.
     * @param name Table name.
     * @param tblId Table id.
     * @param partitions Count of partitions.
     * @return Future that will be completed when local changes related to the table creation are applied.
     */
    private CompletableFuture<?> createTableLocally(long causalityToken, String name, UUID tblId, int partitions) {
        LOG.trace("Creating local table: name={}, id={}, token={}", name, tblId, causalityToken);

        TableConfiguration tableCfg = tablesCfg.tables().get(name);

        MvTableStorage tableStorage = createTableStorage(tableCfg, tablesCfg);
        TxStateTableStorage txStateStorage = createTxStateTableStorage(tableCfg);

        InternalTableImpl internalTable = new InternalTableImpl(name, tblId, new Int2ObjectOpenHashMap<>(partitions),
                partitions, clusterNodeResolver, txManager, tableStorage, txStateStorage, replicaSvc, clock);

        // TODO: IGNITE-16288 directIndexIds should use async configuration API
        var table = new TableImpl(internalTable, lockMgr, () -> CompletableFuture.supplyAsync(() -> directIndexIds()));

        tablesByIdVv.update(causalityToken, (previous, e) -> inBusyLock(busyLock, () -> {
            if (e != null) {
                return failedFuture(e);
            }

            var val = new HashMap<>(previous);

            val.put(tblId, table);

            return completedFuture(val);
        }));

        CompletableFuture<?> schemaFut = schemaManager.schemaRegistry(causalityToken, tblId)
                .thenAccept(schema -> inBusyLock(busyLock, () -> table.schemaView(schema)))
                .thenCompose(
                        v -> inBusyLock(busyLock, () -> fireEvent(TableEvent.CREATE, new TableEventParameters(causalityToken, table)))
                );

        beforeTablesVvComplete.add(schemaFut);

        tablesToStopInCaseOfError.put(tblId, table);

        tablesByIdVv.get(causalityToken)
                .thenRun(() -> inBusyLock(busyLock, () -> completeApiCreateFuture(table)));

        // TODO should be reworked in IGNITE-16763
        return completedFuture(null);
    }

    /**
     * Creates data storage for the provided table.
     *
     * @param tableCfg Table configuration.
     * @param tablesCfg Tables configuration.
     * @return Table data storage.
     */
    protected MvTableStorage createTableStorage(TableConfiguration tableCfg, TablesConfiguration tablesCfg) {
        MvTableStorage tableStorage = dataStorageMgr.engine(tableCfg.dataStorage()).createMvTable(tableCfg, tablesCfg);

        tableStorage.start();

        return tableStorage;
    }

    /**
     * Creates transaction state storage for the provided table.
     *
     * @param tableCfg Table configuration.
     * @return Transaction state storage.
     */
    protected TxStateTableStorage createTxStateTableStorage(TableConfiguration tableCfg) {
        Path path = storagePath.resolve(TX_STATE_DIR + tableCfg.value().tableId());

        try {
            Files.createDirectories(path);
        } catch (IOException e) {
            throw new StorageException("Failed to create transaction state storage directory for " + tableCfg.value().name(), e);
        }

        TxStateTableStorage txStateTableStorage = new TxStateRocksDbTableStorage(
                tableCfg,
                path,
                txStateStorageScheduledPool,
                txStateStoragePool,
                TX_STATE_STORAGE_FLUSH_DELAY_SUPPLIER
        );

        txStateTableStorage.start();

        return txStateTableStorage;
    }

    /**
     * Completes appropriate future to return result from API {@link TableManager#createTableAsync(String, Consumer)}.
     *
     * @param table Table.
     */
    private void completeApiCreateFuture(TableImpl table) {
        LOG.trace("Finish creating table: name={}, id={}", table.name(), table.tableId());

        CompletableFuture<Table> tblFut = tableCreateFuts.get(table.tableId());

        if (tblFut != null) {
            tblFut.complete(table);

            tableCreateFuts.values().removeIf(fut -> fut == tblFut);
        }
    }

    /**
     * Drops local structures for a table.
     *
     * @param causalityToken Causality token.
     * @param name Table name.
     * @param tblId Table id.
     * @param assignment Affinity assignment.
     */
    private void dropTableLocally(long causalityToken, String name, UUID tblId, List<Set<ClusterNode>> assignment) {
        try {
            int partitions = assignment.size();

            CompletableFuture<?>[] removeStorageFromGcFutures = new CompletableFuture<?>[partitions];

            for (int p = 0; p < partitions; p++) {
                TablePartitionId replicationGroupId = new TablePartitionId(tblId, p);

                raftMgr.stopRaftNodes(replicationGroupId);

                replicaMgr.stopReplica(replicationGroupId);

                removeStorageFromGcFutures[p] = mvGc.removeStorage(replicationGroupId);
            }

            tablesByIdVv.update(causalityToken, (previousVal, e) -> inBusyLock(busyLock, () -> {
                if (e != null) {
                    return failedFuture(e);
                }

                var map = new HashMap<>(previousVal);

                map.remove(tblId);

                return completedFuture(map);
            }));

            TableImpl table = tablesByIdVv.latest().get(tblId);

            assert table != null : IgniteStringFormatter.format("There is no table with the name specified [name={}, id={}]",
                    name, tblId);

            // TODO: IGNITE-18703 Destroy raft log and meta

            CompletableFuture<Void> destroyTableStoragesFuture = allOf(removeStorageFromGcFutures)
                    .thenCompose(unused -> allOf(
                            table.internalTable().storage().destroy(),
                            runAsync(() -> table.internalTable().txStateStorage().destroy(), ioExecutor))
                    );

            CompletableFuture<?> dropSchemaRegistryFuture = schemaManager.dropRegistry(causalityToken, table.tableId())
                    .thenCompose(
                            v -> inBusyLock(busyLock, () -> fireEvent(TableEvent.DROP, new TableEventParameters(causalityToken, table)))
                    );

            // TODO: IGNITE-18687 Must be asynchronous
            destroyTableStoragesFuture.join();

            beforeTablesVvComplete.add(allOf(destroyTableStoragesFuture, dropSchemaRegistryFuture));
        } catch (Exception e) {
            fireEvent(TableEvent.DROP, new TableEventParameters(causalityToken, tblId, name), e);
        }
    }

    private Set<Assignment> calculateAssignments(TableConfiguration tableCfg, int partNum) {
        return AffinityUtils.calculateAssignmentForPartition(
                baselineMgr.nodes().stream().map(ClusterNode::name).collect(toList()),
                partNum,
                tableCfg.value().replicas()
        );
    }

    /**
     * Creates a new table with the given {@code name} asynchronously. If a table with the same name already exists, a future will be
     * completed with {@link TableAlreadyExistsException}.
     *
     * @param name Table name.
     * @param tableInitChange Table changer.
     * @return Future representing pending completion of the operation.
     * @throws IgniteException If an unspecified platform exception has happened internally. Is thrown when:
     *                         <ul>
     *                             <li>the node is stopping.</li>
     *                         </ul>
     * @see TableAlreadyExistsException
     */
    public CompletableFuture<Table> createTableAsync(String name, Consumer<TableChange> tableInitChange) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }
        try {
            return createTableAsyncInternal(name, tableInitChange);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** See {@link #createTableAsync(String, Consumer)} for details. */
    private CompletableFuture<Table> createTableAsyncInternal(String name, Consumer<TableChange> tableInitChange) {
        CompletableFuture<Table> tblFut = new CompletableFuture<>();

        tableAsyncInternal(name).thenAccept(tbl -> {
            if (tbl != null) {
                tblFut.completeExceptionally(new TableAlreadyExistsException(DEFAULT_SCHEMA_NAME, name));
            } else {
                tablesCfg.change(tablesChange -> tablesChange.changeTables(tablesListChange -> {
                    if (tablesListChange.get(name) != null) {
                        throw new TableAlreadyExistsException(DEFAULT_SCHEMA_NAME, name);
                    }

                    tablesListChange.create(name, (tableChange) -> {
                        tableChange.changeDataStorage(
                                dataStorageMgr.defaultTableDataStorageConsumer(tablesChange.defaultDataStorage())
                        );

                        tableInitChange.accept(tableChange);

                        var extConfCh = ((ExtendedTableChange) tableChange);

                        int intTableId = tablesChange.globalIdCounter() + 1;
                        tablesChange.changeGlobalIdCounter(intTableId);

                        extConfCh.changeTableId(intTableId);

                        extConfCh.changeSchemaId(INITIAL_SCHEMA_VERSION);

                        tableCreateFuts.put(extConfCh.id(), tblFut);

                        // Affinity assignments calculation.
                        extConfCh.changeAssignments(ByteUtils.toBytes(AffinityUtils.calculateAssignments(
                                baselineMgr.nodes().stream().map(ClusterNode::name).collect(toList()),
                                tableChange.partitions(),
                                tableChange.replicas())));
                    });
                })).exceptionally(t -> {
                    Throwable ex = getRootCause(t);

                    if (ex instanceof TableAlreadyExistsException) {
                        tblFut.completeExceptionally(ex);
                    } else {
                        LOG.debug("Unable to create table [name={}]", ex, name);

                        tblFut.completeExceptionally(ex);

                        tableCreateFuts.values().removeIf(fut -> fut == tblFut);
                    }

                    return null;
                });
            }
        });

        return tblFut;
    }

    /**
     * Alters a cluster table. If an appropriate table does not exist, a future will be completed with {@link TableNotFoundException}.
     *
     * @param name Table name.
     * @param tableChange Table changer.
     * @return Future representing pending completion of the operation.
     * @throws IgniteException If an unspecified platform exception has happened internally. Is thrown when:
     *                         <ul>
     *                             <li>the node is stopping.</li>
     *                         </ul>
     * @see TableNotFoundException
     */
    public CompletableFuture<Void> alterTableAsync(String name, Function<TableChange, Boolean> tableChange) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }
        try {
            return alterTableAsyncInternal(name, tableChange);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** See {@link #alterTableAsync(String, Function)} for details. */
    private CompletableFuture<Void> alterTableAsyncInternal(String name, Function<TableChange, Boolean> tableChange) {
        CompletableFuture<Void> tblFut = new CompletableFuture<>();

        tableAsync(name).thenAccept(tbl -> {
            if (tbl == null) {
                tblFut.completeExceptionally(new TableNotFoundException(DEFAULT_SCHEMA_NAME, name));
            } else {
                tablesCfg.tables().change(ch -> {
                    if (ch.get(name) == null) {
                        throw new TableNotFoundException(DEFAULT_SCHEMA_NAME, name);
                    }

                    ch.update(name, tblCh -> {
                        if (!tableChange.apply(tblCh)) {
                            return;
                        }

                        ExtendedTableChange exTblChange = (ExtendedTableChange) tblCh;

                        exTblChange.changeSchemaId(exTblChange.schemaId() + 1);
                    });
                }).whenComplete((res, t) -> {
                    if (t != null) {
                        Throwable ex = getRootCause(t);

                        if (ex instanceof TableNotFoundException) {
                            tblFut.completeExceptionally(ex);
                        } else {
                            LOG.debug("Unable to modify table [name={}]", ex, name);

                            tblFut.completeExceptionally(ex);
                        }
                    } else {
                        tblFut.complete(res);
                    }
                });
            }
        }).exceptionally(th -> {
            tblFut.completeExceptionally(th);

            return null;
        });

        return tblFut;
    }

    /**
     * Gets a cause exception for a client.
     *
     * @param t Exception wrapper.
     * @return A root exception which will be acceptable to throw for public API.
     */
    //TODO: IGNITE-16051 Implement exception converter for public API.
    private @NotNull IgniteException getRootCause(Throwable t) {
        Throwable ex;

        if (t instanceof CompletionException) {
            if (t.getCause() instanceof ConfigurationChangeException) {
                ex = t.getCause().getCause();
            } else {
                ex = t.getCause();
            }

        } else {
            ex = t;
        }

        return ex instanceof IgniteException ? (IgniteException) ex : new IgniteException(ex);
    }

    /**
     * Drops a table with the name specified. If appropriate table does not be found, a future will be completed with
     * {@link TableNotFoundException}.
     *
     * @param name Table name.
     * @return Future representing pending completion of the operation.
     * @throws IgniteException If an unspecified platform exception has happened internally. Is thrown when:
     *                         <ul>
     *                             <li>the node is stopping.</li>
     *                         </ul>
     * @see TableNotFoundException
     */
    public CompletableFuture<Void> dropTableAsync(String name) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }
        try {
            return dropTableAsyncInternal(name);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** See {@link #dropTableAsync(String)} for details. */
    private CompletableFuture<Void> dropTableAsyncInternal(String name) {
        CompletableFuture<Void> dropTblFut = new CompletableFuture<>();

        tableAsyncInternal(name).thenAccept(tbl -> {
            // In case of drop it's an optimization that allows not to fire drop-change-closure if there's no such
            // distributed table and the local config has lagged behind.
            if (tbl == null) {
                dropTblFut.completeExceptionally(new TableNotFoundException(DEFAULT_SCHEMA_NAME, name));
            } else {
                tablesCfg.change(chg ->
                        chg.changeTables(tblChg -> {
                            TableView tableCfg = tblChg.get(name);

                            if (tableCfg == null) {
                                throw new TableNotFoundException(DEFAULT_SCHEMA_NAME, name);
                            }

                            tblChg.delete(name);
                        }).changeIndexes(idxChg -> {
                            List<String> indicesNames = tablesCfg.indexes().value().namedListKeys();

                            indicesNames.stream().filter(idx ->
                                    tablesCfg.indexes().get(idx).tableId().value().equals(tbl.tableId())).forEach(idxChg::delete);
                        }))
                        .whenComplete((res, t) -> {
                            if (t != null) {
                                Throwable ex = getRootCause(t);

                                if (ex instanceof TableNotFoundException) {
                                    dropTblFut.completeExceptionally(ex);
                                } else {
                                    LOG.debug("Unable to drop table [name={}]", ex, name);

                                    dropTblFut.completeExceptionally(ex);
                                }
                            } else {
                                dropTblFut.complete(res);
                            }
                        });
            }
        });

        return dropTblFut;
    }

    /** {@inheritDoc} */
    @Override
    public List<Table> tables() {
        return join(tablesAsync());
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<List<Table>> tablesAsync() {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }
        try {
            return tablesAsyncInternal();
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Internal method for getting table.
     *
     * @return Future representing pending completion of the operation.
     */
    private CompletableFuture<List<Table>> tablesAsyncInternal() {
        // TODO: IGNITE-16288 directTableIds should use async configuration API
        return CompletableFuture.supplyAsync(() -> inBusyLock(busyLock, this::directTableIds))
                .thenCompose(tableIds -> inBusyLock(busyLock, () -> {
                    var tableFuts = new CompletableFuture[tableIds.size()];

                    var i = 0;

                    for (UUID tblId : tableIds) {
                        tableFuts[i++] = tableAsyncInternal(tblId, false);
                    }

                    return allOf(tableFuts).thenApply(unused -> inBusyLock(busyLock, () -> {
                        var tables = new ArrayList<Table>(tableIds.size());

                        for (var fut : tableFuts) {
                            var table = fut.join();

                            if (table != null) {
                                tables.add((Table) table);
                            }
                        }

                        return tables;
                    }));
                }));
    }

    /**
     * Collects a list of direct table ids.
     *
     * @return A list of direct table ids.
     */
    private List<UUID> directTableIds() {
        return directProxy(tablesCfg.tables()).internalIds();
    }

    /**
     * Collects a list of direct index ids.
     *
     * @return A list of direct index ids.
     */
    private List<UUID> directIndexIds() {
        return directProxy(tablesCfg.indexes()).internalIds();
    }

    /**
     * Gets direct id of table with {@code tblName}.
     *
     * @param tblName Name of the table.
     * @return Direct id of the table, or {@code null} if the table with the {@code tblName} has not been found.
     */
    @Nullable
    private UUID directTableId(String tblName) {
        try {
            ExtendedTableConfiguration exTblCfg = ((ExtendedTableConfiguration) directProxy(tablesCfg.tables()).get(tblName));

            if (exTblCfg == null) {
                return null;
            } else {
                return exTblCfg.id().value();
            }
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    /**
     * Actual tables map.
     *
     * @return Actual tables map.
     */
    @TestOnly
    public Map<UUID, TableImpl> latestTables() {
        return unmodifiableMap(tablesByIdVv.latest());
    }

    /** {@inheritDoc} */
    @Override
    public Table table(String name) {
        return join(tableAsync(name));
    }

    /** {@inheritDoc} */
    @Override
    public TableImpl table(UUID id) throws NodeStoppingException {
        return join(tableAsync(id));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Table> tableAsync(String name) {
        return tableAsyncInternal(IgniteNameUtils.parseSimpleName(name))
                .thenApply(Function.identity());
    }

    /**
     * Asynchronously gets the table using causality token.
     *
     * @param causalityToken Causality token.
     * @param id Table id.
     * @return Future.
     */
    public CompletableFuture<TableImpl> tableAsync(long causalityToken, UUID id) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }
        try {
            return tablesByIdVv.get(causalityToken).thenApply(tablesById -> tablesById.get(id));
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<TableImpl> tableAsync(UUID id) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }
        try {
            return tableAsyncInternal(id, true);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override
    public TableImpl tableImpl(String name) {
        return join(tableImplAsync(name));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<TableImpl> tableImplAsync(String name) {
        return tableAsyncInternal(IgniteNameUtils.parseSimpleName(name));
    }

    /**
     * Gets a table by name, if it was created before. Doesn't parse canonical name.
     *
     * @param name Table name.
     * @return Future representing pending completion of the {@code TableManager#tableAsyncInternal} operation.
     */
    public CompletableFuture<TableImpl> tableAsyncInternal(String name) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }
        try {
            // TODO: IGNITE-16288 directTableId should use async configuration API
            return CompletableFuture.supplyAsync(() -> inBusyLock(busyLock, () -> directTableId(name)))
                    .thenCompose(tableId -> inBusyLock(busyLock, () -> {
                        if (tableId == null) {
                            return completedFuture(null);
                        }

                        return tableAsyncInternal(tableId, false);
                    }));
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Internal method for getting table by id.
     *
     * @param id Table id.
     * @param checkConfiguration {@code True} when the method checks a configuration before trying to get a table, {@code false} otherwise.
     * @return Future representing pending completion of the operation.
     */
    public CompletableFuture<TableImpl> tableAsyncInternal(UUID id, boolean checkConfiguration) {
        CompletableFuture<Boolean> tblCfgFut = checkConfiguration
                // TODO: IGNITE-16288 isTableConfigured should use async configuration API
                ? CompletableFuture.supplyAsync(() -> inBusyLock(busyLock, () -> isTableConfigured(id)))
                : completedFuture(true);

        return tblCfgFut.thenCompose(isCfg -> inBusyLock(busyLock, () -> {
            if (!isCfg) {
                return completedFuture(null);
            }

            var tbl = tablesByIdVv.latest().get(id);

            if (tbl != null) {
                return completedFuture(tbl);
            }

            CompletableFuture<TableImpl> getTblFut = new CompletableFuture<>();

            IgniteTriConsumer<Long, Map<UUID, TableImpl>, Throwable> tablesListener = (token, tables, th) -> {
                if (th == null) {
                    TableImpl table = tables.get(id);

                    if (table != null) {
                        getTblFut.complete(table);
                    }
                } else {
                    getTblFut.completeExceptionally(th);
                }
            };

            tablesByIdVv.whenComplete(tablesListener);

            // This check is needed for the case when we have registered tablesListener,
            // but tablesByIdVv has already been completed, so listener would be triggered only for the next versioned value update.
            tbl = tablesByIdVv.latest().get(id);

            if (tbl != null) {
                tablesByIdVv.removeWhenComplete(tablesListener);

                return completedFuture(tbl);
            }

            return getTblFut.whenComplete((unused, throwable) -> tablesByIdVv.removeWhenComplete(tablesListener));
        }));
    }

    /**
     * Checks that the table is configured with specific id.
     *
     * @param id Table id.
     * @return True when the table is configured into cluster, false otherwise.
     */
    private boolean isTableConfigured(UUID id) {
        try {
            NamedConfigurationTree<TableConfiguration, ?, ?> cfg = directProxy(tablesCfg.tables());
            ((ExtendedTableConfiguration) cfg.get(id)).id().value();

            return true;
        } catch (NoSuchElementException e) {
            return false;
        }
    }

    /**
     * Waits for future result and return, or unwraps {@link CompletionException} to {@link IgniteException} if failed.
     *
     * @param future Completable future.
     * @return Future result.
     */
    private <T> T join(CompletableFuture<T> future) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }

        try {
            return future.join();
        } catch (CompletionException ex) {
            throw convertThrowable(ex.getCause());
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Convert to public throwable.
     *
     * @param th Throwable.
     * @return Public throwable.
     */
    private RuntimeException convertThrowable(Throwable th) {
        if (th instanceof RuntimeException) {
            return (RuntimeException) th;
        }

        return new IgniteException(th);
    }

    /**
     * Creates meta storage listener for distribution zones data nodes updates.
     *
     * @return The watch listener.
     */
    private WatchListener createDistributionZonesDataNodesListener() {
        return new WatchListener() {
            @Override
            public void onUpdate(WatchEvent evt) {
                if (!busyLock.enterBusy()) {
                    throw new IgniteInternalException(new NodeStoppingException());
                }

                try {
                    byte[] dataNodesBytes = evt.entryEvent().newEntry().value();

                    if (dataNodesBytes == null) {
                        //The zone was removed so data nodes was removed too.
                        return;
                    }

                    NamedConfigurationTree<TableConfiguration, TableView, TableChange> tables = tablesCfg.tables();

                    int zoneId = extractZoneId(evt.entryEvent().newEntry().key());

                    Set<String> dataNodes = dataNodes(ByteUtils.fromBytes(dataNodesBytes));

                    for (int i = 0; i < tables.value().size(); i++) {
                        TableView tableView = tables.value().get(i);

                        int tableZoneId = tableView.zoneId();

                        if (zoneId == tableZoneId) {
                            TableConfiguration tableCfg = tables.get(tableView.name());

                            byte[] assignmentsBytes = ((ExtendedTableConfiguration) tableCfg).assignments().value();

                            List<Set<Assignment>> tableAssignments = ByteUtils.fromBytes(assignmentsBytes);

                            for (int part = 0; part < tableView.partitions(); part++) {
                                UUID tableId = ((ExtendedTableConfiguration) tableCfg).id().value();

                                TablePartitionId replicaGrpId = new TablePartitionId(tableId, part);

                                int replicas = tableView.replicas();

                                int partId = part;

                                updatePendingAssignmentsKeys(
                                        tableView.name(), replicaGrpId, dataNodes, replicas,
                                        evt.entryEvent().newEntry().revision(), metaStorageMgr, part, tableAssignments.get(part)
                                ).exceptionally(e -> {
                                    LOG.error(
                                            "Exception on updating assignments for [table={}, partition={}]", e, tableView.name(),
                                            partId
                                    );

                                    return null;
                                });
                            }
                        }
                    }
                } finally {
                    busyLock.leaveBusy();
                }
            }

            @Override
            public void onError(Throwable e) {
                LOG.warn("Unable to process data nodes event", e);
            }
        };
    }

    /**
     * Creates meta storage listener for pending assignments updates.
     *
     * @return The watch listener.
     */
    private WatchListener createPendingAssignmentsRebalanceListener() {
        return new WatchListener() {
            @Override
            public void onUpdate(WatchEvent evt) {
                if (!busyLock.enterBusy()) {
                    throw new IgniteInternalException(new NodeStoppingException());
                }

                try {
                    assert evt.single();

                    Entry pendingAssignmentsWatchEvent = evt.entryEvent().newEntry();

                    if (pendingAssignmentsWatchEvent.value() == null) {
                        return;
                    }

                    int partId = extractPartitionNumber(pendingAssignmentsWatchEvent.key());
                    UUID tblId = extractTableId(pendingAssignmentsWatchEvent.key(), PENDING_ASSIGNMENTS_PREFIX);

                    TablePartitionId replicaGrpId = new TablePartitionId(tblId, partId);

                    Entry pendingAssignmentsEntry = metaStorageMgr.get(pendingPartAssignmentsKey(replicaGrpId)).join();

                    assert pendingAssignmentsWatchEvent.revision() <= pendingAssignmentsEntry.revision()
                            : "Meta Storage watch cannot notify about an event with the revision that is more than the actual revision.";

                    // Assignments of the pending rebalance that we received through Meta storage watch mechanism.
                    Set<Assignment> pendingAssignments = ByteUtils.fromBytes(pendingAssignmentsWatchEvent.value());

                    PeersAndLearners pendingConfiguration = configurationFromAssignments(pendingAssignments);

                    TableImpl tbl = tablesByIdVv.latest().get(tblId);

                    ExtendedTableConfiguration tblCfg = (ExtendedTableConfiguration) tablesCfg.tables().get(tbl.name());

                    // Stable assignments from the meta store, which revision is bounded by the current pending event.
                    byte[] stableAssignmentsBytes = metaStorageMgr.get(stablePartAssignmentsKey(replicaGrpId),
                            pendingAssignmentsWatchEvent.revision()).join().value();

                    Set<Assignment> stableAssignments = stableAssignmentsBytes == null
                            // This is for the case when the first rebalance occurs.
                            ? ((List<Set<Assignment>>) ByteUtils.fromBytes(tblCfg.assignments().value())).get(partId)
                            : ByteUtils.fromBytes(stableAssignmentsBytes);

                    PeersAndLearners stableConfiguration = configurationFromAssignments(stableAssignments);

                    placementDriver.updateAssignment(
                            replicaGrpId,
                            stableConfiguration.peers().stream().map(Peer::consistentId).collect(toList())
                    );

                    ClusterNode localMember = clusterService.topologyService().localMember();

                    // Start a new Raft node and Replica if this node has appeared in the new assignments.
                    boolean shouldStartLocalServices = pendingAssignments.stream()
                            .filter(assignment -> localMember.name().equals(assignment.consistentId()))
                            .anyMatch(assignment -> !stableAssignments.contains(assignment));

                    PendingComparableValuesTracker<HybridTimestamp> safeTime =
                            new PendingComparableValuesTracker<>(new HybridTimestamp(1, 0));

                    InternalTable internalTable = tbl.internalTable();

                    LOG.info("Received update on pending assignments. Check if new raft group should be started"
                                    + " [key={}, partition={}, table={}, localMemberAddress={}]",
                            pendingAssignmentsWatchEvent.key(), partId, tbl.name(), localMember.address());

                    if (shouldStartLocalServices) {
                        PartitionStorages partitionStorages = getOrCreatePartitionStorages(tbl, partId).join();

                        MvPartitionStorage mvPartitionStorage = partitionStorages.getMvPartitionStorage();
                        TxStateStorage txStatePartitionStorage = partitionStorages.getTxStateStorage();

                        PartitionDataStorage partitionDataStorage = partitionDataStorage(mvPartitionStorage, internalTable, partId);
                        StorageUpdateHandler storageUpdateHandler = new StorageUpdateHandler(
                                partId,
                                partitionDataStorage,
                                tbl.indexStorageAdapters(partId),
                                tblCfg.dataStorage()
                        );

                        RaftGroupOptions groupOptions = groupOptionsForPartition(
                                internalTable.storage(),
                                internalTable.txStateStorage(),
                                partitionKey(internalTable, partId),
                                storageUpdateHandler
                        );

                        RaftGroupListener raftGrpLsnr = new PartitionListener(
                                partitionDataStorage,
                                storageUpdateHandler,
                                txStatePartitionStorage,
                                safeTime
                        );

                        RaftGroupEventsListener raftGrpEvtsLsnr = new RebalanceRaftGroupEventsListener(
                                metaStorageMgr,
                                tblCfg,
                                replicaGrpId,
                                partId,
                                busyLock,
                                createPartitionMover(internalTable, partId),
                                TableManager.this::calculateAssignments,
                                rebalanceScheduler
                        );

                        Peer serverPeer = pendingConfiguration.peer(localMember.name());

                        var raftNodeId = new RaftNodeId(replicaGrpId, serverPeer);

                        try {
                            // TODO: use RaftManager interface, see https://issues.apache.org/jira/browse/IGNITE-18273
                            ((Loza) raftMgr).startRaftGroupNode(
                                    raftNodeId,
                                    stableConfiguration,
                                    raftGrpLsnr,
                                    raftGrpEvtsLsnr,
                                    groupOptions
                            );

                            replicaMgr.startReplica(replicaGrpId,
                                    new PartitionReplicaListener(
                                            mvPartitionStorage,
                                            internalTable.partitionRaftGroupService(partId),
                                            txManager,
                                            lockMgr,
                                            scanRequestExecutor,
                                            partId,
                                            tblId,
                                            tbl.indexesLockers(partId),
                                            new Lazy<>(() -> tbl.indexStorageAdapters(partId).get().get(tbl.pkId())),
                                            () -> tbl.indexStorageAdapters(partId).get(),
                                            clock,
                                            safeTime,
                                            txStatePartitionStorage,
                                            placementDriver,
                                            storageUpdateHandler,
                                            TableManager.this::isLocalPeer,
                                            completedFuture(schemaManager.schemaRegistry(tblId))
                                    )
                            );
                        } catch (NodeStoppingException e) {
                            // no-op
                        }
                    }

                    // Do not change peers of the raft group if this is a stale event.
                    // Note that we start raft node before for the sake of the consistency in a starting and stopping raft nodes.
                    if (pendingAssignmentsWatchEvent.revision() < pendingAssignmentsEntry.revision()) {
                        return;
                    }

                    RaftGroupService partGrpSvc = internalTable.partitionRaftGroupService(partId);

                    LeaderWithTerm leaderWithTerm = partGrpSvc.refreshAndGetLeaderWithTerm().join();

                    // run update of raft configuration if this node is a leader
                    if (isLocalPeer(leaderWithTerm.leader())) {
                        LOG.info("Current node={} is the leader of partition raft group={}. "
                                        + "Initiate rebalance process for partition={}, table={}",
                                localMember.address(), replicaGrpId, partId, tbl.name());

                        partGrpSvc.changePeersAsync(pendingConfiguration, leaderWithTerm.term()).join();
                    }
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

    /**
     * Creates Meta storage listener for stable assignments updates.
     *
     * @return The watch listener.
     */
    private WatchListener createStableAssignmentsRebalanceListener() {
        return new WatchListener() {
            @Override
            public void onUpdate(WatchEvent evt) {
                handleChangeStableAssignmentEvent(evt);
            }

            @Override
            public void onError(Throwable e) {
                LOG.warn("Unable to process stable assignments event", e);
            }
        };
    }

    /**
     * Creates Meta storage listener for switch reduce assignments updates.
     *
     * @return The watch listener.
     */
    private WatchListener createAssignmentsSwitchRebalanceListener() {
        return new WatchListener() {
            @Override
            public void onUpdate(WatchEvent evt) {
                byte[] key = evt.entryEvent().newEntry().key();

                int partitionNumber = extractPartitionNumber(key);
                UUID tblId = extractTableId(key, ASSIGNMENTS_SWITCH_REDUCE_PREFIX);

                TablePartitionId replicaGrpId = new TablePartitionId(tblId, partitionNumber);

                TableImpl tbl = tablesByIdVv.latest().get(tblId);

                TableConfiguration tblCfg = tablesCfg.tables().get(tbl.name());

                RebalanceUtil.handleReduceChanged(
                        metaStorageMgr,
                        baselineMgr.nodes().stream().map(ClusterNode::name).collect(toList()),
                        tblCfg.value().replicas(),
                        partitionNumber,
                        replicaGrpId,
                        evt
                );
            }

            @Override
            public void onError(Throwable e) {
                LOG.warn("Unable to process switch reduce event", e);
            }
        };
    }

    private PartitionMover createPartitionMover(InternalTable internalTable, int partId) {
        return new PartitionMover(busyLock, () -> internalTable.partitionRaftGroupService(partId));
    }

    /**
     * Gets a direct accessor for the configuration distributed property. If the metadata access only locally configured the method will
     * return local property accessor.
     *
     * @param property Distributed configuration property to receive direct access.
     * @param <T> Type of the property accessor.
     * @return An accessor for distributive property.
     * @see #getMetadataLocallyOnly
     */
    private <T extends ConfigurationProperty<?>> T directProxy(T property) {
        return getMetadataLocallyOnly ? property : (T) property.directProxy();
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
     * Creates partition stores. If one of the storages has not completed the rebalance, then the storages are cleared.
     *
     * @param table Table.
     * @param partitionId Partition ID.
     * @return Future of creating or getting partition stores.
     */
    // TODO: IGNITE-18619 Maybe we should wait here to create indexes, if you add now, then the tests start to hang
    // TODO: IGNITE-18939 Create storages only once, then only get them
    private CompletableFuture<PartitionStorages> getOrCreatePartitionStorages(TableImpl table, int partitionId) {
        InternalTable internalTable = table.internalTable();

        MvPartitionStorage mvPartition = internalTable.storage().getMvPartition(partitionId);

        return (mvPartition != null ? completedFuture(mvPartition) : internalTable.storage().createMvPartition(partitionId))
                .thenComposeAsync(mvPartitionStorage -> {
                    TxStateStorage txStateStorage = internalTable.txStateStorage().getOrCreateTxStateStorage(partitionId);

                    if (mvPartitionStorage.persistedIndex() == MvPartitionStorage.REBALANCE_IN_PROGRESS
                            || txStateStorage.persistedIndex() == TxStateStorage.REBALANCE_IN_PROGRESS) {
                        return allOf(
                                internalTable.storage().clearPartition(partitionId),
                                txStateStorage.clear()
                        ).thenApply(unused -> new PartitionStorages(mvPartitionStorage, txStateStorage));
                    } else {
                        return completedFuture(new PartitionStorages(mvPartitionStorage, txStateStorage));
                    }
                }, ioExecutor);
    }

    /**
     * Handles the {@link RebalanceUtil#STABLE_ASSIGNMENTS_PREFIX} update event.
     *
     * @param evt Event.
     */
    protected void handleChangeStableAssignmentEvent(WatchEvent evt) {
        inBusyLock(busyLock, () -> {
            assert evt.single() : evt;

            Entry stableAssignmentsWatchEvent = evt.entryEvent().newEntry();

            if (stableAssignmentsWatchEvent.value() == null) {
                return;
            }

            int partitionId = extractPartitionNumber(stableAssignmentsWatchEvent.key());
            UUID tableId = extractTableId(stableAssignmentsWatchEvent.key(), STABLE_ASSIGNMENTS_PREFIX);

            TablePartitionId tablePartitionId = new TablePartitionId(tableId, partitionId);

            Set<Assignment> stableAssignments = ByteUtils.fromBytes(stableAssignmentsWatchEvent.value());

            byte[] pendingAssignmentsFromMetaStorage = metaStorageMgr.get(
                    pendingPartAssignmentsKey(tablePartitionId),
                    stableAssignmentsWatchEvent.revision()
            ).join().value();

            Set<Assignment> pendingAssignments = pendingAssignmentsFromMetaStorage == null
                    ? Set.of()
                    : ByteUtils.fromBytes(pendingAssignmentsFromMetaStorage);

            String localMemberName = clusterService.topologyService().localMember().name();

            boolean shouldStopLocalServices = Stream.concat(stableAssignments.stream(), pendingAssignments.stream())
                    .noneMatch(assignment -> assignment.consistentId().equals(localMemberName));

            if (shouldStopLocalServices) {
                try {
                    raftMgr.stopRaftNodes(tablePartitionId);

                    replicaMgr.stopReplica(tablePartitionId);
                } catch (NodeStoppingException e) {
                    // no-op
                }

                InternalTable internalTable = tablesByIdVv.latest().get(tableId).internalTable();

                // TODO: IGNITE-18703 Destroy raft log and meta

                // Should be done fairly quickly.
                mvGc.removeStorage(tablePartitionId)
                        .thenCompose(unused -> allOf(
                                internalTable.storage().destroyPartition(partitionId),
                                runAsync(() -> internalTable.txStateStorage().destroyTxStateStorage(partitionId), ioExecutor)
                        ))
                        .join();
            }
        });
    }

    private static void handleExceptionOnCleanUpTablesResources(
            Throwable t,
            AtomicReference<Throwable> throwable,
            AtomicBoolean nodeStoppingEx
    ) {
        if (t instanceof CompletionException || t instanceof ExecutionException) {
            t = t.getCause();
        }

        if (!throwable.compareAndSet(null, t)) {
            if (!(t instanceof NodeStoppingException) || !nodeStoppingEx.get()) {
                throwable.get().addSuppressed(t);
            }
        }

        if (t instanceof NodeStoppingException) {
            nodeStoppingEx.set(true);
        }
    }
}
