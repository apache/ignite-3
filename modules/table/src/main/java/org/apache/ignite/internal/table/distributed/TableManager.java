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
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.getZoneById;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.configuration.ConfigurationChangeException;
import org.apache.ignite.configuration.ConfigurationProperty;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.affinity.AffinityUtils;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.baseline.BaselineManager;
import org.apache.ignite.internal.causality.CompletionListener;
import org.apache.ignite.internal.causality.IncrementalVersionedValue;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneConfiguration;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.distributionzones.exception.DistributionZoneNotFoundException;
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
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupService;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.raft.storage.impl.LogStorageFactoryCreator;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.configuration.ExtendedTableChange;
import org.apache.ignite.internal.schema.configuration.ExtendedTableConfiguration;
import org.apache.ignite.internal.schema.configuration.ExtendedTableView;
import org.apache.ignite.internal.schema.configuration.TableChange;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.schema.configuration.TableView;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.schema.configuration.index.TableIndexView;
import org.apache.ignite.internal.schema.configuration.storage.DataStorageConfiguration;
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
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.lang.IgniteSystemProperties;
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

    private static final long QUERY_DATA_NODES_COUNT_TIMEOUT = TimeUnit.SECONDS.toMillis(3);

    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(TableManager.class);

    /** Name of a transaction state directory. */
    private static final String TX_STATE_DIR = "tx-state-";

    /** Transaction storage flush delay. */
    private static final int TX_STATE_STORAGE_FLUSH_DELAY = 1000;
    private static final IntSupplier TX_STATE_STORAGE_FLUSH_DELAY_SUPPLIER = () -> TX_STATE_STORAGE_FLUSH_DELAY;

    /**
     * If this property is set to {@code true} then an attempt to get the configuration property directly from Meta storage will be skipped,
     * and the local property will be returned.
     * TODO: IGNITE-16774 This property and overall approach, access configuration directly through Meta storage,
     * TODO: will be removed after fix of the issue.
     */
    private final boolean getMetadataLocallyOnly = IgniteSystemProperties.getBoolean("IGNITE_GET_METADATA_LOCALLY_ONLY");

    /** Tables configuration. */
    private final TablesConfiguration tablesCfg;

    /** Distribution zones configuration. */
    private final DistributionZonesConfiguration distributionZonesConfiguration;

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
    private final IncrementalVersionedValue<Map<UUID, TableImpl>> tablesByIdVv;

    /**
     * {@link TableImpl} is created during update of tablesByIdVv, we store reference to it in case of updating of tablesByIdVv fails, so we
     * can stop resources associated with the table or to clean up table resources on {@code TableManager#stop()}.
     */
    private final Map<UUID, TableImpl> pendingTables = new ConcurrentHashMap<>();

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

    private final TopologyAwareRaftGroupServiceFactory raftGroupServiceFactory;

    private final ClusterManagementGroupManager cmgMgr;

    private final DistributionZoneManager distributionZoneManager;

    /** Partitions storage path. */
    private final Path storagePath;

    /** Assignment change event listeners. */
    private final CopyOnWriteArrayList<Consumer<IgniteTablesInternal>> assignmentsChangeListeners = new CopyOnWriteArrayList<>();

    /** Incoming RAFT snapshots executor. */
    private final ExecutorService incomingSnapshotsExecutor;

    /** Rebalance scheduler pool size. */
    private static final int REBALANCE_SCHEDULER_POOL_SIZE = Math.min(Runtime.getRuntime().availableProcessors() * 3, 20);

    private static final TableMessagesFactory TABLE_MESSAGES_FACTORY = new TableMessagesFactory();

    /** Meta storage listener for pending assignments. */
    private final WatchListener pendingAssignmentsRebalanceListener;

    /** Meta storage listener for stable assignments. */
    private final WatchListener stableAssignmentsRebalanceListener;

    /** Meta storage listener for switch reduce assignments. */
    private final WatchListener assignmentsSwitchRebalanceListener;

    private final MvGc mvGc;

    private final LowWatermark lowWatermark;

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
     * @param volatileLogStorageFactoryCreator Creator for {@link org.apache.ignite.internal.raft.storage.LogStorageFactory} for
     *         volatile tables.
     * @param raftGroupServiceFactory Factory that is used for creation of raft group services for replication groups.
     * @param vaultManager Vault manager.
     */
    public TableManager(
            String nodeName,
            Consumer<Function<Long, CompletableFuture<?>>> registry,
            TablesConfiguration tablesCfg,
            DistributionZonesConfiguration distributionZonesConfiguration,
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
            OutgoingSnapshotsManager outgoingSnapshotsManager,
            TopologyAwareRaftGroupServiceFactory raftGroupServiceFactory,
            VaultManager vaultManager,
            ClusterManagementGroupManager cmgMgr,
            DistributionZoneManager distributionZoneManager
    ) {
        this.tablesCfg = tablesCfg;
        this.distributionZonesConfiguration = distributionZonesConfiguration;
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
        this.raftGroupServiceFactory = raftGroupServiceFactory;
        this.cmgMgr = cmgMgr;
        this.distributionZoneManager = distributionZoneManager;

        clusterNodeResolver = topologyService::getByConsistentId;

        placementDriver = new PlacementDriver(replicaSvc, clusterNodeResolver);

        tablesByIdVv = new IncrementalVersionedValue<>(registry, HashMap::new);

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

        pendingAssignmentsRebalanceListener = createPendingAssignmentsRebalanceListener();

        stableAssignmentsRebalanceListener = createStableAssignmentsRebalanceListener();

        assignmentsSwitchRebalanceListener = createAssignmentsSwitchRebalanceListener();

        mvGc = new MvGc(nodeName, tablesCfg);

        lowWatermark = new LowWatermark(nodeName, tablesCfg.lowWatermark(), clock, txManager, vaultManager, mvGc);
    }

    @Override
    public void start() {
        mvGc.start();

        lowWatermark.start();

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
            public CompletableFuture<?> onRename(ConfigurationNotificationEvent<TableView> ctx) {
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
                var eventParameters = new TableEventParameters(parameters.causalityToken(), parameters.tableId());

                return fireEvent(TableEvent.ALTER, eventParameters).thenApply(v -> false);
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
            UUID tblId = ((ExtendedTableView) ctx.newValue()).id();

            fireEvent(TableEvent.CREATE,
                    new TableEventParameters(ctx.storageRevision(), tblId),
                    new NodeStoppingException()
            );

            return failedFuture(new NodeStoppingException());
        }

        try {
            return createTableLocally(
                    ctx.storageRevision(),
                    ctx.newValue().name(),
                    ((ExtendedTableView) ctx.newValue()).id()
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
            UUID tblId = ((ExtendedTableView) ctx.oldValue()).id();

            fireEvent(
                    TableEvent.DROP,
                    new TableEventParameters(ctx.storageRevision(), tblId),
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
            return updateAssignmentInternal(assignmentsCtx)
                    .whenComplete((v, e) -> {
                        if (e == null) {
                            for (var listener : assignmentsChangeListeners) {
                                listener.accept(this);
                            }
                        }
                    });
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Updates or creates partition raft groups.
     *
     * @param assignmentsCtx Change assignment event.
     */
    private CompletableFuture<?> updateAssignmentInternal(ConfigurationNotificationEvent<byte[]> assignmentsCtx) {
        ExtendedTableView tblCfg = assignmentsCtx.newValue(ExtendedTableView.class);

        UUID tblId = tblCfg.id();

        DistributionZoneConfiguration dstCfg = getZoneById(distributionZonesConfiguration, tblCfg.zoneId());

        DataStorageConfiguration dsCfg = dstCfg.dataStorage();

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
        return tablesByIdVv.update(causalityToken, (tablesById, e) -> inBusyLock(busyLock, () -> {
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

                var safeTimeTracker = new PendingComparableValuesTracker<>(new HybridTimestamp(1, 0));
                var storageIndexTracker = new PendingComparableValuesTracker<>(0L);

                ((InternalTableImpl) internalTbl).updatePartitionTrackers(partId, safeTimeTracker, storageIndexTracker);

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
                                    dsCfg,
                                    safeTimeTracker,
                                    lowWatermark
                            );

                            mvGc.addStorage(replicaGrpId, storageUpdateHandler);

                            return storageUpdateHandler;
                        });

                CompletableFuture<Void> startGroupFut;

                // start new nodes, only if it is table creation, other cases will be covered by rebalance logic
                if (oldPartAssignment.isEmpty() && localMemberAssignment != null) {
                    startGroupFut = partitionStoragesFut.thenComposeAsync(partitionStorages -> {
                        CompletableFuture<Boolean> fut;

                        // If Raft is running in in-memory mode or the PDS has been cleared, we need to remove the current node
                        // from the Raft group in order to avoid the double vote problem.
                        // <MUTED> See https://issues.apache.org/jira/browse/IGNITE-16668 for details.
                        // TODO: https://issues.apache.org/jira/browse/IGNITE-19046 Restore "|| !hasData"
                        if (internalTbl.storage().isVolatile()) {
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

                        return fut.thenCompose(startGroup -> inBusyLock(busyLock, () -> {
                            if (!startGroup) {
                                return completedFuture(null);
                            }

                            return partitionDataStorageFut.thenAcceptBothAsync(storageUpdateHandlerFut,
                                    (partitionDataStorage, storageUpdateHandler) -> inBusyLock(busyLock, () -> {
                                        TxStateStorage txStatePartitionStorage = partitionStorages.getTxStateStorage();

                                        RaftGroupOptions groupOptions = groupOptionsForPartition(
                                                internalTbl.storage(),
                                                internalTbl.txStateStorage(),
                                                partitionKey(internalTbl, partId),
                                                storageUpdateHandler
                                        );

                                        Peer serverPeer = newConfiguration.peer(localMemberName);

                                        var raftNodeId = new RaftNodeId(replicaGrpId, serverPeer);

                                        try {
                                            // TODO: use RaftManager interface, see https://issues.apache.org/jira/browse/IGNITE-18273
                                            ((Loza) raftMgr).startRaftGroupNode(
                                                    raftNodeId,
                                                    newConfiguration,
                                                    new PartitionListener(
                                                            partitionDataStorage,
                                                            storageUpdateHandler,
                                                            txStatePartitionStorage,
                                                            safeTimeTracker,
                                                            storageIndexTracker
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
                                    }), ioExecutor);
                        }));
                    }, ioExecutor);
                } else {
                    startGroupFut = completedFuture(null);
                }

                startGroupFut
                        .thenComposeAsync(v -> inBusyLock(busyLock, () -> {
                            try {
                                return raftMgr.startRaftGroupService(replicaGrpId, newConfiguration, raftGroupServiceFactory);
                            } catch (NodeStoppingException ex) {
                                return failedFuture(ex);
                            }
                        }), ioExecutor)
                        .thenComposeAsync(updatedRaftGroupService -> inBusyLock(busyLock, () -> {
                            ((InternalTableImpl) internalTbl).updateInternalTableRaftGroupService(partId, updatedRaftGroupService);

                            if (localMemberAssignment == null) {
                                return completedFuture(null);
                            }

                            return partitionStoragesFut.thenAcceptBoth(storageUpdateHandlerFut,
                                    (partitionStorages, storageUpdateHandler) -> {
                                        MvPartitionStorage partitionStorage = partitionStorages.getMvPartitionStorage();
                                        TxStateStorage txStateStorage = partitionStorages.getTxStateStorage();

                                        try {
                                            replicaMgr.startReplica(
                                                    replicaGrpId,
                                                    allOf(
                                                            ((Loza) raftMgr).raftNodeReadyFuture(replicaGrpId),
                                                            table.pkIndexesReadyFuture()
                                                    ),
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
                                                            safeTimeTracker,
                                                            txStateStorage,
                                                            placementDriver,
                                                            storageUpdateHandler,
                                                            this::isLocalPeer,
                                                            schemaManager.schemaRegistry(causalityToken, tblId)
                                                    ),
                                                    updatedRaftGroupService,
                                                    storageIndexTracker
                                            );
                                        } catch (NodeStoppingException ex) {
                                            throw new AssertionError("Loza was stopped before Table manager", ex);
                                        }
                                    });
                        }), ioExecutor)
                        .whenComplete((res, ex) -> {
                            if (ex != null) {
                                LOG.warn("Unable to update raft groups on the node", ex);
                            }

                            futures[partId].complete(null);
                        });
            }

            return allOf(futures).thenApply(unused -> tablesById);
        }));
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

    @Override
    public void stop() {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        metaStorageMgr.unregisterWatch(pendingAssignmentsRebalanceListener);
        metaStorageMgr.unregisterWatch(stableAssignmentsRebalanceListener);
        metaStorageMgr.unregisterWatch(assignmentsSwitchRebalanceListener);

        Map<UUID, TableImpl> tablesToStop = Stream.concat(tablesByIdVv.latest().entrySet().stream(), pendingTables.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v1));

        cleanUpTablesResources(tablesToStop);

        try {
            IgniteUtils.closeAllManually(lowWatermark, mvGc);
        } catch (Throwable t) {
            LOG.error("Failed to close internal components", t);
        }

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

            InternalTable internalTable = table.internalTable();

            for (int p = 0; p < internalTable.partitions(); p++) {
                int partitionId = p;

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
                        closePartitionTrackers(internalTable, partitionId);
                    } catch (Throwable t) {
                        handleExceptionOnCleanUpTablesResources(t, throwable, nodeStoppingEx);
                    }
                });

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
                        internalTable.storage(),
                        internalTable.txStateStorage(),
                        internalTable
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
     * @return Future that will be completed when local changes related to the table creation are applied.
     */
    private CompletableFuture<?> createTableLocally(long causalityToken, String name, UUID tblId) {
        LOG.trace("Creating local table: name={}, id={}, token={}", name, tblId, causalityToken);

        TableConfiguration tableCfg = tablesCfg.tables().get(name);

        DistributionZoneConfiguration distributionZoneConfiguration =
                getZoneById(distributionZonesConfiguration, tableCfg.value().zoneId());

        MvTableStorage tableStorage = createTableStorage(tableCfg, tablesCfg, distributionZoneConfiguration);
        TxStateTableStorage txStateStorage = createTxStateTableStorage(tableCfg, distributionZoneConfiguration);

        InternalTableImpl internalTable = new InternalTableImpl(name, tblId,
                new Int2ObjectOpenHashMap<>(distributionZoneConfiguration.partitions().value()),
                distributionZoneConfiguration.partitions().value(), clusterNodeResolver, txManager, tableStorage,
                txStateStorage, replicaSvc, clock);

        var table = new TableImpl(internalTable, lockMgr);

        // TODO: IGNITE-19082 Need another way to wait for indexes
        table.addIndexesToWait(collectTableIndexes(tblId));

        tablesByIdVv.update(causalityToken, (previous, e) -> inBusyLock(busyLock, () -> {
            if (e != null) {
                return failedFuture(e);
            }

            return schemaManager.schemaRegistry(causalityToken, tblId)
                    .thenApply(schema -> {
                        table.schemaView(schema);

                        var val = new HashMap<>(previous);

                        val.put(tblId, table);

                        return val;
                    });
        }));

        pendingTables.put(tblId, table);

        tablesByIdVv.get(causalityToken).thenAccept(ignored -> inBusyLock(busyLock, () -> {
            pendingTables.remove(tblId);
        }));

        tablesByIdVv.get(causalityToken)
                .thenRun(() -> inBusyLock(busyLock, () -> completeApiCreateFuture(table)));

        // TODO should be reworked in IGNITE-16763
        // We use the event notification future as the result so that dependent components can complete the schema updates.
        return fireEvent(TableEvent.CREATE, new TableEventParameters(causalityToken, tblId));
    }

    /**
     * Creates data storage for the provided table.
     *
     * @param tableCfg Table configuration.
     * @param tablesCfg Tables configuration.
     * @return Table data storage.
     */
    protected MvTableStorage createTableStorage(
            TableConfiguration tableCfg, TablesConfiguration tablesCfg, DistributionZoneConfiguration distributionZoneCfg) {
        MvTableStorage tableStorage = dataStorageMgr.engine(distributionZoneCfg.dataStorage())
                .createMvTable(tableCfg, tablesCfg, distributionZoneCfg);

        tableStorage.start();

        return tableStorage;
    }

    /**
     * Creates transaction state storage for the provided table.
     *
     * @param tableCfg Table configuration.
     * @return Transaction state storage.
     */
    protected TxStateTableStorage createTxStateTableStorage(
            TableConfiguration tableCfg, DistributionZoneConfiguration distributionZoneCfg) {
        Path path = storagePath.resolve(TX_STATE_DIR + tableCfg.value().tableId());

        try {
            Files.createDirectories(path);
        } catch (IOException e) {
            throw new StorageException("Failed to create transaction state storage directory for " + tableCfg.value().name(), e);
        }

        TxStateTableStorage txStateTableStorage = new TxStateRocksDbTableStorage(
                tableCfg,
                distributionZoneCfg,
                path,
                txStateStorageScheduledPool,
                txStateStoragePool,
                TX_STATE_STORAGE_FLUSH_DELAY_SUPPLIER
        );

        txStateTableStorage.start();

        return txStateTableStorage;
    }

    /**
     * Completes appropriate future to return result from API {@link TableManager#createTableAsync(String, String, Consumer)}.
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

                TableImpl table = map.remove(tblId);

                assert table != null : IgniteStringFormatter.format("There is no table with the name specified [name={}, id={}]",
                        name, tblId);

                InternalTable internalTable = table.internalTable();

                for (int partitionId = 0; partitionId < partitions; partitionId++) {
                    closePartitionTrackers(internalTable, partitionId);
                }

                // TODO: IGNITE-18703 Destroy raft log and meta

                CompletableFuture<Void> destroyTableStoragesFuture = allOf(removeStorageFromGcFutures)
                        .thenCompose(unused -> allOf(
                                internalTable.storage().destroy(),
                                runAsync(() -> internalTable.txStateStorage().destroy(), ioExecutor))
                        );

                CompletableFuture<?> dropSchemaRegistryFuture = schemaManager.dropRegistry(causalityToken, table.tableId());

                return allOf(destroyTableStoragesFuture, dropSchemaRegistryFuture)
                        .thenApply(v -> map);
            }));

            fireEvent(TableEvent.DROP, new TableEventParameters(causalityToken, tblId))
                    .whenComplete((v, e) -> {
                        if (e != null) {
                            LOG.error("Error on " + TableEvent.DROP + " notification", e);
                        }
                    });
        } catch (NodeStoppingException e) {
            fireEvent(TableEvent.DROP, new TableEventParameters(causalityToken, tblId), e);
        }
    }

    private Set<Assignment> calculateAssignments(TableConfiguration tableCfg, int partNum) {
        return AffinityUtils.calculateAssignmentForPartition(
                baselineMgr.nodes().stream().map(ClusterNode::name).collect(toList()),
                partNum,
                getZoneById(distributionZonesConfiguration, tableCfg.zoneId().value()).replicas().value()
        );
    }

    /**
     * Creates a new table with the given {@code name} asynchronously. If a table with the same name already exists, a future will be
     * completed with {@link TableAlreadyExistsException}.
     *
     * @param name Table name.
     * @param zoneName Distribution zone name.
     * @param tableInitChange Table changer.
     * @return Future representing pending completion of the operation.
     * @throws IgniteException If an unspecified platform exception has happened internally. Is thrown when:
     *         <ul>
     *             <li>the node is stopping.</li>
     *         </ul>
     * @see TableAlreadyExistsException
     */
    public CompletableFuture<Table> createTableAsync(String name, String zoneName, Consumer<TableChange> tableInitChange) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }
        try {
            return createTableAsyncInternal(name, zoneName, tableInitChange);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** See {@link #createTableAsync(String, String, Consumer)} for details. */
    private CompletableFuture<Table> createTableAsyncInternal(
            String name,
            String zoneName,
            Consumer<TableChange> tableInitChange
    ) {
        CompletableFuture<Table> tblFut = new CompletableFuture<>();

        tableAsyncInternal(name)
                .handle((tbl, tblEx) -> {
                    if (tbl != null) {
                        tblFut.completeExceptionally(new TableAlreadyExistsException(DEFAULT_SCHEMA_NAME, name));
                    } else if (tblEx != null) {
                        tblFut.completeExceptionally(tblEx);
                    } else {
                        if (!busyLock.enterBusy()) {
                            NodeStoppingException nodeStoppingException = new NodeStoppingException();

                            tblFut.completeExceptionally(nodeStoppingException);

                            throw new IgniteException(nodeStoppingException);
                        }

                        try {
                            distributionZoneManager.zoneIdAsyncInternal(zoneName).handle((zoneId, zoneIdEx) -> {
                                if (zoneId == null) {
                                    tblFut.completeExceptionally(new DistributionZoneNotFoundException(zoneName));
                                } else if (zoneIdEx != null) {
                                    tblFut.completeExceptionally(zoneIdEx);
                                } else {
                                    if (!busyLock.enterBusy()) {
                                        NodeStoppingException nodeStoppingException = new NodeStoppingException();

                                        tblFut.completeExceptionally(nodeStoppingException);

                                        throw new IgniteException(nodeStoppingException);
                                    }

                                    try {
                                        cmgMgr.logicalTopology()
                                                .handle((cmgTopology, e) -> {
                                                    if (e == null) {
                                                        if (!busyLock.enterBusy()) {
                                                            NodeStoppingException nodeStoppingException = new NodeStoppingException();

                                                            tblFut.completeExceptionally(nodeStoppingException);

                                                            throw new IgniteException(nodeStoppingException);
                                                        }

                                                        try {
                                                            distributionZoneManager.topologyVersionedDataNodes(zoneId,
                                                                            cmgTopology.version())
                                                                    .handle((dataNodes, e0) -> {
                                                                        if (e0 == null) {
                                                                            changeTablesConfiguration(
                                                                                    name,
                                                                                    zoneId,
                                                                                    dataNodes,
                                                                                    tableInitChange,
                                                                                    tblFut
                                                                            );
                                                                        } else {
                                                                            tblFut.completeExceptionally(e0);
                                                                        }

                                                                        return null;
                                                                    });
                                                        } finally {
                                                            busyLock.leaveBusy();
                                                        }
                                                    } else {
                                                        tblFut.completeExceptionally(e);
                                                    }

                                                    return null;
                                                });
                                    } finally {
                                        busyLock.leaveBusy();
                                    }
                                }

                                return null;
                            });
                        } finally {
                            busyLock.leaveBusy();
                        }
                    }

                    return null;
                });

        return tblFut;
    }

    /**
     * Creates a new table in {@link TablesConfiguration}.
     *
     * @param name Table name.
     * @param zoneId Distribution zone id.
     * @param dataNodes Data nodes.
     * @param tableInitChange Table changer.
     * @param tblFut Future representing pending completion of the table creation.
     */
    private void changeTablesConfiguration(
            String name,
            int zoneId,
            Collection<String> dataNodes,
            Consumer<TableChange> tableInitChange,
            CompletableFuture<Table> tblFut
    ) {
        tablesCfg.change(tablesChange -> tablesChange.changeTables(tablesListChange -> {
            if (tablesListChange.get(name) != null) {
                throw new TableAlreadyExistsException(DEFAULT_SCHEMA_NAME, name);
            }

            tablesListChange.create(name, (tableChange) -> {
                tableInitChange.accept(tableChange);

                tableChange.changeZoneId(zoneId);

                var extConfCh = ((ExtendedTableChange) tableChange);

                int intTableId = tablesChange.globalIdCounter() + 1;
                tablesChange.changeGlobalIdCounter(intTableId);

                extConfCh.changeTableId(intTableId);

                extConfCh.changeSchemaId(INITIAL_SCHEMA_VERSION);

                tableCreateFuts.put(extConfCh.id(), tblFut);

                DistributionZoneConfiguration distributionZoneConfiguration =
                        getZoneById(distributionZonesConfiguration, zoneId);

                // Affinity assignments calculation.
                extConfCh.changeAssignments(
                        ByteUtils.toBytes(AffinityUtils.calculateAssignments(
                                dataNodes,
                                distributionZoneConfiguration.partitions().value(),
                                distributionZoneConfiguration.replicas().value())));
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

    /**
     * Alters a cluster table. If an appropriate table does not exist, a future will be completed with {@link TableNotFoundException}.
     *
     * @param name Table name.
     * @param tableChange Table changer.
     * @return Future representing pending completion of the operation.
     * @throws IgniteException If an unspecified platform exception has happened internally. Is thrown when:
     *         <ul>
     *             <li>the node is stopping.</li>
     *         </ul>
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

        return (ex instanceof IgniteException) ? (IgniteException) ex : IgniteException.wrap(ex);
    }

    /**
     * Drops a table with the name specified. If appropriate table does not be found, a future will be completed with
     * {@link TableNotFoundException}.
     *
     * @param name Table name.
     * @return Future representing pending completion of the operation.
     * @throws IgniteException If an unspecified platform exception has happened internally. Is thrown when:
     *         <ul>
     *             <li>the node is stopping.</li>
     *         </ul>
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
        return tableAsyncInternal(name).thenCompose(tbl -> {
            // In case of drop it's an optimization that allows not to fire drop-change-closure if there's no such
            // distributed table and the local config has lagged behind.
            if (tbl == null) {
                return failedFuture(new TableNotFoundException(DEFAULT_SCHEMA_NAME, name));
            }

            return tablesCfg
                    .change(chg -> chg
                            .changeTables(tblChg -> {
                                if (tblChg.get(name) == null) {
                                    throw new TableNotFoundException(DEFAULT_SCHEMA_NAME, name);
                                }

                                tblChg.delete(name);
                            })
                            .changeIndexes(idxChg -> {
                                for (TableIndexView index : idxChg) {
                                    if (index.tableId().equals(tbl.tableId())) {
                                        idxChg.delete(index.name());
                                    }
                                }
                            }))
                    .exceptionally(t -> {
                        Throwable ex = getRootCause(t);

                        if (!(ex instanceof TableNotFoundException)) {
                            LOG.debug("Unable to drop table [name={}]", ex, name);
                        }

                        throw new CompletionException(ex);
                    });
        });
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
        return supplyAsync(() -> inBusyLock(busyLock, this::directTableIds), ioExecutor)
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
            return supplyAsync(() -> inBusyLock(busyLock, () -> directTableId(name)), ioExecutor)
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
     * @param checkConfiguration {@code True} when the method checks a configuration before trying to get a table, {@code false}
     *         otherwise.
     * @return Future representing pending completion of the operation.
     */
    public CompletableFuture<TableImpl> tableAsyncInternal(UUID id, boolean checkConfiguration) {
        CompletableFuture<Boolean> tblCfgFut = checkConfiguration
                // TODO: IGNITE-16288 isTableConfigured should use async configuration API
                ? supplyAsync(() -> inBusyLock(busyLock, () -> isTableConfigured(id)), ioExecutor)
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

            CompletionListener<Map<UUID, TableImpl>> tablesListener = (token, tables, th) -> {
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
            ((ExtendedTableConfiguration) directProxy(tablesCfg.tables()).get(id)).id().value();

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
                    return handleChangePendingAssignmentEvent(evt);
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

    private CompletableFuture<Void> handleChangePendingAssignmentEvent(WatchEvent evt) {
        assert evt.single();

        Entry pendingAssignmentsWatchEntry = evt.entryEvent().newEntry();

        if (pendingAssignmentsWatchEntry.value() == null) {
            return completedFuture(null);
        }

        int partId = extractPartitionNumber(pendingAssignmentsWatchEntry.key());
        UUID tblId = extractTableId(pendingAssignmentsWatchEntry.key(), PENDING_ASSIGNMENTS_PREFIX);

        var replicaGrpId = new TablePartitionId(tblId, partId);

        // Stable assignments from the meta store, which revision is bounded by the current pending event.
        CompletableFuture<Entry> stableAssignmentsFuture = metaStorageMgr.get(stablePartAssignmentsKey(replicaGrpId), evt.revision());

        return tablesByIdVv.get(evt.revision())
                .thenCombineAsync(stableAssignmentsFuture, (tables, stableAssignmentsEntry) -> {
                    if (!busyLock.enterBusy()) {
                        return CompletableFuture.<Void>failedFuture(new NodeStoppingException());
                    }

                    try {
                        return handleChangePendingAssignmentEvent(
                                replicaGrpId,
                                tables.get(tblId),
                                pendingAssignmentsWatchEntry,
                                stableAssignmentsEntry
                        );
                    } finally {
                        busyLock.leaveBusy();
                    }
                }, ioExecutor)
                .thenCompose(Function.identity());
    }

    private CompletableFuture<Void> handleChangePendingAssignmentEvent(
            TablePartitionId replicaGrpId,
            TableImpl tbl,
            Entry pendingAssignmentsEntry,
            Entry stableAssignmentsEntry
    ) {
        // Assignments of the pending rebalance that we received through the Meta storage watch mechanism.
        Set<Assignment> pendingAssignments = ByteUtils.fromBytes(pendingAssignmentsEntry.value());

        PeersAndLearners pendingConfiguration = configurationFromAssignments(pendingAssignments);

        ExtendedTableConfiguration tblCfg = (ExtendedTableConfiguration) tablesCfg.tables().get(tbl.name());

        DistributionZoneConfiguration dstZoneCfg = getZoneById(distributionZonesConfiguration, tblCfg.zoneId().value());

        int partId = replicaGrpId.partitionId();

        byte[] stableAssignmentsBytes = stableAssignmentsEntry.value();

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

        var safeTimeTracker = new PendingComparableValuesTracker<>(new HybridTimestamp(1, 0));
        var storageIndexTracker = new PendingComparableValuesTracker<>(0L);

        InternalTable internalTable = tbl.internalTable();

        ((InternalTableImpl) internalTable).updatePartitionTrackers(partId, safeTimeTracker, storageIndexTracker);

        LOG.info("Received update on pending assignments. Check if new raft group should be started"
                        + " [key={}, partition={}, table={}, localMemberAddress={}]",
                pendingAssignmentsEntry.key(), partId, tbl.name(), localMember.address());

        CompletableFuture<Void> localServicesStartFuture;

        if (shouldStartLocalServices) {
            localServicesStartFuture = getOrCreatePartitionStorages(tbl, partId)
                    .thenAcceptAsync(partitionStorages -> {
                        MvPartitionStorage mvPartitionStorage = partitionStorages.getMvPartitionStorage();
                        TxStateStorage txStatePartitionStorage = partitionStorages.getTxStateStorage();

                        PartitionDataStorage partitionDataStorage = partitionDataStorage(mvPartitionStorage, internalTable, partId);
                        StorageUpdateHandler storageUpdateHandler = new StorageUpdateHandler(
                                partId,
                                partitionDataStorage,
                                tbl.indexStorageAdapters(partId),
                                dstZoneCfg.dataStorage(),
                                safeTimeTracker,
                                lowWatermark
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
                                safeTimeTracker,
                                storageIndexTracker
                        );

                        RaftGroupEventsListener raftGrpEvtsLsnr = new RebalanceRaftGroupEventsListener(
                                metaStorageMgr,
                                tblCfg,
                                replicaGrpId,
                                partId,
                                busyLock,
                                createPartitionMover(internalTable, partId),
                                this::calculateAssignments,
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

                            UUID tblId = tbl.tableId();

                            replicaMgr.startReplica(
                                    replicaGrpId,
                                    allOf(
                                            ((Loza) raftMgr).raftNodeReadyFuture(replicaGrpId),
                                            tbl.pkIndexesReadyFuture()
                                    ),
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
                                            safeTimeTracker,
                                            txStatePartitionStorage,
                                            placementDriver,
                                            storageUpdateHandler,
                                            this::isLocalPeer,
                                            completedFuture(schemaManager.schemaRegistry(tblId))
                                    ),
                                    (TopologyAwareRaftGroupService) internalTable.partitionRaftGroupService(partId),
                                    storageIndexTracker
                            );
                        } catch (NodeStoppingException ignored) {
                            // no-op
                        }
                    }, ioExecutor);
        } else {
            localServicesStartFuture = completedFuture(null);
        }

        return localServicesStartFuture
                .thenCompose(v -> metaStorageMgr.get(pendingPartAssignmentsKey(replicaGrpId)))
                .thenCompose(latestPendingAssignmentsEntry -> {
                    // Do not change peers of the raft group if this is a stale event.
                    // Note that we start raft node before for the sake of the consistency in a starting and stopping raft nodes.
                    if (pendingAssignmentsEntry.revision() < latestPendingAssignmentsEntry.revision()) {
                        return completedFuture(null);
                    }

                    RaftGroupService partGrpSvc = internalTable.partitionRaftGroupService(partId);

                    return partGrpSvc.refreshAndGetLeaderWithTerm()
                            .thenCompose(leaderWithTerm -> {
                                // run update of raft configuration if this node is a leader
                                if (isLocalPeer(leaderWithTerm.leader())) {
                                    LOG.info("Current node={} is the leader of partition raft group={}. "
                                                    + "Initiate rebalance process for partition={}, table={}",
                                            localMember.address(), replicaGrpId, partId, tbl.name());

                                    return partGrpSvc.changePeersAsync(pendingConfiguration, leaderWithTerm.term());
                                } else {
                                    return completedFuture(null);
                                }
                            });
                });
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

    /**
     * Creates Meta storage listener for switch reduce assignments updates.
     *
     * @return The watch listener.
     */
    private WatchListener createAssignmentsSwitchRebalanceListener() {
        return new WatchListener() {
            @Override
            public CompletableFuture<Void> onUpdate(WatchEvent evt) {
                if (!busyLock.enterBusy()) {
                    return failedFuture(new NodeStoppingException());
                }

                try {
                    byte[] key = evt.entryEvent().newEntry().key();

                    int partitionNumber = extractPartitionNumber(key);
                    UUID tblId = extractTableId(key, ASSIGNMENTS_SWITCH_REDUCE_PREFIX);

                    TablePartitionId replicaGrpId = new TablePartitionId(tblId, partitionNumber);

                    return tablesByIdVv.get(evt.revision())
                            .thenCompose(tables -> {
                                if (!busyLock.enterBusy()) {
                                    return failedFuture(new NodeStoppingException());
                                }

                                try {
                                    TableImpl tbl = tables.get(tblId);

                                    TableConfiguration tblCfg = tablesCfg.tables().get(tbl.name());

                                    return RebalanceUtil.handleReduceChanged(
                                            metaStorageMgr,
                                            baselineMgr.nodes().stream().map(ClusterNode::name).collect(toList()),
                                            getZoneById(distributionZonesConfiguration, tblCfg.zoneId().value()).replicas().value(),
                                            partitionNumber,
                                            replicaGrpId,
                                            evt
                                    );
                                } finally {
                                    busyLock.leaveBusy();
                                }
                            });
                } finally {
                    busyLock.leaveBusy();
                }
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
    protected CompletableFuture<Void> handleChangeStableAssignmentEvent(WatchEvent evt) {
        assert evt.single() : evt;

        Entry stableAssignmentsWatchEvent = evt.entryEvent().newEntry();

        if (stableAssignmentsWatchEvent.value() == null) {
            return completedFuture(null);
        }

        int partitionId = extractPartitionNumber(stableAssignmentsWatchEvent.key());
        UUID tableId = extractTableId(stableAssignmentsWatchEvent.key(), STABLE_ASSIGNMENTS_PREFIX);

        TablePartitionId tablePartitionId = new TablePartitionId(tableId, partitionId);

        Set<Assignment> stableAssignments = ByteUtils.fromBytes(stableAssignmentsWatchEvent.value());

        return metaStorageMgr.get(pendingPartAssignmentsKey(tablePartitionId), stableAssignmentsWatchEvent.revision())
                .thenComposeAsync(pendingAssignmentsEntry -> {
                    byte[] pendingAssignmentsFromMetaStorage = pendingAssignmentsEntry.value();

                    Set<Assignment> pendingAssignments = pendingAssignmentsFromMetaStorage == null
                            ? Set.of()
                            : ByteUtils.fromBytes(pendingAssignmentsFromMetaStorage);

                    String localMemberName = clusterService.topologyService().localMember().name();

                    boolean shouldStopLocalServices = Stream.concat(stableAssignments.stream(), pendingAssignments.stream())
                            .noneMatch(assignment -> assignment.consistentId().equals(localMemberName));

                    if (!shouldStopLocalServices) {
                        return completedFuture(null);
                    }

                    try {
                        raftMgr.stopRaftNodes(tablePartitionId);

                        replicaMgr.stopReplica(tablePartitionId);
                    } catch (NodeStoppingException e) {
                        // No-op.
                    }

                    return tablesByIdVv.get(evt.revision())
                            // TODO: IGNITE-18703 Destroy raft log and meta
                            .thenCombine(mvGc.removeStorage(tablePartitionId), (tables, unused) -> {
                                InternalTable internalTable = tables.get(tableId).internalTable();

                                closePartitionTrackers(internalTable, partitionId);

                                return allOf(
                                        internalTable.storage().destroyPartition(partitionId),
                                        runAsync(() -> internalTable.txStateStorage().destroyTxStateStorage(partitionId), ioExecutor)
                                );
                            })
                            .thenCompose(Function.identity());
                }, ioExecutor);
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

    private Collection<UUID> collectTableIndexes(UUID tableId) {
        NamedListView<? extends TableIndexView> indexes = tablesCfg.value().indexes();

        List<UUID> indexIds = new ArrayList<>();

        for (int i = 0; i < indexes.size(); i++) {
            TableIndexView indexConfig = indexes.get(i);

            if (indexConfig.tableId().equals(tableId)) {
                indexIds.add(indexConfig.id());
            }
        }

        return indexIds;
    }

    private static void closePartitionTrackers(InternalTable internalTable, int partitionId) {
        closeTracker(internalTable.getPartitionSafeTimeTracker(partitionId));

        closeTracker(internalTable.getPartitionStorageIndexTracker(partitionId));
    }

    private static void closeTracker(@Nullable PendingComparableValuesTracker<?> tracker) {
        if (tracker != null) {
            tracker.close();
        }
    }
}
