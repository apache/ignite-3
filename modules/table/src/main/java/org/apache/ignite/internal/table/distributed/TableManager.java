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

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.causality.IncrementalVersionedValue.dependingOn;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.getZoneById;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.partitionAssignments;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.tableAssignments;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneCatalogDescriptorUtils.toZoneDescriptor;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.schema.CatalogDescriptorUtils.toTableDescriptor;
import static org.apache.ignite.internal.schema.SchemaManager.INITIAL_SCHEMA_VERSION;
import static org.apache.ignite.internal.schema.configuration.SchemaConfigurationUtils.findTableView;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.apache.ignite.internal.utils.RebalanceUtil.ASSIGNMENTS_SWITCH_REDUCE_PREFIX;
import static org.apache.ignite.internal.utils.RebalanceUtil.PENDING_ASSIGNMENTS_PREFIX;
import static org.apache.ignite.internal.utils.RebalanceUtil.STABLE_ASSIGNMENTS_PREFIX;
import static org.apache.ignite.internal.utils.RebalanceUtil.extractPartitionNumber;
import static org.apache.ignite.internal.utils.RebalanceUtil.extractTableId;
import static org.apache.ignite.internal.utils.RebalanceUtil.pendingPartAssignmentsKey;
import static org.apache.ignite.internal.utils.RebalanceUtil.stablePartAssignmentsKey;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
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
import java.util.function.LongFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.configuration.ConfigurationChangeException;
import org.apache.ignite.configuration.ConfigurationProperty;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.affinity.AffinityUtils;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.baseline.BaselineManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogDataStorageDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.causality.CompletionListener;
import org.apache.ignite.internal.causality.IncrementalVersionedValue;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.DistributionZoneNotFoundException;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
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
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Conditions;
import org.apache.ignite.internal.metastorage.dsl.Operation;
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
import org.apache.ignite.internal.schema.configuration.GcConfiguration;
import org.apache.ignite.internal.schema.configuration.TableChange;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.schema.configuration.TableView;
import org.apache.ignite.internal.schema.configuration.TablesChange;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.schema.configuration.index.TableIndexView;
import org.apache.ignite.internal.schema.event.SchemaEvent;
import org.apache.ignite.internal.schema.event.SchemaEventParameters;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptorSupplier;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.gc.GcUpdateHandler;
import org.apache.ignite.internal.table.distributed.gc.MvGc;
import org.apache.ignite.internal.table.distributed.index.IndexBuilder;
import org.apache.ignite.internal.table.distributed.index.IndexUpdateHandler;
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
import org.apache.ignite.internal.table.distributed.schema.NonHistoricSchemas;
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
import org.apache.ignite.internal.util.ExceptionUtils;
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
import org.apache.ignite.lang.util.IgniteNameUtils;
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
    private final DistributionZonesConfiguration zonesConfig;

    /** Garbage collector configuration. */
    private final GcConfiguration gcConfig;

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

    /** Vault manager. */
    private final VaultManager vaultManager;

    /** Data storage manager. */
    private final DataStorageManager dataStorageMgr;

    /** Placement driver. */
    private final PlacementDriver placementDriver;

    /** Here a table future stores during creation (until the table can be provided to client). */
    private final Map<Integer, CompletableFuture<Table>> tableCreateFuts = new ConcurrentHashMap<>();

    /** Used to propagate context. */
    // TODO: 04.09.2023 most probably it is useless in case of multi node cluster
    private final Map<Integer, Context> tableCreateContext = new ConcurrentHashMap<>();

    /**
     * Versioned store for tables by id. Only table instances are created here, local storages and RAFT groups may not be initialized yet.
     *
     * @see #localPartsByTableIdVv
     * @see #assignmentsUpdatedVv
     */
    private final IncrementalVersionedValue<Map<Integer, TableImpl>> tablesByIdVv;

    /**
     * Versioned store for local partition set by table id.
     * Completed strictly after {@link #tablesByIdVv} and strictly before {@link #assignmentsUpdatedVv}.
     */
    private final IncrementalVersionedValue<Map<Integer, PartitionSet>> localPartsByTableIdVv;

    /**
     * Versioned store for tracking RAFT groups initialization and starting completion.
     * Only explicitly updated in {@link #createTablePartitionsLocally(long, List, int, TableImpl)}.
     * Completed strictly after {@link #localPartsByTableIdVv}.
     */
    private final IncrementalVersionedValue<Void> assignmentsUpdatedVv;

    /**
     * {@link TableImpl} is created during update of tablesByIdVv, we store reference to it in case of updating of tablesByIdVv fails, so we
     * can stop resources associated with the table or to clean up table resources on {@code TableManager#stop()}.
     */
    private final Map<Integer, TableImpl> pendingTables = new ConcurrentHashMap<>();

    /** Started tables. */
    private final Map<Integer, TableImpl> startedTables = new ConcurrentHashMap<>();

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

    private final IndexBuilder indexBuilder;

    private final ConfiguredTablesCache configuredTablesCache;

    /**
     * Creates a new table manager.
     *
     * @param nodeName Node name.
     * @param registry Registry for versioned values.
     * @param tablesCfg Tables configuration.
     * @param zonesConfig Distribution zones configuration.
     * @param gcConfig Garbage collector configuration.
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
            Consumer<LongFunction<CompletableFuture<?>>> registry,
            TablesConfiguration tablesCfg,
            DistributionZonesConfiguration zonesConfig,
            GcConfiguration gcConfig,
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
        this.zonesConfig = zonesConfig;
        this.gcConfig = gcConfig;
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
        this.vaultManager = vaultManager;
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

        localPartsByTableIdVv = new IncrementalVersionedValue<>(dependingOn(tablesByIdVv), HashMap::new);

        assignmentsUpdatedVv = new IncrementalVersionedValue<>(dependingOn(localPartsByTableIdVv));

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

        mvGc = new MvGc(nodeName, gcConfig);

        lowWatermark = new LowWatermark(nodeName, gcConfig.lowWatermark(), clock, txManager, vaultManager, mvGc);

        indexBuilder = new IndexBuilder(nodeName, cpus);

        configuredTablesCache = new ConfiguredTablesCache(tablesCfg, getMetadataLocallyOnly);
    }

    @Override
    public void start() {
        mvGc.start();

        lowWatermark.start();

        metaStorageMgr.registerPrefixWatch(ByteArray.fromString(PENDING_ASSIGNMENTS_PREFIX), pendingAssignmentsRebalanceListener);
        metaStorageMgr.registerPrefixWatch(ByteArray.fromString(STABLE_ASSIGNMENTS_PREFIX), stableAssignmentsRebalanceListener);
        metaStorageMgr.registerPrefixWatch(ByteArray.fromString(ASSIGNMENTS_SWITCH_REDUCE_PREFIX), assignmentsSwitchRebalanceListener);

        tablesCfg.tables().listenElements(new ConfigurationNamedListListener<>() {
            @Override
            public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<TableView> ctx) {
                Context context = null;
                if (ctx.newValue() != null) {
                    context = tableCreateContext.remove(ctx.newValue().id());
                }
                if(context == null) {
                    context = Context.current();
                }
                try (Scope scope = context.makeCurrent()) {
                    return onTableCreate(ctx);
                }
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

                int tableId = msg.tableId();
                int partitionId = msg.partitionId();

                boolean contains = false;

                TableImpl table = latestTablesById().get(tableId);

                if (table != null) {
                    MvTableStorage storage = table.internalTable().storage();

                    MvPartitionStorage mvPartition = storage.getMvPartition(partitionId);

                    // If node's recovery process is incomplete (no partition storage), then we consider this node's
                    // partition storage empty.
                    if (mvPartition != null) {
                        contains = mvPartition.closestRowId(RowId.lowestRowId(partitionId)) != null;
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
    @WithSpan
    private CompletableFuture<?> onTableCreate(ConfigurationNotificationEvent<TableView> ctx) {
        if (!busyLock.enterBusy()) {
            fireEvent(TableEvent.CREATE,
                    new TableEventParameters(ctx.storageRevision(), ctx.newValue().id()),
                    new NodeStoppingException()
            );

            return failedFuture(new NodeStoppingException());
        }

        try {
            CatalogTableDescriptor tableDescriptor = toTableDescriptor(ctx.newValue());
            CatalogZoneDescriptor zoneDescriptor = getZoneDescriptor(tableDescriptor.zoneId());

            CompletableFuture<List<Set<Assignment>>> assignmentsFuture;

            int tableId = tableDescriptor.id();

            // Check if the table already has assignments in the vault.
            // So, it means, that it is a recovery process and we should use the vault assignments instead of calculation for the new ones.
            if (partitionAssignments(vaultManager, tableId, 0) != null) {
                assignmentsFuture = completedFuture(tableAssignments(vaultManager, tableId, zoneDescriptor.partitions()));
            } else {
                assignmentsFuture = distributionZoneManager.dataNodes(ctx.storageRevision(), tableDescriptor.zoneId())
                        .thenApply(dataNodes -> AffinityUtils.calculateAssignments(
                                dataNodes,
                                zoneDescriptor.partitions(),
                                zoneDescriptor.replicas()
                        ));
            }

            CompletableFuture<?> createTableFut = createTableLocally(
                    ctx.storageRevision(),
                    tableDescriptor,
                    zoneDescriptor,
                    assignmentsFuture
            ).whenComplete((v, e) -> {
                if (e == null) {
                    for (var listener : assignmentsChangeListeners) {
                        listener.accept(this);
                    }
                }
            }).thenCompose(ignored -> writeTableAssignmentsToMetastore(tableId, assignmentsFuture));

            return createTableFut;
        } finally {
            busyLock.leaveBusy();
        }
    }

    private CompletableFuture<Boolean> writeTableAssignmentsToMetastore(
            int tableId,
            CompletableFuture<List<Set<Assignment>>> assignmentsFuture
    ) {
        return assignmentsFuture.thenCompose(newAssignments -> {
            assert !newAssignments.isEmpty();

            List<Operation> partitionAssignments = new ArrayList<>(newAssignments.size());

            for (int i = 0; i < newAssignments.size(); i++) {
                partitionAssignments.add(put(
                        stablePartAssignmentsKey(
                                new TablePartitionId(tableId, i)),
                        ByteUtils.toBytes(newAssignments.get(i))));
            }

            Condition condition = Conditions.notExists(new ByteArray(partitionAssignments.get(0).key()));

            return metaStorageMgr
                    .invoke(condition, partitionAssignments, Collections.emptyList())
                    .exceptionally(e -> {
                        LOG.error("Couldn't write assignments to metastore", e);

                        return null;
                    });
        });
    }

    /**
     * Listener of table drop configuration change.
     *
     * @param ctx Table configuration context.
     * @return A future.
     */
    private CompletableFuture<?> onTableDelete(ConfigurationNotificationEvent<TableView> ctx) {
        if (!busyLock.enterBusy()) {
            fireEvent(
                    TableEvent.DROP,
                    new TableEventParameters(ctx.storageRevision(), ctx.oldValue().id()),
                    new NodeStoppingException()
            );

            return failedFuture(new NodeStoppingException());
        }

        try {
            CatalogTableDescriptor tableDescriptor = toTableDescriptor(ctx.oldValue());
            CatalogZoneDescriptor zoneDescriptor = getZoneDescriptor(tableDescriptor.zoneId());

            dropTableLocally(ctx.storageRevision(), tableDescriptor, zoneDescriptor);
        } finally {
            busyLock.leaveBusy();
        }

        return completedFuture(null);
    }

    /**
     * Updates or creates partition raft groups and storages.
     *
     * @param causalityToken Causality token.
     * @param assignmentsFuture Table assignments.
     * @param zoneId Distributed zone ID.
     * @param table Initialized table entity.
     * @return future, which will be completed when the partitions creations done.
     */
    @WithSpan
    private CompletableFuture<?> createTablePartitionsLocally(
            long causalityToken,
            CompletableFuture<List<Set<Assignment>>> assignmentsFuture,
            int zoneId,
            TableImpl table
    ) {
        int tableId = table.tableId();

        // Create new raft nodes according to new assignments.
        Supplier<CompletableFuture<Void>> updateAssignmentsClosure = () -> assignmentsFuture.thenCompose(newAssignments -> {
            // Empty assignments might be a valid case if tables are created from within cluster init HOCON
            // configuration, which is not supported now.
            assert newAssignments != null : IgniteStringFormatter.format("Table [id={}] has empty assignments.", tableId);

            int partitions = newAssignments.size();

            CompletableFuture<?>[] futures = new CompletableFuture<?>[partitions];

            // TODO: https://issues.apache.org/jira/browse/IGNITE-19713 Process assignments and set partitions only for assigned partitions.
            PartitionSet parts = new BitSetPartitionSet();

            for (int i = 0; i < futures.length; i++) {
                futures[i] = new CompletableFuture<>();

                parts.set(i);
            }

            String localMemberName = localNode().name();

            for (int i = 0; i < partitions; i++) {
                int partId = i;

                Set<Assignment> newPartAssignment = newAssignments.get(partId);

                InternalTable internalTbl = table.internalTable();

                Assignment localMemberAssignment = newPartAssignment.stream()
                        .filter(a -> a.consistentId().equals(localMemberName))
                        .findAny()
                        .orElse(null);

                PeersAndLearners newConfiguration = configurationFromAssignments(newPartAssignment);

                TablePartitionId replicaGrpId = new TablePartitionId(tableId, partId);

                placementDriver.updateAssignment(replicaGrpId, newConfiguration.peers().stream().map(Peer::consistentId)
                        .collect(toList()));

                var safeTimeTracker = new PendingComparableValuesTracker<HybridTimestamp, Void>(
                        new HybridTimestamp(1, 0)
                );
                var storageIndexTracker = new PendingComparableValuesTracker<Long, Void>(0L);

                ((InternalTableImpl) internalTbl).updatePartitionTrackers(partId, safeTimeTracker, storageIndexTracker);

                PartitionStorages partitionStorages = getPartitionStorages(table, partId);

                PartitionDataStorage partitionDataStorage = partitionDataStorage(partitionStorages.getMvPartitionStorage(),
                        internalTbl, partId);

                PartitionUpdateHandlers partitionUpdateHandlers = createPartitionUpdateHandlers(
                        partId,
                        partitionDataStorage,
                        table,
                        safeTimeTracker
                );

                mvGc.addStorage(replicaGrpId, partitionUpdateHandlers.gcUpdateHandler);

                CompletableFuture<Boolean> startGroupFut;

                // start new nodes, only if it is table creation, other cases will be covered by rebalance logic
                if (localMemberAssignment != null) {
                    CompletableFuture<Boolean> shouldStartGroupFut;

                    // If Raft is running in in-memory mode or the PDS has been cleared, we need to remove the current node
                    // from the Raft group in order to avoid the double vote problem.
                    // <MUTED> See https://issues.apache.org/jira/browse/IGNITE-16668 for details.
                    // TODO: https://issues.apache.org/jira/browse/IGNITE-19046 Restore "|| !hasData"
                    if (internalTbl.storage().isVolatile()) {
                        shouldStartGroupFut = queryDataNodesCount(tableId, partId, newConfiguration.peers())
                                .thenApply(dataNodesCount -> {
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
                        shouldStartGroupFut = completedFuture(true);
                    }

                    startGroupFut = shouldStartGroupFut.thenApplyAsync(startGroup -> inBusyLock(busyLock, () -> {
                        if (!startGroup) {
                            return false;
                        }
                        TxStateStorage txStatePartitionStorage = partitionStorages.getTxStateStorage();

                        RaftGroupOptions groupOptions = groupOptionsForPartition(
                                internalTbl.storage(),
                                internalTbl.txStateStorage(),
                                partitionKey(internalTbl, partId),
                                partitionUpdateHandlers
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
                                            partitionUpdateHandlers.storageUpdateHandler,
                                            txStatePartitionStorage,
                                            safeTimeTracker,
                                            storageIndexTracker
                                    ),
                                    new RebalanceRaftGroupEventsListener(
                                            metaStorageMgr,
                                            replicaGrpId,
                                            busyLock,
                                            createPartitionMover(internalTbl, partId),
                                            this::calculateAssignments,
                                            rebalanceScheduler
                                    ),
                                    groupOptions
                            );

                            return true;
                        } catch (NodeStoppingException ex) {
                            throw new CompletionException(ex);
                        }
                    }), ioExecutor);
                } else {
                    startGroupFut = completedFuture(false);
                }

                startGroupFut
                        .thenComposeAsync(v -> inBusyLock(busyLock, () -> {
                            try {
                                //TODO IGNITE-19614 This procedure takes 10 seconds if there's no majority online.
                                return raftMgr.startRaftGroupService(replicaGrpId, newConfiguration, raftGroupServiceFactory);
                            } catch (NodeStoppingException ex) {
                                return failedFuture(ex);
                            }
                        }), ioExecutor)
                        .thenAcceptAsync(updatedRaftGroupService -> inBusyLock(busyLock, () -> {
                            ((InternalTableImpl) internalTbl).updateInternalTableRaftGroupService(partId, updatedRaftGroupService);

                            boolean startedRaftNode = startGroupFut.join();
                            if (localMemberAssignment == null || !startedRaftNode) {
                                return;
                            }

                            MvPartitionStorage partitionStorage = partitionStorages.getMvPartitionStorage();
                            TxStateStorage txStateStorage = partitionStorages.getTxStateStorage();

                            try {
                                startReplicaWithNewListener(
                                        replicaGrpId,
                                        table,
                                        safeTimeTracker,
                                        storageIndexTracker,
                                        partitionStorage,
                                        txStateStorage,
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

                                futures[partId].completeExceptionally(ex);
                            } else {
                                futures[partId].complete(null);
                            }
                        });
            }

            return allOf(futures);
        });

        // NB: all vv.update() calls must be made from the synchronous part of the method (not in thenCompose()/etc!).
        CompletableFuture<?> localPartsUpdateFuture = localPartsByTableIdVv.update(causalityToken,
                (previous, throwable) -> inBusyLock(busyLock, () -> assignmentsFuture.thenCompose(newAssignments -> {
                    PartitionSet parts = new BitSetPartitionSet();

                    for (int i = 0; i < newAssignments.size(); i++) {
                        parts.set(i);
                    }

                    return getOrCreatePartitionStorages(table, parts).thenApply(u -> {
                        var newValue = new HashMap<>(previous);

                        newValue.put(tableId, parts);

                        return newValue;
                    });
                })));

        return assignmentsUpdatedVv.update(causalityToken, (token, e) -> {
            if (e != null) {
                return failedFuture(e);
            }

            return localPartsUpdateFuture.thenCompose(unused ->
                    tablesByIdVv.get(causalityToken)
                            .thenComposeAsync(tablesById -> inBusyLock(busyLock, updateAssignmentsClosure), ioExecutor)
            );
        });
    }

    private void startReplicaWithNewListener(
            TablePartitionId replicaGrpId,
            TableImpl table,
            PendingComparableValuesTracker<HybridTimestamp, Void> safeTimeTracker,
            PendingComparableValuesTracker<Long, Void> storageIndexTracker,
            MvPartitionStorage mvPartitionStorage,
            TxStateStorage txStatePartitionStorage,
            PartitionUpdateHandlers partitionUpdateHandlers,
            TopologyAwareRaftGroupService raftGroupService
    ) throws NodeStoppingException {
        PartitionReplicaListener listener = createReplicaListener(
                replicaGrpId,
                table,
                safeTimeTracker,
                mvPartitionStorage,
                txStatePartitionStorage,
                partitionUpdateHandlers,
                raftGroupService
        );

        replicaMgr.startReplica(
                replicaGrpId,
                allOf(
                        ((Loza) raftMgr).raftNodeReadyFuture(replicaGrpId),
                        table.pkIndexesReadyFuture()
                ),
                listener,
                raftGroupService,
                storageIndexTracker
        );
    }

    private PartitionReplicaListener createReplicaListener(
            TablePartitionId tablePartitionId,
            TableImpl table,
            PendingComparableValuesTracker<HybridTimestamp, Void> safeTimeTracker,
            MvPartitionStorage mvPartitionStorage,
            TxStateStorage txStatePartitionStorage,
            PartitionUpdateHandlers partitionUpdateHandlers,
            RaftGroupService raftClient
    ) {
        int tableId = tablePartitionId.tableId();
        int partId = tablePartitionId.partitionId();

        return new PartitionReplicaListener(
                mvPartitionStorage,
                raftClient,
                txManager,
                lockMgr,
                scanRequestExecutor,
                partId,
                tableId,
                table.indexesLockers(partId),
                new Lazy<>(() -> table.indexStorageAdapters(partId).get().get(table.pkId())),
                () -> table.indexStorageAdapters(partId).get(),
                clock,
                safeTimeTracker,
                txStatePartitionStorage,
                placementDriver,
                partitionUpdateHandlers.storageUpdateHandler,
                new NonHistoricSchemas(schemaManager),
                localNode(),
                table.internalTable().storage(),
                indexBuilder,
                tablesCfg
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
    private CompletableFuture<Long> queryDataNodesCount(int tblId, int partId, Collection<Peer> peers) {
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
                clusterService.topologyService(),
                outgoingSnapshotsManager,
                new PartitionAccessImpl(
                        partitionKey,
                        mvTableStorage,
                        txStateTableStorage,
                        mvGc,
                        partitionUpdateHandlers.indexUpdateHandler,
                        partitionUpdateHandlers.gcUpdateHandler
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

        Map<Integer, TableImpl> tablesToStop = Stream.concat(latestTablesById().entrySet().stream(), pendingTables.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v1));

        cleanUpTablesResources(tablesToStop);

        try {
            IgniteUtils.closeAllManually(lowWatermark, mvGc, indexBuilder);
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
    private void cleanUpTablesResources(Map<Integer, TableImpl> tables) {
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
                        replicaMgr.stopReplica(replicationGroupId).join();
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
    public CompletableFuture<List<String>> assignmentsAsync(int tableId) {
        return tableAsync(tableId).thenApply(table -> {
            if (table == null) {
                return null;
            }

            return table.internalTable().assignments();
        });
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
     * @param tableDescriptor Catalog table descriptor.
     * @param zoneDescriptor Catalog distributed zone descriptor.
     * @param assignmentsFuture Future with assignments.
     * @return Future that will be completed when local changes related to the table creation are applied.
     */
    @WithSpan
    private CompletableFuture<?> createTableLocally(
            long causalityToken,
            CatalogTableDescriptor tableDescriptor,
            CatalogZoneDescriptor zoneDescriptor,
            CompletableFuture<List<Set<Assignment>>> assignmentsFuture
    ) {
        String tableName = tableDescriptor.name();
        int tableId = tableDescriptor.id();

        LOG.trace("Creating local table: name={}, id={}, token={}", tableDescriptor.name(), tableDescriptor.id(), causalityToken);

        MvTableStorage tableStorage = createTableStorage(tableDescriptor, zoneDescriptor);
        TxStateTableStorage txStateStorage = createTxStateTableStorage(tableDescriptor, zoneDescriptor);

        int partitions = zoneDescriptor.partitions();

        InternalTableImpl internalTable = new InternalTableImpl(tableName, tableId,
                new Int2ObjectOpenHashMap<>(partitions),
                partitions, clusterNodeResolver, txManager, tableStorage,
                txStateStorage, replicaSvc, clock);

        var table = new TableImpl(internalTable, lockMgr);

        // TODO: IGNITE-19082 Need another way to wait for indexes
        table.addIndexesToWait(collectTableIndexIds(tableId));

        tablesByIdVv.update(causalityToken, (previous, e) -> inBusyLock(busyLock, () -> {
            if (e != null) {
                return failedFuture(e);
            }

            return schemaManager.schemaRegistry(causalityToken, tableId)
                    .thenApply(schema -> {
                        table.schemaView(schema);

                        var val = new HashMap<>(previous);

                        val.put(tableId, table);

                        return val;
                    });
        }));

        CompletableFuture<?> createPartsFut = createTablePartitionsLocally(causalityToken, assignmentsFuture, zoneDescriptor.id(), table);

        pendingTables.put(tableId, table);
        startedTables.put(tableId, table);

        tablesById(causalityToken).thenAccept(ignored -> inBusyLock(busyLock, () -> {
            pendingTables.remove(tableId);
        }));

        tablesById(causalityToken)
                .thenRun(() -> inBusyLock(busyLock, () -> completeApiCreateFuture(table)));

        // TODO should be reworked in IGNITE-16763
        // We use the event notification future as the result so that dependent components can complete the schema updates.

        // TODO: https://issues.apache.org/jira/browse/IGNITE-19913 Possible performance degradation.
        return allOf(createPartsFut, fireEvent(TableEvent.CREATE, new TableEventParameters(causalityToken, tableId)));
    }

    /**
     * Creates data storage for the provided table.
     *
     * @param tableDescriptor Catalog table descriptor.
     * @param zoneDescriptor Catalog distributed zone descriptor.
     */

    @WithSpan
    protected MvTableStorage createTableStorage(CatalogTableDescriptor tableDescriptor, CatalogZoneDescriptor zoneDescriptor) {
        CatalogDataStorageDescriptor dataStorage = zoneDescriptor.dataStorage();

        StorageEngine engine = dataStorageMgr.engine(dataStorage.engine());

        assert engine != null : "tableId=" + tableDescriptor.id() + ", engine=" + dataStorage.engine();

        MvTableStorage tableStorage = engine.createMvTable(
                new StorageTableDescriptor(tableDescriptor.id(), zoneDescriptor.partitions(), dataStorage.dataRegion()),
                new StorageIndexDescriptorSupplier(tablesCfg)
        );

        tableStorage.start();

        return tableStorage;
    }

    /**
     * Creates transaction state storage for the provided table.
     *
     * @param tableDescriptor Catalog table descriptor.
     * @param zoneDescriptor Catalog distributed zone descriptor.
     */
    protected TxStateTableStorage createTxStateTableStorage(CatalogTableDescriptor tableDescriptor, CatalogZoneDescriptor zoneDescriptor) {
        int tableId = tableDescriptor.id();

        Path path = storagePath.resolve(TX_STATE_DIR + tableId);

        try {
            Files.createDirectories(path);
        } catch (IOException e) {
            throw new StorageException("Failed to create transaction state storage directory for table: " + tableId, e);
        }

        TxStateTableStorage txStateTableStorage = new TxStateRocksDbTableStorage(
                tableId,
                zoneDescriptor.partitions(),
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
     * @param tableDescriptor Catalog table descriptor.
     * @param zoneDescriptor Catalog distributed zone descriptor.
     */
    private void dropTableLocally(long causalityToken, CatalogTableDescriptor tableDescriptor, CatalogZoneDescriptor zoneDescriptor) {
        int tableId = tableDescriptor.id();
        int partitions = zoneDescriptor.partitions();

        try {
            CompletableFuture<?>[] removeStorageFromGcFutures = new CompletableFuture<?>[partitions];

            for (int p = 0; p < partitions; p++) {
                TablePartitionId replicationGroupId = new TablePartitionId(tableId, p);

                raftMgr.stopRaftNodes(replicationGroupId);

                removeStorageFromGcFutures[p] = replicaMgr
                        .stopReplica(replicationGroupId)
                        .thenCompose((notUsed) -> mvGc.removeStorage(replicationGroupId));
            }

            localPartsByTableIdVv.update(causalityToken, (previousVal, e) -> inBusyLock(busyLock, () -> {
                if (e != null) {
                    return failedFuture(e);
                }

                var newMap = new HashMap<>(previousVal);
                newMap.remove(tableId);

                return completedFuture(newMap);
            }));

            tablesByIdVv.update(causalityToken, (previousVal, e) -> inBusyLock(busyLock, () -> {
                if (e != null) {
                    return failedFuture(e);
                }

                var map = new HashMap<>(previousVal);

                TableImpl table = map.remove(tableId);

                assert table != null : tableId;

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

            startedTables.remove(tableId);

            fireEvent(TableEvent.DROP, new TableEventParameters(causalityToken, tableId))
                    .whenComplete((v, e) -> {
                        Set<ByteArray> assignmentKeys = new HashSet<>();

                        for (int p = 0; p < partitions; p++) {
                            assignmentKeys.add(stablePartAssignmentsKey(new TablePartitionId(tableId, p)));
                        }

                        metaStorageMgr.removeAll(assignmentKeys);

                        if (e != null) {
                            LOG.error("Error on " + TableEvent.DROP + " notification", e);
                        }
                    });
        } catch (NodeStoppingException e) {
            fireEvent(TableEvent.DROP, new TableEventParameters(causalityToken, tableId), e);
        }
    }

    private Set<Assignment> calculateAssignments(TablePartitionId tablePartitionId) {
        CatalogTableDescriptor tableDescriptor = getTableDescriptor(tablePartitionId.tableId());

        assert tableDescriptor != null : tablePartitionId;

        return AffinityUtils.calculateAssignmentForPartition(
                // TODO: https://issues.apache.org/jira/browse/IGNITE-19425 we must use distribution zone keys here
                baselineMgr.nodes().stream().map(ClusterNode::name).collect(toList()),
                tablePartitionId.partitionId(),
                getZoneDescriptor(tableDescriptor.zoneId()).replicas()
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
    @WithSpan
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
    @WithSpan
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
                                        Context context = Context.current();
                                        cmgMgr.logicalTopology()
                                                .handle(context.wrapFunction((cmgTopology, e) -> {
                                                    if (e == null) {
                                                        if (!busyLock.enterBusy()) {
                                                            NodeStoppingException nodeStoppingException = new NodeStoppingException();

                                                            tblFut.completeExceptionally(nodeStoppingException);

                                                            throw new IgniteException(nodeStoppingException);
                                                        }

                                                        try {
                                                            changeTablesConfigurationOnTableCreate(
                                                                    name,
                                                                    zoneId,
                                                                    tableInitChange,
                                                                    tblFut
                                                            );
                                                        } finally {
                                                            busyLock.leaveBusy();
                                                        }
                                                    } else {
                                                        tblFut.completeExceptionally(e);
                                                    }

                                                    return null;
                                                }));
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
     * @param tableInitChange Table changer.
     * @param tblFut Future representing pending completion of the table creation.
     */
    @WithSpan
    private void changeTablesConfigurationOnTableCreate(
            String name,
            int zoneId,
            Consumer<TableChange> tableInitChange,
            CompletableFuture<Table> tblFut
    ) {
        Context context = Context.current();
        tablesCfg.change(tablesChange -> {
            incrementTablesGeneration(tablesChange);

            tablesChange.changeTables(tablesListChange -> {
                if (tablesListChange.get(name) != null) {
                    throw new TableAlreadyExistsException(DEFAULT_SCHEMA_NAME, name);
                }

                tablesListChange.create(name, (tableChange) -> {
                    tableInitChange.accept(tableChange);

                    tableChange.changeZoneId(zoneId);

                    var extConfCh = ((ExtendedTableChange) tableChange);

                    int tableId = tablesChange.globalIdCounter() + 1;

                    extConfCh.changeId(tableId);

                    tablesChange.changeGlobalIdCounter(tableId);

                    extConfCh.changeSchemaId(INITIAL_SCHEMA_VERSION);

                    tableCreateFuts.put(extConfCh.id(), tblFut);
                    tableCreateContext.put(extConfCh.id(), context);
                });
            });
        }).exceptionally(t -> {
            Throwable ex = getRootCause(t);

            if (ex instanceof TableAlreadyExistsException) {
                tblFut.completeExceptionally(ex);
            } else {
                LOG.debug("Unable to create table [name={}]", ex, name);

                tblFut.completeExceptionally(ex);

                tableCreateFuts.values().removeIf(fut -> fut == tblFut);
                tableCreateContext.values().removeIf(ctx -> ctx == context);
            }

            return null;
        });
    }

    private static void incrementTablesGeneration(TablesChange tablesChange) {
        tablesChange.changeTablesGeneration(tablesChange.tablesGeneration() + 1);
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
    @WithSpan
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
    @WithSpan
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

        // TODO https://issues.apache.org/jira/browse/IGNITE-19539
        return (ex instanceof IgniteException) ? (IgniteException) ex : ExceptionUtils.wrap(ex);
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
    @WithSpan
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
    @WithSpan
    private CompletableFuture<Void> dropTableAsyncInternal(String name) {
        return tableAsyncInternal(name).thenCompose(tbl -> {
            // In case of drop it's an optimization that allows not to fire drop-change-closure if there's no such
            // distributed table and the local config has lagged behind.
            if (tbl == null) {
                return failedFuture(new TableNotFoundException(DEFAULT_SCHEMA_NAME, name));
            }

            return tablesCfg
                    .change(chg -> {
                        incrementTablesGeneration(chg);

                        chg
                                .changeTables(tblChg -> {
                                    if (tblChg.get(name) == null) {
                                        throw new TableNotFoundException(DEFAULT_SCHEMA_NAME, name);
                                    }

                                    tblChg.delete(name);
                                })
                                .changeIndexes(idxChg -> {
                                    for (TableIndexView index : idxChg) {
                                        if (index.tableId() == tbl.tableId()) {
                                            idxChg.delete(index.name());
                                        }
                                    }
                                });
                    })
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
    @WithSpan
    @Override
    public List<Table> tables() {
        return join(tablesAsync());
    }

    /** {@inheritDoc} */
    @WithSpan
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
    @WithSpan
    private CompletableFuture<List<Table>> tablesAsyncInternal() {
        return supplyAsync(() -> inBusyLock(busyLock, this::directTableIds), ioExecutor)
                .thenCompose(tableIds -> inBusyLock(busyLock, () -> {
                    var tableFuts = new CompletableFuture[tableIds.size()];

                    var i = 0;

                    for (int tblId : tableIds) {
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
    private List<Integer> directTableIds() {
        return configuredTablesCache.configuredTableIds();
    }

    /**
     * Gets direct id of table with {@code tblName}.
     *
     * @param tblName Name of the table.
     * @return Direct id of the table, or {@code null} if the table with the {@code tblName} has not been found.
     */
    @Nullable
    private Integer directTableId(String tblName) {
        try {
            TableConfiguration exTblCfg = directProxy(tablesCfg.tables()).get(tblName);

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
     * Returns the tables by ID future for the given causality token.
     * The future will only be completed when corresponding assignments update completes.
     *
     * @param causalityToken Causality token.
     * @return The future with tables map.
     * @see #assignmentsUpdatedVv
     */
    private CompletableFuture<Map<Integer, TableImpl>> tablesById(long causalityToken) {
        return assignmentsUpdatedVv.get(causalityToken).thenCompose(v -> tablesByIdVv.get(causalityToken));
    }

    /**
     * Returns the latest tables by ID map, for which all assignment updates have been completed.
     */
    private Map<Integer, TableImpl> latestTablesById() {
        long latestCausalityToken = assignmentsUpdatedVv.latestCausalityToken();

        if (latestCausalityToken < 0L) {
            // No tables at all in case of empty causality token.
            return emptyMap();
        } else {
            CompletableFuture<Map<Integer, TableImpl>> tablesByIdFuture = tablesByIdVv.get(latestCausalityToken);

            assert tablesByIdFuture.isDone()
                    : "'tablesByIdVv' is always completed strictly before the 'assignmentsUpdatedVv'";

            return tablesByIdFuture.join();
        }
    }

    /**
     * Actual tables map.
     *
     * @return Actual tables map.
     */
    @TestOnly
    public Map<Integer, TableImpl> latestTables() {
        return unmodifiableMap(latestTablesById());
    }

    /** {@inheritDoc} */
    @Override
    public Table table(String name) {
        return join(tableAsync(name));
    }

    /** {@inheritDoc} */
    @Override
    public TableImpl table(int id) throws NodeStoppingException {
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
    public CompletableFuture<TableImpl> tableAsync(long causalityToken, int id) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }
        try {
            return tablesById(causalityToken).thenApply(tablesById -> tablesById.get(id));
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<TableImpl> tableAsync(int id) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }
        try {
            return tableAsyncInternal(id, true);
        } finally {
            busyLock.leaveBusy();
        }
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
            return localPartsByTableIdVv.get(causalityToken).thenApply(partitionSetById -> partitionSetById.get(tableId));
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @WithSpan
    @Override
    public TableImpl tableImpl(String name) {
        return join(tableImplAsync(name));
    }

    /** {@inheritDoc} */
    @WithSpan
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
    @WithSpan
    public CompletableFuture<TableImpl> tableAsyncInternal(String name) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }

        try {
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
    @WithSpan
    public CompletableFuture<TableImpl> tableAsyncInternal(int id, boolean checkConfiguration) {
        CompletableFuture<Boolean> tblCfgFut = checkConfiguration
                ? supplyAsync(() -> inBusyLock(busyLock, () -> isTableConfigured(id)), ioExecutor)
                : completedFuture(true);

        return tblCfgFut.thenCompose(isCfg -> inBusyLock(busyLock, () -> {
            if (!isCfg) {
                return completedFuture(null);
            }

            TableImpl tbl = latestTablesById().get(id);

            if (tbl != null) {
                return completedFuture(tbl);
            }

            CompletableFuture<TableImpl> getTblFut = new CompletableFuture<>();

            CompletionListener<Void> tablesListener = (token, v, th) -> {
                if (th == null) {
                    CompletableFuture<Map<Integer, TableImpl>> tablesFut = tablesByIdVv.get(token);

                    tablesFut.whenComplete((tables, e) -> {
                        if (e != null) {
                            getTblFut.completeExceptionally(e);
                        } else {
                            TableImpl table = tables.get(id);

                            if (table != null) {
                                getTblFut.complete(table);
                            }
                        }
                    });
                } else {
                    getTblFut.completeExceptionally(th);
                }
            };

            assignmentsUpdatedVv.whenComplete(tablesListener);

            // This check is needed for the case when we have registered tablesListener,
            // but tablesByIdVv has already been completed, so listener would be triggered only for the next versioned value update.
            tbl = latestTablesById().get(id);

            if (tbl != null) {
                assignmentsUpdatedVv.removeWhenComplete(tablesListener);

                return completedFuture(tbl);
            }

            return getTblFut.whenComplete((unused, throwable) -> assignmentsUpdatedVv.removeWhenComplete(tablesListener));
        }));
    }

    /**
     * Checks that the table is configured with specific id.
     *
     * @param id Table id.
     * @return True when the table is configured into cluster, false otherwise.
     */
    private boolean isTableConfigured(int id) {
        return configuredTablesCache.isTableConfigured(id);
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
        int tblId = extractTableId(pendingAssignmentsWatchEntry.key(), PENDING_ASSIGNMENTS_PREFIX);

        var replicaGrpId = new TablePartitionId(tblId, partId);

        // Stable assignments from the meta store, which revision is bounded by the current pending event.
        CompletableFuture<Entry> stableAssignmentsFuture = metaStorageMgr.get(stablePartAssignmentsKey(replicaGrpId), evt.revision());

        return tablesById(evt.revision())
                .thenCombineAsync(stableAssignmentsFuture, (tables, stableAssignmentsEntry) -> {
                    if (!busyLock.enterBusy()) {
                        return CompletableFuture.<Void>failedFuture(new NodeStoppingException());
                    }

                    try {
                        return handleChangePendingAssignmentEvent(
                                evt.revision(),
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
            long causalityToken,
            TablePartitionId replicaGrpId,
            TableImpl tbl,
            Entry pendingAssignmentsEntry,
            Entry stableAssignmentsEntry
    ) {
        // Assignments of the pending rebalance that we received through the Meta storage watch mechanism.
        Set<Assignment> pendingAssignments = ByteUtils.fromBytes(pendingAssignmentsEntry.value());

        PeersAndLearners pendingConfiguration = configurationFromAssignments(pendingAssignments);

        int tableId = tbl.tableId();
        int partId = replicaGrpId.partitionId();

        byte[] stableAssignmentsBytes = stableAssignmentsEntry.value();

        Set<Assignment> stableAssignments = ByteUtils.fromBytes(stableAssignmentsBytes);

        PeersAndLearners stableConfiguration = configurationFromAssignments(stableAssignments);

        placementDriver.updateAssignment(
                replicaGrpId,
                stableConfiguration.peers().stream().map(Peer::consistentId).collect(toList())
        );

        ClusterNode localMember = localNode();

        // Start a new Raft node and Replica if this node has appeared in the new assignments.
        boolean shouldStartLocalServices = pendingAssignments.stream()
                .filter(assignment -> localMember.name().equals(assignment.consistentId()))
                .anyMatch(assignment -> !stableAssignments.contains(assignment));

        PendingComparableValuesTracker<HybridTimestamp, Void> safeTimeTracker =
                new PendingComparableValuesTracker<>(new HybridTimestamp(1, 0));
        PendingComparableValuesTracker<Long, Void> storageIndexTracker = new PendingComparableValuesTracker<>(0L);

        InternalTable internalTable = tbl.internalTable();

        LOG.info("Received update on pending assignments. Check if new raft group should be started"
                        + " [key={}, partition={}, table={}, localMemberAddress={}]",
                new String(pendingAssignmentsEntry.key(), StandardCharsets.UTF_8), partId, tbl.name(), localMember.address());

        CompletableFuture<Void> localServicesStartFuture;

        if (shouldStartLocalServices) {
            localServicesStartFuture = localPartsByTableIdVv.get(causalityToken).thenComposeAsync(oldMap -> {
                PartitionSet partitionSet = oldMap.get(tableId).copy();

                return getOrCreatePartitionStorages(tbl, partitionSet).thenApply(u -> {
                    var newMap = new HashMap<>(oldMap);

                    newMap.put(tableId, partitionSet);

                    return newMap;
                });
            }).thenComposeAsync(unused -> {
                PartitionStorages partitionStorages = getPartitionStorages(tbl, partId);

                MvPartitionStorage mvPartitionStorage = partitionStorages.getMvPartitionStorage();
                TxStateStorage txStatePartitionStorage = partitionStorages.getTxStateStorage();

                PartitionDataStorage partitionDataStorage = partitionDataStorage(mvPartitionStorage, internalTable, partId);

                PartitionUpdateHandlers partitionUpdateHandlers = createPartitionUpdateHandlers(
                        partId,
                        partitionDataStorage,
                        tbl,
                        safeTimeTracker
                );

                return runAsync(() -> inBusyLock(busyLock, () -> {
                    try {
                        startPartitionRaftGroupNode(
                                replicaGrpId,
                                pendingConfiguration,
                                stableConfiguration,
                                safeTimeTracker,
                                storageIndexTracker,
                                internalTable,
                                txStatePartitionStorage,
                                partitionDataStorage,
                                partitionUpdateHandlers
                        );

                        startReplicaWithNewListener(
                                replicaGrpId,
                                tbl,
                                safeTimeTracker,
                                storageIndexTracker,
                                mvPartitionStorage,
                                txStatePartitionStorage,
                                partitionUpdateHandlers,
                                (TopologyAwareRaftGroupService) internalTable.partitionRaftGroupService(partId)
                        );
                    } catch (NodeStoppingException ignored) {
                        // No-op.
                    }
                }), ioExecutor);
            });
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

    private void startPartitionRaftGroupNode(
            TablePartitionId replicaGrpId,
            PeersAndLearners pendingConfiguration,
            PeersAndLearners stableConfiguration,
            PendingComparableValuesTracker<HybridTimestamp, Void> safeTimeTracker,
            PendingComparableValuesTracker<Long, Void> storageIndexTracker,
            InternalTable internalTable,
            TxStateStorage txStatePartitionStorage,
            PartitionDataStorage partitionDataStorage,
            PartitionUpdateHandlers partitionUpdateHandlers
    ) throws NodeStoppingException {
        ClusterNode localMember = localNode();

        RaftGroupOptions groupOptions = groupOptionsForPartition(
                internalTable.storage(),
                internalTable.txStateStorage(),
                partitionKey(internalTable, replicaGrpId.partitionId()),
                partitionUpdateHandlers
        );

        RaftGroupListener raftGrpLsnr = new PartitionListener(
                partitionDataStorage,
                partitionUpdateHandlers.storageUpdateHandler,
                txStatePartitionStorage,
                safeTimeTracker,
                storageIndexTracker
        );

        RaftGroupEventsListener raftGrpEvtsLsnr = new RebalanceRaftGroupEventsListener(
                metaStorageMgr,
                replicaGrpId,
                busyLock,
                createPartitionMover(internalTable, replicaGrpId.partitionId()),
                this::calculateAssignments,
                rebalanceScheduler
        );

        Peer serverPeer = pendingConfiguration.peer(localMember.name());

        var raftNodeId = new RaftNodeId(replicaGrpId, serverPeer);

        // TODO: use RaftManager interface, see https://issues.apache.org/jira/browse/IGNITE-18273
        ((Loza) raftMgr).startRaftGroupNode(
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

                    int partitionId = extractPartitionNumber(key);
                    int tableId = extractTableId(key, ASSIGNMENTS_SWITCH_REDUCE_PREFIX);

                    TablePartitionId replicaGrpId = new TablePartitionId(tableId, partitionId);

                    return tablesById(evt.revision())
                            .thenCompose(tables -> {
                                if (!busyLock.enterBusy()) {
                                    return failedFuture(new NodeStoppingException());
                                }

                                try {
                                    CatalogTableDescriptor tableDescriptor = getTableDescriptor(tableId);

                                    assert tableDescriptor != null : replicaGrpId;

                                    return distributionZoneManager.dataNodes(evt.revision(), tableDescriptor.zoneId())
                                            .thenCompose(dataNodes -> RebalanceUtil.handleReduceChanged(
                                                    metaStorageMgr,
                                                    dataNodes,
                                                    getZoneDescriptor(tableDescriptor.zoneId()).replicas(),
                                                    replicaGrpId,
                                                    evt
                                            ));
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
     * Gets partition stores.
     *
     * @param table Table.
     * @param partitionId Partition ID.
     * @return PartitionStorages.
     */
    private PartitionStorages getPartitionStorages(TableImpl table, int partitionId) {
        InternalTable internalTable = table.internalTable();

        MvPartitionStorage mvPartition = internalTable.storage().getMvPartition(partitionId);

        assert mvPartition != null;

        TxStateStorage txStateStorage = internalTable.txStateStorage().getTxStateStorage(partitionId);

        assert txStateStorage != null;

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
                            ).thenApply(unused -> new PartitionStorages(mvPartitionStorage, txStateStorage));
                        } else {
                            return completedFuture(new PartitionStorages(mvPartitionStorage, txStateStorage));
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
            return completedFuture(null);
        }

        if (!evt.single()) {
            // If there is not a single entry, then all entries must be tombstones (this happens after table drop).
            assert evt.entryEvents().stream().allMatch(entryEvent -> entryEvent.newEntry().tombstone()) : evt;

            return completedFuture(null);
        }

        // here we can receive only update from the rebalance logic
        // these updates always processing only 1 partition, so, only 1 stable partition key.
        assert evt.single() : evt;

        Entry stableAssignmentsWatchEvent = evt.entryEvent().newEntry();

        if (stableAssignmentsWatchEvent.value() == null) {
            return completedFuture(null);
        }

        int partitionId = extractPartitionNumber(stableAssignmentsWatchEvent.key());
        int tableId = extractTableId(stableAssignmentsWatchEvent.key(), STABLE_ASSIGNMENTS_PREFIX);

        TablePartitionId tablePartitionId = new TablePartitionId(tableId, partitionId);

        Set<Assignment> stableAssignments = ByteUtils.fromBytes(stableAssignmentsWatchEvent.value());

        return metaStorageMgr.get(pendingPartAssignmentsKey(tablePartitionId), stableAssignmentsWatchEvent.revision())
                .thenComposeAsync(pendingAssignmentsEntry -> {
                    byte[] pendingAssignmentsFromMetaStorage = pendingAssignmentsEntry.value();

                    Set<Assignment> pendingAssignments = pendingAssignmentsFromMetaStorage == null
                            ? Set.of()
                            : ByteUtils.fromBytes(pendingAssignmentsFromMetaStorage);

                    String localMemberName = localNode().name();

                    boolean shouldStopLocalServices = Stream.concat(stableAssignments.stream(), pendingAssignments.stream())
                            .noneMatch(assignment -> assignment.consistentId().equals(localMemberName));

                    if (shouldStopLocalServices) {
                        return stopAndDestroyPartition(tablePartitionId, evt.revision());
                    } else {
                        return completedFuture(null);
                    }
                }, ioExecutor);
    }

    private CompletableFuture<Void> stopAndDestroyPartition(TablePartitionId tablePartitionId, long revision) {
        try {
            raftMgr.stopRaftNodes(tablePartitionId);
        } catch (NodeStoppingException e) {
            // No-op
        }

        CompletableFuture<Boolean> stopReplicaFut;
        try {
            stopReplicaFut = replicaMgr.stopReplica(tablePartitionId);
        } catch (NodeStoppingException e) {
            stopReplicaFut = completedFuture(true);
        }

        return destroyPartitionStorages(tablePartitionId, revision, stopReplicaFut);
    }

    private CompletableFuture<Void> destroyPartitionStorages(
            TablePartitionId tablePartitionId,
            long revision,
            CompletableFuture<Boolean> stopReplicaFut
    ) {
        int partitionId = tablePartitionId.partitionId();

        return tablesById(revision)
                // TODO: IGNITE-18703 Destroy raft log and meta
                .thenCombine(mvGc.removeStorage(tablePartitionId), (tables, unused) -> {
                    TableImpl table = tables.get(tablePartitionId.tableId());

                    // TODO: IGNITE-19905 - remove the check.
                    if (table == null) {
                        return allOf(stopReplicaFut);
                    }

                    InternalTable internalTable = table.internalTable();

                    closePartitionTrackers(internalTable, partitionId);

                    return allOf(
                            stopReplicaFut,
                            internalTable.storage().destroyPartition(partitionId),
                            runAsync(() -> internalTable.txStateStorage().destroyTxStateStorage(partitionId), ioExecutor)
                    );
                })
                .thenCompose(Function.identity());
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

    private int[] collectTableIndexIds(int tableId) {
        return tablesCfg.value().indexes().stream()
                .filter(tableIndexView -> tableIndexView.tableId() == tableId)
                .mapToInt(TableIndexView::id)
                .toArray();
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
        return clusterService.topologyService().localMember();
    }

    private PartitionUpdateHandlers createPartitionUpdateHandlers(
            int partitionId,
            PartitionDataStorage partitionDataStorage,
            TableImpl table,
            PendingComparableValuesTracker<HybridTimestamp, Void> safeTimeTracker
    ) {
        TableIndexStoragesSupplier indexes = table.indexStorageAdapters(partitionId);

        IndexUpdateHandler indexUpdateHandler = new IndexUpdateHandler(indexes);

        GcUpdateHandler gcUpdateHandler = new GcUpdateHandler(partitionDataStorage, safeTimeTracker, indexUpdateHandler);

        StorageUpdateHandler storageUpdateHandler = new StorageUpdateHandler(
                partitionId,
                partitionDataStorage,
                gcConfig,
                lowWatermark,
                indexUpdateHandler,
                gcUpdateHandler
        );

        return new PartitionUpdateHandlers(storageUpdateHandler, indexUpdateHandler, gcUpdateHandler);
    }

    /**
     * Returns a table instance if it exists, {@code null} otherwise.
     *
     * @param tableId Table id.
     */
    public @Nullable TableImpl getTable(int tableId) {
        return startedTables.get(tableId);
    }

    private @Nullable CatalogTableDescriptor getTableDescriptor(int id) {
        TableView tableView = findTableView(tablesCfg.value(), id);

        return tableView == null ? null : toTableDescriptor(tableView);
    }

    private CatalogZoneDescriptor getZoneDescriptor(int id) {
        return toZoneDescriptor(getZoneById(zonesConfig, id).value());
    }
}
