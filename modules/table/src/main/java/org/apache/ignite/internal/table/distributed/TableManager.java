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
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.anyOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.causality.IncrementalVersionedValue.dependingOn;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.partitionAssignments;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.tableAssignments;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.LongFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.affinity.AffinityUtils;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogDataStorageDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CreateTableEventParameters;
import org.apache.ignite.internal.catalog.events.DropTableEventParameters;
import org.apache.ignite.internal.causality.CompletionListener;
import org.apache.ignite.internal.causality.IncrementalVersionedValue;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Conditions;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
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
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.raft.storage.impl.LogStorageFactoryCreator;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.configuration.GcConfiguration;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptorSupplier;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.gc.GcUpdateHandler;
import org.apache.ignite.internal.table.distributed.gc.MvGc;
import org.apache.ignite.internal.table.distributed.index.IndexUpdateHandler;
import org.apache.ignite.internal.table.distributed.raft.PartitionDataStorage;
import org.apache.ignite.internal.table.distributed.raft.PartitionListener;
import org.apache.ignite.internal.table.distributed.raft.RebalanceRaftGroupEventsListener;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionAccessImpl;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionKey;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionSnapshotStorageFactory;
import org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing.SnapshotAwarePartitionDataStorage;
import org.apache.ignite.internal.table.distributed.replicator.PartitionReplicaListener;
import org.apache.ignite.internal.table.distributed.replicator.TransactionStateResolver;
import org.apache.ignite.internal.table.distributed.schema.CatalogValidationSchemasSource;
import org.apache.ignite.internal.table.distributed.schema.SchemaSyncService;
import org.apache.ignite.internal.table.distributed.schema.SchemaVersions;
import org.apache.ignite.internal.table.distributed.schema.SchemaVersionsImpl;
import org.apache.ignite.internal.table.distributed.schema.ThreadLocalPartitionCommandsMarshaller;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.table.distributed.storage.PartitionStorages;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateTableStorage;
import org.apache.ignite.internal.tx.storage.state.rocksdb.TxStateRocksDbTableStorage;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.Lazy;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.internal.utils.RebalanceUtil;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.util.IgniteNameUtils;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.raft.jraft.storage.impl.VolatileRaftMetaStorage;
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
    private static final String TX_STATE_DIR = "tx-state-";

    /** Transaction storage flush delay. */
    private static final int TX_STATE_STORAGE_FLUSH_DELAY = 1000;
    private static final IntSupplier TX_STATE_STORAGE_FLUSH_DELAY_SUPPLIER = () -> TX_STATE_STORAGE_FLUSH_DELAY;

    private final ClusterService clusterService;

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

    /** Vault manager. */
    private final VaultManager vaultManager;

    /** Data storage manager. */
    private final DataStorageManager dataStorageMgr;

    /** Transaction state resolver. */
    private final TransactionStateResolver transactionStateResolver;

    /** Here a table future stores during creation (until the table can be provided to client). */
    private final Map<Integer, CompletableFuture<Table>> tableCreateFuts = new ConcurrentHashMap<>();

    /**
     * Versioned store for tables by id. Only table instances are created here, local storages and RAFT groups may not be initialized yet.
     *
     * @see #localPartsByTableIdVv
     * @see #assignmentsUpdatedVv
     */
    private final IncrementalVersionedValue<Map<Integer, TableImpl>> tablesByIdVv;

    /**
     * Versioned store for local partition set by table id.
     *
     * <p>Completed strictly after {@link #tablesByIdVv} and strictly before {@link #assignmentsUpdatedVv}.
     */
    private final IncrementalVersionedValue<Map<Integer, PartitionSet>> localPartsByTableIdVv;

    /**
     * Versioned store for tracking RAFT groups initialization and starting completion.
     *
     * <p>Only explicitly updated in {@link #createTablePartitionsLocally(long, CompletableFuture, TableImpl)}.
     *
     * <p>Completed strictly after {@link #localPartsByTableIdVv}.
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
    private final AtomicBoolean beforeStopGuard = new AtomicBoolean();

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

    private final DistributionZoneManager distributionZoneManager;

    private final SchemaSyncService schemaSyncService;

    private final CatalogService catalogService;

    /** Partitions storage path. */
    private final Path storagePath;

    /** Assignment change event listeners. */
    private final CopyOnWriteArrayList<Consumer<IgniteTablesInternal>> assignmentsChangeListeners = new CopyOnWriteArrayList<>();

    /** Incoming RAFT snapshots executor. */
    private final ExecutorService incomingSnapshotsExecutor;

    /** Rebalance scheduler pool size. */
    private static final int REBALANCE_SCHEDULER_POOL_SIZE = Math.min(Runtime.getRuntime().availableProcessors() * 3, 20);

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
    private final PlacementDriver placementDriver;

    private final SchemaVersions schemaVersions;

    private final PartitionReplicatorNodeRecovery partitionReplicatorNodeRecovery;

    /** Versioned value used only at manager startup to correctly fire table creation events. */
    private final IncrementalVersionedValue<Void> startVv;

    /** Ends at the {@link #stop()} with an {@link NodeStoppingException}. */
    private final CompletableFuture<Void> stopManagerFuture = new CompletableFuture<>();

    /**
     * Creates a new table manager.
     *
     * @param nodeName Node name.
     * @param registry Registry for versioned values.
     * @param gcConfig Garbage collector configuration.
     * @param raftMgr Raft manager.
     * @param replicaMgr Replica manager.
     * @param lockMgr Lock manager.
     * @param replicaSvc Replica service.
     * @param txManager Transaction manager.
     * @param dataStorageMgr Data storage manager.
     * @param schemaManager Schema manager.
     * @param volatileLogStorageFactoryCreator Creator for {@link org.apache.ignite.internal.raft.storage.LogStorageFactory} for
     *         volatile tables.
     * @param raftGroupServiceFactory Factory that is used for creation of raft group services for replication groups.
     * @param vaultManager Vault manager.
     * @param placementDriver Placement driver.
     */
    public TableManager(
            String nodeName,
            Consumer<LongFunction<CompletableFuture<?>>> registry,
            GcConfiguration gcConfig,
            ClusterService clusterService,
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
            HybridClock clock,
            OutgoingSnapshotsManager outgoingSnapshotsManager,
            TopologyAwareRaftGroupServiceFactory raftGroupServiceFactory,
            VaultManager vaultManager,
            DistributionZoneManager distributionZoneManager,
            SchemaSyncService schemaSyncService,
            CatalogService catalogService,
            HybridTimestampTracker observableTimestampTracker,
            PlacementDriver placementDriver
    ) {
        this.clusterService = clusterService;
        this.raftMgr = raftMgr;
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
        this.distributionZoneManager = distributionZoneManager;
        this.schemaSyncService = schemaSyncService;
        this.catalogService = catalogService;
        this.observableTimestampTracker = observableTimestampTracker;
        this.placementDriver = placementDriver;

        TopologyService topologyService = clusterService.topologyService();

        clusterNodeResolver = topologyService::getByConsistentId;

        transactionStateResolver = new TransactionStateResolver(
                replicaSvc,
                txManager,
                clock,
                clusterNodeResolver,
                topologyService::getById,
                clusterService.messagingService()
        );

        schemaVersions = new SchemaVersionsImpl(schemaSyncService, catalogService, clock);

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

        raftCommandsMarshaller = new ThreadLocalPartitionCommandsMarshaller(clusterService.serializationRegistry());

        partitionReplicatorNodeRecovery = new PartitionReplicatorNodeRecovery(
                metaStorageMgr,
                clusterService.messagingService(),
                topologyService,
                clusterNodeResolver,
                tableId -> latestTablesById().get(tableId)
        );

        startVv = new IncrementalVersionedValue<>(registry);
    }

    @Override
    public void start() {
        inBusyLock(busyLock, () -> {
            mvGc.start();

            lowWatermark.start();

            transactionStateResolver.start();

            CompletableFuture<Long> recoveryFinishFuture = metaStorageMgr.recoveryFinishedFuture();

            assert recoveryFinishFuture.isDone();

            long recoveryRevision = recoveryFinishFuture.join();

            startTables(recoveryRevision);

            performRebalanceOnRecovery(recoveryRevision);

            metaStorageMgr.registerPrefixWatch(ByteArray.fromString(PENDING_ASSIGNMENTS_PREFIX), pendingAssignmentsRebalanceListener);
            metaStorageMgr.registerPrefixWatch(ByteArray.fromString(STABLE_ASSIGNMENTS_PREFIX), stableAssignmentsRebalanceListener);
            metaStorageMgr.registerPrefixWatch(ByteArray.fromString(ASSIGNMENTS_SWITCH_REDUCE_PREFIX), assignmentsSwitchRebalanceListener);

            catalogService.listen(CatalogEvent.TABLE_CREATE, (parameters, exception) -> {
                assert exception == null : parameters;

                return onTableCreate((CreateTableEventParameters) parameters).thenApply(unused -> false);
            });

            catalogService.listen(CatalogEvent.TABLE_DROP, (parameters, exception) -> {
                assert exception == null : parameters;

                return onTableDelete(((DropTableEventParameters) parameters)).thenApply(unused -> false);
            });

            partitionReplicatorNodeRecovery.start();
        });
    }

    private void performRebalanceOnRecovery(long recoveryRevision) {
        CompletableFuture<Void> pendingAssignmentsRecoveryFuture;

        var prefix = new ByteArray(PENDING_ASSIGNMENTS_PREFIX);

        try (Cursor<Entry> cursor = metaStorageMgr.prefixLocally(prefix, recoveryRevision)) {
            CompletableFuture<?>[] futures = cursor.stream()
                    .map(pendingAssignmentEntry -> {
                        if (LOG.isInfoEnabled()) {
                            LOG.info(
                                    "Missed pending assignments for key '{}' discovered, performing recovery",
                                    new String(pendingAssignmentEntry.key(), UTF_8)
                            );
                        }

                        // We use the Meta Storage recovery revision here instead of the entry revision, because
                        // 'handleChangePendingAssignmentEvent' accesses some Versioned Values that only store values starting with
                        // tokens equal to Meta Storage recovery revision. In other words, if the entry has a lower revision than the
                        // recovery revision, there will never be a Versioned Value corresponding to its revision.
                        return handleChangePendingAssignmentEvent(pendingAssignmentEntry, recoveryRevision);
                    })
                    .toArray(CompletableFuture[]::new);

            pendingAssignmentsRecoveryFuture = allOf(futures)
                    // Simply log any errors, we don't want to block watch processing.
                    .exceptionally(e -> {
                        LOG.error("Error when performing pending assignments recovery", e);

                        return null;
                    });
        }

        startVv.update(recoveryRevision, (v, e) -> pendingAssignmentsRecoveryFuture);
    }

    private CompletableFuture<?> onTableCreate(CreateTableEventParameters parameters) {
        return createTableLocally(parameters.causalityToken(), parameters.catalogVersion(), parameters.tableDescriptor());
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

    private CompletableFuture<Void> onTableDelete(DropTableEventParameters parameters) {
        return inBusyLockAsync(busyLock, () -> {
            long causalityToken = parameters.causalityToken();
            int catalogVersion = parameters.catalogVersion();

            int tableId = parameters.tableId();

            CatalogTableDescriptor tableDescriptor = getTableDescriptor(tableId, catalogVersion - 1);
            CatalogZoneDescriptor zoneDescriptor = getZoneDescriptor(tableDescriptor, catalogVersion - 1);

            dropTableLocally(causalityToken, tableDescriptor, zoneDescriptor);

            return completedFuture(null);
        });
    }

    /**
     * Updates or creates partition raft groups and storages.
     *
     * @param causalityToken Causality token.
     * @param assignmentsFuture Table assignments.
     * @param table Initialized table entity.
     * @return future, which will be completed when the partitions creations done.
     */
    private CompletableFuture<?> createTablePartitionsLocally(
            long causalityToken,
            CompletableFuture<List<Set<Assignment>>> assignmentsFuture,
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

                transactionStateResolver.updateAssignment(replicaGrpId, newConfiguration.peers().stream().map(Peer::consistentId)
                        .collect(toList()));

                var safeTimeTracker = new PendingComparableValuesTracker<HybridTimestamp, Void>(
                        new HybridTimestamp(1, 0)
                );
                var storageIndexTracker = new PendingComparableValuesTracker<Long, Void>(0L);

                ((InternalTableImpl) internalTbl).updatePartitionTrackers(partId, safeTimeTracker, storageIndexTracker);

                PartitionStorages partitionStorages = getPartitionStorages(table, partId);

                PartitionDataStorage partitionDataStorage = partitionDataStorage(partitionStorages.getMvPartitionStorage(),
                        internalTbl, partId);

                storageIndexTracker.update(partitionDataStorage.lastAppliedIndex(), null);

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
                    CompletableFuture<Boolean> shouldStartGroupFut = partitionReplicatorNodeRecovery.shouldStartGroup(
                            replicaGrpId,
                            internalTbl,
                            newConfiguration,
                            localMemberAssignment
                    );

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
                                            txManager,
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
                                return raftMgr.startRaftGroupService(
                                        replicaGrpId, newConfiguration, raftGroupServiceFactory, raftCommandsMarshaller
                                );
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

        // We bring the future outside to avoid OutdatedTokenException.
        CompletableFuture<Map<Integer, TableImpl>> tablesByIdFuture = tablesByIdVv.get(causalityToken);

        return assignmentsUpdatedVv.update(causalityToken, (token, e) -> {
            if (e != null) {
                return failedFuture(e);
            }

            return localPartsUpdateFuture.thenCompose(unused ->
                    tablesByIdFuture.thenComposeAsync(tablesById -> inBusyLock(busyLock, updateAssignmentsClosure), ioExecutor)
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

        CompletableFuture<Void> whenReplicaReady = allOf(
                ((Loza) raftMgr).raftNodeReadyFuture(replicaGrpId),
                table.pkIndexesReadyFuture()
        );

        replicaMgr.startReplica(
                replicaGrpId,
                whenReplicaReady,
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
                transactionStateResolver,
                partitionUpdateHandlers.storageUpdateHandler,
                new CatalogValidationSchemasSource(catalogService, schemaManager),
                localNode(),
                schemaSyncService,
                catalogService,
                placementDriver
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

        var tablesToStop = new HashMap<Integer, TableImpl>();

        tablesToStop.putAll(latestTablesById());
        tablesToStop.putAll(pendingTables);

        cleanUpTablesResources(tablesToStop);
    }

    @Override
    public void stop() throws Exception {
        assert beforeStopGuard.get() : "'stop' called before 'beforeNodeStop'";

        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        IgniteUtils.closeAllManually(
                lowWatermark,
                mvGc,
                () -> shutdownAndAwaitTermination(rebalanceScheduler, 10, TimeUnit.SECONDS),
                () -> shutdownAndAwaitTermination(ioExecutor, 10, TimeUnit.SECONDS),
                () -> shutdownAndAwaitTermination(txStateStoragePool, 10, TimeUnit.SECONDS),
                () -> shutdownAndAwaitTermination(txStateStorageScheduledPool, 10, TimeUnit.SECONDS),
                () -> shutdownAndAwaitTermination(scanRequestExecutor, 10, TimeUnit.SECONDS),
                () -> shutdownAndAwaitTermination(incomingSnapshotsExecutor, 10, TimeUnit.SECONDS)
        );
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

                stopping.add(table::beforeClose);

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
     * @param catalogVersion Catalog version on which the table was created.
     * @param tableDescriptor Catalog table descriptor.
     * @return Future that will be completed when local changes related to the table creation are applied.
     */
    private CompletableFuture<?> createTableLocally(long causalityToken, int catalogVersion, CatalogTableDescriptor tableDescriptor) {
        return inBusyLockAsync(busyLock, () -> {
            int tableId = tableDescriptor.id();
            int zoneId = tableDescriptor.zoneId();

            CatalogZoneDescriptor zoneDescriptor = getZoneDescriptor(tableDescriptor, catalogVersion);

            CompletableFuture<List<Set<Assignment>>> assignmentsFuture;

            // Check if the table already has assignments in the vault.
            // So, it means, that it is a recovery process and we should use the vault assignments instead of calculation for the new ones.
            // TODO: IGNITE-20210 Fix it
            if (partitionAssignments(vaultManager, tableId, 0) != null) {
                assignmentsFuture = completedFuture(tableAssignments(vaultManager, tableId, zoneDescriptor.partitions()));
            } else {
                assignmentsFuture = distributionZoneManager.dataNodes(causalityToken, zoneId)
                        .thenApply(dataNodes -> AffinityUtils.calculateAssignments(
                                dataNodes,
                                zoneDescriptor.partitions(),
                                zoneDescriptor.replicas()
                        ));
            }

            return createTableLocally(
                    causalityToken,
                    tableDescriptor,
                    zoneDescriptor,
                    assignmentsFuture,
                    catalogVersion
            ).whenComplete((v, e) -> {
                if (e == null) {
                    for (var listener : assignmentsChangeListeners) {
                        listener.accept(this);
                    }
                }
            }).thenCompose(ignored -> writeTableAssignmentsToMetastore(tableId, assignmentsFuture));
        });
    }

    /**
     * Creates local structures for a table.
     *
     * @param causalityToken Causality token.
     * @param tableDescriptor Catalog table descriptor.
     * @param zoneDescriptor Catalog distributed zone descriptor.
     * @param assignmentsFuture Future with assignments.
     * @param catalogVersion Catalog version on which the table was created.
     * @return Future that will be completed when local changes related to the table creation are applied.
     */
    private CompletableFuture<Void> createTableLocally(
            long causalityToken,
            CatalogTableDescriptor tableDescriptor,
            CatalogZoneDescriptor zoneDescriptor,
            CompletableFuture<List<Set<Assignment>>> assignmentsFuture,
            int catalogVersion
    ) {
        String tableName = tableDescriptor.name();
        int tableId = tableDescriptor.id();

        LOG.trace("Creating local table: name={}, id={}, token={}", tableDescriptor.name(), tableDescriptor.id(), causalityToken);

        MvTableStorage tableStorage = createTableStorage(tableDescriptor, zoneDescriptor);
        TxStateTableStorage txStateStorage = createTxStateTableStorage(tableDescriptor, zoneDescriptor);

        int partitions = zoneDescriptor.partitions();

        InternalTableImpl internalTable = new InternalTableImpl(
                tableName,
                tableId,
                new Int2ObjectOpenHashMap<>(partitions),
                partitions, clusterNodeResolver, txManager, tableStorage,
                txStateStorage, replicaSvc, clock, observableTimestampTracker, placementDriver);

        var table = new TableImpl(internalTable, lockMgr, schemaVersions);

        // TODO: IGNITE-19082 Need another way to wait for indexes
        table.addIndexesToWait(collectTableIndexIds(tableId, catalogVersion));

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

        CompletableFuture<?> createPartsFut = createTablePartitionsLocally(causalityToken, assignmentsFuture, table);

        pendingTables.put(tableId, table);
        startedTables.put(tableId, table);

        tablesById(causalityToken).thenAccept(ignored -> inBusyLock(busyLock, () -> {
            pendingTables.remove(tableId);
        }));

        tablesById(causalityToken).thenRun(() -> inBusyLock(busyLock, () -> completeApiCreateFuture(table)));

        // TODO should be reworked in IGNITE-16763

        // TODO: https://issues.apache.org/jira/browse/IGNITE-19913 Possible performance degradation.
        return createPartsFut.thenApply(ignore -> null);
    }

    /**
     * Creates data storage for the provided table.
     *
     * @param tableDescriptor Catalog table descriptor.
     * @param zoneDescriptor Catalog distributed zone descriptor.
     */
    protected MvTableStorage createTableStorage(CatalogTableDescriptor tableDescriptor, CatalogZoneDescriptor zoneDescriptor) {
        CatalogDataStorageDescriptor dataStorage = zoneDescriptor.dataStorage();

        StorageEngine engine = dataStorageMgr.engine(dataStorage.engine());

        assert engine != null : "tableId=" + tableDescriptor.id() + ", engine=" + dataStorage.engine();

        MvTableStorage tableStorage = engine.createMvTable(
                new StorageTableDescriptor(tableDescriptor.id(), zoneDescriptor.partitions(), dataStorage.dataRegion()),
                new StorageIndexDescriptorSupplier(catalogService)
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
     * Completes appropriate future to return result from API {@link TableManager#createTableLocally}.
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

            CompletableFuture<?>[] stopReplicaFutures = new CompletableFuture<?>[partitions];

            for (int partitionId = 0; partitionId < partitions; partitionId++) {
                var replicationGroupId = new TablePartitionId(tableId, partitionId);

                stopReplicaFutures[partitionId] = stopPartition(replicationGroupId, table);
            }

            // TODO: IGNITE-18703 Destroy raft log and meta
            CompletableFuture<Void> destroyTableStoragesFuture = allOf(stopReplicaFutures)
                    .thenCompose(unused -> allOf(
                            internalTable.storage().destroy(),
                            runAsync(() -> internalTable.txStateStorage().destroy(), ioExecutor))
                    );

            CompletableFuture<?> dropSchemaRegistryFuture = schemaManager.dropRegistry(causalityToken, table.tableId());

            return allOf(destroyTableStoragesFuture, dropSchemaRegistryFuture)
                    .thenApply(v -> map);
        }));

        startedTables.remove(tableId);

        Set<ByteArray> assignmentKeys = IntStream.range(0, partitions)
                .mapToObj(p -> stablePartAssignmentsKey(new TablePartitionId(tableId, p)))
                .collect(Collectors.toSet());

        metaStorageMgr.removeAll(assignmentKeys);
    }

    private CompletableFuture<Set<Assignment>> calculateAssignments(TablePartitionId tablePartitionId) {
        int catalogVersion = catalogService.latestCatalogVersion();

        CatalogTableDescriptor tableDescriptor = getTableDescriptor(tablePartitionId.tableId(), catalogVersion);

        CatalogZoneDescriptor zoneDescriptor = getZoneDescriptor(tableDescriptor, catalogVersion);

        return distributionZoneManager.dataNodes(zoneDescriptor.updateToken(), tableDescriptor.zoneId()).thenApply(dataNodes ->
                AffinityUtils.calculateAssignmentForPartition(
                        dataNodes,
                        tablePartitionId.partitionId(),
                        zoneDescriptor.replicas()
                ));
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
        HybridTimestamp now = clock.now();

        return orStopManagerFuture(schemaSyncService.waitForMetadataCompleteness(now))
                .thenComposeAsync(unused -> inBusyLockAsync(busyLock, () -> {
                    int catalogVersion = catalogService.activeCatalogVersion(now.longValue());

                    Collection<CatalogTableDescriptor> tableDescriptors = catalogService.tables(catalogVersion);

                    if (tableDescriptors.isEmpty()) {
                        return completedFuture(List.of());
                    }

                    CompletableFuture<TableImpl>[] tableImplFutures = tableDescriptors.stream()
                            .map(tableDescriptor -> tableAsyncInternalBusy(tableDescriptor.id()))
                            .toArray(CompletableFuture[]::new);

                    return allOf(tableImplFutures)
                            .thenApply(unused1 -> Stream.of(tableImplFutures).map(CompletableFuture::join).collect(toList()));
                }), ioExecutor);
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
        // We bring the future outside to avoid OutdatedTokenException.
        CompletableFuture<Map<Integer, TableImpl>> tablesByIdFuture = tablesByIdVv.get(causalityToken);

        return assignmentsUpdatedVv.get(causalityToken).thenCompose(v -> tablesByIdFuture);
    }

    /**
     * Returns the latest tables by ID map, for which all assignment updates have been completed.
     */
    private Map<Integer, TableImpl> latestTablesById() {
        if (assignmentsUpdatedVv.latestCausalityToken() < 0L) {
            // No tables at all in case of empty causality token.
            return emptyMap();
        } else {
            CompletableFuture<Map<Integer, TableImpl>> tablesByIdFuture = tablesByIdVv.get(tablesByIdVv.latestCausalityToken());

            assert tablesByIdFuture.isDone() : "'tablesByIdVv' is always completed strictly before the 'assignmentsUpdatedVv'";

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
                .thenApply(Function.identity());
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
            HybridTimestamp now = clock.now();

            return orStopManagerFuture(schemaSyncService.waitForMetadataCompleteness(now))
                    .thenComposeAsync(unused -> inBusyLockAsync(busyLock, () -> {
                        int catalogVersion = catalogService.activeCatalogVersion(now.longValue());

                        // Check if the table has been deleted.
                        if (catalogService.table(tableId, catalogVersion) == null) {
                            return completedFuture(null);
                        }

                        return tableAsyncInternalBusy(tableId);
                    }), ioExecutor);
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
            return localPartsByTableIdVv.get(causalityToken).thenApply(partitionSetById -> partitionSetById.get(tableId));
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
            HybridTimestamp now = clock.now();

            return orStopManagerFuture(schemaSyncService.waitForMetadataCompleteness(now))
                    .thenComposeAsync(unused -> inBusyLockAsync(busyLock, () -> {
                        CatalogTableDescriptor tableDescriptor = catalogService.table(name, now.longValue());

                        // Check if the table has been deleted.
                        if (tableDescriptor == null) {
                            return completedFuture(null);
                        }

                        return tableAsyncInternalBusy(tableDescriptor.id());
                    }));
        });
    }

    private CompletableFuture<TableViewInternal> tableAsyncInternalBusy(int tableId) {
        TableImpl tableImpl = latestTablesById().get(tableId);

        if (tableImpl != null) {
            return completedFuture(tableImpl);
        }

        CompletableFuture<TableViewInternal> getLatestTableFuture = new CompletableFuture<>();

        CompletionListener<Void> tablesListener = (token, v, th) -> {
            if (th == null) {
                CompletableFuture<Map<Integer, TableImpl>> tablesFuture = tablesByIdVv.get(token);

                tablesFuture.whenComplete((tables, e) -> {
                    if (e != null) {
                        getLatestTableFuture.completeExceptionally(e);
                    } else {
                        TableImpl table = tables.get(tableId);

                        if (table != null) {
                            getLatestTableFuture.complete(table);
                        }
                    }
                });
            } else {
                getLatestTableFuture.completeExceptionally(th);
            }
        };

        assignmentsUpdatedVv.whenComplete(tablesListener);

        // This check is needed for the case when we have registered tablesListener,
        // but tablesByIdVv has already been completed, so listener would be triggered only for the next versioned value update.
        tableImpl = latestTablesById().get(tableId);

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

                    return handleChangePendingAssignmentEvent(newEntry, evt.revision());
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

    private CompletableFuture<Void> handleChangePendingAssignmentEvent(Entry pendingAssignmentsEntry, long revision) {
        if (pendingAssignmentsEntry.value() == null) {
            return completedFuture(null);
        }

        int partId = extractPartitionNumber(pendingAssignmentsEntry.key());
        int tblId = extractTableId(pendingAssignmentsEntry.key(), PENDING_ASSIGNMENTS_PREFIX);

        var replicaGrpId = new TablePartitionId(tblId, partId);

        // Stable assignments from the meta store, which revision is bounded by the current pending event.
        CompletableFuture<Entry> stableAssignmentsFuture = metaStorageMgr.get(stablePartAssignmentsKey(replicaGrpId), revision);

        return tablesById(revision)
                .thenCombineAsync(stableAssignmentsFuture, (tables, stableAssignmentsEntry) -> {
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

                            return CompletableFuture.<Void>completedFuture(null);
                        }

                        return handleChangePendingAssignmentEvent(
                                replicaGrpId,
                                table,
                                pendingAssignmentsEntry,
                                stableAssignmentsEntry,
                                revision
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
            Entry stableAssignmentsEntry,
            long revision
    ) {
        ClusterNode localMember = localNode();

        int partId = replicaGrpId.partitionId();

        if (LOG.isInfoEnabled()) {
            var stringKey = new String(pendingAssignmentsEntry.key(), UTF_8);

            LOG.info("Received update on pending assignments. Check if new raft group should be started"
                            + " [key={}, partition={}, table={}, localMemberAddress={}]",
                    stringKey, partId, tbl.name(), localMember.address());
        }

        Set<Assignment> pendingAssignments = ByteUtils.fromBytes(pendingAssignmentsEntry.value());

        Set<Assignment> stableAssignments = ByteUtils.fromBytes(stableAssignmentsEntry.value());

        transactionStateResolver.updateAssignment(
                replicaGrpId,
                stableAssignments.stream().filter(Assignment::isPeer).map(Assignment::consistentId).collect(toList())
        );

        // Start a new Raft node and Replica if this node has appeared in the new assignments.
        boolean shouldStartLocalServices = pendingAssignments.stream()
                .filter(assignment -> localMember.name().equals(assignment.consistentId()))
                .anyMatch(assignment -> !stableAssignments.contains(assignment));

        CompletableFuture<Void> localServicesStartFuture;

        if (shouldStartLocalServices) {
            localServicesStartFuture = localPartsByTableIdVv.get(revision)
                    .thenComposeAsync(oldMap -> {
                        int tableId = tbl.tableId();

                        PartitionSet partitionSet = oldMap.get(tableId).copy();

                        return getOrCreatePartitionStorages(tbl, partitionSet).thenApply(u -> {
                            var newMap = new HashMap<>(oldMap);

                            newMap.put(tableId, partitionSet);

                            return newMap;
                        });
                    }, ioExecutor)
                    .thenRunAsync(() -> inBusyLock(busyLock, () -> {
                        InternalTable internalTable = tbl.internalTable();

                        var safeTimeTracker = new PendingComparableValuesTracker<HybridTimestamp, Void>(HybridTimestamp.MIN_VALUE);
                        var storageIndexTracker = new PendingComparableValuesTracker<Long, Void>(0L);

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

                        PeersAndLearners pendingConfiguration = configurationFromAssignments(pendingAssignments);

                        try {
                            Peer serverPeer = pendingConfiguration.peer(localMember.name());

                            RaftNodeId raftNodeId = new RaftNodeId(replicaGrpId, serverPeer);

                            if (!((Loza) raftMgr).isStarted(raftNodeId)) {
                                PeersAndLearners stableConfiguration = configurationFromAssignments(stableAssignments);

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
                            }

                            if (!replicaMgr.isReplicaStarted(replicaGrpId)) {
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
                            }
                        } catch (NodeStoppingException ignored) {
                            // No-op.
                        }
                    }), ioExecutor);
        } else {
            localServicesStartFuture = completedFuture(null);
        }

        return localServicesStartFuture
                .thenCompose(v -> {
                    RaftGroupService partGrpSvc = tbl.internalTable().partitionRaftGroupService(partId);

                    return partGrpSvc.refreshAndGetLeaderWithTerm()
                            .thenCompose(leaderWithTerm -> {
                                if (!isLocalPeer(leaderWithTerm.leader())) {
                                    return completedFuture(null);
                                }

                                // run update of raft configuration if this node is a leader
                                LOG.info("Current node={} is the leader of partition raft group={}. "
                                                + "Initiate rebalance process for partition={}, table={}",
                                        leaderWithTerm.leader(), replicaGrpId, partId, tbl.name());

                                return metaStorageMgr.get(pendingPartAssignmentsKey(replicaGrpId))
                                        .thenCompose(latestPendingAssignmentsEntry -> {
                                            // Do not change peers of the raft group if this is a stale event.
                                            // Note that we start raft node before for the sake of the consistency in a starting and
                                            // stopping raft nodes.
                                            if (revision < latestPendingAssignmentsEntry.revision()) {
                                                return completedFuture(null);
                                            }

                                            PeersAndLearners newConfiguration = configurationFromAssignments(pendingAssignments);

                                            return partGrpSvc.changePeersAsync(newConfiguration, leaderWithTerm.term());
                                        });
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
                txManager,
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

                    // It is safe to get the latest version of the catalog as we are in the metastore thread.
                    int catalogVersion = catalogService.latestCatalogVersion();

                    return tablesById(evt.revision())
                            .thenCompose(tables -> inBusyLockAsync(busyLock, () -> {
                                CatalogTableDescriptor tableDescriptor = getTableDescriptor(tableId, catalogVersion);

                                CatalogZoneDescriptor zoneDescriptor = getZoneDescriptor(tableDescriptor, catalogVersion);

                                return distributionZoneManager.dataNodes(zoneDescriptor.updateToken(), tableDescriptor.zoneId())
                                        .thenCompose(dataNodes -> RebalanceUtil.handleReduceChanged(
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
        return new PartitionMover(busyLock, () -> internalTable.partitionRaftGroupService(partId));
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
                    // Update raft client peers and learners according to the actual assignments.
                    CompletableFuture<Void> raftClientUpdateFuture = tablesById(evt.revision()).thenAccept(t -> {
                        t.get(tableId).internalTable()
                                .partitionRaftGroupService(tablePartitionId.partitionId())
                                .updateConfiguration(configurationFromAssignments(stableAssignments));
                    });

                    byte[] pendingAssignmentsFromMetaStorage = pendingAssignmentsEntry.value();

                    Set<Assignment> pendingAssignments = pendingAssignmentsFromMetaStorage == null
                            ? Set.of()
                            : ByteUtils.fromBytes(pendingAssignmentsFromMetaStorage);

                    String localMemberName = localNode().name();

                    boolean shouldStopLocalServices = Stream.concat(stableAssignments.stream(), pendingAssignments.stream())
                            .noneMatch(assignment -> assignment.consistentId().equals(localMemberName));

                    if (shouldStopLocalServices) {
                        return allOf(
                                raftClientUpdateFuture,
                                stopAndDestroyPartition(tablePartitionId, evt.revision()));
                    } else {
                        return raftClientUpdateFuture;
                    }
                }, ioExecutor);
    }

    private CompletableFuture<Void> stopAndDestroyPartition(TablePartitionId tablePartitionId, long causalityToken) {
        return tablesById(causalityToken)
                .thenCompose(tables -> {
                    TableImpl table = tables.get(tablePartitionId.tableId());

                    return stopPartition(tablePartitionId, table)
                            .thenCompose(v -> destroyPartitionStorages(tablePartitionId, table));
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
        // TODO: IGNITE-19905 - remove the check.
        if (table != null) {
            closePartitionTrackers(table.internalTable(), tablePartitionId.partitionId());
        }

        CompletableFuture<Boolean> stopReplicaFuture;

        try {
            stopReplicaFuture = replicaMgr.stopReplica(tablePartitionId);
        } catch (NodeStoppingException e) {
            // No-op.
            stopReplicaFuture = completedFuture(false);
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

    private CompletableFuture<Void> destroyPartitionStorages(TablePartitionId tablePartitionId, TableImpl table) {
        // TODO: IGNITE-18703 Destroy raft log and meta
        // TODO: IGNITE-19905 - remove the check.
        if (table == null) {
            return completedFuture(null);
        }

        InternalTable internalTable = table.internalTable();

        int partitionId = tablePartitionId.partitionId();

        return allOf(
                internalTable.storage().destroyPartition(partitionId),
                runAsync(() -> internalTable.txStateStorage().destroyTxStateStorage(partitionId), ioExecutor)
        );
    }

    private int[] collectTableIndexIds(int tableId, int catalogVersion) {
        return catalogService.indexes(catalogVersion, tableId).stream()
                .mapToInt(CatalogIndexDescriptor::id)
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

    private static PartitionUpdateHandlers createPartitionUpdateHandlers(
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
                indexUpdateHandler
        );

        return new PartitionUpdateHandlers(storageUpdateHandler, indexUpdateHandler, gcUpdateHandler);
    }

    /**
     * Returns a table instance if it exists, {@code null} otherwise.
     *
     * @param tableId Table id.
     */
    public @Nullable TableViewInternal getTable(int tableId) {
        return startedTables.get(tableId);
    }

    /**
     * Returns a table instance if it exists, {@code null} otherwise.
     *
     * @param name Table name.
     */
    @TestOnly
    public @Nullable TableViewInternal getTable(String name) {
        return findTableImplByName(startedTables.values(), name);
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

    private void startTables(long recoveryRevision) {
        int catalogVersion = catalogService.latestCatalogVersion();

        List<CompletableFuture<?>> startTableFutures = new ArrayList<>();

        // TODO: IGNITE-20384 Clean up abandoned resources for dropped zones from volt and metastore
        for (CatalogTableDescriptor tableDescriptor : catalogService.tables(catalogVersion)) {
            startTableFutures.add(createTableLocally(recoveryRevision, catalogVersion, tableDescriptor));
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
        return anyOf(future, stopManagerFuture).thenApply(o -> (T) o);
    }
}
