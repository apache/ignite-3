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

package org.apache.ignite.internal.replicaendpointmanager;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.apache.ignite.internal.utils.RebalanceUtil.updatePendingAssignmentsKeys;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.affinity.AffinityUtils;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.baseline.BaselineManager;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftManager;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.raft.storage.impl.LogStorageFactoryCreator;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.configuration.ExtendedTableConfiguration;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.PartitionMover;
import org.apache.ignite.internal.table.distributed.StorageUpdateHandler;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.TableMessagesFactory;
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
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateTableStorage;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.Lazy;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.internal.utils.RebalanceUtil;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.raft.jraft.storage.impl.VolatileRaftMetaStorage;


public class ReplicaEndpointManager implements IgniteComponent {
    private static final IgniteLogger LOG = Loggers.forClass(ReplicaEndpointManager.class);

    private static final TableMessagesFactory TABLE_MESSAGES_FACTORY = new TableMessagesFactory();

    private static final long QUERY_DATA_NODES_COUNT_TIMEOUT = TimeUnit.SECONDS.toMillis(3);

    private static final int REBALANCE_SCHEDULER_POOL_SIZE = Math.min(Runtime.getRuntime().availableProcessors() * 3, 20);

    private final ClusterService clusterService;

    private final RaftManager raftMgr;

    private final MetaStorageManager metaStorageMgr;

    private final TableManager tableManager;
    /**
     * Separate executor for IO operations like partition storage initialization or partition raft group meta data persisting.
     */
    private final ExecutorService ioExecutor;

    private final TablesConfiguration tablesCfg;

    private final HybridClock clock;

    private final ScheduledExecutorService rebalanceScheduler;

    private final OutgoingSnapshotsManager outgoingSnapshotsManager;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    private final ReplicaManager replicaMgr;

    /** Lock manager. */
    private final LockManager lockMgr;

    /** Replica service. */
    private final ReplicaService replicaSvc;

    /** Baseline manager. */
    private final BaselineManager baselineMgr;

    /** Transaction manager. */
    private final TxManager txManager;

    private final ExecutorService scanRequestExecutor;

    private final PlacementDriver placementDriver;

    private final SchemaManager schemaManager;

    private final Function<String, ClusterNode> clusterNodeResolver;

    private final LogStorageFactoryCreator volatileLogStorageFactoryCreator;

    private final ExecutorService incomingSnapshotsExecutor;


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
    public ReplicaEndpointManager(
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
            OutgoingSnapshotsManager outgoingSnapshotsManager,
            TableManager tableManager
    ) {
        this.tablesCfg = tablesCfg;
        this.clusterService = clusterService;
        this.raftMgr = raftMgr;
        this.baselineMgr = baselineMgr;
        this.replicaMgr = replicaMgr;
        this.lockMgr = lockMgr;
        this.replicaSvc = replicaSvc;
        this.txManager = txManager;
        this.metaStorageMgr = metaStorageMgr;
        this.schemaManager = schemaManager;
        this.volatileLogStorageFactoryCreator = volatileLogStorageFactoryCreator;
        this.clock = clock;
        this.outgoingSnapshotsManager = outgoingSnapshotsManager;
        this.tableManager = tableManager;

        clusterNodeResolver = topologyService::getByConsistentId;

        placementDriver = new PlacementDriver(replicaSvc, clusterNodeResolver);

        int cpus = Runtime.getRuntime().availableProcessors();

        scanRequestExecutor = Executors.newSingleThreadExecutor(
                NamedThreadFactory.create(nodeName, "scan-query-executor-", LOG));

        rebalanceScheduler = new ScheduledThreadPoolExecutor(REBALANCE_SCHEDULER_POOL_SIZE,
                NamedThreadFactory.create(nodeName, "rebalance-scheduler", LOG));

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
    }

    @Override
    public void start() {
        ((ExtendedTableConfiguration) tablesCfg.tables().any()).assignments().listen(this::onUpdateAssignments);
    }

    @Override
    public void stop() throws Exception {
        // TODO: sanpwc Cleaup table resources on stop.
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }
        // TODO: sanpwc Implement
//        metaStorageMgr.unregisterWatch(distributionZonesDataNodesListener);
//
//        metaStorageMgr.unregisterWatch(pendingAssignmentsRebalanceListener);
//        metaStorageMgr.unregisterWatch(stableAssignmentsRebalanceListener);
//        metaStorageMgr.unregisterWatch(assignmentsSwitchRebalanceListener);

        busyLock.block();

//        Map<UUID, TableImpl> tables = tableManager.tablesByIdVv.latest();
//
//        cleanUpTablesResources(tables);
//
//        cleanUpTablesResources(tablesToStopInCaseOfError);
//
//        tablesToStopInCaseOfError.clear();

        shutdownAndAwaitTermination(rebalanceScheduler, 10, TimeUnit.SECONDS);
        shutdownAndAwaitTermination(ioExecutor, 10, TimeUnit.SECONDS);
//        shutdownAndAwaitTermination(txStateStoragePool, 10, TimeUnit.SECONDS);
//        shutdownAndAwaitTermination(txStateStorageScheduledPool, 10, TimeUnit.SECONDS);
        shutdownAndAwaitTermination(scanRequestExecutor, 10, TimeUnit.SECONDS);
        shutdownAndAwaitTermination(incomingSnapshotsExecutor, 10, TimeUnit.SECONDS);
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

        tableManager.tableAsync(causalityToken, tblId).thenAccept(
                table -> {
                    for (int i = 0; i < partitions; i++) {
                        int partId = i;

                        Set<Assignment> oldPartAssignment = oldAssignments == null ? Set.of() : oldAssignments.get(partId);

                        Set<Assignment> newPartAssignment = newAssignments.get(partId);

                        InternalTable internalTbl = table.internalTable();

                        Assignment localMemberAssignment = newPartAssignment.stream()
                                .filter(a -> a.consistentId().equals(localMemberName))
                                .findAny()
                                .orElse(null);

                        PeersAndLearners newConfiguration = configurationFromAssignments(newPartAssignment);

                        TablePartitionId replicaGrpId = new TablePartitionId(tblId, partId);

                        // TODO: sanpwc Probably we don't need this here.
                        placementDriver.updateAssignment(replicaGrpId, newConfiguration.peers().stream().map(Peer::consistentId).collect(toList()));

                        // TODO: sanpwc Probably we don't need this here.
                        PendingComparableValuesTracker<HybridTimestamp> safeTime = new PendingComparableValuesTracker<>(clock.now());

                        CompletableFuture<PartitionStorages> partitionStoragesFut = getOrCreatePartitionStorages(table, partId);

                        CompletableFuture<PartitionDataStorage> partitionDataStorageFut = partitionStoragesFut
                                .thenApply(partitionStorages -> partitionDataStorage(partitionStorages.getMvPartitionStorage(),
                                        internalTbl, partId));

                        CompletableFuture<StorageUpdateHandler> storageUpdateHandlerFut = partitionDataStorageFut
                                .thenApply(storage -> new StorageUpdateHandler(partId, storage, table.indexStorageAdapters(partId)));

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
                                                        table
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

                        // TODO: sanpwc support in-memory storage, or better remove it.
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

                        futures[partId] = startGroupFut;
                    }
                }
        );

//        allOf(futures).join();
//                .thenAccept(table -> {

//        // Create new raft nodes according to new assignments.
//        tablesByIdVv.update(causalityToken, (tablesById, e) -> {
//            if (e != null) {
//                return failedFuture(e);
//            }
//
//
//
//            return allOf(futures).thenApply(unused -> tablesById);
//        }).join();
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
     * Creates or gets partition stores. If one of the storages has not completed the rebalance, then the storages are cleared.
     *
     * @param table Table.
     * @param partitionId Partition ID.
     * @return Future of creating or getting partition stores.
     */
    // TODO: IGNITE-18619 Maybe we should wait here to create indexes, if you add now, then the tests start to hang
    private CompletableFuture<PartitionStorages> getOrCreatePartitionStorages(TableImpl table, int partitionId) {
        return CompletableFuture
                .supplyAsync(() -> {
                    MvPartitionStorage mvPartitionStorage = table.internalTable().storage().getOrCreateMvPartition(partitionId);
                    TxStateStorage txStateStorage = table.internalTable().txStateStorage().getOrCreateTxStateStorage(partitionId);

                    if (mvPartitionStorage.persistedIndex() == MvPartitionStorage.REBALANCE_IN_PROGRESS
                            || txStateStorage.persistedIndex() == TxStateStorage.REBALANCE_IN_PROGRESS) {
                        return allOf(
                                table.internalTable().storage().clearPartition(partitionId),
                                txStateStorage.clear()
                        ).thenApply(unused -> new PartitionStorages(mvPartitionStorage, txStateStorage));
                    } else {
                        return completedFuture(new PartitionStorages(mvPartitionStorage, txStateStorage));
                    }
                }, ioExecutor)
                .thenCompose(Function.identity());
    }

    private PartitionDataStorage partitionDataStorage(MvPartitionStorage partitionStorage, InternalTable internalTbl, int partId) {
        return new SnapshotAwarePartitionDataStorage(
                partitionStorage,
                outgoingSnapshotsManager,
                partitionKey(internalTbl, partId)
        );
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
            TableImpl tableImpl
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
                        () -> tableImpl.indexStorageAdapters(partitionKey.partitionId()).get().values()
                ),
                incomingSnapshotsExecutor
        ));

        return raftGroupOptions;
    }

    private PartitionKey partitionKey(InternalTable internalTbl, int partId) {
        return new PartitionKey(internalTbl.tableId(), partId);
    }

    private PartitionMover createPartitionMover(InternalTable internalTable, int partId) {
        return new PartitionMover(busyLock, () -> internalTable.partitionRaftGroupService(partId));
    }

    private Set<Assignment> calculateAssignments(TableConfiguration tableCfg, int partNum) {
        return AffinityUtils.calculateAssignmentForPartition(
                baselineMgr.nodes().stream().map(ClusterNode::name).collect(toList()),
                partNum,
                tableCfg.value().replicas()
        );
    }

    private boolean isLocalPeer(Peer peer) {
        return peer.consistentId().equals(clusterService.topologyService().localMember().name());
    }
}
