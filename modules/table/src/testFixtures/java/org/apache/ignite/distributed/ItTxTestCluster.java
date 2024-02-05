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

package org.apache.ignite.distributed;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.network.utils.ClusterServiceTestUtils.findLocalAddresses;
import static org.apache.ignite.internal.network.utils.ClusterServiceTestUtils.waitForTopology;
import static org.apache.ignite.internal.replicator.ReplicaManager.DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CollectionUtils.first;
import static org.apache.ignite.internal.util.CompletableFutures.emptySetCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.apache.ignite.internal.affinity.AffinityUtils;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.NodeFinder;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.network.utils.ClusterServiceTestUtils;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.TestPlacementDriver;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftGroupEventsListener;
import org.apache.ignite.internal.raft.RaftGroupServiceImpl;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.schema.BinaryRowConverter;
import org.apache.ignite.internal.schema.ColumnsExtractor;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.configuration.StorageUpdateConfiguration;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor.StorageHashIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.impl.TestHashIndexStorage;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.HashIndexLocker;
import org.apache.ignite.internal.table.distributed.IndexLocker;
import org.apache.ignite.internal.table.distributed.StorageUpdateHandler;
import org.apache.ignite.internal.table.distributed.TableMessageGroup;
import org.apache.ignite.internal.table.distributed.TableSchemaAwareIndexStorage;
import org.apache.ignite.internal.table.distributed.index.IndexUpdateHandler;
import org.apache.ignite.internal.table.distributed.raft.PartitionDataStorage;
import org.apache.ignite.internal.table.distributed.raft.PartitionListener;
import org.apache.ignite.internal.table.distributed.replicator.PartitionReplicaListener;
import org.apache.ignite.internal.table.distributed.replicator.TransactionStateResolver;
import org.apache.ignite.internal.table.distributed.schema.AlwaysSyncedSchemaSyncService;
import org.apache.ignite.internal.table.distributed.schema.ConstantSchemaVersions;
import org.apache.ignite.internal.table.distributed.schema.SchemaSyncService;
import org.apache.ignite.internal.table.distributed.schema.ThreadLocalPartitionCommandsMarshaller;
import org.apache.ignite.internal.table.distributed.schema.ValidationSchemasSource;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.internal.table.impl.DummyValidationSchemasSource;
import org.apache.ignite.internal.thread.LogUncaughtExceptionHandler;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.thread.StripedThreadPoolExecutor;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.IgniteTransactionsImpl;
import org.apache.ignite.internal.tx.impl.TransactionIdGenerator;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.impl.TxMessageSender;
import org.apache.ignite.internal.tx.message.TxMessageGroup;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateTableStorage;
import org.apache.ignite.internal.tx.storage.state.test.TestTxStateStorage;
import org.apache.ignite.internal.tx.test.TestLocalRwTxCounter;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.Lazy;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterNodeResolver;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.tx.IgniteTransactions;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.TestInfo;

/**
 * Class that allows to mock a cluster for transaction tests' purposes.
 */
public class ItTxTestCluster {
    private static final int SCHEMA_VERSION = 1;

    private final List<NetworkAddress> localAddresses;

    private final NodeFinder nodeFinder;

    private final RaftConfiguration raftConfig;

    private final TransactionConfiguration txConfiguration;

    private final StorageUpdateConfiguration storageUpdateConfiguration;

    private final Path workDir;

    private final int nodes;

    private final int replicas;

    private final boolean startClient;

    private static final IgniteLogger LOG = Loggers.forClass(ItTxTestCluster.class);

    public static final int NODE_PORT_BASE = 20_000;

    private static final RaftMessagesFactory FACTORY = new RaftMessagesFactory();

    private ClusterService client;

    private HybridClock clientClock;

    private ReplicaService clientReplicaSvc;

    protected Map<String, HybridClock> clocks;

    protected Map<String, Loza> raftServers;

    protected Map<String, ReplicaManager> replicaManagers;

    protected Map<String, ReplicaService> replicaServices;

    protected Map<String, TxManager> txManagers;

    protected TxManager clientTxManager;

    protected TransactionStateResolver clientTxStateResolver;

    protected Map<String, List<RaftGroupService>> raftClients = new HashMap<>();

    protected Map<String, TxStateStorage> txStateStorages;

    private Map<String, ClusterService> clusterServices;

    protected final List<ClusterService> cluster = new CopyOnWriteArrayList<>();

    protected PlacementDriver placementDriver;

    private ScheduledThreadPoolExecutor executor;

    private StripedThreadPoolExecutor partitionOperationsExecutor;

    protected IgniteTransactions igniteTransactions;

    protected String localNodeName;

    private final ClusterNodeResolver nodeResolver = new ClusterNodeResolver() {

        @Override
        public @Nullable ClusterNode getById(String id) {
            for (ClusterService service : cluster) {
                ClusterNode clusterNode = service.topologyService().localMember();

                if (clusterNode.id().equals(id)) {
                    return clusterNode;
                }
            }

            if (client != null && client.topologyService().localMember().id().equals(id)) {
                return client.topologyService().localMember();
            }

            return null;
        }

        @Override
        public @Nullable ClusterNode getByConsistentId(String consistentId) {
            for (ClusterService service : cluster) {
                ClusterNode clusterNode = service.topologyService().localMember();

                if (clusterNode.name().equals(consistentId)) {
                    return clusterNode;
                }
            }

            return null;
        }
    };

    private final TestInfo testInfo;

    /** Observable timestamp tracker. */
    private final HybridTimestampTracker timestampTracker;

    /**
     * The constructor.
     */
    public ItTxTestCluster(
            TestInfo testInfo,
            RaftConfiguration raftConfig,
            TransactionConfiguration txConfiguration,
            StorageUpdateConfiguration storageUpdateConfiguration,
            Path workDir,
            int nodes,
            int replicas,
            boolean startClient,
            HybridTimestampTracker timestampTracker
    ) {
        this.raftConfig = raftConfig;
        this.txConfiguration = txConfiguration;
        this.storageUpdateConfiguration = storageUpdateConfiguration;
        this.workDir = workDir;
        this.nodes = nodes;
        this.replicas = replicas;
        this.startClient = startClient;
        this.testInfo = testInfo;
        this.timestampTracker = timestampTracker;

        localAddresses = findLocalAddresses(NODE_PORT_BASE, NODE_PORT_BASE + nodes);
        nodeFinder = new StaticNodeFinder(localAddresses);
    }

    /**
     * Initialize the test state.
     */
    protected void prepareCluster() throws Exception {
        assertTrue(nodes > 0);
        assertTrue(replicas > 0);

        clusterServices = new ConcurrentHashMap<>(nodes);

        nodeFinder.findNodes().parallelStream()
                .forEach(addr -> {
                    ClusterService svc = startNode(testInfo, addr.toString(), addr.port(), nodeFinder);
                    cluster.add(svc);
                    clusterServices.put(svc.topologyService().localMember().name(), svc);
                });

        for (ClusterService node : cluster) {
            assertTrue(waitForTopology(node, nodes, 1000));
        }

        ClusterNode firstNode = first(cluster).topologyService().localMember();

        placementDriver = new TestPlacementDriver(firstNode);

        LOG.info("The cluster has been started");

        if (startClient) {
            startClient();
        }

        // Start raft servers. Each raft server can hold multiple groups.
        clocks = new HashMap<>(nodes);
        raftServers = new HashMap<>(nodes);
        replicaManagers = new HashMap<>(nodes);
        replicaServices = new HashMap<>(nodes);
        txManagers = new HashMap<>(nodes);
        txStateStorages = new HashMap<>(nodes);

        executor = new ScheduledThreadPoolExecutor(20,
                new NamedThreadFactory(Loza.CLIENT_POOL_NAME, LOG));

        partitionOperationsExecutor = new StripedThreadPoolExecutor(
                20,
                NamedThreadFactory.threadPrefix("test", "partition-operations"),
                new LogUncaughtExceptionHandler(LOG),
                false,
                0
        );

        for (int i = 0; i < nodes; i++) {
            ClusterNode node = cluster.get(i).topologyService().localMember();

            HybridClock clock = new HybridClockImpl();

            clocks.put(node.name(), clock);

            var raftSrv = new Loza(
                    cluster.get(i),
                    raftConfig,
                    workDir.resolve("node" + i),
                    clock
            );

            raftSrv.start();

            raftServers.put(node.name(), raftSrv);

            var cmgManager = mock(ClusterManagementGroupManager.class);

            // This test is run without Meta storage.
            when(cmgManager.metaStorageNodes()).thenReturn(emptySetCompletedFuture());

            ReplicaManager replicaMgr = new ReplicaManager(
                    node.name(),
                    cluster.get(i),
                    cmgManager,
                    clock,
                    Set.of(TableMessageGroup.class, TxMessageGroup.class),
                    placementDriver,
                    partitionOperationsExecutor
            );

            replicaMgr.start();

            replicaManagers.put(node.name(), replicaMgr);

            LOG.info("Replica manager has been started, node=[" + node + ']');

            ReplicaService replicaSvc = spy(new ReplicaService(
                    cluster.get(i).messagingService(),
                    clock
            ));

            replicaServices.put(node.name(), replicaSvc);

            TxManagerImpl txMgr = newTxManager(
                    cluster.get(i),
                    replicaSvc,
                    clock,
                    new TransactionIdGenerator(i),
                    node,
                    placementDriver
            );

            txMgr.start();

            txManagers.put(node.name(), txMgr);

            txStateStorages.put(node.name(), new TestTxStateStorage());
        }

        LOG.info("Raft servers have been started");

        LOG.info("Partition groups have been started");

        localNodeName = cluster.get(0).topologyService().localMember().name();

        if (startClient) {
            initializeClientTxComponents();
        } else {
            // Collocated mode.
            clientTxManager = txManagers.get(localNodeName);
        }

        igniteTransactions = new IgniteTransactionsImpl(clientTxManager, timestampTracker);

        assertNotNull(clientTxManager);
    }

    protected TxManagerImpl newTxManager(
            ClusterService clusterService,
            ReplicaService replicaSvc,
            HybridClock clock,
            TransactionIdGenerator generator,
            ClusterNode node,
            PlacementDriver placementDriver
    ) {
        return new TxManagerImpl(
                txConfiguration,
                clusterService,
                replicaSvc,
                new HeapLockManager(),
                clock,
                generator,
                placementDriver,
                () -> DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS,
                new TestLocalRwTxCounter()
        );
    }

    public IgniteTransactions igniteTransactions() {
        return igniteTransactions;
    }

    /**
     * Starts a table.
     *
     * @param tableName Table name.
     * @param tableId Table id.
     * @param schemaDescriptor Schema descriptor.
     * @return Groups map.
     */
    public TableViewInternal startTable(String tableName, int tableId, SchemaDescriptor schemaDescriptor) throws Exception {
        CatalogService catalogService = mock(CatalogService.class);

        CatalogTableDescriptor tableDescriptor = mock(CatalogTableDescriptor.class);
        when(tableDescriptor.tableVersion()).thenReturn(SCHEMA_VERSION);

        lenient().when(catalogService.table(anyInt(), anyLong())).thenReturn(tableDescriptor);

        List<Set<Assignment>> calculatedAssignments = AffinityUtils.calculateAssignments(
                cluster.stream().map(node -> node.topologyService().localMember().name()).collect(toList()),
                1,
                replicas
        );

        List<Set<String>> assignments = calculatedAssignments.stream()
                .map(a -> a.stream().map(Assignment::consistentId).collect(toSet()))
                .collect(toList());

        List<TablePartitionId> grpIds = IntStream.range(0, assignments.size())
                .mapToObj(i -> new TablePartitionId(tableId, i))
                .collect(toList());

        Int2ObjectOpenHashMap<RaftGroupService> clients = new Int2ObjectOpenHashMap<>();

        List<CompletableFuture<Void>> partitionReadyFutures = new ArrayList<>();

        int globalIndexId = 1;

        ThreadLocalPartitionCommandsMarshaller commandsMarshaller =
                new ThreadLocalPartitionCommandsMarshaller(cluster.get(0).serializationRegistry());

        for (int p = 0; p < assignments.size(); p++) {
            Set<String> partAssignments = assignments.get(p);

            TablePartitionId grpId = grpIds.get(p);

            for (String assignment : partAssignments) {
                int partId = p;

                var mvPartStorage = new TestMvPartitionStorage(partId);
                var txStateStorage = txStateStorages.get(assignment);
                TxMessageSender txMessageSender =
                        new TxMessageSender(
                                clusterServices.get(assignment).messagingService(),
                                replicaServices.get(assignment),
                                clocks.get(assignment)
                        );

                var transactionStateResolver = new TransactionStateResolver(
                        txManagers.get(assignment),
                        clocks.get(assignment),
                        nodeResolver,
                        clusterServices.get(assignment).messagingService(),
                        placementDriver,
                        txMessageSender
                );
                transactionStateResolver.start();

                int indexId = globalIndexId++;

                ColumnsExtractor row2Tuple = BinaryRowConverter.keyExtractor(schemaDescriptor);

                StorageHashIndexDescriptor pkIndexDescriptor = mock(StorageHashIndexDescriptor.class);

                when(pkIndexDescriptor.columns()).then(invocation -> Collections.nCopies(
                        schemaDescriptor.keyColumns().columns().length,
                        mock(StorageHashIndexColumnDescriptor.class)
                ));

                Lazy<TableSchemaAwareIndexStorage> pkStorage = new Lazy<>(() -> new TableSchemaAwareIndexStorage(
                        indexId,
                        new TestHashIndexStorage(partId, pkIndexDescriptor),
                        row2Tuple
                ));

                IndexLocker pkLocker = new HashIndexLocker(indexId, true, txManagers.get(assignment).lockManager(), row2Tuple);

                PeersAndLearners configuration = PeersAndLearners.fromConsistentIds(partAssignments);

                PendingComparableValuesTracker<HybridTimestamp, Void> safeTime =
                        new PendingComparableValuesTracker<>(clocks.get(assignment).now());
                PendingComparableValuesTracker<Long, Void> storageIndexTracker = new PendingComparableValuesTracker<>(0L);

                PartitionDataStorage partitionDataStorage = new TestPartitionDataStorage(tableId, partId, mvPartStorage);

                IndexUpdateHandler indexUpdateHandler = new IndexUpdateHandler(
                        DummyInternalTableImpl.createTableIndexStoragesSupplier(Map.of(pkStorage.get().id(), pkStorage.get()))
                );

                StorageUpdateHandler storageUpdateHandler = new StorageUpdateHandler(
                        partId,
                        partitionDataStorage,
                        indexUpdateHandler,
                        storageUpdateConfiguration
                );

                TopologyAwareRaftGroupServiceFactory topologyAwareRaftGroupServiceFactory = new TopologyAwareRaftGroupServiceFactory(
                        clusterServices.get(assignment),
                        logicalTopologyService(clusterServices.get(assignment)),
                        Loza.FACTORY,
                        new RaftGroupEventsClientListener()
                );

                PartitionListener partitionListener = new PartitionListener(
                        txManagers.get(assignment),
                        partitionDataStorage,
                        storageUpdateHandler,
                        txStateStorage,
                        safeTime,
                        storageIndexTracker
                );

                CompletableFuture<Void> partitionReadyFuture = raftServers.get(assignment).startRaftGroupNode(
                        new RaftNodeId(grpId, configuration.peer(assignment)),
                        configuration,
                        partitionListener,
                        RaftGroupEventsListener.noopLsnr,
                        topologyAwareRaftGroupServiceFactory
                ).thenAccept(
                        raftSvc -> {
                            try {
                                DummySchemaManagerImpl schemaManager = new DummySchemaManagerImpl(schemaDescriptor);

                                PartitionReplicaListener listener = newReplicaListener(
                                        mvPartStorage,
                                        raftSvc,
                                        txManagers.get(assignment),
                                        Runnable::run,
                                        partId,
                                        tableId,
                                        () -> Map.of(pkLocker.id(), pkLocker),
                                        pkStorage,
                                        Map::of,
                                        clocks.get(assignment),
                                        safeTime,
                                        txStateStorage,
                                        transactionStateResolver,
                                        storageUpdateHandler,
                                        new DummyValidationSchemasSource(schemaManager),
                                        nodeResolver.getByConsistentId(assignment),
                                        new AlwaysSyncedSchemaSyncService(),
                                        catalogService,
                                        placementDriver,
                                        nodeResolver
                                );

                                replicaManagers.get(assignment).startReplica(
                                        new TablePartitionId(tableId, partId),
                                        nullCompletedFuture(),
                                        listener,
                                        raftSvc,
                                        storageIndexTracker
                                );
                            } catch (NodeStoppingException e) {
                                fail("Unexpected node stopping", e);
                            }
                        }
                );

                partitionReadyFutures.add(partitionReadyFuture);
            }

            PeersAndLearners membersConf = PeersAndLearners.fromConsistentIds(partAssignments);

            if (startClient) {
                RaftGroupService service = RaftGroupServiceImpl
                        .start(grpId, client, FACTORY, raftConfig, membersConf, true, executor, commandsMarshaller)
                        .get(5, TimeUnit.SECONDS);

                clients.put(p, service);
            } else {
                // Create temporary client to find a leader address.
                ClusterService tmpSvc = cluster.get(0);

                RaftGroupService service = RaftGroupServiceImpl
                        .start(grpId, tmpSvc, FACTORY, raftConfig, membersConf, true, executor, commandsMarshaller)
                        .get(5, TimeUnit.SECONDS);

                Peer leader = service.leader();

                service.shutdown();

                ClusterService leaderSrv = cluster.stream()
                        .filter(cluster -> cluster.topologyService().localMember().name().equals(leader.consistentId()))
                        .findAny()
                        .orElseThrow();

                RaftGroupService leaderClusterSvc = RaftGroupServiceImpl
                        .start(grpId, leaderSrv, FACTORY, raftConfig, membersConf, true, executor, commandsMarshaller)
                        .get(5, TimeUnit.SECONDS);

                clients.put(p, leaderClusterSvc);
            }
        }

        allOf(partitionReadyFutures.toArray(new CompletableFuture[0])).join();

        raftClients.computeIfAbsent(tableName, t -> new ArrayList<>()).addAll(clients.values());

        return new TableImpl(
                new InternalTableImpl(
                        tableName,
                        tableId,
                        clients,
                        1,
                        nodeResolver,
                        clientTxManager,
                        mock(MvTableStorage.class),
                        mock(TxStateTableStorage.class),
                        startClient ? clientReplicaSvc : replicaServices.get(localNodeName),
                        startClient ? clientClock : clocks.get(localNodeName),
                        timestampTracker,
                        placementDriver
                ),
                new DummySchemaManagerImpl(schemaDescriptor),
                clientTxManager.lockManager(),
                new ConstantSchemaVersions(SCHEMA_VERSION),
                mock(IgniteSql.class)
        );
    }

    protected PartitionReplicaListener newReplicaListener(
            MvPartitionStorage mvDataStorage,
            RaftGroupService raftClient,
            TxManager txManager,
            Executor scanRequestExecutor,
            int partId,
            int tableId,
            Supplier<Map<Integer, IndexLocker>> indexesLockers,
            Lazy<TableSchemaAwareIndexStorage> pkIndexStorage,
            Supplier<Map<Integer, TableSchemaAwareIndexStorage>> secondaryIndexStorages,
            HybridClock hybridClock,
            PendingComparableValuesTracker<HybridTimestamp, Void> safeTime,
            TxStateStorage txStateStorage,
            TransactionStateResolver transactionStateResolver,
            StorageUpdateHandler storageUpdateHandler,
            ValidationSchemasSource validationSchemasSource,
            ClusterNode localNode,
            SchemaSyncService schemaSyncService,
            CatalogService catalogService,
            PlacementDriver placementDriver,
            ClusterNodeResolver clusterNodeResolver
    ) {
        return new PartitionReplicaListener(
                mvDataStorage,
                raftClient,
                txManager,
                txManager.lockManager(),
                Runnable::run,
                partId,
                tableId,
                indexesLockers,
                pkIndexStorage,
                secondaryIndexStorages,
                hybridClock,
                safeTime,
                txStateStorage,
                transactionStateResolver,
                storageUpdateHandler,
                validationSchemasSource,
                localNode,
                schemaSyncService,
                catalogService,
                placementDriver,
                clusterNodeResolver
        );
    }

    private LogicalTopologyService logicalTopologyService(ClusterService clusterService) {
        return new LogicalTopologyService() {
            @Override
            public void addEventListener(LogicalTopologyEventListener listener) {

            }

            @Override
            public void removeEventListener(LogicalTopologyEventListener listener) {

            }

            @Override
            public CompletableFuture<LogicalTopologySnapshot> logicalTopologyOnLeader() {
                return completedFuture(new LogicalTopologySnapshot(
                        1,
                        clusterService.topologyService().allMembers().stream().map(LogicalNode::new).collect(toSet())));
            }

            @Override
            public CompletableFuture<Set<ClusterNode>> validatedNodesOnLeader() {
                return completedFuture(Set.copyOf(clusterService.topologyService().allMembers()));
            }
        };
    }

    /**
     * Returns a raft manager for a group.
     *
     * @param tableName Table name.
     * @return Raft manager hosting a leader for group.
     */
    protected Loza getLeader(String tableName) {
        var services = raftClients.get(tableName);

        Peer leader = services.get(0).leader();

        assertNotNull(leader);

        return raftServers.get(leader.consistentId());
    }

    protected Peer getLeaderId(String tableName) {
        var services = raftClients.get(tableName);

        return services.get(0).leader();
    }

    /**
     * Shutdowns all cluster nodes after each test.
     *
     * @throws Exception If failed.
     */
    public void shutdownCluster() throws Exception {
        cluster.parallelStream().forEach(ClusterService::stop);

        if (client != null) {
            client.stop();
        }

        if (executor != null) {
            IgniteUtils.shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);
        }

        if (partitionOperationsExecutor != null) {
            IgniteUtils.shutdownAndAwaitTermination(partitionOperationsExecutor, 10, TimeUnit.SECONDS);
        }

        if (raftServers != null) {
            for (Entry<String, Loza> entry : raftServers.entrySet()) {
                Loza rs = entry.getValue();

                ReplicaManager replicaMgr = replicaManagers.get(entry.getKey());

                CompletableFuture<?>[] replicaStopFutures = replicaMgr.startedGroups().stream()
                        .map(grp -> {
                            try {
                                return replicaMgr.stopReplica(grp);
                            } catch (NodeStoppingException e) {
                                throw new AssertionError(e);
                            }
                        })
                        .toArray(CompletableFuture[]::new);

                assertThat(allOf(replicaStopFutures), willCompleteSuccessfully());

                rs.localNodes().parallelStream().forEach(nodeId -> {
                    try {
                        rs.stopRaftNode(nodeId);
                    } catch (NodeStoppingException e) {
                        throw new AssertionError(e);
                    }
                });

                replicaMgr.stop();
                rs.stop();
            }
        }

        if (txManagers != null) {
            for (TxManager txMgr : txManagers.values()) {
                txMgr.stop();
            }
        }

        if (clientTxManager != null) {
            clientTxManager.stop();
        }

        for (Map.Entry<String, List<RaftGroupService>> e : raftClients.entrySet()) {
            for (RaftGroupService svc : e.getValue()) {
                svc.shutdown();
            }
        }
    }

    /**
     * Starts a node.
     *
     * @param name Node name.
     * @param port Local port.
     * @param nodeFinder Node finder.
     * @return The client cluster view.
     */
    protected static ClusterService startNode(TestInfo testInfo, String name, int port,
            NodeFinder nodeFinder) {
        var network = ClusterServiceTestUtils.clusterService(testInfo, port, nodeFinder);

        network.start();

        return network;
    }

    private void startClient() throws InterruptedException {
        client = startNode(testInfo, "client", NODE_PORT_BASE - 1, nodeFinder);

        assertTrue(waitForTopology(client, nodes + 1, 1000));

        clientClock = new HybridClockImpl();

        LOG.info("Replica manager has been started, node=[" + client.topologyService().localMember() + ']');

        clientReplicaSvc = spy(new ReplicaService(
                client.messagingService(),
                clientClock
        ));

        LOG.info("The client has been started");
    }

    private void initializeClientTxComponents() {
        clientTxManager = new TxManagerImpl(
                txConfiguration,
                client,
                clientReplicaSvc,
                new HeapLockManager(),
                clientClock,
                new TransactionIdGenerator(-1),
                placementDriver,
                () -> DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS,
                new TestLocalRwTxCounter()
        );

        clientTxStateResolver = new TransactionStateResolver(
                clientTxManager,
                clientClock,
                nodeResolver,
                client.messagingService(),
                placementDriver,
                new TxMessageSender(client.messagingService(), clientReplicaSvc, clientClock)
        );

        clientTxStateResolver.start();
        clientTxManager.start();
    }
}
