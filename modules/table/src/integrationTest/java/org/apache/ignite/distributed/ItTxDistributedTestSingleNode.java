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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_PARTITION_COUNT;
import static org.apache.ignite.utils.ClusterServiceTestUtils.findLocalAddresses;
import static org.apache.ignite.utils.ClusterServiceTestUtils.waitForTopology;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.ignite.internal.affinity.AffinityUtils;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftGroupEventsListener;
import org.apache.ignite.internal.raft.RaftGroupServiceImpl;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.schema.BinaryRowConverter;
import org.apache.ignite.internal.schema.ColumnsExtractor;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.configuration.GcConfiguration;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.storage.impl.TestMvTableStorage;
import org.apache.ignite.internal.storage.index.impl.TestHashIndexStorage;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.TxAbstractTest;
import org.apache.ignite.internal.table.distributed.HashIndexLocker;
import org.apache.ignite.internal.table.distributed.IndexLocker;
import org.apache.ignite.internal.table.distributed.LowWatermark;
import org.apache.ignite.internal.table.distributed.StorageUpdateHandler;
import org.apache.ignite.internal.table.distributed.TableMessageGroup;
import org.apache.ignite.internal.table.distributed.TableSchemaAwareIndexStorage;
import org.apache.ignite.internal.table.distributed.gc.GcUpdateHandler;
import org.apache.ignite.internal.table.distributed.index.IndexBuilder;
import org.apache.ignite.internal.table.distributed.index.IndexUpdateHandler;
import org.apache.ignite.internal.table.distributed.raft.PartitionDataStorage;
import org.apache.ignite.internal.table.distributed.raft.PartitionListener;
import org.apache.ignite.internal.table.distributed.replicator.PartitionReplicaListener;
import org.apache.ignite.internal.table.distributed.replicator.PlacementDriver;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.internal.table.impl.DummySchemas;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.IgniteTransactionsImpl;
import org.apache.ignite.internal.tx.impl.TransactionIdGenerator;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.message.TxMessageGroup;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateTableStorage;
import org.apache.ignite.internal.tx.storage.state.test.TestTxStateStorage;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.Lazy;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NodeFinder;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.apache.ignite.table.Table;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.apache.ignite.utils.ClusterServiceTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Distributed transaction test using a single partition table.
 */
@ExtendWith(ConfigurationExtension.class)
public class ItTxDistributedTestSingleNode extends TxAbstractTest {
    protected static final int ACC_TABLE_ID = 1;

    protected static final int CUST_TABLE_ID = 2;

    //TODO fsync can be turned on again after https://issues.apache.org/jira/browse/IGNITE-20195
    @InjectConfiguration("mock: { fsync: false }")
    private static RaftConfiguration raftConfiguration;

    @InjectConfiguration
    private static GcConfiguration gcConfig;

    @InjectConfiguration("mock.tables.foo {}")
    private static TablesConfiguration tablesConfig;

    private static final IgniteLogger LOG = Loggers.forClass(ItTxDistributedTestSingleNode.class);

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

    protected Int2ObjectOpenHashMap<RaftGroupService> accRaftClients;

    protected Int2ObjectOpenHashMap<RaftGroupService> custRaftClients;

    protected Map<String, TxStateStorage> txStateStorages;

    private Map<String, ClusterService> clusterServices;

    protected final List<ClusterService> cluster = new CopyOnWriteArrayList<>();

    private ScheduledThreadPoolExecutor executor;

    private final Function<String, ClusterNode> consistentIdToNode = consistentId -> {
        for (ClusterService service : cluster) {
            ClusterNode clusterNode = service.topologyService().localMember();

            if (clusterNode.name().equals(consistentId)) {
                return clusterNode;
            }
        }

        return null;
    };

    /**
     * Returns a count of nodes.
     *
     * @return Nodes.
     */
    protected int nodes() {
        return 1;
    }

    /**
     * Returns a count of replicas.
     *
     * @return Replicas.
     */
    protected int replicas() {
        return 1;
    }

    /**
     * Returns {@code true} to disable collocation by using dedicated client node.
     *
     * @return {@code true} to disable collocation.
     */
    protected boolean startClient() {
        return true;
    }

    private final TestInfo testInfo;

    /**
     * The constructor.
     *
     * @param testInfo Test info.
     */
    public ItTxDistributedTestSingleNode(TestInfo testInfo) {
        this.testInfo = testInfo;
    }

    /**
     * Initialize the test state.
     */
    @Override
    @BeforeEach
    public void before() throws Exception {
        int nodes = nodes();
        int replicas = replicas();

        assertTrue(nodes > 0);
        assertTrue(replicas > 0);

        List<NetworkAddress> localAddresses = findLocalAddresses(NODE_PORT_BASE,
                NODE_PORT_BASE + nodes);

        var nodeFinder = new StaticNodeFinder(localAddresses);

        clusterServices = new HashMap<>(nodes);

        nodeFinder.findNodes().parallelStream()
                .forEach(addr -> {
                    ClusterService svc = startNode(testInfo, addr.toString(), addr.port(), nodeFinder);
                    cluster.add(svc);
                    clusterServices.put(svc.topologyService().localMember().name(), svc);
                });

        for (ClusterService node : cluster) {
            assertTrue(waitForTopology(node, nodes, 1000));
        }

        log.info("The cluster has been started");

        if (startClient()) {
            client = startNode(testInfo, "client", NODE_PORT_BASE - 1, nodeFinder);

            assertTrue(waitForTopology(client, nodes + 1, 1000));

            clientClock = new HybridClockImpl();

            log.info("Replica manager has been started, node=[" + client.topologyService().localMember() + ']');

            clientReplicaSvc = new ReplicaService(
                    client.messagingService(),
                    clientClock
            );

            log.info("The client has been started");
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

        for (int i = 0; i < nodes; i++) {
            ClusterNode node = cluster.get(i).topologyService().localMember();

            HybridClock clock = new HybridClockImpl();

            clocks.put(node.name(), clock);

            var raftSrv = new Loza(
                    cluster.get(i),
                    raftConfiguration,
                    workDir.resolve("node" + i),
                    clock
            );

            raftSrv.start();

            raftServers.put(node.name(), raftSrv);

            var cmgManager = mock(ClusterManagementGroupManager.class);

            // This test is run without Meta storage.
            when(cmgManager.metaStorageNodes()).thenReturn(completedFuture(Set.of()));

            ReplicaManager replicaMgr = new ReplicaManager(
                    node.name(),
                    cluster.get(i),
                    cmgManager,
                    clock,
                    Set.of(TableMessageGroup.class, TxMessageGroup.class)
            );

            replicaMgr.start();

            replicaManagers.put(node.name(), replicaMgr);

            log.info("Replica manager has been started, node=[" + node + ']');

            ReplicaService replicaSvc = new ReplicaService(
                    cluster.get(i).messagingService(),
                    clock
            );

            replicaServices.put(node.name(), replicaSvc);

            TxManagerImpl txMgr = new TxManagerImpl(replicaSvc, new HeapLockManager(), clock, new TransactionIdGenerator(i), node::id);

            txMgr.start();

            txManagers.put(node.name(), txMgr);

            txStateStorages.put(node.name(), new TestTxStateStorage());
        }

        log.info("Raft servers have been started");

        final String accountsName = "accounts";
        final String customersName = "customers";

        accRaftClients = startTable(ACC_TABLE_ID, ACCOUNTS_SCHEMA);
        custRaftClients = startTable(CUST_TABLE_ID, CUSTOMERS_SCHEMA);

        log.info("Partition groups have been started");

        String localNodeName = accRaftClients.get(0).clusterService().topologyService().localMember().name();

        if (startClient()) {
            clientTxManager = new TxManagerImpl(clientReplicaSvc, new HeapLockManager(), clientClock, new TransactionIdGenerator(-1),
                    () -> accRaftClients.get(0).clusterService().topologyService().localMember().id());
        } else {
            // Collocated mode.
            clientTxManager = txManagers.get(localNodeName);
        }

        assertNotNull(clientTxManager);

        igniteTransactions = new IgniteTransactionsImpl(clientTxManager);

        this.accounts = new TableImpl(new InternalTableImpl(
                accountsName,
                ACC_TABLE_ID,
                accRaftClients,
                1,
                consistentIdToNode,
                clientTxManager,
                mock(MvTableStorage.class),
                mock(TxStateTableStorage.class),
                startClient() ? clientReplicaSvc : replicaServices.get(localNodeName),
                startClient() ? clientClock : clocks.get(localNodeName)
        ), new DummySchemaManagerImpl(ACCOUNTS_SCHEMA), clientTxManager.lockManager());

        this.customers = new TableImpl(new InternalTableImpl(
                customersName,
                CUST_TABLE_ID,
                custRaftClients,
                1,
                consistentIdToNode,
                clientTxManager,
                mock(MvTableStorage.class),
                mock(TxStateTableStorage.class),
                startClient() ? clientReplicaSvc : replicaServices.get(localNodeName),
                startClient() ? clientClock : clocks.get(localNodeName)
        ), new DummySchemaManagerImpl(CUSTOMERS_SCHEMA), clientTxManager.lockManager());

        log.info("Tables have been started");
    }

    /**
     * Starts a table.
     *
     * @param tblId Table id.
     * @param schemaDescriptor Schema descriptor.
     * @return Groups map.
     */
    private Int2ObjectOpenHashMap<RaftGroupService> startTable(int tblId, SchemaDescriptor schemaDescriptor) throws Exception {
        List<Set<Assignment>> calculatedAssignments = AffinityUtils.calculateAssignments(
                cluster.stream().map(node -> node.topologyService().localMember().name()).collect(toList()),
                1,
                replicas()
        );

        List<Set<String>> assignments = calculatedAssignments.stream()
                .map(a -> a.stream().map(Assignment::consistentId).collect(toSet()))
                .collect(toList());

        List<TablePartitionId> grpIds = IntStream.range(0, assignments.size())
                .mapToObj(i -> new TablePartitionId(tblId, i))
                .collect(toList());

        Int2ObjectOpenHashMap<RaftGroupService> clients = new Int2ObjectOpenHashMap<>();

        List<CompletableFuture<Void>> partitionReadyFutures = new ArrayList<>();

        int globalIndexId = 1;

        for (int p = 0; p < assignments.size(); p++) {
            Set<String> partAssignments = assignments.get(p);

            TablePartitionId grpId = grpIds.get(p);

            for (String assignment : partAssignments) {
                var mvTableStorage = new TestMvTableStorage(tblId, DEFAULT_PARTITION_COUNT);
                var mvPartStorage = new TestMvPartitionStorage(0);
                var txStateStorage = txStateStorages.get(assignment);
                var placementDriver = new PlacementDriver(replicaServices.get(assignment), consistentIdToNode);

                for (int part = 0; part < assignments.size(); part++) {
                    placementDriver.updateAssignment(grpIds.get(part), assignments.get(part));
                }

                int partId = p;

                int indexId = globalIndexId++;

                ColumnsExtractor row2Tuple = BinaryRowConverter.keyExtractor(schemaDescriptor);

                Lazy<TableSchemaAwareIndexStorage> pkStorage = new Lazy<>(() -> new TableSchemaAwareIndexStorage(
                        indexId,
                        new TestHashIndexStorage(partId, null),
                        row2Tuple
                ));

                IndexLocker pkLocker = new HashIndexLocker(indexId, true, txManagers.get(assignment).lockManager(), row2Tuple);

                PeersAndLearners configuration = PeersAndLearners.fromConsistentIds(partAssignments);

                PendingComparableValuesTracker<HybridTimestamp, Void> safeTime =
                        new PendingComparableValuesTracker<>(clocks.get(assignment).now());
                PendingComparableValuesTracker<Long, Void> storageIndexTracker = new PendingComparableValuesTracker<>(0L);

                PartitionDataStorage partitionDataStorage = new TestPartitionDataStorage(mvPartStorage);

                IndexUpdateHandler indexUpdateHandler = new IndexUpdateHandler(
                        DummyInternalTableImpl.createTableIndexStoragesSupplier(Map.of(pkStorage.get().id(), pkStorage.get()))
                );

                StorageUpdateHandler storageUpdateHandler = new StorageUpdateHandler(
                        partId,
                        partitionDataStorage,
                        gcConfig,
                        mock(LowWatermark.class),
                        indexUpdateHandler,
                        new GcUpdateHandler(partitionDataStorage, safeTime, indexUpdateHandler)
                );

                TopologyAwareRaftGroupServiceFactory topologyAwareRaftGroupServiceFactory = new TopologyAwareRaftGroupServiceFactory(
                        clusterServices.get(assignment),
                        logicalTopologyService(clusterServices.get(assignment)),
                        Loza.FACTORY,
                        new RaftGroupEventsClientListener()
                );

                TxManager txManager = txManagers.get(assignment);

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
                                replicaManagers.get(assignment).startReplica(
                                        new TablePartitionId(tblId, partId),
                                        completedFuture(null),
                                        new PartitionReplicaListener(
                                                mvPartStorage,
                                                raftSvc,
                                                txManagers.get(assignment),
                                                txManagers.get(assignment).lockManager(),
                                                Runnable::run,
                                                partId,
                                                tblId,
                                                () -> Map.of(pkLocker.id(), pkLocker),
                                                pkStorage,
                                                Map::of,
                                                clocks.get(assignment),
                                                safeTime,
                                                txStateStorage,
                                                placementDriver,
                                                storageUpdateHandler,
                                                new DummySchemas(schemaManager),
                                                consistentIdToNode.apply(assignment),
                                                mvTableStorage,
                                                mock(IndexBuilder.class),
                                                tablesConfig
                                        ),
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

            if (startClient()) {
                RaftGroupService service = RaftGroupServiceImpl
                        .start(grpId, client, FACTORY, raftConfiguration, membersConf, true, executor)
                        .get(5, TimeUnit.SECONDS);

                clients.put(p, service);
            } else {
                // Create temporary client to find a leader address.
                ClusterService tmpSvc = cluster.get(0);

                RaftGroupService service = RaftGroupServiceImpl
                        .start(grpId, tmpSvc, FACTORY, raftConfiguration, membersConf, true, executor)
                        .get(5, TimeUnit.SECONDS);

                Peer leader = service.leader();

                service.shutdown();

                ClusterService leaderSrv = cluster.stream()
                        .filter(cluster -> cluster.topologyService().localMember().name().equals(leader.consistentId()))
                        .findAny()
                        .orElseThrow();

                RaftGroupService leaderClusterSvc = RaftGroupServiceImpl
                        .start(grpId, leaderSrv, FACTORY, raftConfiguration, membersConf, true, executor)
                        .get(5, TimeUnit.SECONDS);

                clients.put(p, leaderClusterSvc);
            }
        }

        CompletableFuture.allOf(partitionReadyFutures.toArray(new CompletableFuture[0])).join();

        return clients;
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
     * @param svc The service.
     * @return Raft manager hosting a leader for group.
     */
    protected Loza getLeader(RaftGroupService svc) {
        Peer leader = svc.leader();

        assertNotNull(leader);

        return raftServers.get(leader.consistentId());
    }

    /**
     * Shutdowns all cluster nodes after each test.
     *
     * @throws Exception If failed.
     */
    @AfterEach
    public void after() throws Exception {
        cluster.parallelStream().map(c -> {
            c.stop();
            return null;
        }).forEach(o -> {
        });

        if (client != null) {
            client.stop();
        }

        if (executor != null) {
            IgniteUtils.shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);
        }

        if (raftServers != null) {
            for (Entry<String, Loza> entry : raftServers.entrySet()) {
                Loza rs = entry.getValue();

                ReplicaManager replicaMgr = replicaManagers.get(entry.getKey());

                for (ReplicationGroupId grp : replicaMgr.startedGroups()) {
                    replicaMgr.stopReplica(grp).join();
                }

                for (RaftNodeId nodeId : rs.localNodes()) {
                    rs.stopRaftNode(nodeId);
                }

                replicaMgr.stop();
                rs.stop();
            }
        }

        if (txManagers != null) {
            for (TxManager txMgr : txManagers.values()) {
                txMgr.stop();
            }
        }

        if (accRaftClients != null) {
            for (RaftGroupService svc : accRaftClients.values()) {
                svc.shutdown();
            }
        }

        if (custRaftClients != null) {
            for (RaftGroupService svc : custRaftClients.values()) {
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

    /** {@inheritDoc} */
    @Override
    protected TxManager clientTxManager() {
        return clientTxManager;
    }

    /** {@inheritDoc} */
    @Override
    protected TxManager txManager(Table t) {
        Int2ObjectOpenHashMap<RaftGroupService> clients = null;

        if (t == accounts) {
            clients = accRaftClients;
        } else if (t == customers) {
            clients = custRaftClients;
        } else {
            fail("Unknown table " + t.name());
        }

        TxManager manager = txManagers.get(clients.get(0).leader().consistentId());

        assertNotNull(manager);

        return manager;
    }

    /**
     * Check the storage of partition is the same across all nodes.
     * The checking is based on {@link MvPartitionStorage#lastAppliedIndex()} that is increased on all update storage operation.
     * TODO: IGNITE-18869 The method must be updated when a proper way to compare storages will be implemented.
     *
     * @param table The table.
     * @param partId Partition id.
     * @return True if {@link MvPartitionStorage#lastAppliedIndex()} is equivalent across all nodes, false otherwise.
     */
    @Override
    protected boolean assertPartitionsSame(TableImpl table, int partId) {
        long storageIdx = 0;

        for (Map.Entry<String, Loza> entry : raftServers.entrySet()) {
            Loza svc = entry.getValue();

            var server = (JraftServerImpl) svc.server();

            var groupId = new TablePartitionId(table.tableId(), partId);

            Peer serverPeer = server.localPeers(groupId).get(0);

            org.apache.ignite.raft.jraft.RaftGroupService grp = server.raftGroupService(new RaftNodeId(groupId, serverPeer));

            var fsm = (JraftServerImpl.DelegatingStateMachine) grp.getRaftNode().getOptions().getFsm();

            PartitionListener listener = (PartitionListener) fsm.getListener();

            MvPartitionStorage storage = listener.getMvStorage();

            if (storageIdx == 0) {
                storageIdx = storage.lastAppliedIndex();
            } else if (storageIdx != storage.lastAppliedIndex()) {
                return false;
            }
        }

        return true;
    }

    @Test
    public void testIgniteTransactionsAndReadTimestamp() {
        Transaction readWriteTx = igniteTransactions.begin();
        assertFalse(readWriteTx.isReadOnly());
        assertNull(((InternalTransaction) readWriteTx).readTimestamp());

        Transaction readOnlyTx = igniteTransactions.begin(new TransactionOptions().readOnly(true));
        assertTrue(readOnlyTx.isReadOnly());
        assertNotNull(((InternalTransaction) readOnlyTx).readTimestamp());

        readWriteTx.commit();

        Transaction readOnlyTx2 = igniteTransactions.begin(new TransactionOptions().readOnly(true));
        readOnlyTx2.rollback();
    }
}
