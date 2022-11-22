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

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.raft.jraft.test.TestUtils.waitForTopology;
import static org.apache.ignite.utils.ClusterServiceTestUtils.findLocalAddresses;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.ignite.internal.affinity.AffinityUtils;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.RaftGroupServiceImpl;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.storage.index.impl.TestHashIndexStorage;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.TxAbstractTest;
import org.apache.ignite.internal.table.distributed.HashIndexLocker;
import org.apache.ignite.internal.table.distributed.IndexLocker;
import org.apache.ignite.internal.table.distributed.TableMessageGroup;
import org.apache.ignite.internal.table.distributed.TableSchemaAwareIndexStorage;
import org.apache.ignite.internal.table.distributed.raft.PartitionListener;
import org.apache.ignite.internal.table.distributed.replicator.PartitionReplicaListener;
import org.apache.ignite.internal.table.distributed.replicator.PlacementDriver;
import org.apache.ignite.internal.table.distributed.replicator.TablePartitionId;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.IgniteTransactionsImpl;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.message.TxMessageGroup;
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
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.table.Table;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.utils.ClusterServiceTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

/**
 * Distributed transaction test using a single partition table.
 */
@ExtendWith(ConfigurationExtension.class)
public class ItTxDistributedTestSingleNode extends TxAbstractTest {
    @InjectConfiguration
    private static RaftConfiguration raftConfiguration;

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

    protected List<ClusterService> cluster = new CopyOnWriteArrayList<>();

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

        nodeFinder.findNodes().parallelStream()
                .map(addr -> startNode(testInfo, addr.toString(), addr.port(), nodeFinder))
                .forEach(cluster::add);

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
                    clock,
                    new PendingComparableValuesTracker<>(clock.now())
            );

            raftSrv.start();

            raftServers.put(node.name(), raftSrv);

            ReplicaManager replicaMgr = new ReplicaManager(
                    cluster.get(i),
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

            TxManagerImpl txMgr = new TxManagerImpl(replicaSvc, new HeapLockManager(), clock);

            txMgr.start();

            txManagers.put(node.name(), txMgr);
        }

        log.info("Raft servers have been started");

        final String accountsName = "accounts";
        final String customersName = "customers";

        UUID accTblId = UUID.randomUUID();
        UUID custTblId = UUID.randomUUID();

        accRaftClients = startTable(accountsName, accTblId);
        custRaftClients = startTable(customersName, custTblId);

        log.info("Partition groups have been started");

        String localNodeName = accRaftClients.get(0).clusterService().topologyService().localMember().name();

        TxManager txMgr;

        if (startClient()) {
            txMgr = new TxManagerImpl(clientReplicaSvc, new HeapLockManager(), clientClock);
        } else {
            // Collocated mode.
            txMgr = txManagers.get(localNodeName);
        }

        assertNotNull(txMgr);

        igniteTransactions = new IgniteTransactionsImpl(txMgr);

        this.accounts = new TableImpl(new InternalTableImpl(
                accountsName,
                accTblId,
                accRaftClients,
                1,
                consistentIdToNode,
                txMgr,
                Mockito.mock(MvTableStorage.class),
                Mockito.mock(TxStateTableStorage.class),
                startClient() ? clientReplicaSvc : replicaServices.get(localNodeName),
                startClient() ? clientClock : clocks.get(localNodeName)
        ), new DummySchemaManagerImpl(ACCOUNTS_SCHEMA), txMgr.lockManager());

        this.customers = new TableImpl(new InternalTableImpl(
                customersName,
                custTblId,
                custRaftClients,
                1,
                consistentIdToNode,
                txMgr,
                Mockito.mock(MvTableStorage.class),
                Mockito.mock(TxStateTableStorage.class),
                startClient() ? clientReplicaSvc : replicaServices.get(localNodeName),
                startClient() ? clientClock : clocks.get(localNodeName)
        ), new DummySchemaManagerImpl(CUSTOMERS_SCHEMA), txMgr.lockManager());

        log.info("Tables have been started");
    }

    /**
     * Starts a table.
     *
     * @param name The name.
     * @param tblId Table id.
     * @return Groups map.
     */
    protected Int2ObjectOpenHashMap<RaftGroupService> startTable(String name, UUID tblId)
            throws Exception {
        List<Set<Assignment>> assignments = AffinityUtils.calculateAssignments(
                cluster.stream().map(node -> node.topologyService().localMember()).collect(toList()),
                1,
                replicas()
        );

        Int2ObjectOpenHashMap<RaftGroupService> clients = new Int2ObjectOpenHashMap<>();

        List<CompletableFuture<Void>> partitionReadyFutures = new ArrayList<>();

        for (int p = 0; p < assignments.size(); p++) {
            Set<Assignment> partAssignments = assignments.get(p);

            TablePartitionId grpId = new TablePartitionId(tblId, p);

            List<Peer> conf = partAssignments.stream().map(a -> new Peer(a.consistentId())).collect(toList());

            for (Assignment assignment : partAssignments) {
                String nodeName = assignment.consistentId();

                var testMpPartStorage = new TestMvPartitionStorage(0);
                var txStateStorage = new TestTxStateStorage();
                var placementDriver = new PlacementDriver(replicaServices.get(nodeName), consistentIdToNode);

                for (int part = 0; part < assignments.size(); part++) {
                    ReplicationGroupId replicaGrpId = new TablePartitionId(tblId, part);

                    List<String> replicaAssignment = assignments.get(part).stream().map(Assignment::consistentId).collect(toList());

                    placementDriver.updateAssignment(replicaGrpId, replicaAssignment);
                }

                int partId = p;

                UUID indexId = UUID.randomUUID();

                BinaryTupleSchema pkSchema = BinaryTupleSchema.create(new Element[]{
                        new Element(NativeTypes.BYTES, false)
                });

                Function<BinaryRow, BinaryTuple> row2tuple =
                        tableRow -> new BinaryTuple(pkSchema, tableRow.keySlice());

                Lazy<TableSchemaAwareIndexStorage> pkStorage = new Lazy<>(() -> new TableSchemaAwareIndexStorage(
                        indexId,
                        new TestHashIndexStorage(null),
                        row2tuple
                ));

                IndexLocker pkLocker = new HashIndexLocker(indexId, true, txManagers.get(nodeName).lockManager(), row2tuple);

                CompletableFuture<Void> partitionReadyFuture = raftServers.get(nodeName).prepareRaftGroup(
                        grpId,
                        partAssignments.stream().map(Assignment::consistentId).collect(toList()),
                        () -> new PartitionListener(
                                new TestPartitionDataStorage(testMpPartStorage),
                                new TestTxStateStorage(),
                                txManagers.get(nodeName),
                                () -> Map.of(pkStorage.get().id(), pkStorage.get()),
                                partId
                        ),
                        RaftGroupOptions.defaults()
                ).thenAccept(
                        raftSvc -> {
                            try {
                                PendingComparableValuesTracker<HybridTimestamp> safeTime =
                                        new PendingComparableValuesTracker<>(clocks.get(nodeName).now());

                                replicaManagers.get(nodeName).startReplica(
                                        new TablePartitionId(tblId, partId),
                                        new PartitionReplicaListener(
                                                testMpPartStorage,
                                                raftSvc,
                                                txManagers.get(nodeName),
                                                txManagers.get(nodeName).lockManager(),
                                                Runnable::run,
                                                partId,
                                                tblId,
                                                () -> Map.of(pkLocker.id(), pkLocker),
                                                pkStorage,
                                                () -> Map.of(),
                                                clocks.get(nodeName),
                                                safeTime,
                                                txStateStorage,
                                                placementDriver,
                                                peer -> nodeName.equals(peer.consistentId())
                                        )
                                );
                            } catch (NodeStoppingException e) {
                                fail("Unexpected node stopping", e);
                            }
                        }
                );

                partitionReadyFutures.add(partitionReadyFuture);
            }

            if (startClient()) {
                RaftGroupService service = RaftGroupServiceImpl
                        .start(grpId, client, FACTORY, 10_000, conf, true, 200, executor)
                        .get(5, TimeUnit.SECONDS);

                clients.put(p, service);
            } else {
                // Create temporary client to find a leader address.
                ClusterService tmpSvc = raftServers.values().stream().findFirst().get().service();

                RaftGroupService service = RaftGroupServiceImpl
                        .start(grpId, tmpSvc, FACTORY, 10_000, conf, true, 200, executor)
                        .get(5, TimeUnit.SECONDS);

                Peer leader = service.leader();

                service.shutdown();

                Loza leaderSrv = raftServers.get(leader.consistentId());

                RaftGroupService leaderClusterSvc = RaftGroupServiceImpl
                        .start(grpId, leaderSrv.service(), FACTORY,
                                10_000, conf, true, 200, executor).get(5, TimeUnit.SECONDS);

                clients.put(p, leaderClusterSvc);
            }
        }

        CompletableFuture.allOf(partitionReadyFutures.toArray(new CompletableFuture[0])).join();

        return clients;
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

        IgniteUtils.shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);

        for (Entry<String, Loza> entry : raftServers.entrySet()) {
            Loza rs = entry.getValue();

            ReplicaManager replicaMgr = replicaManagers.get(entry.getKey());

            Set<ReplicationGroupId> replicaGrps = replicaMgr.startedGroups();

            for (ReplicationGroupId grp : replicaGrps) {
                replicaMgr.stopReplica(grp);
            }

            Set<ReplicationGroupId> grps = rs.startedGroups();

            for (ReplicationGroupId grp : grps) {
                rs.stopRaftGroup(grp);
            }

            replicaMgr.stop();
            rs.stop();
        }

        for (TxManager txMgr : txManagers.values()) {
            txMgr.stop();
        }

        for (RaftGroupService svc : accRaftClients.values()) {
            svc.shutdown();
        }

        for (RaftGroupService svc : custRaftClients.values()) {
            svc.shutdown();
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

    /** {@inheritDoc} */
    @Override
    protected boolean assertPartitionsSame(TableImpl table, int partId) {
        int hash = 0;

        for (Map.Entry<String, Loza> entry : raftServers.entrySet()) {
            Loza svc = entry.getValue();
            JraftServerImpl server = (JraftServerImpl) svc.server();
            org.apache.ignite.raft.jraft.RaftGroupService grp = server.raftGroupService(new TablePartitionId(table.tableId(), partId));
            JraftServerImpl.DelegatingStateMachine fsm = (JraftServerImpl.DelegatingStateMachine) grp
                    .getRaftNode().getOptions().getFsm();
            PartitionListener listener = (PartitionListener) fsm.getListener();
            MvPartitionStorage storage = listener.getMvStorage();

            if (hash == 0) {
                hash = storage.hashCode();
            } else if (hash != storage.hashCode()) {
                return false;
            }
        }

        return true;
    }

    @Test
    public void testIgniteTransactionsAndReadTimestamp() {
        Transaction readWriteTx = igniteTransactions.begin();
        assertFalse(readWriteTx.isReadOnly());
        assertNull(readWriteTx.readTimestamp());

        Transaction readOnlyTx = igniteTransactions.readOnly().begin();
        assertTrue(readOnlyTx.isReadOnly());
        assertNotNull(readOnlyTx.readTimestamp());

        readWriteTx.commit();

        Transaction readOnlyTx2 = igniteTransactions.readOnly().begin();
        readOnlyTx2.rollback();
    }
}
