/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ignite.internal.affinity.RendezvousAffinityFunction;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.storage.basic.ConcurrentHashMapPartitionStorage;
import org.apache.ignite.internal.storage.engine.TableStorage;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.TxAbstractTest;
import org.apache.ignite.internal.table.distributed.raft.PartitionListener;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.table.distributed.storage.VersionedRowStore;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.IgniteTransactionsImpl;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.lang.IgniteUuidGenerator;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.ClusterServiceFactory;
import org.apache.ignite.network.MessageSerializationRegistryImpl;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NodeFinder;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.network.scalecube.TestScaleCubeClusterServiceFactory;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupServiceImpl;
import org.apache.ignite.table.Table;
import org.apache.ignite.utils.ClusterServiceTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.mockito.Mockito;

import static org.apache.ignite.raft.jraft.test.TestUtils.waitForTopology;
import static org.apache.ignite.utils.ClusterServiceTestUtils.findLocalAddresses;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 *
 */
public class ItTxDistributedTest_1_1_1 extends TxAbstractTest {
    /**
     * Base network port.
     */
    public static final int NODE_PORT_BASE = 20_000;
    
    /**
     * Factory.
     */
    private static final RaftMessagesFactory FACTORY = new RaftMessagesFactory();
    
    /**
     * Network factory.
     */
    private static final ClusterServiceFactory NETWORK_FACTORY = new TestScaleCubeClusterServiceFactory();
    
    /**
     *
     */
    private static final MessageSerializationRegistry SERIALIZATION_REGISTRY = new MessageSerializationRegistryImpl();
    
    /**
     *
     */
    private ClusterService client;
    
    /**
     *
     */
    protected Map<ClusterNode, RaftServer> raftServers;
    
    /**
     *
     */
    protected Map<Integer, RaftGroupService> accRaftClients;
    
    /**
     *
     */
    protected Map<Integer, RaftGroupService> custRaftClients;
    
    /**
     * Cluster.
     */
    protected List<ClusterService> cluster = new CopyOnWriteArrayList<>();
    
    /**
     *
     */
    private ScheduledThreadPoolExecutor executor;
    
    /**
     * @return Nodes.
     */
    protected int nodes() {
        return 1;
    }
    
    /**
     * @return Replicas.
     */
    protected int replicas() {
        return 1;
    }
    
    /**
     * @return {@code True} to disable collocation.
     */
    protected boolean startClient() {
        return true;
    }
    
    /**
     *
     */
    private final TestInfo testInfo;
    
    /**
     * @param testInfo Test info.
     */
    public ItTxDistributedTest_1_1_1(TestInfo testInfo) {
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
            
            log.info("The client has been started");
        }
        
        // Start raft servers. Each raft server can hold multiple groups.
        raftServers = new HashMap<>(nodes);
        
        executor = new ScheduledThreadPoolExecutor(20,
                new NamedThreadFactory(Loza.CLIENT_POOL_NAME));
        
        for (int i = 0; i < nodes; i++) {
            TxManagerImpl txManager = new TxManagerImpl(cluster.get(i), new HeapLockManager());
            
            var raftSrv = new JraftServerImpl(cluster.get(i), txManager, workDir);
            
            raftSrv.start();
            
            raftServers.put(cluster.get(i).topologyService().localMember(), raftSrv);
        }
        
        log.info("Raft servers have been started");
        
        final String accountsName = "accounts";
        final String customersName = "customers";
        
        IgniteUuid accTblId = new IgniteUuidGenerator(UUID.randomUUID(), 0).randomUuid();
        IgniteUuid custTblId = new IgniteUuidGenerator(UUID.randomUUID(), 0).randomUuid();
        
        accRaftClients = startTable(accountsName, accTblId);
        custRaftClients = startTable(customersName, custTblId);
        
        log.info("Partition groups have been started");
        
        TxManagerImpl nearTxManager;
    
        if (startClient()) {
            nearTxManager = new TxManagerImpl(client, new HeapLockManager());
        } else {
            nearTxManager = (TxManagerImpl) getLeader(accRaftClients.get(0)).transactionManager();
        }
        
        assertNotNull(nearTxManager);
        
        igniteTransactions = new IgniteTransactionsImpl(nearTxManager);
        
        this.accounts = new TableImpl(new InternalTableImpl(
                accountsName,
                accTblId,
                accRaftClients,
                1,
                NetworkAddress::toString,
                nearTxManager,
                Mockito.mock(TableStorage.class)
        ), new SchemaRegistry() {
            @Override
            public SchemaDescriptor schema() {
                return ACCOUNTS_SCHEMA;
            }
            
            @Override
            public int lastSchemaVersion() {
                return ACCOUNTS_SCHEMA.version();
            }
            
            @Override
            public SchemaDescriptor schema(int ver) {
                return ACCOUNTS_SCHEMA;
            }
            
            @Override
            public Row resolve(BinaryRow row) {
                return new Row(ACCOUNTS_SCHEMA, row);
            }
        }, null);
        
        this.customers = new TableImpl(new InternalTableImpl(
                customersName,
                custTblId,
                custRaftClients,
                1,
                NetworkAddress::toString,
                nearTxManager,
                Mockito.mock(TableStorage.class)
        ), new SchemaRegistry() {
            @Override
            public SchemaDescriptor schema() {
                return CUSTOMERS_SCHEMA;
            }
            
            @Override
            public int lastSchemaVersion() {
                return CUSTOMERS_SCHEMA.version();
            }
            
            @Override
            public SchemaDescriptor schema(int ver) {
                return CUSTOMERS_SCHEMA;
            }
            
            @Override
            public Row resolve(BinaryRow row) {
                return new Row(CUSTOMERS_SCHEMA, row);
            }
        }, null);
        
        log.info("Tables have been started");
    }
    
    /**
     * @param name The name.
     * @return Groups map.
     */
    protected Map<Integer, RaftGroupService> startTable(String name, IgniteUuid tblId)
            throws Exception {
        List<List<ClusterNode>> assignment = RendezvousAffinityFunction.assignPartitions(
                cluster.stream().map(node -> node.topologyService().localMember())
                        .collect(Collectors.toList()),
                1,
                replicas(),
                false,
                null
        );
        
        Map<Integer, RaftGroupService> clients = new HashMap<>();
        
        for (int p = 0; p < assignment.size(); p++) {
            List<ClusterNode> partNodes = assignment.get(p);
            
            String grpId = name + "-part-" + p;
            
            List<Peer> conf = partNodes.stream().map(n -> n.address()).map(Peer::new)
                    .collect(Collectors.toList());
            
            for (ClusterNode node : partNodes) {
                RaftServer rs = raftServers.get(node);
                assertNotNull(rs.transactionManager());
                
                rs.startRaftGroup(
                        grpId,
                        new PartitionListener(tblId,
                                new VersionedRowStore(new ConcurrentHashMapPartitionStorage(),
                                        rs.transactionManager())),
                        conf
                );
            }
            
            if (startClient()) {
                RaftGroupService service = RaftGroupServiceImpl
                        .start(grpId, client, FACTORY, 10_000, conf, true, 200, executor)
                        .get(5, TimeUnit.SECONDS);
                
                clients.put(p, service);
            } else {
                // Create temporary client to find a leader address.
                ClusterService tmpSvc = raftServers.values().stream().findFirst().get()
                        .clusterService();
                
                RaftGroupService service = RaftGroupServiceImpl
                        .start(grpId, tmpSvc, FACTORY, 10_000, conf, true, 200, executor)
                        .get(5, TimeUnit.SECONDS);
                
                Peer leader = service.leader();
                
                service.shutdown();
                
                RaftServer leaderSrv = raftServers
                        .get(tmpSvc.topologyService().getByAddress(leader.address()));
                
                RaftGroupService leaderClusterSvc = RaftGroupServiceImpl
                        .start(grpId, leaderSrv.clusterService(), FACTORY,
                                10_000, conf, true, 200, executor).get(5, TimeUnit.SECONDS);
                
                clients.put(p, leaderClusterSvc);
            }
        }
        
        return clients;
    }
    
    /**
     * @param svc The service.
     * @return Raft server hosting leader for group.
     */
    protected RaftServer getLeader(RaftGroupService svc) {
        Peer leader = svc.leader();
        
        assertNotNull(leader);
        
        return raftServers
                .get(svc.clusterService().topologyService().getByAddress(leader.address()));
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
    
        for (RaftServer rs : raftServers.values()) {
            rs.stop();
        }
    
        for (RaftGroupService svc : accRaftClients.values()) {
            svc.shutdown();
        }
    
        for (RaftGroupService svc : custRaftClients.values()) {
            svc.shutdown();
        }
    }
    
    /**
     * @param name Node name.
     * @param port Local port.
     * @param nodeFinder Node finder.
     * @return The client cluster view.
     */
    protected static ClusterService startNode(TestInfo testInfo, String name, int port,
            NodeFinder nodeFinder) {
        var network = ClusterServiceTestUtils.clusterService(
                testInfo,
                port,
                nodeFinder,
                SERIALIZATION_REGISTRY,
                NETWORK_FACTORY
        );
        
        network.start();
        
        return network;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected TxManager txManager(Table t) {
        Map<Integer, RaftGroupService> clients = null;
    
        if (t == accounts) {
            clients = accRaftClients;
        } else if (t == customers) {
            clients = custRaftClients;
        } else {
            fail("Unknown table " + t.tableName());
        }
        
        TxManager manager = getLeader(clients.get(0)).transactionManager();
        
        assertNotNull(manager);
        
        return manager;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean assertPartitionsSame(Table t, int partId) {
        int hash = 0;
        
        for (Map.Entry<ClusterNode, RaftServer> entry : raftServers.entrySet()) {
            JraftServerImpl svc = (JraftServerImpl) entry.getValue();
            org.apache.ignite.raft.jraft.RaftGroupService grp = svc
                    .raftGroupService(t.tableName() + "-part-" + partId);
            JraftServerImpl.DelegatingStateMachine fsm = (JraftServerImpl.DelegatingStateMachine) grp
                    .getRaftNode().getOptions().getFsm();
            PartitionListener listener = (PartitionListener) fsm.getListener();
            VersionedRowStore storage = listener.getStorage();
    
            if (hash == 0) {
                hash = storage.delegate().hashCode();
            } else if (hash != storage.delegate().hashCode()) {
                return false;
            }
        }
        
        return true;
    }
}
