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

package org.apache.ignite.internal.table;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ignite.internal.affinity.RendezvousAffinityFunction;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.server.impl.JRaftServerImpl;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.storage.basic.ConcurrentHashMapStorage;
import org.apache.ignite.internal.table.distributed.raft.PartitionListener;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.table.distributed.storage.VersionedRowStore;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.IgniteTransactionsImpl;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.ClusterServiceFactory;
import org.apache.ignite.network.LocalPortRangeNodeFinder;
import org.apache.ignite.network.MessageSerializationRegistryImpl;
import org.apache.ignite.network.NodeFinder;
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
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.apache.ignite.raft.jraft.test.TestUtils.waitForTopology;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;


@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
/** TODO asch parallelize startup */
public class TxDistributedTest_1_1_1 extends TxAbstractTest {
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
    private ArrayList<ClusterService> cluster = new ArrayList<>();

    /**
     *
     */
    @WorkDirectory
    private Path dataPath;

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
     * Initialize the test state.
     */
    @Override @BeforeEach
    public void before() throws Exception {
        int nodes = nodes();
        int replicas = replicas();

        assertTrue(nodes > 0);
        assertTrue(replicas > 0);

        var nodeFinder = new LocalPortRangeNodeFinder(NODE_PORT_BASE, NODE_PORT_BASE + nodes);

        nodeFinder.findNodes().stream()
            .map(addr -> startNode(addr.toString(), addr.port(), nodeFinder))
            .forEach(cluster::add);

        for (ClusterService node : cluster)
            assertTrue(waitForTopology(node, nodes, 1000));

        log.info("The cluster has been started");

        client = startNode("client", NODE_PORT_BASE + nodes, nodeFinder);

        assertTrue(waitForTopology(client, nodes + 1, 1000));

        log.info("The client has been started");

        // Start raft servers. Each raft server can hold multiple groups.
        raftServers = new HashMap<>(nodes);

        for (int i = 0; i < nodes; i++) {
            TxManagerImpl txManager = new TxManagerImpl(cluster.get(i), new HeapLockManager());

            var raftSrv = new JRaftServerImpl(cluster.get(i), txManager, dataPath);

            raftSrv.start();

            raftServers.put(cluster.get(i).topologyService().localMember(), raftSrv);
        }

        log.info("Raft servers have been started");

        final String accountsName = "accounts";
        final String customersName = "customers";

        accRaftClients = startTable(accountsName);
        custRaftClients = startTable(customersName);

        log.info("Partition groups have been started");

        TxManagerImpl nearTxManager = new TxManagerImpl(client, new HeapLockManager());

        igniteTransactions = new IgniteTransactionsImpl(nearTxManager);

        this.accounts = new TableImpl(new InternalTableImpl(
            accountsName,
            tableId,
            accRaftClients,
            1,
            nearTxManager
        ), new SchemaRegistry() {
            @Override public SchemaDescriptor schema() {
                return ACCOUNTS_SCHEMA;
            }

            @Override public int lastSchemaVersion() {
                return ACCOUNTS_SCHEMA.version();
            }

            @Override public SchemaDescriptor schema(int ver) {
                return ACCOUNTS_SCHEMA;
            }

            @Override public Row resolve(BinaryRow row) {
                return new Row(ACCOUNTS_SCHEMA, row);
            }
        }, null, null);

        this.customers = new TableImpl(new InternalTableImpl(
            customersName,
            tableId2,
            custRaftClients,
            1,
            nearTxManager
        ), new SchemaRegistry() {
            @Override public SchemaDescriptor schema() {
                return CUSTOMERS_SCHEMA;
            }

            @Override public int lastSchemaVersion() {
                return CUSTOMERS_SCHEMA.version();
            }

            @Override public SchemaDescriptor schema(int ver) {
                return CUSTOMERS_SCHEMA;
            }

            @Override public Row resolve(BinaryRow row) {
                return new Row(CUSTOMERS_SCHEMA, row);
            }
        }, null, null);

        log.info("Tables have been started");
    }

    /**
     * @param name The name.
     * @return Groups map.
     * @throws Exception
     */
    private Map<Integer, RaftGroupService> startTable(String name) throws Exception {
        List<List<ClusterNode>> assignment = RendezvousAffinityFunction.assignPartitions(
            cluster.stream().map(node -> node.topologyService().localMember()).collect(Collectors.toList()),
            1,
            replicas(),
            false,
            null
        );

        Map<Integer, RaftGroupService> clients = new HashMap<>();

        for (int p = 0; p < assignment.size(); p++) {
            List<ClusterNode> partNodes = assignment.get(p);

            String grpId = name + "-part-" + p;

            List<Peer> conf = partNodes.stream().map(n -> n.address()).map(Peer::new).collect(Collectors.toList());

            for (ClusterNode node : partNodes) {
                RaftServer rs = raftServers.get(node);
                assertNotNull(rs.transactionManager());

                rs.startRaftGroup(
                    grpId,
                    new PartitionListener(UUID.randomUUID(), new VersionedRowStore(
                        new ConcurrentHashMapStorage(),
                        rs.transactionManager())),
                    conf
                );
            }

            RaftGroupService service = RaftGroupServiceImpl.start(grpId, client, FACTORY, 100_000, conf, true, 200)
                .get(5, TimeUnit.SECONDS);

            clients.put(p, service);
        }

        return clients;
    }

    /**
     * Shutdowns all cluster nodes after each test.
     *
     * @throws Exception If failed.
     */
    @AfterEach
    public void after() throws Exception {
        for (RaftServer rs : raftServers.values())
            rs.stop();

        for (RaftGroupService svc : accRaftClients.values())
            svc.shutdown();

        for (RaftGroupService svc : custRaftClients.values())
            svc.shutdown();

        for (ClusterService node : cluster)
            node.stop();

        client.stop();
    }

    /**
     * @param name       Node name.
     * @param port       Local port.
     * @param nodeFinder Node finder.
     * @return The client cluster view.
     */
    private static ClusterService startNode(String name, int port, NodeFinder nodeFinder) {
        var network = ClusterServiceTestUtils.clusterService(
            name,
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
    @Override protected TxManager txManager(Table t) {
        Map<Integer, RaftGroupService> clients = null;

        if (t == accounts)
            clients = accRaftClients;
        else if (t == customers)
            clients = custRaftClients;
        else
            fail("Unknown table " + t.tableName());

        RaftGroupService svc = clients.get(0);

        Peer leader = svc.leader();

        assertNotNull(leader);

        RaftServer leaderSrv = raftServers.get(client.topologyService().getByAddress(leader.address()));

        return leaderSrv.transactionManager();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected boolean assertPartitionsSame(Table t, int partId) {
        int hash = 0;

        for (Map.Entry<ClusterNode, RaftServer> entry : raftServers.entrySet()) {
            JRaftServerImpl svc = (JRaftServerImpl) entry.getValue();
            org.apache.ignite.raft.jraft.RaftGroupService grp = svc.raftGroupService(t.tableName() + "-part-" + partId);
            JRaftServerImpl.DelegatingStateMachine fsm = (JRaftServerImpl.DelegatingStateMachine) grp.getRaftNode().getOptions().getFsm();
            PartitionListener listener = (PartitionListener) fsm.getListener();
            VersionedRowStore storage = listener.getStorage();

            if (hash == 0)
                hash = storage.delegate().hashCode();
            else if (hash != storage.delegate().hashCode())
                return false;
        }

        return true;
    }
}
