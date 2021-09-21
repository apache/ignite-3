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

import java.nio.ByteBuffer;
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
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.storage.basic.ConcurrentHashMapStorage;
import org.apache.ignite.internal.storage.rocksdb.RocksDbStorage;
import org.apache.ignite.internal.table.distributed.raft.PartitionListener;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.table.distributed.storage.VersionedRowStore;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
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

/** */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class TxDistributedTest extends TxAbstractTest {
    /** Base network port. */
    public static final int NODE_PORT_BASE = 20_000;

    /** Nodes. */
    private int nodes = 1;

    /** Partitions. */
    public int parts = 1;

    /** Replicas. */
    public int replicas = 1;

    /** Factory. */
    private static final RaftMessagesFactory FACTORY = new RaftMessagesFactory();

    /** Network factory. */
    private static final ClusterServiceFactory NETWORK_FACTORY = new TestScaleCubeClusterServiceFactory();

    /** */
    private static final MessageSerializationRegistry SERIALIZATION_REGISTRY = new MessageSerializationRegistryImpl();

    /** */
    private ClusterService client;

    /** */
    private Map<ClusterNode, RaftServer> raftServers;

    /** */
    private Map<Integer, RaftGroupService> raftClients;

    /** Cluster. */
    private ArrayList<ClusterService> cluster = new ArrayList<>();

    /** */
    @WorkDirectory
    private Path dataPath;

    /**
     * Initialize the test state.
     */
    @Override @BeforeEach
    public void before() throws Exception {
        assertTrue(nodes > 0);
        assertTrue(parts > 0);

        var nodeFinder = new LocalPortRangeNodeFinder(NODE_PORT_BASE, NODE_PORT_BASE + nodes);

        nodeFinder.findNodes().stream()
            .map(addr -> startClient(addr.toString(), addr.port(), nodeFinder))
            .forEach(cluster::add);

        for (ClusterService node : cluster)
            assertTrue(waitForTopology(node, nodes, 1000));

        log.info("Cluster started.");

        client = startClient("client", NODE_PORT_BASE + nodes, nodeFinder);

        assertTrue(waitForTopology(client, nodes + 1, 1000));

        log.info("Client started.");

        raftServers = new HashMap<>(nodes);

        for (int i = 0; i < nodes; i++) {
            TxManagerImpl txManager = new TxManagerImpl(cluster.get(i), new HeapLockManager());

            var raftSrv = new JRaftServerImpl(cluster.get(i), txManager, dataPath);

            raftSrv.start();

            raftServers.put(cluster.get(i).topologyService().localMember(), raftSrv);
        }

        List<List<ClusterNode>> assignment = RendezvousAffinityFunction.assignPartitions(
            cluster.stream().map(node -> node.topologyService().localMember()).collect(Collectors.toList()),
            parts,
            replicas,
            false,
            null
        );

        int p = 0;

        Map<Integer, RaftGroupService> raftClients = new HashMap<>();

        for (List<ClusterNode> partNodes : assignment) {
            String grpId = "part-" + p;

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

            RaftGroupService service = RaftGroupServiceImpl.start(grpId, client, FACTORY, 10_000, conf, true, 200)
                .get(5, TimeUnit.SECONDS);

            raftClients.put(p, service);

            p++;
        }

        // Originator's tx manager.
        TxManager nearMgr = new TxManagerImpl(client, new HeapLockManager());

        SchemaDescriptor schema = new SchemaDescriptor(
            tableId,
            1,
            new Column[]{new Column("accountNumber", NativeTypes.INT64, false)},
            new Column[]{new Column("balance", NativeTypes.DOUBLE, false)}
        );

        accounts = new TableImpl(new InternalTableImpl(
            "tbl",
            UUID.randomUUID(),
            raftClients,
            parts,
            nearMgr
        ), new SchemaRegistry() {
            @Override public SchemaDescriptor schema() {
                return schema;
            }

            @Override public int lastSchemaVersion() {
                return schema.version();
            }

            @Override public SchemaDescriptor schema(int ver) {
                return schema;
            }

            @Override public Row resolve(BinaryRow row) {
                return new Row(schema, row);
            }
        }, null, null);
    }

    /**
     * Shutdowns all cluster nodes after each test.
     *
     * @throws Exception If failed.
     */
    @AfterEach
    public void afterTest() throws Exception {
        for (RaftServer rs : raftServers.values())
            rs.stop();

        for (RaftGroupService svc : raftClients.values())
            svc.shutdown();

        for (ClusterService node : cluster)
            node.stop();

        client.stop();
    }

    /**
     * @param name Node name.
     * @param port Local port.
     * @param nodeFinder Node finder.
     * @return The client cluster view.
     */
    private static ClusterService startClient(String name, int port, NodeFinder nodeFinder) {
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
}
