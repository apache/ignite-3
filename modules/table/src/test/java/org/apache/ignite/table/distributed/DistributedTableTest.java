package org.apache.ignite.table.distributed;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.affinity.RendezvousAffinityFunction;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.storage.TableStorageImpl;
import org.apache.ignite.lang.LogWrapper;
import org.apache.ignite.network.Network;
import org.apache.ignite.network.NetworkCluster;
import org.apache.ignite.network.NetworkMember;
import org.apache.ignite.network.message.DefaultMessageMapperProvider;
import org.apache.ignite.network.scalecube.ScaleCubeMemberResolver;
import org.apache.ignite.network.scalecube.ScaleCubeNetworkClusterFactory;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.message.RaftClientMessageFactory;
import org.apache.ignite.raft.client.message.impl.RaftClientMessageFactoryImpl;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.apache.ignite.raft.client.service.RaftGroupCommandListener;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.client.service.impl.RaftGroupServiceImpl;
import org.apache.ignite.raft.server.RaftServer;
import org.apache.ignite.raft.server.impl.RaftServerImpl;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class DistributedTableTest {

    public static final int NODE_PORT_BASE = 20_000;
    public static final int NODES = 5;
    public static final int PARTS = 10;
    private static RaftClientMessageFactory FACTORY = new RaftClientMessageFactoryImpl();

    private ArrayList<NetworkCluster> cluster = new ArrayList<>();

    private LogWrapper log = new LogWrapper(DistributedTableTest.class);

    @BeforeEach
    public void beforeTest() {
        for (int i = 0; i < NODES; i++) {
            Network network = new Network(new ScaleCubeNetworkClusterFactory(
                "node_" + i,
                NODE_PORT_BASE + i,
                IntStream.range(NODE_PORT_BASE, NODE_PORT_BASE + NODES).boxed().map((port) -> "localhost:" + port).collect(Collectors.toList()),
                new ScaleCubeMemberResolver()
            ));

            network.registerMessageMapper((short)1000, new DefaultMessageMapperProvider());
            network.registerMessageMapper((short)1001, new DefaultMessageMapperProvider());
            network.registerMessageMapper((short)1005, new DefaultMessageMapperProvider());
            network.registerMessageMapper((short)1006, new DefaultMessageMapperProvider());
            network.registerMessageMapper((short)1009, new DefaultMessageMapperProvider());

            cluster.add(network.start());
        }

        for (NetworkCluster node : cluster)
            assertTrue(waitForTopology(node, NODES, 1000));

        log.info("Cluster started.");
    }

    @AfterEach
    public void afterTest() throws Exception {
        for (NetworkCluster node : cluster) {
            node.shutdown();
        }
    }

    private void raftTableListener() {
        NetworkCluster client = startClient(
            "client",
            NODE_PORT_BASE + NODES,
            IntStream.range(NODE_PORT_BASE, NODE_PORT_BASE + NODES).boxed().map((port) -> "localhost:" + port).collect(Collectors.toList())
        );

        assertTrue(waitForTopology(client, NODES + 1, 1000));



    }

    @Test
    public void partitionedTable() {
        NetworkCluster client = startClient(
            "client",
            NODE_PORT_BASE + NODES,
            IntStream.range(NODE_PORT_BASE, NODE_PORT_BASE + NODES).boxed().map((port) -> "localhost:" + port).collect(Collectors.toList())
        );

        assertTrue(waitForTopology(client, NODES + 1, 1000));

        HashMap<NetworkMember, RaftServer> raftServers = new HashMap<>(NODES);

        for (int i = 0; i < NODES; i++) {
            raftServers.put(cluster.get(i).localMember(), new RaftServerImpl(
                cluster.get(i),
                FACTORY,
                1000,
                Map.of()
            ));
        }

        List<List<NetworkMember>> assignment = RendezvousAffinityFunction.assignPartitions(
            cluster.stream().map(node -> node.localMember()).collect(Collectors.toList()),
            PARTS,
            1,
            false,
            null
        );

        int p = 0;

        Map<Integer, RaftGroupService> partMap = new HashMap<>();

        for (List<NetworkMember> partMembers : assignment) {
            RaftServer rs = raftServers.get(partMembers.get(0));

            String grpId = "part-" + p;

            rs.setListener("part-" + p, new RaftGroupCommandListener() {
                @Override public void onRead(Iterator<CommandClosure<ReadCommand>> iterator) {

                }

                @Override public void onWrite(Iterator<CommandClosure<WriteCommand>> iterator) {

                }
            });

            partMap.put(p, new RaftGroupServiceImpl(grpId, client, FACTORY, 1000,
                List.of(new Peer(partMembers.get(0))), true, 200));

            p++;
        }

        Table tbl = new TableImpl(new TableStorageImpl(
            UUID.randomUUID(),
            partMap,
            PARTS
        ));

        //Nothing to do until marshaller will be implemented.
//        tbl.kvView().get(new TestTableRowImpl("Hello!".getBytes()));
    }


    /**
     * @param name Node name.
     * @param port Local port.
     * @param servers Server nodes of the cluster.
     * @return The client cluster view.
     */
    private NetworkCluster startClient(String name, int port, List<String> servers) {
        Network network = new Network(
            new ScaleCubeNetworkClusterFactory(name, port, servers, new ScaleCubeMemberResolver())
        );

        network.registerMessageMapper((short)1000, new DefaultMessageMapperProvider());
        network.registerMessageMapper((short)1001, new DefaultMessageMapperProvider());
        network.registerMessageMapper((short)1005, new DefaultMessageMapperProvider());
        network.registerMessageMapper((short)1006, new DefaultMessageMapperProvider());
        network.registerMessageMapper((short)1009, new DefaultMessageMapperProvider());

        return network.start();
    }

    /**
     * @param cluster The cluster.
     * @param expected Expected count.
     * @param timeout The timeout in millis.
     * @return {@code True} if topology size is equal to expected.
     */
    private boolean waitForTopology(NetworkCluster cluster, int expected, int timeout) {
        long stop = System.currentTimeMillis() + timeout;

        while (System.currentTimeMillis() < stop) {
            if (cluster.allMembers().size() >= expected)
                return true;

            try {
                Thread.sleep(50);
            }
            catch (InterruptedException e) {
                return false;
            }
        }

        return false;
    }
}
