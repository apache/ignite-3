package org.apache.ignite.raft.server;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.service.impl.RaftGroupServiceImpl;
import org.apache.ignite.raft.jraft.test.TestUtils;
import org.apache.ignite.raft.jraft.util.Utils;
import org.apache.ignite.raft.server.impl.JRaftServerImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

import static org.junit.Assert.assertTrue;

class ITJRaftCounterServerTest extends RaftCounterServerAbstractTest {
    /** */
    private List<RaftServer> servers = new ArrayList<>();

    /** */
    private Map<String, List<RaftNode>> nodes = new HashMap<>();

    /** */
    private String dataPath;

    @BeforeEach
    void before(TestInfo testInfo) {
        LOG.info(">>>> Starting test " + testInfo.getTestMethod().orElseThrow().getName());

        this.dataPath = TestUtils.mkTempDir();

        List<String> addresses = new ArrayList<>();
        List<Peer> peers = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            int port = PORT + i;

            String addr = "localhost:" + port;

            addresses.add(addr);

            ClusterService service = clusterService(addr, port, addresses, false);

            RaftServer server = new JRaftServerImpl(service, dataPath, FACTORY, false);

            servers.add(server);

            peers.add(new Peer(server.clusterService().topologyService().localMember()));
        }

        for (RaftServer server : servers) {
            assertTrue(waitForTopology(server.clusterService(), 3, 5_000));
        }

        for (RaftServer server : servers) {
            RaftNode node0 = server.startRaftNode(COUNTER_GROUP_ID_0, new CounterCommandListener(), peers);

            nodes.computeIfAbsent(node0.groupId(), k -> new ArrayList<>()).add(node0);

            RaftNode node1 = server.startRaftNode(COUNTER_GROUP_ID_1, new CounterCommandListener(), peers);

            nodes.computeIfAbsent(node1.groupId(), k -> new ArrayList<>()).add(node0);
        }

        ClusterService clientNode1 = clusterService("localhost:" + (PORT - 1), PORT - 1, addresses, false);

        client1 = new RaftGroupServiceImpl(COUNTER_GROUP_ID_0, clientNode1, FACTORY, 1000, peers, false, 200, false);

        ClusterService clientNode2 = clusterService("localhost:" + (PORT - 2), PORT - 2, addresses, false);

        client2 = new RaftGroupServiceImpl(COUNTER_GROUP_ID_0, clientNode2, FACTORY, 1000, peers, false, 200, false);

        assertTrue(waitForTopology(clientNode1, 4, 5_000));
        assertTrue(waitForTopology(clientNode2, 4, 5_000));

        for (List<RaftNode> grpNodes : nodes.values()) {
            // Wait for leader.
            while(grpNodes.get(0).leader() == null)
                Thread.onSpinWait();
        }
    }

    @AfterEach
    void after() throws Exception {
        for (RaftServer server : servers)
            server.shutdown();

        client1.shutdown();
        client2.shutdown();

        assertTrue(Utils.delete(new File(this.dataPath)));
    }
}
