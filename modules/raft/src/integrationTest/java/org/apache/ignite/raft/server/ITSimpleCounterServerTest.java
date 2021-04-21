package org.apache.ignite.raft.server;

import java.util.List;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.service.impl.RaftGroupServiceImpl;
import org.apache.ignite.raft.server.impl.SimpleRaftServerImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ITSimpleCounterServerTest extends RaftCounterServerAbstractTest {
    /** */
    protected RaftServer server;

    /**
     * @param testInfo Test info.
     */
    @BeforeEach
    void before(TestInfo testInfo) {
        LOG.info(">>>> Starting test " + testInfo.getTestMethod().orElseThrow().getName());

        String id = "localhost:" + PORT;

        ClusterService service = clusterService(id, PORT, List.of(), false);

        server = new SimpleRaftServerImpl(service, null, FACTORY, false);

        ClusterNode serverNode = this.server.clusterService().topologyService().localMember();

        this.server.startRaftNode(COUNTER_GROUP_ID_0, new CounterCommandListener(), List.of(new Peer(serverNode)));
        this.server.startRaftNode(COUNTER_GROUP_ID_1, new CounterCommandListener(), List.of(new Peer(serverNode)));

        ClusterService clientNode1 = clusterService("localhost:" + (PORT + 1), PORT + 1, List.of(id), false);

        client1 = new RaftGroupServiceImpl(COUNTER_GROUP_ID_0, clientNode1, FACTORY, 1000, List.of(new Peer(serverNode)), false, 200, false);

        ClusterService clientNode2 = clusterService("localhost:" + (PORT + 2), PORT + 2, List.of(id), false);

        client2 = new RaftGroupServiceImpl(COUNTER_GROUP_ID_1, clientNode2, FACTORY, 1000, List.of(new Peer(serverNode)), false, 200, false);

        assertTrue(waitForTopology(service, 2, 1000));
        assertTrue(waitForTopology(clientNode1, 2, 1000));
        assertTrue(waitForTopology(clientNode2, 2, 1000));
    }

    /**
     * @throws Exception
     */
    @AfterEach
    void after() throws Exception {
        server.shutdown();
        client1.shutdown();
        client2.shutdown();
    }
}
