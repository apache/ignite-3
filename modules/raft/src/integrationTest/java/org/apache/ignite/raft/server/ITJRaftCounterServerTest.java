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

package org.apache.ignite.raft.server;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.client.service.impl.RaftGroupServiceImpl;
import org.apache.ignite.raft.jraft.test.TestUtils;
import org.apache.ignite.raft.jraft.util.Utils;
import org.apache.ignite.raft.server.impl.JRaftServerImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ITJRaftCounterServerTest extends RaftCounterServerAbstractTest {
    /** */
    private List<RaftServer> servers = new ArrayList<>();

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

            String name = "server" + i;

            addresses.add(TestUtils.getMyIp() + ":" + port);

            ClusterService service = clusterService(name, port, addresses, false);

            RaftServer server = new JRaftServerImpl(service, dataPath, FACTORY, false);

            servers.add(server);

            ClusterNode node = server.clusterService().topologyService().localMember();

            peers.add(new Peer(node.address()));
        }

        for (RaftServer server : servers) {
            assertTrue(waitForTopology(server.clusterService(), 3, 5_000));
        }

        for (RaftServer server : servers) {
            server.startRaftNode(COUNTER_GROUP_ID_0, new CounterCommandListener(), peers);
            server.startRaftNode(COUNTER_GROUP_ID_1, new CounterCommandListener(), peers);
        }

        ClusterService clientNode1 = clusterService("client0", PORT - 1, addresses, false);

        // The client for group 0.
        client1 = new RaftGroupServiceImpl(COUNTER_GROUP_ID_0, clientNode1, FACTORY, 10_000, peers, false, 200, false);

        ClusterService clientNode2 = clusterService("client1:" + (PORT - 2), PORT - 2, addresses, false);

        // The client for group 1.
        client2 = new RaftGroupServiceImpl(COUNTER_GROUP_ID_1, clientNode2, FACTORY, 10_000, peers, false, 200, false);
    }

    @Test
    public void testCreateSnapshot() throws Exception {
        client1.refreshLeader().get();
        client2.refreshLeader().get();

        RaftServer server = servers.get(0);

        Peer peer0 = new Peer(server.clusterService().topologyService().localMember().address());

        long val = applyIncrements(client1, 1, 10);

        assertEquals(sum(10), val);

        client1.snapshot(peer0).get();

        long val2 = applyIncrements(client2, 1, 20);

        assertEquals(sum(20), val2);

        client1.snapshot(peer0).get();
        client2.snapshot(peer0).get();

        String snapshotDir0 = server.getServerDataPath(COUNTER_GROUP_ID_0) + File.separator + "snapshot";
        assertEquals(1, new File(snapshotDir0).list().length);

        String snapshotDir1 = server.getServerDataPath(COUNTER_GROUP_ID_1) + File.separator + "snapshot";
        assertEquals(1, new File(snapshotDir1).list().length);
    }

    @Test
    public void testCreateSnapshotFailure() {

    }

    @Test
    public void testFollowerCatchUp() throws Exception {
        client1.refreshLeader().get();
        client2.refreshLeader().get();

        Peer leader1 = client1.leader();
        Assertions.assertNotNull(leader1);

        Peer leader2 = client2.leader();
        Assertions.assertNotNull(leader2);

        assertEquals(2, client1.<Integer>run(new IncrementAndGetCommand(2)).get());
        assertEquals(2, client1.<Integer>run(new GetValueCommand()).get());
        assertEquals(3, client1.<Integer>run(new IncrementAndGetCommand(1)).get());
        assertEquals(3, client1.<Integer>run(new GetValueCommand()).get());

        assertEquals(4, client2.<Integer>run(new IncrementAndGetCommand(4)).get());
        assertEquals(4, client2.<Integer>run(new GetValueCommand()).get());
        assertEquals(7, client2.<Integer>run(new IncrementAndGetCommand(3)).get());
        assertEquals(7, client2.<Integer>run(new GetValueCommand()).get());

        RaftServer srv = servers.remove(1);
        ClusterNode srvNode = srv.clusterService().topologyService().localMember();
        srv.shutdown();

        assertEquals(6, client1.<Integer>run(new IncrementAndGetCommand(3)).get());
        assertEquals(12, client2.<Integer>run(new IncrementAndGetCommand(5)).get());

        assertNotEquals(client1.leader().address(), srvNode.address());
        assertNotEquals(client2.leader().address(), srvNode.address());
    }

    @AfterEach
    void after() throws Exception {
        LOG.info("Start server shutdown servers={}", servers.size());

        for (RaftServer server : servers)
            server.shutdown();

        LOG.info("Start client shutdown");

        client1.shutdown();
        client2.shutdown();

        assertTrue(Utils.delete(new File(this.dataPath)));
    }

    /**
     * @param client The client
     * @param start Start element.
     * @param stop Stop element.
     * @return The counter value.
     * @throws Exception If failed.
     */
    private long applyIncrements(RaftGroupService client, int start, int stop) throws Exception {
        long val = 0;

        for (int i = start; i <= stop; i++) {
            val = client.<Long>run(new IncrementAndGetCommand(i)).get();

            LOG.info("Val=" + val + ", i=" + i);
        }

        return val;
    }

    /**
     * Calculates a progression sum.
     *
     * @param until Until value.
     * @return The sum.
     */
    public long sum(long until) {
        return (1 + until) * until / 2;
    }
}
