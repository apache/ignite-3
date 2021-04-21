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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.network.ClusterNode;
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

            // Must use host + port as a node id.
            String name = "server" + i;

            addresses.add(TestUtils.getMyIp() + ":" + port);

            ClusterService service = clusterService(name, port, addresses, false);

            RaftServer server = new JRaftServerImpl(service, dataPath, FACTORY, false);

            servers.add(server);

            ClusterNode node = server.clusterService().topologyService().localMember();

            peers.add(new Peer(node));
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

        ClusterService clientNode1 = clusterService("client0", PORT - 1, addresses, false);

        client1 = new RaftGroupServiceImpl(COUNTER_GROUP_ID_0, clientNode1, FACTORY, 1000, peers, false, 200, false);

        ClusterService clientNode2 = clusterService("client1:" + (PORT - 2), PORT - 2, addresses, false);

        client2 = new RaftGroupServiceImpl(COUNTER_GROUP_ID_0, clientNode2, FACTORY, 1000, peers, false, 200, false);

        assertTrue(waitForTopology(clientNode1, 5, 5_000)); // TODO asch test client colocated on server.
        assertTrue(waitForTopology(clientNode2, 5, 5_000));

        for (List<RaftNode> grpNodes : nodes.values()) {
            // Wait for leader.
            while(grpNodes.get(0).leader() == null)
                Thread.onSpinWait();
        }
    }

    @AfterEach
    void after() throws Exception {
        LOG.info("Start server shutdown");

        for (RaftServer server : servers)
            server.shutdown();

        LOG.info("Start client shutdown");

        client1.shutdown();
        client2.shutdown();

        assertTrue(Utils.delete(new File(this.dataPath)));
    }
}
