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

import java.util.List;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.service.impl.RaftGroupServiceImpl;
import org.apache.ignite.raft.server.impl.RaftServerImpl;
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

        server = new RaftServerImpl(service, FACTORY, false);

        ClusterNode serverNode = this.server.clusterService().topologyService().localMember();

        this.server.startRaftNode(COUNTER_GROUP_ID_0, new CounterCommandListener(), List.of(new Peer(serverNode.address())));
        this.server.startRaftNode(COUNTER_GROUP_ID_1, new CounterCommandListener(), List.of(new Peer(serverNode.address())));

        ClusterService clientNode1 = clusterService("localhost:" + (PORT + 1), PORT + 1, List.of(id), false);

        client1 = new RaftGroupServiceImpl(COUNTER_GROUP_ID_0, clientNode1, FACTORY, 1000, List.of(new Peer(serverNode.address())), false, 200, false);

        ClusterService clientNode2 = clusterService("localhost:" + (PORT + 2), PORT + 2, List.of(id), false);

        client2 = new RaftGroupServiceImpl(COUNTER_GROUP_ID_1, clientNode2, FACTORY, 1000, List.of(new Peer(serverNode.address())), false, 200, false);

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
