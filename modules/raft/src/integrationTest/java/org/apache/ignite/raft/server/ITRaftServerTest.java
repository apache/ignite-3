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
import java.util.Timer;
import org.apache.ignite.lang.LogWrapper;
import org.apache.ignite.network.MessageHandlerHolder;
import org.apache.ignite.network.NetworkCluster;
import org.apache.ignite.network.NetworkClusterFactory;
import org.apache.ignite.network.scalecube.ScaleCubeMemberResolver;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.message.impl.RaftClientMessageFactory;
import org.apache.ignite.raft.client.message.impl.RaftClientMessageFactoryImpl;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.client.service.impl.RaftGroupServiceImpl;
import org.apache.ignite.raft.server.impl.RaftServerImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/** */
class ITRaftServerTest {
    private static LogWrapper LOG = new LogWrapper(ITRaftServerTest.class);

    private static RaftClientMessageFactory FACTORY = new RaftClientMessageFactoryImpl();

    /** */
    private RaftServer server;

    /** */
    private static final String SERVER_ID = "testSrv";

    /** */
    private static final String CLIENT_ID = "testClient";

    /** */
    public static final String COUNTER_GROUP_ID = "counter";

    /**
     * @param testInfo Test info.
     */
    @BeforeEach
    void before(TestInfo testInfo) {
        LOG.info(">>>> Starting test " + testInfo.getTestMethod().orElseThrow().getName());

        server = new RaftServerImpl();
        server.setListener("counter0", new CounterCommandListener());
        server.setListener("counter1", new CounterCommandListener());

        RaftServerOptions opts = new RaftServerOptions();

        opts.localPort = 20100;
        opts.id = SERVER_ID;
        opts.msgFactory = FACTORY;

        server.init(opts);
    }

    @AfterEach
    void after() throws Exception {
        server.destroy();
    }

    @Test
    public void testRefreshLeader() throws Exception {
        NetworkCluster clientNode = startClient(CLIENT_ID, 20101, List.of("localhost:20100"));

        Thread.sleep(1000);

        Peer peer = new Peer(clientNode.allMembers().stream().filter(m -> SERVER_ID.equals(m.name())).findFirst().orElseThrow());

        RaftGroupService service = new RaftGroupServiceImpl("test", clientNode, FACTORY, 1000, List.of(peer), true, 200, new Timer());

        Peer leader = service.leader();

        assertNotNull(leader);
        assertEquals(peer.getNode().name(), leader.getNode().name());
    }

    @Test
    public void testCounterStateMachine() throws Exception {
        NetworkCluster clientNode = startClient(CLIENT_ID, 20101, List.of("localhost:20100"));

        Thread.sleep(1000);

        Peer peer = new Peer(clientNode.allMembers().stream().filter(m -> SERVER_ID.equals(m.name())).findFirst().orElseThrow());

        RaftGroupService service = new RaftGroupServiceImpl("test", clientNode, FACTORY, 1000, List.of(peer), true, 200, new Timer());

        Peer leader = service.leader();

        assertNotNull(leader);
        assertEquals(peer.getNode().name(), leader.getNode().name());
    }

    private NetworkCluster startClient(String name, int port, List<String> servers) {
        return new NetworkClusterFactory(name, port, servers)
            .startScaleCubeBasedCluster(new ScaleCubeMemberResolver(), new MessageHandlerHolder());
    }
}