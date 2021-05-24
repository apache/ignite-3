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
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.ClusterServiceFactory;
import org.apache.ignite.network.scalecube.TestScaleCubeClusterServiceFactory;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.message.RaftClientMessageFactory;
import org.apache.ignite.raft.client.message.impl.RaftClientMessageFactoryImpl;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.client.service.impl.RaftGroupServiceImpl;
import org.apache.ignite.raft.jraft.test.TestUtils;
import org.apache.ignite.raft.jraft.util.Utils;
import org.apache.ignite.raft.server.impl.JRaftServerImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

import static org.junit.Assert.assertTrue;

/** */
abstract class ITJRaftServerAbstractTest extends RaftCounterServerAbstractTest {
    /** */
    protected static final IgniteLogger LOG = IgniteLogger.forClass(ITJRaftServerAbstractTest.class);

    /** */
    protected static final RaftClientMessageFactory FACTORY = new RaftClientMessageFactoryImpl();

    /** Network factory. */
    protected static final ClusterServiceFactory NETWORK_FACTORY = new TestScaleCubeClusterServiceFactory();

    /** */
    protected static final int PORT = 5003;

    /** */
    protected static final int CLIENT_PORT = 6003;

    /** */
    protected List<RaftServer> servers = new ArrayList<>();

    /** */
    protected List<RaftGroupService> clients = new ArrayList<>();

    /** */
    private String dataPath;

    @BeforeEach
    void before(TestInfo testInfo) {
        LOG.info(">>>> Starting test " + testInfo.getTestMethod().orElseThrow().getName());

        this.dataPath = TestUtils.mkTempDir();
    }

    @AfterEach
    void after() throws Exception {
        LOG.info("Start client shutdown");

        for (RaftGroupService client : clients) {
            client.shutdown();
        }

        LOG.info("Start server shutdown servers={}", servers.size());

        for (RaftServer server : servers)
            server.shutdown();

        assertTrue("Failed to delete " + this.dataPath, Utils.delete(new File(this.dataPath)));
    }

    /**
     * @param idx The index.
     * @return Raft server instance.
     */
    protected JRaftServerImpl startServer(int idx, Consumer<RaftServer> clo) {
        ClusterService service = clusterService("server" + idx, PORT + idx, List.of(TestUtils.getMyIp() + ":" + PORT), false);

        JRaftServerImpl server = new JRaftServerImpl(service, dataPath, FACTORY, false);

        clo.accept(server);

        servers.add(server);

        assertTrue(waitForTopology(service, servers.size(), 5_000));

        return server;
    }

    /**
     * @param groupId Group id.
     * @return The client.
     */
    protected RaftGroupService startClient(String groupId) {
        String addr = TestUtils.getMyIp() + ":" + PORT;

        ClusterService clientNode1 = clusterService("client_" + groupId + "_", CLIENT_PORT + clients.size(), List.of(addr), false);

        RaftGroupServiceImpl client =
            new RaftGroupServiceImpl(groupId, clientNode1, FACTORY, 10_000, List.of(new Peer(addr)), false, 200, false);

        clients.add(client);

        return client;
    }
}
