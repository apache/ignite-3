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
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.ClusterServiceFactory;
import org.apache.ignite.network.message.MessageSerializationRegistry;
import org.apache.ignite.network.scalecube.ScaleCubeClusterServiceFactory;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.message.RaftClientMessageFactory;
import org.apache.ignite.raft.client.message.impl.RaftClientMessageFactoryImpl;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.client.service.impl.RaftGroupServiceImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** */
abstract class RaftCounterServerAbstractTest {
    /** */
    private static final IgniteLogger LOG = IgniteLogger.forClass(RaftCounterServerAbstractTest.class);

    /** */
    protected static final RaftClientMessageFactory FACTORY = new RaftClientMessageFactoryImpl();

    /** Network factory. */
    protected static final ClusterServiceFactory NETWORK_FACTORY = new ScaleCubeClusterServiceFactory();

    /** */
    protected static final int PORT = 20010;

    /** TODO: IGNITE-14088: Uncomment and use real serializer provider */
    private static final MessageSerializationRegistry SERIALIZATION_REGISTRY = new MessageSerializationRegistry();

    /** */
    private RaftServer server;

    /** */
    private ClusterService client;

    protected static final String SERVER_ID = "localhost:" + PORT;

    /** */
    private static final String COUNTER_GROUP_ID_0 = "counter0";

    /** */
    private static final String COUNTER_GROUP_ID_1 = "counter1";

    /**
     * @param testInfo Test info.
     */
    @BeforeEach
    void before(TestInfo testInfo) {
        LOG.info(">>>> Starting test " + testInfo.getTestMethod().orElseThrow().getName());

        server = createServer();

        server.startRaftGroup(COUNTER_GROUP_ID_0, new CounterCommandListener(), List.of());
        server.startRaftGroup(COUNTER_GROUP_ID_1, new CounterCommandListener(), List.of());

        client = clusterService("localhost:" + (PORT + 1), PORT + 1, List.of(SERVER_ID));

        assertTrue(waitForTopology(client, 2, 1000));
    }

    protected abstract RaftServer createServer();

    /**
     * @throws Exception
     */
    @AfterEach
    void after() throws Exception {
        server.shutdown();
        client.shutdown();
    }

    /**
     */
    @Test
    public void testRefreshLeader() {
        Peer server = new Peer(client.topologyService().allMembers().stream().filter(m -> SERVER_ID.equals(m.name())).findFirst().orElseThrow());

        RaftGroupService service = new RaftGroupServiceImpl(COUNTER_GROUP_ID_0, client, FACTORY, 1000,
            List.of(server), true, 200);

        Peer leader = service.leader();

        assertNotNull(leader);
        assertEquals(server.getNode().name(), leader.getNode().name());
    }

    /**
     * @throws Exception
     */
    @Test
    public void testCounterCommandListener() throws Exception {
        Peer server = new Peer(client.topologyService().allMembers().stream().filter(m -> SERVER_ID.equals(m.name())).findFirst().orElseThrow());

        RaftGroupService service0 = new RaftGroupServiceImpl(COUNTER_GROUP_ID_0, client, FACTORY, 1000,
            List.of(server), true, 200);

        RaftGroupService service1 = new RaftGroupServiceImpl(COUNTER_GROUP_ID_1, client, FACTORY, 1000,
            List.of(server), true, 200);

        assertNotNull(service0.leader());
        assertNotNull(service1.leader());

        assertEquals(2, service0.<Integer>run(new IncrementAndGetCommand(2)).get());
        assertEquals(2, service0.<Integer>run(new GetValueCommand()).get());
        assertEquals(3, service0.<Integer>run(new IncrementAndGetCommand(1)).get());
        assertEquals(3, service0.<Integer>run(new GetValueCommand()).get());

        assertEquals(4, service1.<Integer>run(new IncrementAndGetCommand(4)).get());
        assertEquals(4, service1.<Integer>run(new GetValueCommand()).get());
        assertEquals(7, service1.<Integer>run(new IncrementAndGetCommand(3)).get());
        assertEquals(7, service1.<Integer>run(new GetValueCommand()).get());
    }

    /**
     * @param name Node name.
     * @param port Local port.
     * @param servers Server nodes of the cluster.
     * @return The client cluster view.
     */
    protected ClusterService clusterService(String name, int port, List<String> servers) {
        var context = new ClusterLocalConfiguration(name, port, servers, SERIALIZATION_REGISTRY);
        var network = NETWORK_FACTORY.createClusterService(context);
        network.start();
        return network;
    }

    /**
     * @param cluster The cluster.
     * @param expected Expected count.
     * @param timeout The timeout in millis.
     * @return {@code True} if topology size is equal to expected.
     */
    private boolean waitForTopology(ClusterService cluster, int expected, int timeout) {
        long stop = System.currentTimeMillis() + timeout;

        while(System.currentTimeMillis() < stop) {
            if (cluster.topologyService().allMembers().size() >= expected)
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
