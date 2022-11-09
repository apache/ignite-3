/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import static org.apache.ignite.internal.raft.server.RaftGroupOptions.defaults;
import static org.apache.ignite.raft.jraft.test.TestUtils.waitForTopology;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.RaftGroupServiceImpl;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.server.counter.CounterListener;
import org.apache.ignite.raft.server.counter.GetValueCommand;
import org.apache.ignite.raft.server.counter.IncrementAndGetCommand;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Single node raft server.
 */
class ItSimpleCounterServerTest extends RaftServerAbstractTest {
    /**
     * The server implementation.
     */
    private RaftServer server;

    /**
     * Counter raft group 0.
     */
    private static final TestReplicationGroupId COUNTER_GROUP_ID_0 = new TestReplicationGroupId("counter0");

    /**
     * Counter raft group 1.
     */
    private static final TestReplicationGroupId COUNTER_GROUP_ID_1 = new TestReplicationGroupId("counter1");

    /**
     * The client 1.
     */
    private RaftGroupService client1;

    /**
     * The client 2.
     */
    private RaftGroupService client2;

    /** Executor for raft group services. */
    private ScheduledExecutorService executor;

    /**
     * Before each.
     */
    @BeforeEach
    void before() throws Exception {
        var addr = new NetworkAddress("localhost", PORT);

        ClusterService service = clusterService(PORT, List.of(addr), true);

        server = new JraftServerImpl(service, workDir) {
            @Override
            public synchronized void stop() throws Exception {
                super.stop();

                service.stop();
            }
        };

        server.start();

        ClusterNode serverNode = server.clusterService().topologyService().localMember();

        assertTrue(
                server.startRaftGroup(COUNTER_GROUP_ID_0, new CounterListener(), List.of(new Peer(serverNode.name())), defaults())
        );
        assertTrue(
                server.startRaftGroup(COUNTER_GROUP_ID_1, new CounterListener(), List.of(new Peer(serverNode.name())), defaults())
        );

        ClusterService clientNode1 = clusterService(PORT + 1, List.of(addr), true);

        executor = new ScheduledThreadPoolExecutor(20, new NamedThreadFactory(Loza.CLIENT_POOL_NAME, logger()));

        client1 = RaftGroupServiceImpl.start(COUNTER_GROUP_ID_0, clientNode1, FACTORY, 1000,
                List.of(new Peer(serverNode.name())), false, 200, executor).get(3, TimeUnit.SECONDS);

        ClusterService clientNode2 = clusterService(PORT + 2, List.of(addr), true);

        client2 = RaftGroupServiceImpl.start(COUNTER_GROUP_ID_1, clientNode2, FACTORY, 1000,
                List.of(new Peer(serverNode.name())), false, 200, executor).get(3, TimeUnit.SECONDS);

        assertTrue(waitForTopology(service, 3, 1000));
        assertTrue(waitForTopology(clientNode1, 3, 1000));
        assertTrue(waitForTopology(clientNode2, 3, 1000));
    }

    /**
     * After each.
     */
    @AfterEach
    @Override
    public void after() throws Exception {
        server.stopRaftGroup(COUNTER_GROUP_ID_0);
        server.stopRaftGroup(COUNTER_GROUP_ID_1);

        server.stop();

        client1.shutdown();
        client2.shutdown();

        IgniteUtils.shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);

        super.after();
    }

    @Test
    public void testRefreshLeader() throws Exception {
        Peer leader = client1.leader();

        assertNull(leader);

        client1.refreshLeader().get();

        assertNotNull(client1.leader());
    }

    @Test
    public void testCounterCommandListener() throws Exception {
        client1.refreshLeader().get();
        client2.refreshLeader().get();

        assertNotNull(client1.leader());
        assertNotNull(client2.leader());

        assertEquals(2, client1.<Long>run(new IncrementAndGetCommand(2)).get());
        assertEquals(2, client1.<Long>run(new GetValueCommand()).get());
        assertEquals(3, client1.<Long>run(new IncrementAndGetCommand(1)).get());
        assertEquals(3, client1.<Long>run(new GetValueCommand()).get());

        assertEquals(4, client2.<Long>run(new IncrementAndGetCommand(4)).get());
        assertEquals(4, client2.<Long>run(new GetValueCommand()).get());
        assertEquals(7, client2.<Long>run(new IncrementAndGetCommand(3)).get());
        assertEquals(7, client2.<Long>run(new GetValueCommand()).get());
    }
}
