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

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.network.utils.ClusterServiceTestUtils.defaultSerializationRegistry;
import static org.apache.ignite.internal.raft.server.RaftGroupOptions.defaults;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.raft.jraft.test.TestUtils.getLocalAddress;
import static org.apache.ignite.raft.jraft.test.TestUtils.waitForTopology;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.raft.util.ThreadLocalOptimizedMarshaller;
import org.apache.ignite.internal.replicator.TestReplicationGroupId;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.jraft.RaftGroupService;
import org.apache.ignite.raft.jraft.core.NodeImpl;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.test.TestUtils;
import org.apache.ignite.raft.server.counter.CounterListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests with hybrid logical clock.
 */
class ItJraftHlcServerTest extends RaftServerAbstractTest {
    /**
     * The server port offset.
     */
    private static final int PORT = 5003;

    /**
     * Initial configuration.
     */
    private PeersAndLearners initialConf;

    /**
     * Listener factory.
     */
    private final Supplier<CounterListener> listenerFactory = CounterListener::new;

    /**
     * Servers list.
     */
    private final List<JraftServerImpl> servers = new ArrayList<>();

    @BeforeEach
    void setUp() {
        initialConf = IntStream.rangeClosed(0, 2)
                .mapToObj(i -> testNodeName(testInfo, PORT + i))
                .collect(collectingAndThen(toSet(), PeersAndLearners::fromConsistentIds));
    }

    /**
     * After each.
     */
    @AfterEach
    @Override
    protected void after() throws Exception {
        super.after();

        logger().info("Start server shutdown servers={}", servers.size());

        Iterator<JraftServerImpl> iterSrv = servers.iterator();

        while (iterSrv.hasNext()) {
            JraftServerImpl server = iterSrv.next();

            iterSrv.remove();

            for (RaftNodeId nodeId : server.localNodes()) {
                server.stopRaftNode(nodeId);
            }

            server.beforeNodeStop();

            assertThat(server.stopAsync(new ComponentContext()), willCompleteSuccessfully());
        }

        TestUtils.assertAllJraftThreadsStopped();
    }

    /**
     * Starts server.
     *
     * @param idx The index.
     * @param clo Init closure.
     * @param cons Node options updater.
     *
     * @return Raft server instance.
     */
    private JraftServerImpl startServer(int idx, Consumer<RaftServer> clo, Consumer<NodeOptions> cons) {
        var addr = new NetworkAddress(getLocalAddress(), PORT);

        ClusterService service = clusterService(PORT + idx, List.of(addr), true);

        NodeOptions opts = new NodeOptions();

        cons.accept(opts);

        JraftServerImpl server = jraftServer(idx, service, opts);

        assertThat(server.startAsync(new ComponentContext()), willCompleteSuccessfully());

        clo.accept(server);

        servers.add(server);

        assertTrue(waitForTopology(service, servers.size(), 15_000));

        return server;
    }

    /**
     * Checks that only one instance of clock is created per Ignite node.
     */
    @Test
    public void testHlcOneInstancePerIgniteNode() {
        ThreadLocalOptimizedMarshaller commandsMarshaller = new ThreadLocalOptimizedMarshaller(defaultSerializationRegistry());

        startServer(0, raftServer -> {
            String localNodeName = raftServer.clusterService().topologyService().localMember().name();

            Peer localNode = initialConf.peer(localNodeName);

            var nodeId = new RaftNodeId(new TestReplicationGroupId("test_raft_group"), localNode);

            raftServer.startRaftNode(nodeId, initialConf, listenerFactory.get(), defaults().commandsMarshaller(commandsMarshaller));
        }, opts -> {});

        servers.forEach(srv -> {
            String localNodeName = srv.clusterService().topologyService().localMember().name();

            Peer localNode = initialConf.peer(localNodeName);

            for (int i = 0; i < 5; i++) {
                var nodeId = new RaftNodeId(new TestReplicationGroupId("test_raft_group_" + i), localNode);

                srv.startRaftNode(nodeId, initialConf, listenerFactory.get(), defaults().commandsMarshaller(commandsMarshaller));
            }
        });

        servers.forEach(srv -> {
            List<RaftGroupService> grp = srv.localNodes().stream().map(srv::raftGroupService).collect(toList());

            assertTrue(grp.size() > 1);

            HybridClock clock = ((NodeImpl) grp.get(0).getRaftNode()).clock();

            grp.forEach(grp0 -> assertSame(clock, ((NodeImpl) grp0.getRaftNode()).clock()));
        });

        servers.forEach(srv -> {
            srv.stopRaftNodes(new TestReplicationGroupId("test_raft_group"));

            for (int i = 0; i < 10; i++) {
                srv.stopRaftNodes(new TestReplicationGroupId("test_raft_group_" + i));
            }
        });
    }
}
