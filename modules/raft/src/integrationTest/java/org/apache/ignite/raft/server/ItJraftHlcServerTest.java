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

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.raft.server.RaftGroupOptions.defaults;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.raft.jraft.test.TestUtils.getLocalAddress;
import static org.apache.ignite.raft.jraft.test.TestUtils.waitForTopology;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.client.Peer;
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
    private final List<Peer> initialConf = new ArrayList<>();

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
        IntStream.rangeClosed(0, 2)
                .mapToObj(i -> testNodeName(testInfo, PORT + i))
                .map(Peer::new)
                .forEach(initialConf::add);
    }

    /**
     * After each.
     */
    @AfterEach
    @Override
    protected void after() throws Exception {
        super.after();

        logger().info("Start client shutdown");

        logger().info("Start server shutdown servers={}", servers.size());

        Iterator<JraftServerImpl> iterSrv = servers.iterator();

        while (iterSrv.hasNext()) {
            JraftServerImpl server = iterSrv.next();

            iterSrv.remove();

            Set<ReplicationGroupId> grps = server.startedGroups();

            for (ReplicationGroupId grp : grps) {
                server.stopRaftGroup(grp);
            }

            server.beforeNodeStop();

            server.stop();
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

        JraftServerImpl server = new JraftServerImpl(service, workDir.resolve("node" + idx), opts) {
            @Override
            public void stop() throws Exception {
                servers.remove(this);

                super.stop();

                service.stop();
            }
        };

        server.start();

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
        startServer(0, raftServer -> {
            raftServer.startRaftGroup(new TestReplicationGroupId("test_raft_group"), listenerFactory.get(), initialConf, defaults());
        }, opts -> {});

        servers.forEach(srv -> {
            for (int i = 0; i < 5; i++) {
                srv.startRaftGroup(new TestReplicationGroupId("test_raft_group_" + i), listenerFactory.get(), initialConf, defaults());
            }
        });

        List<List<RaftGroupService>> groups = new ArrayList<>();

        servers.forEach(srv -> {
            groups.add(srv.startedGroups().stream()
                    .map(grpName -> srv.raftGroupService(grpName)).collect(toList()));
        });

        assertFalse(groups.isEmpty());

        groups.forEach(grp -> {
            assertTrue(grp.size() > 1);

            HybridClock clock = ((NodeImpl) grp.get(0).getRaftNode()).clock();

            grp.forEach(grp0 -> {
                assertTrue(clock == ((NodeImpl) grp0.getRaftNode()).clock());
            });
        });

        servers.forEach(srv -> {
            srv.stopRaftGroup(new TestReplicationGroupId("test_raft_group"));

            for (int i = 0; i < 10; i++) {
                srv.stopRaftGroup(new TestReplicationGroupId("test_raft_group_" + i));
            }
        });
    }
}
