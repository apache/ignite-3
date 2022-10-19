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
import static org.apache.ignite.raft.jraft.test.TestUtils.getLocalAddress;
import static org.apache.ignite.raft.jraft.test.TestUtils.waitForTopology;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.jraft.core.NodeImpl;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.test.TestUtils;
import org.apache.ignite.raft.server.counter.CounterListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Integration tests with hybrid logical clock.
 */
@ExtendWith(WorkDirectoryExtension.class)
class ItJraftHlcServerTest extends RaftServerAbstractTest {
    /**
     * The logger.
     */
    private static final IgniteLogger LOG = Loggers.forClass(ItJraftHlcServerTest.class);

    /**
     * The server port offset.
     */
    private static final int PORT = 5003;

    /**
     * Initial configuration.
     */
    private static final List<Peer> INITIAL_CONF = IntStream.rangeClosed(0, 2)
            .mapToObj(i -> new NetworkAddress(getLocalAddress(), PORT + i))
            .map(Peer::new)
            .collect(Collectors.toUnmodifiableList());

    /**
     * Listener factory.
     */
    private Supplier<CounterListener> listenerFactory = CounterListener::new;

    /**
     * Servers list.
     */
    private final List<JraftServerImpl> servers = new ArrayList<>();

    /**
     * Data path.
     */
    @WorkDirectory
    private Path dataPath;

    /**
     * After each.
     */
    @AfterEach
    @Override
    protected void after() throws Exception {
        super.after();

        LOG.info("Start client shutdown");

        LOG.info("Start server shutdown servers={}", servers.size());

        Iterator<JraftServerImpl> iterSrv = servers.iterator();

        while (iterSrv.hasNext()) {
            JraftServerImpl server = iterSrv.next();

            iterSrv.remove();

            Set<String> grps = server.startedGroups();

            for (String grp : grps) {
                server.stopRaftGroup(grp);
            }

            server.beforeNodeStop();

            server.stop();
        }

        TestUtils.assertAllJraftThreadsStopped();

        LOG.info(">>>>>>>>>>>>>>> End test method: {}", testInfo.getTestMethod().orElseThrow().getName());
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

        JraftServerImpl server = new JraftServerImpl(service, dataPath.resolve("node" + idx), opts) {
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
            raftServer.startRaftGroup("test_raft_group", listenerFactory.get(), INITIAL_CONF, defaults());
        }, opts -> {});

        servers.forEach(srv -> {
            for (int i = 0; i < 5; i++) {
                srv.startRaftGroup("test_raft_group_" + i, listenerFactory.get(), INITIAL_CONF, defaults());
            }
        });

        List<List<org.apache.ignite.raft.jraft.RaftGroupService>> groups = new ArrayList<>();

        servers.forEach(srv -> {
            groups.add(srv.startedGroups().stream()
                    .map(grpName -> srv.raftGroupService(grpName)).collect(toList()));
        });

        assertTrue(groups.size() > 0);

        groups.forEach(grp -> {
            assertTrue(grp.size() > 1);

            HybridClock clock = ((NodeImpl) grp.get(0).getRaftNode()).clock();

            grp.forEach(grp0 -> {
                assertTrue(clock == ((NodeImpl) grp0.getRaftNode()).clock());
            });
        });

        servers.forEach(srv -> {
            srv.stopRaftGroup("test_raft_group");

            for (int i = 0; i < 10; i++) {
                srv.stopRaftGroup("test_raft_group_" + i);
            }
        });
    }
}
