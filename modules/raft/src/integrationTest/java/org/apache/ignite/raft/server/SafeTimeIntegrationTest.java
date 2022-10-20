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

import static org.apache.ignite.internal.raft.server.RaftGroupOptions.forVolatileStores;
import static org.apache.ignite.raft.jraft.test.TestUtils.waitForTopology;
import static org.apache.ignite.utils.ClusterServiceTestUtils.findLocalAddresses;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.ignite.hlc.HybridClock;
import org.apache.ignite.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.server.ReplicationGroupOptions;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.apache.ignite.raft.client.service.RaftGroupListener;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupServiceImpl;
import org.apache.ignite.raft.jraft.test.TestUtils;
import org.apache.ignite.raft.server.snasphot.TestWriteCommand;
import org.apache.ignite.raft.server.snasphot.UpdateCountRaftListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;

@ExtendWith(WorkDirectoryExtension.class)
public class SafeTimeIntegrationTest extends RaftServerAbstractTest {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(SafeTimeIntegrationTest.class);

    /** Base network port. */
    private static final int NODE_PORT_BASE = 20_000;

    /** Nodes. */
    private static final int NODES = 3;

    /** Test raft group name. */
    private static final String RAFT_GROUP_NAME = "test";

    /** Factory. */
    private static final RaftMessagesFactory FACTORY = new RaftMessagesFactory();

    /** Cluster. */
    private final ArrayList<ClusterService> clusterServices = new ArrayList<>();

    /** Executor for raft group services. */
    private ScheduledExecutorService executor;

    @WorkDirectory
    private Path workDir;

    /**
     * Mock of a system clock.
     */
    private static MockedStatic<Clock> clockMock;

    @BeforeEach
    public void beforeTest(TestInfo testInfo) {
        List<NetworkAddress> localAddresses = findLocalAddresses(NODE_PORT_BASE, NODE_PORT_BASE + NODES);

        var nodeFinder = new StaticNodeFinder(localAddresses);

        var addr = new NetworkAddress("localhost", PORT);

        for (int i = 0; i < NODES; i++) {
            clusterServices.add(clusterService(PORT + i, List.of(addr), true));
        }

        for (ClusterService node : clusterServices) {
            assertTrue(waitForTopology(node, NODES, 1000));
        }

        LOG.info("Cluster started.");

        executor = new ScheduledThreadPoolExecutor(20, new NamedThreadFactory(Loza.CLIENT_POOL_NAME, LOG));
    }

    @AfterEach
    public void afterEach() {
        if (clockMock != null && !clockMock.isClosed()) {
            clockMock.close();
        }
    }

    @Test
    public void test() throws ExecutionException, InterruptedException {
        List<Peer> peers = new ArrayList<>();
        List<HybridClock> clocks = new ArrayList<>();
        List<HybridClock> safeTimeClocks = new ArrayList<>();
        List<NodeOptions> nodesOptions = new ArrayList<>();
        List<RaftServer> servers = new ArrayList<>();
        List<RaftGroupOptions> groupOptionsList = new ArrayList<>();
        List<RaftGroupService> services = new ArrayList<>();

        HashMap<Path, Integer> snapshotDataStorage = new HashMap<>();

        for (int i = 0; i < NODES; i++) {
            ClusterService clusterService = clusterServices.get(i);

            peers.add(new Peer(clusterService.topologyService().localMember().address()));

            HybridClock clock = new HybridClock();
            clocks.add(clock);

            HybridClock safeTimeClock = new HybridClock();
            safeTimeClocks.add(safeTimeClock);

            NodeOptions opts = new NodeOptions();
            //opts.setClock(clock);
            //opts.setSafeTimeClock(safeTimeClock);

            RaftServer srv = new JraftServerImpl(clusterService, workDir.resolve("node" + i), opts);
            srv.start();

            RaftGroupOptions groupOptions = forVolatileStores();
            groupOptions.replicationGroupOptions(new ReplicationGroupOptions().safeTimeClock(safeTimeClock));

            AtomicInteger counter = new AtomicInteger();

            srv.startRaftGroup("test", new UpdateCountRaftListener(counter, snapshotDataStorage), peers, groupOptions);

            RaftGroupService service = RaftGroupServiceImpl.start(
                RAFT_GROUP_NAME,
                clusterService,
                FACTORY,
                10_000,
                peers,
                true,
                200,
                executor
            ).get();
            services.add(service);
        }

        assertTrue(TestUtils.waitForCondition(
                    () -> sameLeaders(services), 10_000));

        Peer leader = services.get(0).leader();

        int leaderIndex = peers.indexOf(leader);

        clocks.get(leaderIndex).sync(new HybridTimestamp(100, 1));
        services.get(leaderIndex).run(new TestWriteCommand()).join();

        for (int i = 0; i < NODES; i++) {
            if (i != leaderIndex) {
                assertEquals(new HybridTimestamp(100, 1), safeTimeClocks.get(i));
            }
        }
    }

    /**
     * Checks if all raft groups have the same leader.
     *
     * @param groups Raft group services list.
     * @return {@code true} if all raft groups have the same leader.
     */
    private boolean sameLeaders(List<RaftGroupService> groups) {
        groups.forEach(RaftGroupService::refreshLeader);

        Peer leader0 = groups.get(0).leader();

        for (int i = 1; i < groups.size(); i++) {
            if (!Objects.equals(leader0, groups.get(i).leader())) {
                return false;
            }
        }

        return true;
    }

    private static class TestRaftListener implements RaftGroupListener {
        @Override public void onRead(Iterator<CommandClosure<ReadCommand>> iterator) {

        }

        @Override public void onWrite(Iterator<CommandClosure<WriteCommand>> iterator) {

        }

        @Override public void onSnapshotSave(Path path, Consumer<Throwable> doneClo) {

        }

        @Override public boolean onSnapshotLoad(Path path) {
            return false;
        }

        @Override public void onShutdown() {

        }
    }
}
