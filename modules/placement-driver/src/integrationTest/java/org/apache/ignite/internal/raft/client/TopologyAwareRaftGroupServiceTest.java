package org.apache.ignite.internal.raft.client;/*
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.ReadCommand;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.utils.ClusterServiceTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Topology aware client tests.
 */
public class TopologyAwareRaftGroupServiceTest extends IgniteAbstractTest {
    /** RAFT message factory. */
    private static final RaftMessagesFactory FACTORY = new RaftMessagesFactory();

    /** Base node port. */
    private static int PORT_BASE = 1234;

    /** Replication group id. */
    ReplicationGroupId GROUP_ID = new TestReplicationGroup();

    /** RPC executor. */
    private ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(20, new NamedThreadFactory("Raft-Group-Client", log));

    @Test
    public void testOneNodeReplicationGroup(TestInfo testInfo) throws Exception {
        var clusterServices = new HashMap<NetworkAddress, ClusterService>();
        var raftServers = new HashMap<NetworkAddress, JraftServerImpl>();

        TopologyAwareRaftGroupService raftClient = startCluster(
                testInfo,
                clusterServices,
                raftServers,
                addr -> addr.port() == PORT_BASE,
                2,
                PORT_BASE + 1
        );

        CompletableFuture<ClusterNode> leaderFut = new CompletableFuture<>();

        raftClient.subscribeLeader(node -> {
            leaderFut.complete(node);
        });

        ClusterNode leader = leaderFut.get(10, TimeUnit.SECONDS);

        assertNotNull(leader);
        assertEquals(PORT_BASE, leader.address().port());

        stopCluster(clusterServices, raftServers, raftClient, 2);
    }

    @Test
    public void testChangeLeader(TestInfo testInfo) throws Exception {
        var clusterServices = new HashMap<NetworkAddress, ClusterService>();
        var raftServers = new HashMap<NetworkAddress, JraftServerImpl>();

        TopologyAwareRaftGroupService raftClient = startCluster(
                testInfo,
                clusterServices,
                raftServers,
                addr -> addr.port() < PORT_BASE + 3,
                4,
                PORT_BASE + 3
        );

        AtomicReference<ClusterNode> leaderRef = new AtomicReference<>();

        raftClient.subscribeLeader(node -> {
            leaderRef.set(node);
        });

        assertTrue(IgniteTestUtils.waitForCondition(() -> leaderRef.get() != null, 10_000));

        ClusterNode leader = leaderRef.get();

        assertNotNull(leader);

        log.info("Leader: " + leader);

        var raftServiceToStop = raftServers.remove(new NetworkAddress("localhost", leader.address().port()));
        raftServiceToStop.stopRaftNodes(GROUP_ID);
        raftServiceToStop.stop();

        clusterServices.remove(new NetworkAddress("localhost", leader.address().port())).stop();

        assertTrue(IgniteTestUtils.waitForCondition(() -> !leader.equals(leaderRef.get()), 10_000));

        log.info("New Leader: " + leaderRef.get());

        raftClient.refreshLeader().get();

        assertEquals(raftClient.leader().consistentId(), leaderRef.get().name());

        stopCluster(clusterServices, raftServers, raftClient, 4);
    }

    /**
     * Stops cluster.
     *
     * @param clusterServices Cluster services.
     * @param raftServers     RAFT services.
     * @param raftClient      RAFT client.
     * @param nodes           Node count.
     * @throws Exception If failed.
     */
    private void stopCluster(
            HashMap<NetworkAddress, ClusterService> clusterServices,
            HashMap<NetworkAddress, JraftServerImpl> raftServers,
            TopologyAwareRaftGroupService raftClient,
            int nodes
    ) throws Exception {
        if (raftClient != null) {
            raftClient.shutdown();
        }

        for (NetworkAddress addr : getNetworkAddresses(nodes)) {
            if (raftServers.containsKey(addr)) {
                raftServers.get(addr).stopRaftNodes(GROUP_ID);

                raftServers.get(addr).stop();
            }

            if (clusterServices.containsKey(addr)) {
                clusterServices.get(addr).stop();
            }
        }
    }

    /**
     * Starts cluster.
     *
     * @param testInfo        Test info.
     * @param clusterServices Cluster services.
     * @param raftServers     RAFT services.
     * @param isServerAddress Closure to determine a server node.
     * @param nodes           Node count.
     * @param clientPort      Port of node where a client will start.
     * @return Topology aware client.
     */
    private TopologyAwareRaftGroupService startCluster(
            TestInfo testInfo,
            HashMap<NetworkAddress, ClusterService> clusterServices,
            HashMap<NetworkAddress, JraftServerImpl> raftServers,
            Predicate<NetworkAddress> isServerAddress,
            int nodes,
            int clientPort
    ) {
        List<NetworkAddress> addresses = getNetworkAddresses(nodes);

        var nodeFinder = new StaticNodeFinder(addresses);

        TopologyAwareRaftGroupService raftClient = null;

        for (NetworkAddress addr : addresses) {
            var cluster = ClusterServiceTestUtils.clusterService(testInfo, addr.port(), nodeFinder);

            cluster.start();

            clusterServices.put(addr, cluster);
        }

        for (NetworkAddress addr : addresses) {
            var cluster = clusterServices.get(addr);

            PeersAndLearners peersAndLearners = PeersAndLearners.fromConsistentIds(
                    addresses.stream().filter(isServerAddress)
                            .map(netAddr -> clusterServices.get(netAddr).topologyService().localMember().name()).collect(
                                    Collectors.toSet()));

            if (isServerAddress.test(addr)) { //RAFT server node
                var localPeer = peersAndLearners.peers().stream()
                        .filter(peer -> peer.consistentId().equals(cluster.topologyService().localMember().name())).findAny().get();

                var raftServer = new JraftServerImpl(cluster, workDir.resolve("raft_" + localPeer.consistentId()), new NodeOptions());
                raftServer.start();

                raftServer.startRaftNode(
                        new RaftNodeId(GROUP_ID, localPeer),
                        peersAndLearners,
                        new TestRaftGroupListener(),
                        RaftGroupOptions.defaults()
                );

                raftServers.put(addr, raftServer);
            }

            if (addr.port() == clientPort) {
                raftClient = (TopologyAwareRaftGroupService) TopologyAwareRaftGroupService.start(
                        GROUP_ID,
                        cluster,
                        FACTORY,
                        3_000,
                        1_000,
                        peersAndLearners,
                        true,
                        200,
                        executor,
                        new LogicalTopologyServiceTestImpl(cluster)
                ).join();
            }
        }
        return raftClient;
    }

    /**
     * Generates a node address for each node.
     *
     * @param nodes Node count.
     * @return List on network addresses.
     */
    private static List<NetworkAddress> getNetworkAddresses(int nodes) {
        List<NetworkAddress> addresses = IntStream.range(PORT_BASE, PORT_BASE + nodes)
                .mapToObj(port -> new NetworkAddress("localhost", port))
                .collect(Collectors.toList());
        return addresses;
    }

    public static class TestReplicationGroup implements ReplicationGroupId {
        /** {@inheritDoc} */
        @Override
        public String toString() {
            return "TestReplicationGroup";
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof TestReplicationGroup;
        }

        @Override
        public int hashCode() {
            return 1;
        }
    }

    private static class TestRaftGroupListener implements RaftGroupListener {
        @Override
        public void onWrite(Iterator<CommandClosure<WriteCommand>> iterator) {
            iterator.forEachRemaining(closure -> {
                closure.result(null);
            });
        }

        @Override
        public void onRead(Iterator<CommandClosure<ReadCommand>> iterator) {
        }

        @Override
        public void onSnapshotSave(Path path, Consumer<Throwable> doneClo) {
        }

        @Override
        public boolean onSnapshotLoad(Path path) {
            return true;
        }

        @Override
        public void onShutdown() {
        }
    }

    private static class LogicalTopologyServiceTestImpl implements LogicalTopologyService {
        private final ClusterService clusterService;

        public LogicalTopologyServiceTestImpl(ClusterService clusterService) {
            this.clusterService = clusterService;
        }

        @Override
        public void addEventListener(LogicalTopologyEventListener listener) {

        }

        @Override
        public void removeEventListener(LogicalTopologyEventListener listener) {

        }

        @Override
        public CompletableFuture<LogicalTopologySnapshot> logicalTopologyOnLeader() {
            return CompletableFuture.completedFuture(new LogicalTopologySnapshot(1, clusterService.topologyService().allMembers()));
        }
    }
}
