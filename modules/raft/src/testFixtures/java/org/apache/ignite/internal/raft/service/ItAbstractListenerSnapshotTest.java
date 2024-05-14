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

package org.apache.ignite.internal.raft.service;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.raft.server.RaftGroupOptions.defaults;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.MockitoTestUtils.tryCallRealMethod;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.network.utils.ClusterServiceTestUtils;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Marshaller;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftGroupServiceImpl;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.server.JraftServerUtils;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.replicator.TestReplicationGroupId;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.MockitoTestUtils;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Base class for persistent raft group's snapshots tests.
 *
 * @param <T> Type of the raft group listener.
 */
@ExtendWith(ConfigurationExtension.class)
public abstract class ItAbstractListenerSnapshotTest<T extends RaftGroupListener> extends IgniteAbstractTest {
    private static final IgniteLogger LOG = Loggers.forClass(ItAbstractListenerSnapshotTest.class);

    /** Starting server port. */
    private static final int PORT = 5003;

    /** Starting client port. */
    private static final int CLIENT_PORT = 6003;

    /** Factory. */
    private static final RaftMessagesFactory FACTORY = new RaftMessagesFactory();

    /** Initial Raft configuration. */
    private PeersAndLearners initialMemberConf;

    /** Cluster. */
    private final List<ClusterService> cluster = new ArrayList<>();

    /** Servers. */
    private final List<JraftServerImpl> servers = new ArrayList<>();

    /** Clients. */
    private final List<RaftGroupService> clients = new ArrayList<>();

    /** Executor for raft group services. */
    private ScheduledExecutorService executor;

    @InjectConfiguration
    private RaftConfiguration raftConfiguration;

    /**
     * Create executor for raft group services.
     */
    @BeforeEach
    public void beforeTest(TestInfo testInfo) {
        executor = new ScheduledThreadPoolExecutor(20, new NamedThreadFactory(Loza.CLIENT_POOL_NAME, LOG));

        initialMemberConf = IntStream.range(0, nodes())
                .mapToObj(i -> testNodeName(testInfo, PORT + i))
                .collect(collectingAndThen(toSet(), PeersAndLearners::fromConsistentIds));
    }

    /**
     * Shutdown raft server, executor for raft group services and stop all cluster nodes.
     *
     * @throws Exception If failed to shutdown raft server,
     */
    @AfterEach
    public void afterTest() throws Exception {
        Stream<AutoCloseable> stopRaftGroups = servers.stream().map(s -> () -> s.stopRaftNodes(raftGroupId()));

        Stream<AutoCloseable> shutdownClients = clients.stream().map(c -> c::shutdown);

        Stream<AutoCloseable> stopExecutor = Stream.of(() -> IgniteUtils.shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS));

        Stream<AutoCloseable> beforeNodeStop = Stream.concat(servers.stream(), cluster.stream()).map(c -> c::beforeNodeStop);

        List<IgniteComponent> components = Stream.concat(servers.stream(), cluster.stream()).collect(toList());

        Stream<AutoCloseable> nodeStop = Stream.of(() -> assertThat(stopAsync(components), willCompleteSuccessfully()));

        IgniteUtils.closeAll(
                Stream.of(stopRaftGroups, shutdownClients, stopExecutor, beforeNodeStop, nodeStop).flatMap(Function.identity())
        );
    }

    /**
     * Nodes count.
     *
     * @return Nodes count.
     */
    protected int nodes() {
        return 3;
    }

    /**
     * Returns a list of started servers.
     */
    protected List<JraftServerImpl> servers() {
        return List.copyOf(servers);
    }

    /**
     * Test parameters for {@link #testSnapshot}.
     */
    private static class TestData {
        /**
         * {@code true} if the raft group's persistence must be cleared before the follower's restart, {@code false} otherwise.
         */
        private final boolean deleteFolder;

        /**
         * {@code true} if test should interact with the raft group after a snapshot has been captured. In this case, the follower node
         * should catch up with the leader using raft log.
         */
        private final boolean interactAfterSnapshot;

        /**
         * Constructor.
         *
         * @param deleteFolder {@code true} if the raft group's persistence must be cleared before the follower's restart.
         * @param interactAfterSnapshot {@code true} if test should interact with the raft group after a snapshot has been captured.
         *      In this case, the follower node should catch up with the leader using raft log.
         */
        private TestData(boolean deleteFolder, boolean interactAfterSnapshot) {
            this.deleteFolder = deleteFolder;
            this.interactAfterSnapshot = interactAfterSnapshot;
        }

        /** {@inheritDoc} */
        @Override
        public String toString() {
            return String.format("deleteFolder=%s, interactAfterSnapshot=%s", deleteFolder, interactAfterSnapshot);
        }
    }

    /**
     * Returns {@link #testSnapshot} parameters.
     */
    private static List<TestData> testSnapshotData() {
        return List.of(
                new TestData(false, false),
                new TestData(true, true),
                new TestData(false, true),
                new TestData(true, false)
        );
    }

    /**
     * Tests that a joining raft node successfully restores a snapshot.
     *
     * @param testData Test parameters.
     * @param testInfo Test info.
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @MethodSource("testSnapshotData")
    public void testSnapshot(TestData testData, TestInfo testInfo) throws Exception {
        // Set up a raft group service
        RaftGroupService service = prepareRaftGroup(testInfo);

        CompletableFuture<Void> refreshLeaderFuture = service.refreshLeader()
                .thenCompose(v -> {
                    if (service.leader() == null) {
                        return service.refreshLeader();
                    } else {
                        return nullCompletedFuture();
                    }
                });

        assertThat(refreshLeaderFuture, willCompleteSuccessfully());

        // Select any node that is not the leader of the group
        JraftServerImpl toStop = servers.stream()
                .filter(server -> !server.localPeers(raftGroupId()).contains(service.leader()))
                .findAny()
                .orElseThrow();

        beforeFollowerStop(service, toStop);

        var nodeId = new RaftNodeId(raftGroupId(), toStop.localPeers(raftGroupId()).get(0));

        // Get the path to that node's raft directory
        Path serverDataPath = toStop.getServerDataPath(nodeId);

        // Get the path to that node's RocksDB key-value storage
        Path dbPath = getListenerPersistencePath(getListener(toStop, raftGroupId()), toStop);

        int stopIdx = servers.indexOf(toStop);

        // Remove that node from the list of servers
        servers.remove(stopIdx);

        // Shutdown that node
        toStop.stopRaftNode(nodeId);
        toStop.beforeNodeStop();
        assertThat(toStop.stopAsync(), willCompleteSuccessfully());

        // Create a snapshot of the raft group
        service.snapshot(service.leader()).get();

        afterFollowerStop(service, toStop, stopIdx);

        // Create another raft snapshot
        service.snapshot(service.leader()).get();

        if (testData.deleteFolder) {
            // Delete a stopped node's raft directory and key-value storage directory
            // to check if snapshot could be restored by the restarted node
            IgniteUtils.deleteIfExists(dbPath);
            IgniteUtils.deleteIfExists(serverDataPath);
        }

        if (testData.interactAfterSnapshot) {
            // Interact with the raft group after the second snapshot to check if the restarted node would see these
            // interactions after restoring a snapshot and raft logs
            afterSnapshot(service);
        }

        // Restart the node
        JraftServerImpl restarted = startServer(testInfo, stopIdx);

        assertTrue(waitForTopology(cluster.get(0), servers.size(), 3_000));

        BooleanSupplier closure = snapshotCheckClosure(restarted, testData.interactAfterSnapshot);

        boolean success = waitForCondition(closure, 10_000);

        assertTrue(success);
    }

    /**
     * Interacts with the raft group before a follower is stopped.
     *
     * @param service Raft group service.
     * @param server Raft server that is going to be stopped.
     * @throws Exception If failed.
     */
    public abstract void beforeFollowerStop(RaftGroupService service, RaftServer server) throws Exception;

    /**
     * Interacts with the raft group after a follower is stopped.
     *
     * @param service Raft group service.
     * @param server Raft server that has been stopped.
     * @param stoppedNodeIndex index of the stopped node.
     * @throws Exception If failed.
     */
    public abstract void afterFollowerStop(RaftGroupService service, RaftServer server, int stoppedNodeIndex) throws Exception;

    /**
     * Interacts with a raft group after the leader has captured a snapshot.
     *
     * @param service Raft group service.
     * @throws Exception If failed.
     */
    public abstract void afterSnapshot(RaftGroupService service) throws Exception;

    /**
     * Creates a closure that will be executed periodically to check if the snapshot and (conditionally on the {@link
     * TestData#interactAfterSnapshot}) the raft log was successfully restored by the follower node.
     *
     * @param restarted               Restarted follower node.
     * @param interactedAfterSnapshot {@code true} whether raft group was interacted with after the snapshot operation.
     * @return Closure.
     */
    public abstract BooleanSupplier snapshotCheckClosure(JraftServerImpl restarted, boolean interactedAfterSnapshot);

    /**
     * Returns path to the group's persistence.
     *
     * @param listener Raft group listener.
     * @param server Raft server, where the listener has been registered.
     * @return Path to the group's persistence.
     */
    public abstract Path getListenerPersistencePath(T listener, RaftServer server);

    /**
     * Creates raft group listener.
     *
     * @param service                 The cluster service.
     * @param listenerPersistencePath Path to storage persistent data.
     * @param index                   Index of node for which the listener is created.
     * @return Raft group listener.
     */
    public abstract RaftGroupListener createListener(ClusterService service, Path listenerPersistencePath, int index);

    /**
     * Returns raft group id for tests.
     */
    public abstract TestReplicationGroupId raftGroupId();

    /**
     * Get the raft group listener from the jraft server.
     *
     * @param server Server.
     * @param grpId  Raft group id.
     * @return Raft group listener.
     */
    protected T getListener(JraftServerImpl server, TestReplicationGroupId grpId) {
        var nodeId = new RaftNodeId(grpId, server.localPeers(grpId).get(0));

        org.apache.ignite.raft.jraft.RaftGroupService svc = server.raftGroupService(nodeId);

        JraftServerImpl.DelegatingStateMachine fsm =
                (JraftServerImpl.DelegatingStateMachine) svc.getRaftNode().getOptions().getFsm();

        return (T) fsm.getListener();
    }

    /**
     * Wait for topology.
     *
     * @param cluster The cluster.
     * @param exp     Expected count.
     * @param timeout The timeout in millis.
     * @return {@code True} if topology size is equal to expected.
     */
    private boolean waitForTopology(ClusterService cluster, int exp, int timeout) throws InterruptedException {
        return waitForCondition(() -> cluster.topologyService().allMembers().size() >= exp, timeout);
    }

    /**
     * Returns local address.
     */
    private static String getLocalAddress() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a cluster service.
     */
    private ClusterService clusterService(TestInfo testInfo, int port, NetworkAddress otherPeer) {
        var network = ClusterServiceTestUtils.clusterService(
                testInfo,
                port,
                new StaticNodeFinder(List.of(otherPeer))
        );

        assertThat(network.startAsync(), willCompleteSuccessfully());

        cluster.add(network);

        return network;
    }

    /**
     * Starts a raft server.
     *
     * @param testInfo Test info.
     * @param idx      Server index (affects port of the server).
     * @return Server.
     */
    private JraftServerImpl startServer(TestInfo testInfo, int idx) {
        var addr = new NetworkAddress(getLocalAddress(), PORT);

        ClusterService service = clusterService(testInfo, PORT + idx, addr);

        Path jraft = workDir.resolve("jraft" + idx);

        JraftServerImpl server = MockitoTestUtils.spyStubOnly(() -> JraftServerUtils.create(service, jraft, raftConfiguration));
        doAnswer(ans -> IgniteUtils.stopAsync(() -> tryCallRealMethod(ans), service::stopAsync)).when(server).stopAsync();

        assertThat(server.startAsync(), willCompleteSuccessfully());

        Path listenerPersistencePath = workDir.resolve("db" + idx);

        servers.add(server);

        server.startRaftNode(
                new RaftNodeId(raftGroupId(), initialMemberConf.peer(service.topologyService().localMember().name())),
                initialMemberConf,
                createListener(service, listenerPersistencePath, idx),
                defaults().commandsMarshaller(commandsMarshaller(service))
        );

        return server;
    }

    /**
     * Prepares raft group service by instantiating raft servers and a client.
     *
     * @return Raft group service instance.
     */
    private RaftGroupService prepareRaftGroup(TestInfo testInfo) throws Exception {
        for (int i = 0; i < initialMemberConf.peers().size(); i++) {
            startServer(testInfo, i);
        }

        assertTrue(waitForTopology(cluster.get(0), servers.size(), 3_000));

        return startClient(testInfo, raftGroupId(), new NetworkAddress(getLocalAddress(), PORT));
    }

    protected abstract Marshaller commandsMarshaller(ClusterService clusterService);

    /**
     * Starts a client with a specific address.
     *
     * @return The service.
     */
    private RaftGroupService startClient(TestInfo testInfo, TestReplicationGroupId groupId, NetworkAddress addr) {
        ClusterService clientNode = clusterService(testInfo, CLIENT_PORT + clients.size(), addr);

        Marshaller commandsMarshaller = commandsMarshaller(clientNode);

        CompletableFuture<RaftGroupService> clientFuture = RaftGroupServiceImpl
                .start(groupId, clientNode, FACTORY, raftConfiguration, initialMemberConf, true, executor, commandsMarshaller);

        assertThat(clientFuture, willCompleteSuccessfully());

        clients.add(clientFuture.join());

        return clientFuture.join();
    }
}
