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

package org.apache.ignite.internal.raft.client;

import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.network.utils.ClusterServiceTestUtils.clusterService;
import static org.apache.ignite.internal.network.utils.ClusterServiceTestUtils.findLocalAddresses;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.configuration.SystemConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.raft.LeaderElectionListener;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.TestRaftGroupListener;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.raft.server.TestJraftServerFactory;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.raft.util.ThreadLocalOptimizedMarshaller;
import org.apache.ignite.internal.replicator.TestReplicationGroupId;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.topology.LogicalTopologyServiceTestImpl;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.jraft.RaftMessageGroup;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.rpc.CliRequests.LeaderChangeNotification;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Abstract class containing test scenarios for {@link TopologyAwareRaftGroupService} related test classes.
 */
@ExtendWith(ConfigurationExtension.class)
public abstract class AbstractTopologyAwareGroupServiceTest extends IgniteAbstractTest {
    /** RAFT message factory. */
    private static final RaftMessagesFactory FACTORY = new RaftMessagesFactory();

    /** Base node port. */
    private static final int PORT_BASE = 1234;

    /** Wait timeout, in milliseconds. */
    protected static final int WAIT_TIMEOUT_MILLIS = 10_000;

    protected static final TestReplicationGroupId GROUP_ID = new TestReplicationGroupId("group_1");

    /** RPC executor. */
    protected final ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(
            20,
            NamedThreadFactory.create("Test", "Raft-Group-Client", log)
    );

    private final Map<NetworkAddress, ClusterService> clusterServices = new HashMap<>();

    private final Map<NetworkAddress, JraftServerImpl> raftServers = new HashMap<>();

    private final List<TopologyAwareRaftGroupService> raftClients = new ArrayList<>();

    @InjectConfiguration
    protected RaftConfiguration raftConfiguration;

    @InjectConfiguration
    protected SystemConfiguration systemConfiguration;

    @AfterEach
    protected void tearDown() throws Exception {
        IgniteUtils.shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);

        stopCluster();
    }

    /**
     * The method is called after every node of the cluster starts.
     *
     * @param nodeName Node name.
     * @param clusterService Cluster service.
     * @param dataPath Data path for raft node.
     * @param peersAndLearners Peers and learners.
     * @param eventsClientListener Raft events listener for client.
     * @param logicalTopologyService Logical topology service.
     */
    protected abstract void afterNodeStart(
            String nodeName,
            ClusterService clusterService,
            Path dataPath,
            PeersAndLearners peersAndLearners,
            RaftGroupEventsClientListener eventsClientListener,
            LogicalTopologyService logicalTopologyService
    );

    /**
     * Checks the condition after cluster and raft clients initialization.
     *
     * @param leaderName Current leader name.
     */
    protected abstract void afterClusterInit(String leaderName) throws InterruptedException;

    /**
     * Checks the condition after leader change.
     *
     * @param leaderName Current leader name.
     */
    protected abstract void afterLeaderChange(String leaderName) throws InterruptedException;

    /**
     * The method is called after every node of the cluster stops.
     *
     * @param nodeName Node name.
     */
    protected abstract void afterNodeStop(String nodeName) throws Exception;

    @Test
    public void testOneNodeReplicationGroup(TestInfo testInfo) throws Exception {
        int nodes = 2;

        TopologyAwareRaftGroupService raftClient = startCluster(
                testInfo,
                addr -> true,
                nodes,
                PORT_BASE + 1
        );

        assertNotNull(raftClient);

        CompletableFuture<ClusterNode> leaderFut = new CompletableFuture<>();

        subscribeLeader(raftClient, (node, term) -> leaderFut.complete(node), "New leader: {}");

        ClusterNode leader = leaderFut.get(WAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

        assertNotNull(leader);

        afterClusterInit(leader.name());
    }

    /**
     * Starts the cluster, Raft clients and subscribes them to leader change notifications. Returns a pair of two topology aware Raft
     * clients. First one will get all leader change notifications, including the initial one (when joining the cluster). Second
     * one will get all notifications without the initial one.
     *
     * @param testInfo Test info.
     * @param leaderRef Atomic reference where the current leader will be put by notification listener of the first raft client.
     * @param leaderRefNoInitialNotify Atomic reference where the current leader will be put by notification listener of the second raft
     *     client.
     * @return Raft clients.
     */
    private IgniteBiTuple<TopologyAwareRaftGroupService, TopologyAwareRaftGroupService> startClusterWithClientsAndSubscribeToLeaderChange(
            TestInfo testInfo,
            AtomicReference<ClusterNode> leaderRef,
            AtomicReference<ClusterNode> leaderRefNoInitialNotify
    ) throws Exception {
        int nodes = 3;

        assertTrue(clusterServices.isEmpty());
        assertTrue(raftServers.isEmpty());

        Predicate<NetworkAddress> isServerAddress = addr -> true;

        // Start cluster and the first topology aware client.
        TopologyAwareRaftGroupService raftClient = startCluster(
                testInfo,
                isServerAddress,
                nodes,
                PORT_BASE
        );

        assertNotNull(raftClient);

        raftClient.refreshLeader().get();

        // Start client service for the second client.
        int clientPort = PORT_BASE + nodes + 1;
        ClusterService clientClusterService =
                clusterService(testInfo, clientPort, new StaticNodeFinder(findLocalAddresses(PORT_BASE, PORT_BASE + nodes)));
        assertThat(clientClusterService.startAsync(new ComponentContext()), willCompleteSuccessfully());

        // Start the second topology aware client, that should not get the initial leader notification.
        TopologyAwareRaftGroupService raftClientNoInitialNotify = startTopologyAwareClient(
                clientClusterService,
                clusterServices,
                isServerAddress,
                nodes,
                null,
                new LogicalTopologyServiceTestImpl(clientClusterService),
                false
        );

        List<NetworkAddress> clientAddress = findLocalAddresses(clientPort, clientPort + 1);
        assertEquals(1, clientAddress.size());
        clusterServices.put(clientAddress.get(0), clientClusterService);

        AtomicInteger callsCount = new AtomicInteger();

        // Subscribing clients.
        subscribeLeader(raftClient, (node, term) -> leaderRef.set(node), "New leader: {}");

        for (int i = 0; i < 2; i++) {
            unsubscribeLeader(raftClientNoInitialNotify);

            subscribeLeader(raftClientNoInitialNotify, (node, term) -> {
                callsCount.incrementAndGet();
                leaderRefNoInitialNotify.set(node);
            }, "New leader (client without initial notification): {}");
        }

        // Checking invariants.
        assertTrue(callsCount.get() <= 1);

        assertTrue(waitForCondition(() -> leaderRef.get() != null, WAIT_TIMEOUT_MILLIS));

        ClusterNode leader = leaderRef.get();

        assertNotNull(leader);

        log.info("Leader: " + leader);

        afterClusterInit(leader.name());

        raftClients.add(raftClientNoInitialNotify);

        return new IgniteBiTuple<>(raftClient, raftClientNoInitialNotify);
    }

    @Test
    public void testChangeLeaderWhenActualLeft(TestInfo testInfo) throws Exception {
        AtomicReference<ClusterNode> leaderRef = new AtomicReference<>();
        AtomicReference<ClusterNode> leaderRefNoInitialNotify = new AtomicReference<>();

        IgniteBiTuple<TopologyAwareRaftGroupService, TopologyAwareRaftGroupService> raftClients =
                startClusterWithClientsAndSubscribeToLeaderChange(
                    testInfo,
                    leaderRef,
                    leaderRefNoInitialNotify
        );

        TopologyAwareRaftGroupService raftClientNoInitialNotify = raftClients.get2();

        ClusterNode leader = leaderRef.get();

        assertNull(leaderRefNoInitialNotify.get());

        // Forcing the leader change by stopping the actual leader.
        var raftServiceToStop = raftServers.remove(new NetworkAddress("localhost", leader.address().port()));
        raftServiceToStop.stopRaftNodes(GROUP_ID);
        ComponentContext componentContext = new ComponentContext();
        assertThat(raftServiceToStop.stopAsync(componentContext), willCompleteSuccessfully());

        afterNodeStop(leader.name());

        CompletableFuture<Void> stopFuture =
                clusterServices.remove(new NetworkAddress("localhost", leader.address().port()))
                        .stopAsync(componentContext);
        assertThat(stopFuture, willCompleteSuccessfully());

        // Waiting for the notifications to check.
        assertTrue(waitForCondition(() -> !leader.equals(leaderRef.get()), WAIT_TIMEOUT_MILLIS));
        assertTrue(waitForCondition(() -> !leader.equals(leaderRefNoInitialNotify.get()), WAIT_TIMEOUT_MILLIS));

        log.info("New Leader: " + leaderRef.get());

        afterLeaderChange(leaderRef.get().name());

        raftClientNoInitialNotify.refreshLeader().get();

        assertEquals(raftClientNoInitialNotify.leader().consistentId(), leaderRef.get().name());
    }

    @Test
    public void testChangeLeaderForce(TestInfo testInfo) throws Exception {
        AtomicReference<ClusterNode> leaderRef = new AtomicReference<>();
        AtomicReference<ClusterNode> leaderRefNoInitialNotify = new AtomicReference<>();

        IgniteBiTuple<TopologyAwareRaftGroupService, TopologyAwareRaftGroupService> raftClients =
                startClusterWithClientsAndSubscribeToLeaderChange(
                        testInfo,
                        leaderRef,
                        leaderRefNoInitialNotify
                );

        TopologyAwareRaftGroupService raftClient = raftClients.get1();

        ClusterNode leader = leaderRef.get();

        assertNull(leaderRefNoInitialNotify.get());

        // Forcing the leader change by transferring leadership.
        Peer newLeaderPeer = raftClient.peers().stream().filter(peer -> !leader.name().equals(peer.consistentId())).findAny().get();

        log.info("Peer to transfer leader: " + newLeaderPeer);

        raftClient.transferLeadership(newLeaderPeer).get();

        String leaderId = newLeaderPeer.consistentId();

        // Waiting for the notifications to check.
        assertTrue(waitForCondition(() -> leaderId.equals(leaderRef.get().name()), WAIT_TIMEOUT_MILLIS));
        assertTrue(waitForCondition(
                () -> leaderRefNoInitialNotify.get() != null && leaderId.equals(leaderRefNoInitialNotify.get().name()),
                WAIT_TIMEOUT_MILLIS
        ));

        log.info("New Leader: " + leaderRef.get());

        afterLeaderChange(leaderRef.get().name());

        raftClient.refreshLeader().get();

        assertEquals(raftClient.leader().consistentId(), leaderRef.get().name());
    }

    /**
     * Stops cluster.
     *
     * @throws Exception If failed.
     */
    private void stopCluster() throws Exception {
        if (!CollectionUtils.nullOrEmpty(raftClients)) {
            raftClients.forEach(TopologyAwareRaftGroupService::shutdown);

            raftClients.clear();
        }

        ComponentContext componentContext = new ComponentContext();

        for (NetworkAddress addr : clusterServices.keySet()) {
            if (raftServers.containsKey(addr)) {
                raftServers.get(addr).stopRaftNodes(GROUP_ID);

                assertThat(raftServers.get(addr).stopAsync(componentContext), willCompleteSuccessfully());
            }

            assertThat(clusterServices.get(addr).stopAsync(componentContext), willCompleteSuccessfully());
        }

        raftServers.clear();
        clusterServices.clear();
    }

    /**
     * Starts cluster.
     *
     * @param testInfo        Test info.
     * @param isServerAddress Closure to determine a server node.
     * @param nodes           Node count.
     * @param clientPort      Port of node where a client will start.
     * @return Topology aware client.
     */
    private @Nullable TopologyAwareRaftGroupService startCluster(
            TestInfo testInfo,
            Predicate<NetworkAddress> isServerAddress,
            int nodes,
            int clientPort
    ) {
        List<NetworkAddress> addresses = findLocalAddresses(PORT_BASE, PORT_BASE + nodes);

        var nodeFinder = new StaticNodeFinder(addresses);

        TopologyAwareRaftGroupService raftClient = null;

        for (NetworkAddress addr : addresses) {
            var cluster = clusterService(testInfo, addr.port(), nodeFinder);

            assertThat(cluster.startAsync(new ComponentContext()), willCompleteSuccessfully());

            clusterServices.put(addr, cluster);
        }

        PeersAndLearners peersAndLearners = peersAndLearners(clusterServices, isServerAddress, nodes);

        for (NetworkAddress addr : addresses) {
            ClusterService cluster = clusterServices.get(addr);

            LogicalTopologyService logicalTopologyService = new LogicalTopologyServiceTestImpl(cluster);

            RaftGroupEventsClientListener eventsClientListener = new RaftGroupEventsClientListener();

            if (isServerAddress.test(addr)) { // RAFT server node
                var localPeer = peersAndLearners.peers().stream()
                        .filter(peer -> peer.consistentId().equals(cluster.topologyService().localMember().name())).findAny().get();

                var dataPath = workDir.resolve("raft_" + localPeer.consistentId());

                var commandsMarshaller = new ThreadLocalOptimizedMarshaller(cluster.serializationRegistry());

                NodeOptions nodeOptions = new NodeOptions();
                nodeOptions.setCommandsMarshaller(commandsMarshaller);

                var raftServer = TestJraftServerFactory.create(
                        cluster,
                        dataPath,
                        systemConfiguration,
                        nodeOptions,
                        eventsClientListener
                );
                assertThat(raftServer.startAsync(new ComponentContext()), willCompleteSuccessfully());

                raftServer.startRaftNode(
                        new RaftNodeId(GROUP_ID, localPeer),
                        peersAndLearners,
                        new TestRaftGroupListener(),
                        RaftGroupOptions.defaults().commandsMarshaller(commandsMarshaller)
                );

                raftServers.put(addr, raftServer);

                afterNodeStart(localPeer.consistentId(), cluster, dataPath, peersAndLearners, eventsClientListener, logicalTopologyService);
            }

            if (addr.port() == clientPort) {
                assertTrue(isServerAddress.test(addr));

                raftClient = startTopologyAwareClient(cluster, clusterServices, isServerAddress, nodes, eventsClientListener,
                        logicalTopologyService, true);

                raftClients.add(raftClient);
            }
        }

        return raftClient;
    }

    private TopologyAwareRaftGroupService startTopologyAwareClient(
            ClusterService localClusterService,
            Map<NetworkAddress, ClusterService> clusterServices,
            Predicate<NetworkAddress> isServerAddress,
            int nodes,
            RaftGroupEventsClientListener eventsClientListener,
            LogicalTopologyService logicalTopologyService,
            boolean notifyOnSubscription
    ) {
        if (eventsClientListener == null) {
            eventsClientListener = new RaftGroupEventsClientListener();

            var finalEventsClientListener = eventsClientListener;
            localClusterService.messagingService().addMessageHandler(RaftMessageGroup.class, (msg, sender, correlationId) -> {
                if (msg instanceof LeaderChangeNotification) {
                    LeaderChangeNotification msg0 = (LeaderChangeNotification) msg;

                    finalEventsClientListener.onLeaderElected(msg0.groupId(), sender, msg0.term());
                }
            });
        }

        var commandsMarshaller = new ThreadLocalOptimizedMarshaller(localClusterService.serializationRegistry());

        return TopologyAwareRaftGroupService.start(
                GROUP_ID,
                localClusterService,
                FACTORY,
                raftConfiguration,
                peersAndLearners(clusterServices, isServerAddress, nodes),
                true,
                executor,
                logicalTopologyService,
                eventsClientListener,
                notifyOnSubscription,
                commandsMarshaller
        ).join();
    }

    private static PeersAndLearners peersAndLearners(
            Map<NetworkAddress, ClusterService> clusterServices,
            Predicate<NetworkAddress> isServerAddress,
            int nodes
    ) {
        return PeersAndLearners.fromConsistentIds(
                findLocalAddresses(PORT_BASE, PORT_BASE + nodes).stream().filter(isServerAddress)
                        .map(netAddr -> clusterServices.get(netAddr).topologyService().localMember().name()).collect(
                                toSet()));
    }

    private void subscribeLeader(TopologyAwareRaftGroupService client, LeaderElectionListener callback, String logMessage) {
        CompletableFuture<Void> future = client.subscribeLeader((node, term) -> {
            callback.onLeaderElected(node, term);

            log.info(logMessage, node);
        });

        assertThat(future, willCompleteSuccessfully());
    }

    private static void unsubscribeLeader(TopologyAwareRaftGroupService client) {
        CompletableFuture<Void> future = client.unsubscribeLeader();

        assertThat(future, willCompleteSuccessfully());
    }
}
