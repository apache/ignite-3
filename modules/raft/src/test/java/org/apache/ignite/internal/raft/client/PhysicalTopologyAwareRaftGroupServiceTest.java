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
import static org.apache.ignite.internal.raft.TestThrottlingContextHolder.throttlingContextHolder;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.failure.handlers.NoOpFailureHandler;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.StoppingExceptionFactories;
import org.apache.ignite.internal.raft.TestRaftGroupListener;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.raft.server.TestJraftServerFactory;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.internal.raft.util.SharedLogStorageFactoryUtils;
import org.apache.ignite.internal.raft.util.ThreadLocalOptimizedMarshaller;
import org.apache.ignite.internal.replicator.TestReplicationGroupId;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.internal.util.IgniteUtils;
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
 * Tests for {@link PhysicalTopologyAwareRaftGroupService}.
 */
@ExtendWith(ConfigurationExtension.class)
public class PhysicalTopologyAwareRaftGroupServiceTest extends IgniteAbstractTest {
    /** RAFT message factory. */
    private static final RaftMessagesFactory FACTORY = new RaftMessagesFactory();

    /** Base node port. */
    private static final int PORT_BASE = 1234;

    private static final TestReplicationGroupId GROUP_ID = new TestReplicationGroupId("group_1");

    private static final FailureManager NOOP_FAILURE_PROCESSOR = new FailureManager(new NoOpFailureHandler());

    /** RPC executor. */
    private final ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(
            20,
            IgniteThreadFactory.create("Test", "Raft-Group-Client", log)
    );

    private final Map<NetworkAddress, ClusterService> clusterServices = new HashMap<>();

    private final Map<NetworkAddress, JraftServerImpl> raftServers = new HashMap<>();

    private final Map<NetworkAddress, LogStorageFactory> logStorageFactories = new HashMap<>();

    private final List<PhysicalTopologyAwareRaftGroupService> raftClients = new ArrayList<>();

    @InjectConfiguration
    private RaftConfiguration raftConfiguration;

    @AfterEach
    public void afterTest() throws Exception {
        IgniteUtils.shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);

        stopCluster();
    }

    @Test
    public void testOneNodeReplicationGroup(TestInfo testInfo) throws Exception {
        int nodes = 2;

        PhysicalTopologyAwareRaftGroupService raftClient = startCluster(
                testInfo,
                addr -> true,
                nodes,
                PORT_BASE + 1
        );

        assertNotNull(raftClient);

        CompletableFuture<InternalClusterNode> leaderFut = new CompletableFuture<>();

        raftClient.subscribeLeader((node, term) -> {
            log.info("Received leader node: {}", node);

            leaderFut.complete(node);
        });

        assertThat(leaderFut, willCompleteSuccessfully());

        InternalClusterNode leader = leaderFut.get();

        assertNotNull(leader);

        // Below we check that leaderElectionCallback is called once only.
        AtomicInteger leaderElectionCallbackCallsCounter = new AtomicInteger(0);
        raftClient.subscribeLeader((leader0, term) -> leaderElectionCallbackCallsCounter.incrementAndGet());

        // Leader election callback triggering is asynchronous, thus it's required to give it some time to be called.
        await().until(() -> leaderElectionCallbackCallsCounter.get() == 1);

        assertEquals(1, leaderElectionCallbackCallsCounter.get());
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
    private IgniteBiTuple<PhysicalTopologyAwareRaftGroupService, PhysicalTopologyAwareRaftGroupService> startClusterWithClientsAndSubscribe(
            TestInfo testInfo,
            AtomicReference<InternalClusterNode> leaderRef,
            AtomicReference<InternalClusterNode> leaderRefNoInitialNotify
    ) {
        int nodes = 3;

        assertTrue(clusterServices.isEmpty());
        assertTrue(raftServers.isEmpty());

        Predicate<NetworkAddress> isServerAddress = addr -> true;

        // Start cluster and the first topology aware client.
        PhysicalTopologyAwareRaftGroupService firstRaftClient = startCluster(
                testInfo,
                isServerAddress,
                nodes,
                PORT_BASE
        );

        assertNotNull(firstRaftClient);

        assertThat(firstRaftClient.refreshLeader(), willCompleteSuccessfully());

        // Start client service for the second client.
        int clientPort = PORT_BASE + nodes + 1;
        ClusterService clientClusterService =
                clusterService(testInfo, clientPort, new StaticNodeFinder(findLocalAddresses(PORT_BASE, PORT_BASE + nodes)));
        assertThat(clientClusterService.startAsync(new ComponentContext()), willCompleteSuccessfully());

        // Start the second topology aware client, that should not get the initial leader notification.
        PhysicalTopologyAwareRaftGroupService secondRaftClient = startTopologyAwareClient(
                clientClusterService,
                peersAndLearners(testInfo, isServerAddress, nodes),
                null
        );

        List<NetworkAddress> clientAddress = findLocalAddresses(clientPort, clientPort + 1);
        assertEquals(1, clientAddress.size());
        clusterServices.put(clientAddress.get(0), clientClusterService);

        AtomicInteger callsCount = new AtomicInteger();

        // Subscribing clients.
        firstRaftClient.subscribeLeader((leader, term) -> {
            log.info("First client got notification [node={}, term={}].", leader, term);

            leaderRef.set(leader);
        });

        secondRaftClient.subscribeLeader((node, term) -> {
            log.info("Second client got notification [node={}, term={}].", node, term);

            callsCount.incrementAndGet();
            leaderRefNoInitialNotify.set(node);
        });

        await().until(() -> leaderRef.get() != null);

        InternalClusterNode leader = leaderRef.get();

        log.info("Leader: " + leader);

        // Checking invariants.
        await().until(() -> leaderRef.get().equals(leaderRefNoInitialNotify.get()));
        assertEquals(1, callsCount.get());

        raftClients.add(secondRaftClient);

        return new IgniteBiTuple<>(firstRaftClient, secondRaftClient);
    }

    @Test
    public void testChangeLeaderWhenActualLeft(TestInfo testInfo) throws Exception {
        AtomicReference<InternalClusterNode> firstLeaderRef = new AtomicReference<>();
        AtomicReference<InternalClusterNode> secondLeaderRef = new AtomicReference<>();

        IgniteBiTuple<PhysicalTopologyAwareRaftGroupService, PhysicalTopologyAwareRaftGroupService> raftClients =
                startClusterWithClientsAndSubscribe(
                        testInfo,
                        firstLeaderRef,
                        secondLeaderRef
                );

        PhysicalTopologyAwareRaftGroupService secondRaftClient = raftClients.get2();

        InternalClusterNode leader = firstLeaderRef.get();

        // Forcing the leader change by stopping the actual leader.
        var addressToStop = new NetworkAddress("localhost", leader.address().port());

        var raftServerToStop = raftServers.remove(addressToStop);

        raftServerToStop.stopRaftNodes(GROUP_ID);

        assertThat(
                stopAsync(
                        new ComponentContext(),
                        raftServerToStop,
                        logStorageFactories.remove(addressToStop),
                        clusterServices.remove(addressToStop)
                ),
                willCompleteSuccessfully()
        );

        // Waiting for the notifications to check.
        if (leader.address().port() != PORT_BASE) {
            // leaderRef is updated through raftClient hosted on PORT_BASE, thus if corresponding node was stopped (and it will be stopped
            // if it occurred to be a leader) leaderRef won't be updated.
            await().until(() -> !leader.equals(firstLeaderRef.get()));
        }

        await().until(() -> secondLeaderRef.get() != null && !leader.equals(secondLeaderRef.get()));

        log.info("New Leader: " + secondLeaderRef.get());

        secondRaftClient.refreshLeader().get();

        assertEquals(secondRaftClient.leader().consistentId(), secondLeaderRef.get().name());
    }

    @Test
    public void testChangeLeaderForce(TestInfo testInfo) throws Exception {
        AtomicReference<InternalClusterNode> leaderRef = new AtomicReference<>();
        AtomicReference<InternalClusterNode> secondLeaderRef = new AtomicReference<>();

        IgniteBiTuple<PhysicalTopologyAwareRaftGroupService, PhysicalTopologyAwareRaftGroupService> raftClients =
                startClusterWithClientsAndSubscribe(
                        testInfo,
                        leaderRef,
                        secondLeaderRef
                );

        PhysicalTopologyAwareRaftGroupService raftClient = raftClients.get1();

        InternalClusterNode leader = leaderRef.get();

        // Forcing the leader change by transferring leadership.
        Peer newLeaderPeer = raftClient.peers().stream().filter(peer -> !leader.name().equals(peer.consistentId())).findAny().get();

        log.info("Peer to transfer leader: " + newLeaderPeer);

        raftClient.transferLeadership(newLeaderPeer).get();

        String leaderId = newLeaderPeer.consistentId();

        // Waiting for the notifications to check.
        await().until(() -> leaderId.equals(leaderRef.get().name()));
        await().until(() -> secondLeaderRef.get() != null && leaderId.equals(secondLeaderRef.get().name()));

        log.info("New Leader: " + leaderRef.get());

        raftClient.refreshLeader().get();

        assertEquals(raftClient.leader().consistentId(), leaderRef.get().name());
    }

    /**
     * Stops cluster.
     */
    private void stopCluster() {
        if (!CollectionUtils.nullOrEmpty(raftClients)) {
            raftClients.forEach(PhysicalTopologyAwareRaftGroupService::shutdown);

            raftClients.clear();
        }

        var componentsToStop = new ArrayList<IgniteComponent>();

        for (Entry<NetworkAddress, ClusterService> entry : clusterServices.entrySet()) {
            NetworkAddress addr = entry.getKey();

            JraftServerImpl raftServer = raftServers.get(addr);

            if (raftServer != null) {
                raftServer.stopRaftNodes(GROUP_ID);

                componentsToStop.add(raftServer);
            }

            componentsToStop.add(logStorageFactories.get(addr));

            componentsToStop.add(entry.getValue());
        }

        assertThat(stopAsync(new ComponentContext(), componentsToStop), willCompleteSuccessfully());

        raftServers.clear();
        logStorageFactories.clear();
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
    private @Nullable PhysicalTopologyAwareRaftGroupService startCluster(
            TestInfo testInfo,
            Predicate<NetworkAddress> isServerAddress,
            int nodes,
            int clientPort
    ) {
        List<NetworkAddress> addresses = findLocalAddresses(PORT_BASE, PORT_BASE + nodes);

        var nodeFinder = new StaticNodeFinder(addresses);

        PeersAndLearners peersAndLearners = peersAndLearners(testInfo, isServerAddress, nodes);

        PhysicalTopologyAwareRaftGroupService raftClient = null;

        RaftGroupEventsClientListener clientRaftListener = null;

        for (NetworkAddress addr : addresses) {
            var cluster = clusterService(testInfo, addr.port(), nodeFinder);

            assertThat(cluster.startAsync(new ComponentContext()), willCompleteSuccessfully());

            // Starting the topology-aware client earlier than the other components to provoke a situation where the client starts earlier
            // than the other nodes but works anyway.
            if (addr.port() == clientPort) {
                clientRaftListener = new RaftGroupEventsClientListener();

                raftClient = startTopologyAwareClient(cluster, peersAndLearners, clientRaftListener);

                raftClients.add(raftClient);
            }

            clusterServices.put(addr, cluster);
        }

        for (NetworkAddress addr : addresses) {
            ClusterService cluster = clusterServices.get(addr);

            RaftGroupEventsClientListener eventsClientListener =
                    addr.port() == clientPort ? clientRaftListener : new RaftGroupEventsClientListener();

            if (isServerAddress.test(addr)) { // RAFT server node
                var localPeer = peersAndLearners.peers().stream()
                        .filter(peer -> peer.consistentId().equals(cluster.topologyService().localMember().name())).findAny().get();

                var dataPath = workDir.resolve("raft_" + localPeer.consistentId());

                var commandsMarshaller = new ThreadLocalOptimizedMarshaller(cluster.serializationRegistry());

                NodeOptions nodeOptions = new NodeOptions();
                nodeOptions.setCommandsMarshaller(commandsMarshaller);

                Path workingDir = dataPath.resolve("partitions");

                LogStorageFactory partitionsLogStorageFactory = SharedLogStorageFactoryUtils.create(
                        cluster.nodeName(),
                        workingDir.resolve("log")
                );

                logStorageFactories.put(addr, partitionsLogStorageFactory);

                assertThat(partitionsLogStorageFactory.startAsync(new ComponentContext()), willCompleteSuccessfully());

                var raftServer = TestJraftServerFactory.create(
                        cluster,
                        nodeOptions,
                        eventsClientListener
                );
                assertThat(raftServer.startAsync(new ComponentContext()), willCompleteSuccessfully());

                raftServer.startRaftNode(
                        new RaftNodeId(GROUP_ID, localPeer),
                        peersAndLearners,
                        new TestRaftGroupListener(),
                        RaftGroupOptions.defaults()
                                .commandsMarshaller(commandsMarshaller)
                                .setLogStorageFactory(partitionsLogStorageFactory)
                                .serverDataPath(workingDir.resolve("meta"))
                );

                raftServers.put(addr, raftServer);
            }
        }

        return raftClient;
    }

    private PhysicalTopologyAwareRaftGroupService startTopologyAwareClient(
            ClusterService localClusterService,
            PeersAndLearners peersAndLearners,
            RaftGroupEventsClientListener eventsClientListener
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

        return PhysicalTopologyAwareRaftGroupService.start(
                GROUP_ID,
                localClusterService,
                raftConfiguration,
                peersAndLearners,
                executor,
                eventsClientListener,
                commandsMarshaller,
                StoppingExceptionFactories.indicateComponentStop(),
                throttlingContextHolder(),
                NOOP_FAILURE_PROCESSOR
        );
    }

    private static PeersAndLearners peersAndLearners(
            TestInfo testInfo,
            Predicate<NetworkAddress> isServerAddress,
            int nodes
    ) {
        return PeersAndLearners.fromConsistentIds(
                findLocalAddresses(PORT_BASE, PORT_BASE + nodes).stream().filter(isServerAddress)
                        .map(netAddr -> IgniteTestUtils.testNodeName(testInfo, netAddr.port())).collect(toSet()));
    }
}
