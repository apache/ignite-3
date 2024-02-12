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
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.raft.LeaderElectionListener;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.TestRaftGroupListener;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.raft.util.ThreadLocalOptimizedMarshaller;
import org.apache.ignite.internal.replicator.TestReplicationGroupId;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.topology.LogicalTopologyServiceTestImpl;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.jraft.RaftMessageGroup;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.rpc.CliRequests.LeaderChangeNotification;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
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

    protected static final TestReplicationGroupId GROUP_ID = new TestReplicationGroupId("group_1");

    @InjectConfiguration
    protected RaftConfiguration raftConfiguration;

    /** RPC executor. */
    private final ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(
            20,
            NamedThreadFactory.create("common", "Raft-Group-Client", log)
    );

    @AfterEach
    protected void tearDown() throws Exception {
        IgniteUtils.shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);
    }

    /**
     * The method is called after every node of the cluster starts.
     *
     * @param nodeName Node name.
     * @param clusterService Cluster service.
     * @param dataPath Data path for raft node.
     * @param peersAndLearners Peers and learners.
     * @param eventsClientListener Raft events listener for client.
     */
    protected abstract void afterNodeStart(
            String nodeName,
            ClusterService clusterService,
            Path dataPath,
            PeersAndLearners peersAndLearners,
            RaftGroupEventsClientListener eventsClientListener
    );

    /**
     * Checks the condition after cluster and raft clients initialization.
     *
     * @param leaderName Current leader name.
     * @return Condition result.
     */
    protected abstract void afterClusterInit(String leaderName) throws InterruptedException;

    /**
     * Checks the condition after leader change.
     *
     * @param leaderName Current leader name.
     * @return Condition result.
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
        var clusterServices = new HashMap<NetworkAddress, ClusterService>();
        var raftServers = new HashMap<NetworkAddress, JraftServerImpl>();
        int nodes = 2;

        TopologyAwareRaftGroupService raftClient = startCluster(
                testInfo,
                clusterServices,
                raftServers,
                addr -> true,
                nodes,
                PORT_BASE + 1
        );

        CompletableFuture<ClusterNode> leaderFut = new CompletableFuture<>();

        subscribeLeader(raftClient, (node, term) -> leaderFut.complete(node), "New leader: {}");

        ClusterNode leader = leaderFut.get(10, TimeUnit.SECONDS);

        assertNotNull(leader);

        afterClusterInit(leader.name());

        stopCluster(clusterServices, raftServers, List.of(raftClient), nodes);
    }

    @Test
    public void testChangeLeaderWhenActualLeft(TestInfo testInfo) throws Exception {
        var clusterServices = new HashMap<NetworkAddress, ClusterService>();
        var raftServers = new HashMap<NetworkAddress, JraftServerImpl>();
        int nodes = 3;
        Predicate<NetworkAddress> isServerAddress = addr -> true;

        TopologyAwareRaftGroupService raftClient = startCluster(
                testInfo,
                clusterServices,
                raftServers,
                isServerAddress,
                nodes,
                PORT_BASE
        );

        raftClient.refreshLeader().get();

        var clientClusterService = clusterService(testInfo, PORT_BASE + nodes + 1, new StaticNodeFinder(getNetworkAddresses(nodes)));
        clientClusterService.start();

        TopologyAwareRaftGroupService raftClientNoInitialNotify = startTopologyAwareClient(
                clientClusterService,
                clusterServices,
                isServerAddress,
                nodes,
                null,
                false
        );

        AtomicReference<ClusterNode> leaderRef = new AtomicReference<>();
        AtomicReference<ClusterNode> leaderRefNoInitialNotify = new AtomicReference<>();
        AtomicInteger callsCount = new AtomicInteger();

        subscribeLeader(raftClient, (node, term) -> leaderRef.set(node), "New leader: {}");

        for (int i = 0; i < 2; i++) {
            unsubscribeLeader(raftClientNoInitialNotify);

            subscribeLeader(raftClientNoInitialNotify, (node, term) -> {
                callsCount.incrementAndGet();
                leaderRefNoInitialNotify.set(node);
            }, "New leader (client without initial notification): {}");
        }

        assertTrue(callsCount.get() <= 1);

        assertTrue(waitForCondition(() -> leaderRef.get() != null, 10_000));

        ClusterNode leader = leaderRef.get();

        assertNotNull(leader);

        log.info("Leader: " + leader);

        afterClusterInit(leader.name());

        var raftServiceToStop = raftServers.remove(new NetworkAddress("localhost", leader.address().port()));
        raftServiceToStop.stopRaftNodes(GROUP_ID);
        raftServiceToStop.stop();

        afterNodeStop(leader.name());

        clusterServices.remove(new NetworkAddress("localhost", leader.address().port())).stop();

        assertTrue(waitForCondition(() -> !leader.equals(leaderRef.get()), 10_000));
        assertTrue(waitForCondition(() -> !leader.equals(leaderRefNoInitialNotify.get()), 1000));

        log.info("New Leader: " + leaderRef.get());

        afterLeaderChange(leaderRef.get().name());

        raftClientNoInitialNotify.refreshLeader().get();

        assertEquals(raftClientNoInitialNotify.leader().consistentId(), leaderRef.get().name());

        stopCluster(clusterServices, raftServers, List.of(raftClient, raftClientNoInitialNotify), nodes);

        clientClusterService.stop();
    }

    @Test
    public void testChangeLeaderForce(TestInfo testInfo) throws Exception {
        var clusterServices = new HashMap<NetworkAddress, ClusterService>();
        var raftServers = new HashMap<NetworkAddress, JraftServerImpl>();
        int nodes = 3;
        Predicate<NetworkAddress> isServerAddress = addr -> true;

        TopologyAwareRaftGroupService raftClient = startCluster(
                testInfo,
                clusterServices,
                raftServers,
                isServerAddress,
                nodes,
                PORT_BASE
        );

        raftClient.refreshLeader().get();

        var clientClusterService = clusterService(testInfo, PORT_BASE + nodes + 1, new StaticNodeFinder(getNetworkAddresses(nodes)));
        clientClusterService.start();

        TopologyAwareRaftGroupService raftClientNoInitialNotify = startTopologyAwareClient(
                clientClusterService,
                clusterServices,
                isServerAddress,
                nodes,
                null,
                false
        );

        AtomicReference<ClusterNode> leaderRef = new AtomicReference<>();
        AtomicReference<ClusterNode> leaderRefNoInitialNotify = new AtomicReference<>();
        AtomicInteger callsCount = new AtomicInteger();

        subscribeLeader(raftClient, (node, term) -> leaderRef.set(node), "New leader: {}");

        for (int i = 0; i < 2; i++) {
            unsubscribeLeader(raftClientNoInitialNotify);

            subscribeLeader(raftClientNoInitialNotify, (node, term) -> {
                callsCount.incrementAndGet();
                leaderRefNoInitialNotify.set(node);
            }, "New leader (client without initial notification): {}");
        }

        assertTrue(callsCount.get() <= 1);

        assertTrue(waitForCondition(() -> leaderRef.get() != null, 10_000));

        ClusterNode leader = leaderRef.get();

        assertNotNull(leader);

        log.info("Leader: " + leader);

        afterClusterInit(leader.name());

        Peer newLeaderPeer = raftClient.peers().stream().filter(peer -> !leader.name().equals(peer.consistentId())).findAny().get();

        log.info("Peer to transfer leader: " + newLeaderPeer);

        assertNull(leaderRefNoInitialNotify.get());

        raftClient.transferLeadership(newLeaderPeer).get();

        String leaderId = newLeaderPeer.consistentId();

        assertTrue(waitForCondition(() -> leaderId.equals(leaderRef.get().name()), 10_000));
        assertTrue(waitForCondition(
                () -> leaderRefNoInitialNotify.get() != null && leaderId.equals(leaderRefNoInitialNotify.get().name()), 1000)
        );

        log.info("New Leader: " + leaderRef.get());

        afterLeaderChange(leaderRef.get().name());

        raftClient.refreshLeader().get();

        assertEquals(raftClient.leader().consistentId(), leaderRef.get().name());

        stopCluster(clusterServices, raftServers, List.of(raftClient, raftClientNoInitialNotify), nodes);
        clientClusterService.stop();
    }

    /**
     * Stops cluster.
     *
     * @param clusterServices Cluster services.
     * @param raftServers     RAFT services.
     * @param raftClients     RAFT clients.
     * @param nodes           Node count.
     * @throws Exception If failed.
     */
    private void stopCluster(
            HashMap<NetworkAddress, ClusterService> clusterServices,
            HashMap<NetworkAddress, JraftServerImpl> raftServers,
            List<TopologyAwareRaftGroupService> raftClients,
            int nodes
    ) throws Exception {
        if (raftClients != null) {
            raftClients.forEach(client -> client.shutdown());
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
            var cluster = clusterService(testInfo, addr.port(), nodeFinder);

            cluster.start();

            clusterServices.put(addr, cluster);
        }

        PeersAndLearners peersAndLearners = peersAndLearners(clusterServices, isServerAddress, nodes);

        for (NetworkAddress addr : addresses) {
            var cluster = clusterServices.get(addr);

            RaftGroupEventsClientListener eventsClientListener = new RaftGroupEventsClientListener();

            if (isServerAddress.test(addr)) { //RAFT server node
                var localPeer = peersAndLearners.peers().stream()
                        .filter(peer -> peer.consistentId().equals(cluster.topologyService().localMember().name())).findAny().get();

                var dataPath = workDir.resolve("raft_" + localPeer.consistentId());

                var commandsMarshaller = new ThreadLocalOptimizedMarshaller(cluster.serializationRegistry());

                NodeOptions nodeOptions = new NodeOptions();
                nodeOptions.setCommandsMarshaller(commandsMarshaller);

                var raftServer = new JraftServerImpl(
                        cluster,
                        dataPath,
                        nodeOptions,
                        eventsClientListener
                );
                raftServer.start();

                raftServer.startRaftNode(
                        new RaftNodeId(GROUP_ID, localPeer),
                        peersAndLearners,
                        new TestRaftGroupListener(),
                        RaftGroupOptions.defaults().commandsMarshaller(commandsMarshaller)
                );

                raftServers.put(addr, raftServer);

                afterNodeStart(localPeer.consistentId(), cluster, dataPath, peersAndLearners, eventsClientListener);
            }

            if (addr.port() == clientPort) {
                assertTrue(isServerAddress.test(addr));

                raftClient = startTopologyAwareClient(cluster, clusterServices, isServerAddress, nodes, eventsClientListener, true);
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
            boolean notifyOnSubscription
    ) {
        if (eventsClientListener == null) {
            eventsClientListener = new RaftGroupEventsClientListener();

            var finalEventsClientListener = eventsClientListener;
            localClusterService.messagingService().addMessageHandler(RaftMessageGroup.class, (msg, sender, correlationId) -> {
                if (msg instanceof LeaderChangeNotification) {
                    LeaderChangeNotification msg0 = (LeaderChangeNotification) msg;

                    ClusterNode node = localClusterService.topologyService().getByConsistentId(sender);
                    finalEventsClientListener.onLeaderElected(msg0.groupId(), node, msg0.term());
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
                new LogicalTopologyServiceTestImpl(localClusterService),
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
                getNetworkAddresses(nodes).stream().filter(isServerAddress)
                        .map(netAddr -> clusterServices.get(netAddr).topologyService().localMember().name()).collect(
                                toSet()));
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

    private void subscribeLeader(TopologyAwareRaftGroupService client, LeaderElectionListener callback, String logMessage) {
        CompletableFuture<Void> future = client.subscribeLeader((node, term) -> {
            callback.onLeaderElected(node, term);

            log.info(logMessage, node);
        });

        assertThat(future, willCompleteSuccessfully());
    }

    private void unsubscribeLeader(TopologyAwareRaftGroupService client) {
        CompletableFuture<Void> future = client.unsubscribeLeader();

        assertThat(future, willCompleteSuccessfully());
    }
}
