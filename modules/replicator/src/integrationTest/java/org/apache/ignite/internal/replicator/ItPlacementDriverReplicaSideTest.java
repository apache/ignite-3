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

package org.apache.ignite.internal.replicator;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.raft.PeersAndLearners.fromConsistentIds;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CollectionUtils.first;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.apache.ignite.internal.util.IgniteUtils.startAsync;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.NoOpFailureProcessor;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.lang.IgniteTriConsumer;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.metrics.NoOpMetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.NetworkMessageHandler;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.network.utils.ClusterServiceTestUtils;
import org.apache.ignite.internal.placementdriver.TestPlacementDriver;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverActorMessage;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverMessageGroup;
import org.apache.ignite.internal.placementdriver.message.StopLeaseProlongationMessage;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.RaftGroupEventsListener;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.TestRaftGroupListener;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupService;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.replicator.message.ReplicaMessageTestGroup;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.replicator.message.TestReplicaMessagesFactory;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.topology.LogicalTopologyServiceTestImpl;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * These test are using an honest connection to test interconnection between replicas with placement driver.
 */
@ExtendWith(ConfigurationExtension.class)
public class ItPlacementDriverReplicaSideTest extends IgniteAbstractTest {
    private static final int BASE_PORT = 1234;

    private static final TestReplicationGroupId GROUP_ID = new TestReplicationGroupId("group_1");

    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    private static final TestReplicaMessagesFactory TEST_REPLICA_MESSAGES_FACTORY = new TestReplicaMessagesFactory();

    @InjectConfiguration("mock {retryTimeout=2000, responseTimeout=1000}")
    private RaftConfiguration raftConfiguration;

    @InjectConfiguration
    private ReplicationConfiguration replicationConfiguration;

    private final HybridClock clock = new HybridClockImpl();

    private Set<String> placementDriverNodeNames;
    private Set<String> nodeNames;

    /** This closure handles {@link StopLeaseProlongationMessage} to check the replica behavior. */
    private IgniteTriConsumer<StopLeaseProlongationMessage, String, String> denyLeaseHandler;

    /** Cluster service by node name. */
    private Map<String, ClusterService> clusterServices;

    private final Map<String, ReplicaManager> replicaManagers = new HashMap<>();
    private final Map<String, Loza> raftManagers = new HashMap<>();
    private final Map<String, TopologyAwareRaftGroupServiceFactory> raftClientFactory = new HashMap<>();

    private ExecutorService partitionOperationsExecutor;

    /** List of services to have to close before the test will be completed. */
    private final List<Closeable> servicesToClose = new ArrayList<>();

    private BiFunction<ReplicaRequest, String, CompletableFuture<ReplicaResult>> replicaListener = null;

    @BeforeEach
    public void beforeTest(TestInfo testInfo) {
        partitionOperationsExecutor = new ThreadPoolExecutor(
                0, 20,
                0, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                NamedThreadFactory.create("test", "partition-operations", log)
        );

        placementDriverNodeNames = IntStream.range(BASE_PORT, BASE_PORT + 3).mapToObj(port -> testNodeName(testInfo, port))
                .collect(toSet());
        nodeNames = IntStream.range(BASE_PORT, BASE_PORT + 5).mapToObj(port -> testNodeName(testInfo, port))
                .collect(toSet());

        clusterServices = startNodes();

        var cmgManager = mock(ClusterManagementGroupManager.class);

        when(cmgManager.metaStorageNodes()).thenReturn(completedFuture(placementDriverNodeNames));

        Supplier<ClusterNode> primaryReplicaSupplier = () -> first(clusterServices.values()).topologyService().localMember();

        for (String nodeName : nodeNames) {
            ClusterService clusterService = clusterServices.get(nodeName);

            RaftGroupEventsClientListener eventsClientListener = new RaftGroupEventsClientListener();

            var raftManager = new Loza(
                    clusterService,
                    new NoOpMetricManager(),
                    raftConfiguration,
                    workDir.resolve(nodeName + "_loza"),
                    clock,
                    eventsClientListener
            );

            raftManagers.put(nodeName, raftManager);

            TopologyAwareRaftGroupServiceFactory topologyAwareRaftGroupServiceFactory = new TopologyAwareRaftGroupServiceFactory(
                    clusterService,
                    new LogicalTopologyServiceTestImpl(clusterService),
                    Loza.FACTORY,
                    eventsClientListener
            );

            raftClientFactory.put(nodeName, topologyAwareRaftGroupServiceFactory);

            var replicaManager = new ReplicaManager(
                    nodeName,
                    clusterService,
                    cmgManager,
                    new TestClockService(clock),
                    Set.of(ReplicaMessageTestGroup.class),
                    new TestPlacementDriver(primaryReplicaSupplier),
                    partitionOperationsExecutor,
                    new NoOpFailureProcessor()
            );

            replicaManagers.put(nodeName, replicaManager);

            assertThat(startAsync(ForkJoinPool.commonPool(), clusterService, raftManager, replicaManager), willCompleteSuccessfully());

            servicesToClose.add(() -> {
                try {
                    closeAll(
                            replicaManager::beforeNodeStop,
                            raftManager::beforeNodeStop,
                            clusterService::beforeNodeStop,
                            () -> assertThat(stopAsync(replicaManager, raftManager, clusterService), willCompleteSuccessfully())
                    );
                } catch (Exception e) {
                    log.info("Fail to stop services [node={}]", e, nodeName);
                }
            });

            servicesToClose.add(() -> IgniteUtils.shutdownAndAwaitTermination(partitionOperationsExecutor, 10, TimeUnit.SECONDS));
        }
    }

    @AfterEach
    public void afterTest() throws Exception {
        closeAll(servicesToClose);

        replicaListener = null;
    }

    /**
     * Starts cluster nodes.
     *
     * @return Cluster services.
     */
    public Map<String, ClusterService> startNodes() {
        var res = new HashMap<String, ClusterService>(nodeNames.size());

        var nodeFinder = new StaticNodeFinder(IntStream.range(BASE_PORT, BASE_PORT + 5)
                .mapToObj(p -> new NetworkAddress("localhost", p))
                .collect(Collectors.toList()));

        int port = BASE_PORT;

        for (String nodeName : nodeNames) {
            var srvc = ClusterServiceTestUtils.clusterService(nodeName, port++, nodeFinder);

            srvc.messagingService().addMessageHandler(PlacementDriverMessageGroup.class, leaseDenyMessageHandler(srvc));

            res.put(nodeName, srvc);
        }

        return res;
    }

    /**
     * Creates a network handler to intercept {@link StopLeaseProlongationMessage}.
     *
     * @param handlerService Cluster service to handle message.
     * @return Network handler.
     */
    private NetworkMessageHandler leaseDenyMessageHandler(ClusterService handlerService) {
        return (msg, sender, correlationId) -> {
            if (!(msg instanceof PlacementDriverActorMessage)) {
                return;
            }

            var handlerNode = handlerService.topologyService().localMember();

            log.info("Lease is denied [replica={}, actor={}, redirect={}]", sender, handlerNode.name(),
                    ((StopLeaseProlongationMessage) msg).redirectProposal());

            if (denyLeaseHandler != null) {
                denyLeaseHandler.accept((StopLeaseProlongationMessage) msg, sender.name(), handlerNode.name());
            }
        };
    }

    @Test
    public void testNotificationToPlacementDriverAboutConnectivityProblem() throws Exception {
        Set<String> grpNodes = chooseRandomNodes(3);

        log.info("Replication group is based on {}", grpNodes);

        var raftClientFut = createReplicationGroup(GROUP_ID, grpNodes);

        var raftClient = raftClientFut.get();

        raftClient.refreshLeader().get();

        var leaderNodeName = raftClient.leader().consistentId();

        ConcurrentHashMap<String, String> nodesToReceivedDeclineMsg = new ConcurrentHashMap<>();

        denyLeaseHandler = (msg, from, to) -> {
            nodesToReceivedDeclineMsg.put(to, from);
        };

        var anyNode = randomNode(Set.of());

        log.info("Message sent from {} to {}", anyNode, leaderNodeName);

        var clusterService = clusterServices.get(anyNode);

        replicaListener = (request, sender) -> failedFuture(new IOException("test"));

        new ReplicaService(
                clusterService.messagingService(),
                clock,
                replicationConfiguration
        ).invoke(
                clusterService.topologyService().getByConsistentId(leaderNodeName),
                TEST_REPLICA_MESSAGES_FACTORY.primaryReplicaTestRequest()
                        .enlistmentConsistencyToken(1L)
                        .groupId(GROUP_ID)
                        .build()
        );

        assertTrue(waitForCondition(() -> nodesToReceivedDeclineMsg.size() == placementDriverNodeNames.size(), 10_000));

        for (String nodeName : nodesToReceivedDeclineMsg.keySet()) {
            assertEquals(leaderNodeName, nodesToReceivedDeclineMsg.get(nodeName));

            assertTrue(placementDriverNodeNames.contains(nodeName));
        }

        stopReplicationGroup(GROUP_ID, grpNodes);
    }

    @Test
    public void testNotificationToPlacementDriverAboutMajorityLoss() throws Exception {
        Set<String> grpNodes = chooseRandomNodes(3);

        log.info("Replication group is based on {}", grpNodes);

        var raftClientFut = createReplicationGroup(GROUP_ID, grpNodes);

        var raftClient = raftClientFut.get();

        raftClient.refreshLeader().get();

        var leaderNodeName = raftClient.leader().consistentId();

        var grpNodesToStop = grpNodes.stream().filter(n -> !n.equals(leaderNodeName)).collect(toSet());

        log.info(
                "All nodes of the replication group will be unavailable except leader [leader={}, others={}]",
                leaderNodeName,
                grpNodesToStop
        );

        ConcurrentHashMap<String, String> nodesToReceivedDeclineMsg = new ConcurrentHashMap<>();

        denyLeaseHandler = (msg, from, to) -> {
            nodesToReceivedDeclineMsg.put(to, from);
        };

        for (String nodeToStop : grpNodesToStop) {
            var srvc = clusterServices.get(nodeToStop);

            srvc.beforeNodeStop();
            assertThat(srvc.stopAsync(), willCompleteSuccessfully());
        }

        var anyNode = randomNode(grpNodesToStop);

        log.info("Message sent from {} to {}", anyNode, leaderNodeName);

        var clusterService = clusterServices.get(anyNode);

        new ReplicaService(
                clusterService.messagingService(),
                clock,
                replicationConfiguration
        ).invoke(
                clusterService.topologyService().getByConsistentId(leaderNodeName),
                TEST_REPLICA_MESSAGES_FACTORY.primaryReplicaTestRequest()
                        .enlistmentConsistencyToken(1L)
                        .groupId(GROUP_ID)
                        .build()
        );

        var restPlacementDriverNodes = placementDriverNodeNames.stream().filter(n -> !grpNodesToStop.contains(n)).collect(toSet());

        log.info("Rest nodes of placement driver {}", restPlacementDriverNodes);

        assertTrue(waitForCondition(() -> nodesToReceivedDeclineMsg.size() == restPlacementDriverNodes.size(), 10_000));

        for (String nodeName : nodesToReceivedDeclineMsg.keySet()) {
            assertEquals(leaderNodeName, nodesToReceivedDeclineMsg.get(nodeName));

            assertTrue(placementDriverNodeNames.contains(nodeName));
        }

        stopReplicationGroup(GROUP_ID, grpNodes);
    }

    /**
     * Gets a node name randomly.
     *
     * @return Node name.
     */
    private String randomNode(Set<String> exceptNodes) {
        ArrayList<String> list = new ArrayList<>(nodeNames);

        list.removeAll(exceptNodes);

        Collections.shuffle(list);

        return list.get(0);
    }

    /**
     * Prepares a random set of nodes.
     *
     * @return Random node set.
     */
    private Set<String> chooseRandomNodes(int count) {
        assertTrue(count <= nodeNames.size());

        var list = new ArrayList<>(nodeNames);

        Collections.shuffle(list);

        Set<String> randNodes = new HashSet<>(list.subList(0, count));

        return randNodes;
    }

    /**
     * Stops a replication group.
     *
     * @param testGrpId Replication group id.
     * @param grpNodes Participants on the replication group.
     * @throws NodeStoppingException If failed.
     */
    private void stopReplicationGroup(ReplicationGroupId testGrpId, Set<String> grpNodes) throws NodeStoppingException {
        for (String nodeName : grpNodes) {
            var raftManager = raftManagers.get(nodeName);
            var replicaManager = replicaManagers.get(nodeName);

            assertNotNull(raftManager);
            assertNotNull(replicaManager);

            replicaManager.stopReplica(testGrpId).join();
            raftManager.stopRaftNodes(testGrpId);
        }
    }

    /**
     * Creates a replication group on a specific node set.
     *
     * @param groupId Replication group id.
     * @param nodes Participants on the replication group.
     * @return Raft client for the replication group.
     * @throws Exception If failed.
     */
    private CompletableFuture<TopologyAwareRaftGroupService> createReplicationGroup(
            ReplicationGroupId groupId,
            Set<String> nodes
    ) throws Exception {
        var res = new CompletableFuture<TopologyAwareRaftGroupService>();

        List<CompletableFuture<?>> serviceFutures = new ArrayList<>(nodes.size() * 2);

        for (String nodeName : nodes) {
            var replicaManager = replicaManagers.get(nodeName);
            var raftManager = raftManagers.get(nodeName);

            assertNotNull(replicaManager);
            assertNotNull(raftManager);

            var peer = new Peer(nodeName);

            var rftNodeId = new RaftNodeId(groupId, peer);

            CompletableFuture<TopologyAwareRaftGroupService> raftClientFut = raftManager.startRaftGroupNode(
                    rftNodeId,
                    fromConsistentIds(nodes),
                    new TestRaftGroupListener(),
                    RaftGroupEventsListener.noopLsnr,
                    RaftGroupOptions.defaults(),
                    raftClientFactory.get(nodeName)
            );
            serviceFutures.add(raftClientFut);

            CompletableFuture<Replica> replicaFuture = raftClientFut.thenCompose(raftClient -> {
                try {
                    return replicaManager.startReplica(
                            groupId,
                            (request, senderId) -> {
                                log.info("Handle request [type={}]", request.getClass().getSimpleName());

                                return raftClient.run(REPLICA_MESSAGES_FACTORY.safeTimeSyncCommand().build())
                                        .thenCompose(ignored -> {
                                            if (replicaListener == null) {
                                                return completedFuture(new ReplicaResult(null, null));
                                            } else {
                                                return replicaListener.apply(request, senderId);
                                            }
                                        });
                            },
                            raftClient,
                            new PendingComparableValuesTracker<>(Long.MAX_VALUE));
                } catch (NodeStoppingException e) {
                    throw new RuntimeException(e);
                }
            });
            serviceFutures.add(replicaFuture);
        }

        CompletableFuture.allOf(serviceFutures.toArray(new CompletableFuture[0]))
                .thenRun(() -> {
                    try {
                        res.complete((TopologyAwareRaftGroupService) serviceFutures.get(0).get());
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                });

        return res;
    }
}
