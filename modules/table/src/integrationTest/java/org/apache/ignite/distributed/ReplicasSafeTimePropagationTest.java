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

package org.apache.ignite.distributed;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.raft.PeersAndLearners.fromConsistentIds;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.lang.SafeTimeReorderException;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.network.utils.ClusterServiceTestUtils;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.RaftGroupEventsListener;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.TestLozaFactory;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.TestReplicationGroupId;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.table.distributed.StorageUpdateHandler;
import org.apache.ignite.internal.table.distributed.raft.PartitionDataStorage;
import org.apache.ignite.internal.table.distributed.raft.PartitionListener;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Replica safeTime propagation tests.
 */
@ExtendWith(ConfigurationExtension.class)
public class ReplicasSafeTimePropagationTest extends IgniteAbstractTest {
    @InjectConfiguration("mock: { fsync: false }")
    private RaftConfiguration raftConfiguration;

    private static final int BASE_PORT = 1234;

    private static final TestReplicationGroupId GROUP_ID = new TestReplicationGroupId("group_1");

    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    private static final StaticNodeFinder NODE_FINDER = new StaticNodeFinder(
            IntStream.range(BASE_PORT, BASE_PORT + 5)
                    .mapToObj(p -> new NetworkAddress("localhost", p))
                    .collect(Collectors.toList())
    );

    private final AtomicInteger port = new AtomicInteger(BASE_PORT);

    private Map<String, PartialNode> cluster;

    @AfterEach
    public void after() throws Exception {
        for (PartialNode partialNode : cluster.values()) {
            try {
                partialNode.stop();
            } catch (NodeStoppingException ignored) {
                // No-op, multiple stop.
            }
        }
    }

    private static long calculateSafeTime(ClockService clockService) {
        return clockService.now().addPhysicalTime(clockService.maxClockSkewMillis()).longValue();
    }

    private static void sendSafeTimeSyncCommand(
            RaftGroupService raftClient,
            long safeTime,
            boolean expectSafeTimeReorderException
    ) {
        CompletableFuture<Object> safeTimeCommandFuture = raftClient.run(
                REPLICA_MESSAGES_FACTORY
                        .safeTimeSyncCommand()
                        .safeTimeLong(safeTime)
                        .build()
        );

        if (expectSafeTimeReorderException) {
            assertThat(safeTimeCommandFuture, willThrow(SafeTimeReorderException.class));
        } else {
            assertThat(safeTimeCommandFuture, willCompleteSuccessfully());
        }
    }

    /**
     * Test verifies that a new leader will reject a command with safeTime less than previously applied within old leader.
     * <ol>
     *     <li>Start three nodes and a raft group with three peers.</li>
     *     <li>Send command with safe time X.</li>
     *     <li>Stop the leader - the only node that actually do safeTime watermark validation within onBeforeApply.</li>
     *     <li>Send command with safe time less than X to the new leader and verify that SafeTimeReorderException is thrown.</li>
     * </ol>
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-21565")
    public void testSafeTimeReorderingOnLeaderReElection() throws Exception {
        // Start three nodes and a raft group with three peers.
        {
            cluster = Stream.of("node1", "node2", "node3").collect(toMap(identity(), PartialNode::new));

            startCluster(cluster);
        }

        PartialNode someNode = cluster.values().iterator().next();

        RaftGroupService raftClient = someNode.raftClient;

        assertThat(raftClient.refreshLeader(), willCompleteSuccessfully());

        long firstSafeTime = calculateSafeTime(someNode.clockService);

        // Send command with safe time X.
        sendSafeTimeSyncCommand(raftClient, firstSafeTime, false);

        // Stop the leader - the only node that actually do safeTime watermark validation within onBeforeApply.
        assertNotNull(raftClient.leader());

        PartialNode nodeTopStop = cluster.get(raftClient.leader().consistentId());

        assertNotNull(nodeTopStop);

        nodeTopStop.stop();

        // Select alive raft client.
        Optional<PartialNode> aliveNode = cluster.values().stream().filter(node -> !node.nodeName.equals(nodeTopStop.nodeName)).findFirst();

        assertTrue(aliveNode.isPresent());

        RaftGroupService anotherClient = aliveNode.get().raftClient;

        // Send command with safe time less than previously applied to the new leader and verify that SafeTimeReorderException is thrown.
        sendSafeTimeSyncCommand(anotherClient, firstSafeTime - 1, true);

        sendSafeTimeSyncCommand(anotherClient, calculateSafeTime(aliveNode.get().clockService), false);
    }

    private void startCluster(Map<String, PartialNode> cluster) throws Exception {
        Collection<CompletableFuture<Void>> startingFutures = new ArrayList<>(cluster.size());
        for (PartialNode node : cluster.values()) {
            startingFutures.add(node.start());
        }

        CompletableFuture<Void> clusterReadyFuture = CompletableFuture.allOf(startingFutures.toArray(CompletableFuture[]::new));

        assertThat(clusterReadyFuture, willCompleteSuccessfully());
    }

    /**
     * Test verifies that a leader will reject a command with safeTime less than previously applied within leader restart.
     * <ol>
     *     <li>Start two and a raft group with two peer.</li>
     *     <li>Send command with safe time X.</li>
     *     <li>Restart the cluster.</li>
     *     <li>Send command with safe time less than previously applied to the leader before the restart
     *     and verify that SafeTimeReorderException is thrown.</li>
     * </ol>
     */
    @Test
    public void testSafeTimeReorderingOnLeaderRestart() throws Exception {
        // Start two node and a raft group with two peer.
        {
            cluster = Set.of("node1", "node2").parallelStream().collect(toMap(identity(), PartialNode::new));

            startCluster(cluster);
        }

        PartialNode someNode = cluster.values().iterator().next();

        RaftGroupService raftClient = someNode.raftClient;

        assertThat(raftClient.refreshLeader(), willCompleteSuccessfully());

        long firstSafeTime = calculateSafeTime(someNode.clockService);

        // Send command with safe time X.
        sendSafeTimeSyncCommand(raftClient, firstSafeTime, false);

        // Stop all nodes.
        for (PartialNode node : cluster.values()
        ) {
            node.stop();
        }

        // And restart.
        startCluster(cluster);

        // Send command with safe time less than previously applied to the leader before the restart
        // and verify that SafeTimeReorderException is thrown.
        sendSafeTimeSyncCommand(someNode.raftClient, firstSafeTime - 1, true);

        sendSafeTimeSyncCommand(someNode.raftClient, calculateSafeTime(someNode.clockService), false);
    }

    private class PartialNode {
        private final String nodeName;

        private final ClockService clockService = new TestClockService(new HybridClockImpl());

        private ClusterService clusterService;

        private Loza raftManager;

        private RaftGroupService raftClient;

        PartialNode(String nodeName) {
            this.nodeName = nodeName;
        }

        CompletableFuture<Void> start() throws Exception {
            clusterService = ClusterServiceTestUtils.clusterService(nodeName, port.getAndIncrement(), NODE_FINDER);

            assertThat(clusterService.startAsync(), willCompleteSuccessfully());

            raftManager = TestLozaFactory.create(
                    clusterService,
                    raftConfiguration,
                    workDir.resolve(nodeName + "_loza"),
                    new HybridClockImpl(),
                    new RaftGroupEventsClientListener()
            );

            assertThat(raftManager.startAsync(), willCompleteSuccessfully());

            TxManager txManagerMock = mock(TxManager.class);

            return raftManager.startRaftGroupNode(
                            new RaftNodeId(GROUP_ID, new Peer(nodeName)),
                            fromConsistentIds(cluster.keySet()),
                            new PartitionListener(
                                    txManagerMock,
                                    mock(PartitionDataStorage.class),
                                    mock(StorageUpdateHandler.class),
                                    mock(TxStateStorage.class),
                                    mock(PendingComparableValuesTracker.class),
                                    mock(PendingComparableValuesTracker.class),
                                    mock(CatalogService.class),
                                    mock(SchemaRegistry.class),
                                    clockService
                            ),
                            RaftGroupEventsListener.noopLsnr,
                            RaftGroupOptions.defaults()
                    )
                    .thenApply(raftClient -> {
                        this.raftClient = raftClient;
                        return null;
                    });
        }

        void stop() throws Exception {
            closeAll(
                    raftManager == null ? null : () -> raftManager.stopRaftNodes(GROUP_ID),
                    raftManager == null ? null : raftManager::beforeNodeStop,
                    clusterService == null ? null : clusterService::beforeNodeStop,
                    raftManager == null ? null : () -> assertThat(raftManager.stopAsync(), willCompleteSuccessfully()),
                    clusterService == null ? null : () -> assertThat(clusterService.stopAsync(), willCompleteSuccessfully())
            );
        }
    }
}
