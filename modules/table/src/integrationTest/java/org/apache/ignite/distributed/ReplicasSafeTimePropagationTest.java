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
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.TestHybridClock;
import org.apache.ignite.internal.catalog.configuration.SchemaSynchronizationConfiguration;
import org.apache.ignite.internal.configuration.ComponentWorkingDir;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.network.utils.ClusterServiceTestUtils;
import org.apache.ignite.internal.partition.replicator.raft.ZonePartitionRaftListener;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.placementdriver.LeasePlacementDriver;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftGroupEventsListener;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.TestLozaFactory;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.LeaderWithTerm;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.internal.raft.util.SharedLogStorageFactoryUtils;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.table.distributed.schema.ThreadLocalPartitionCommandsMarshaller;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.util.PendingIndependentComparableValuesTracker;
import org.apache.ignite.internal.util.SafeTimeValuesTracker;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.jraft.conf.Configuration;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Replica safeTime propagation tests.
 */
@ExtendWith(ConfigurationExtension.class)
public class ReplicasSafeTimePropagationTest extends IgniteAbstractTest {
    private static final IgniteLogger LOG = Loggers.forClass(ReplicasSafeTimePropagationTest.class);

    @InjectConfiguration("mock: { fsync: false }")
    private RaftConfiguration raftConfiguration;

    @InjectConfiguration
    private SystemLocalConfiguration systemLocalConfiguration;

    @InjectConfiguration("mock: { maxClockSkewMillis: 500 }")
    private SchemaSynchronizationConfiguration schemaSynchronizationConfiguration;

    private static final int BASE_PORT = 1234;

    private static final ZonePartitionId GROUP_ID = new ZonePartitionId(1, 0);

    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    private static final StaticNodeFinder NODE_FINDER = new StaticNodeFinder(
            IntStream.range(BASE_PORT, BASE_PORT + 5)
                    .mapToObj(p -> new NetworkAddress("localhost", p))
                    .collect(Collectors.toList())
    );

    private final AtomicInteger port = new AtomicInteger(BASE_PORT);

    private Map<String, PartialNode> cluster;

    private volatile boolean blockFsmThread = false;

    private final CountDownLatch l1 = new CountDownLatch(1);
    private final CountDownLatch l2 = new CountDownLatch(1);

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

    private static void sendSafeTimeSyncCommand(
            RaftGroupService raftClient,
            HybridTimestamp initiatorTime
    ) {
        assertThat(sendSafeTimeSyncCommandAsync(raftClient, initiatorTime), willCompleteSuccessfully());
    }

    private static CompletableFuture<Object> sendSafeTimeSyncCommandAsync(
            RaftGroupService raftClient,
            HybridTimestamp initiatorTime
    ) {
        return raftClient.run(
                REPLICA_MESSAGES_FACTORY
                        .safeTimeSyncCommand()
                        .initiatorTime(initiatorTime)
                        .build()
        );
    }

    /**
     * Test verifies that a new leader will monotonically assign new safe timestamp.
     * <ol>
     *     <li>Start three nodes and a raft group with three peers.</li>
     *     <li>Send safe ts sync command.</li>
     *     <li>Stop the leader.</li>
     *     <li>Send next safe ts sync command and ensure no reordering happens.</li>
     * </ol>
     */
    @Test
    public void testSafeTimeReorderingOnLeaderReElection() throws Exception {
        // Start three nodes and a raft group with three peers.
        {
            cluster = Stream.of("node1", "node2", "node3").collect(toMap(identity(), PartialNode::new));

            startCluster(cluster);
        }

        PartialNode someNode = cluster.values().iterator().next();

        RaftGroupService raftClient = someNode.raftClient;

        assertThat(raftClient.refreshLeader(), willCompleteSuccessfully());

        // Assumes stable clock on test runner.
        HybridClock initiatorClock = new TestHybridClock(() -> System.currentTimeMillis() - 100);

        HybridTimestamp beginTs = initiatorClock.now();

        sendSafeTimeSyncCommand(raftClient, beginTs);

        assertNotNull(raftClient.leader());

        PartialNode nodeTopStop = cluster.get(raftClient.leader().consistentId());

        assertNotNull(nodeTopStop);

        HybridTimestamp firstSafeTs = nodeTopStop.safeTs.current();

        assertTrue(firstSafeTs.compareTo(beginTs) > 0);

        assertNotNull(nodeTopStop);

        nodeTopStop.stop();

        // Select alive raft client.
        Optional<PartialNode> aliveNode = cluster.values().stream().filter(node -> !node.nodeName.equals(nodeTopStop.nodeName)).findFirst();

        assertTrue(aliveNode.isPresent());

        RaftGroupService anotherClient = aliveNode.get().raftClient;

        assertThat(anotherClient.refreshLeader(), willCompleteSuccessfully());

        HybridTimestamp nextTimestamp = initiatorClock.now();

        sendSafeTimeSyncCommand(anotherClient, nextTimestamp);

        PartialNode newLeader = cluster.get(anotherClient.leader().consistentId());

        assertNotNull(newLeader);

        assertTrue(newLeader.safeTs.current().compareTo(nextTimestamp) > 0);

        assertTrue(newLeader.safeTs.current().compareTo(firstSafeTs) > 0);
    }

    /**
     * Test verifies that a new leader will monotonically assign new safe timestamp.
     * <ol>
     *     <li>Start three nodes and a raft group with three peers.</li>
     *     <li>Send safe ts sync command.</li>
     *     <li>Reset a cluster to a single node.</li>
     *     <li>Send next safe ts sync command and ensure no reordering happens.</li>
     * </ol>
     */
    @Test
    public void testSafeTimeReorderingOnClusterShrink() throws Exception {
        // Start three nodes and a raft group with three peers.
        {
            cluster = Stream.of("node1", "node2", "node3").collect(toMap(identity(), PartialNode::new));

            startCluster(cluster);
        }

        PartialNode someNode = cluster.values().iterator().next();

        RaftGroupService raftClient = someNode.raftClient;

        assertThat(raftClient.refreshLeader(), willCompleteSuccessfully());

        // Assumes stable clock on test runner.
        HybridClock initiatorClock = new TestHybridClock(() -> System.currentTimeMillis() - 100);

        HybridTimestamp beginTs = initiatorClock.now();

        sendSafeTimeSyncCommand(raftClient, beginTs);

        LeaderWithTerm leader = raftClient.refreshAndGetLeaderWithTerm().join();
        assertNotNull(raftClient.leader());
        PartialNode leaderNode = cluster.get(raftClient.leader().consistentId());
        HybridTimestamp firstSafeTs = leaderNode.safeTs.current();
        assertTrue(firstSafeTs.compareTo(beginTs) > 0);

        // Reset topology to a leader with lagging clock.
        String resetToLeader = "node1";

        PeersAndLearners cfg = fromConsistentIds(Set.of(resetToLeader));

        leaderNode.raftClient.changePeersAndLearners(cfg, leader.term(), Configuration.NO_SEQUENCE_TOKEN).join();

        PartialNode leaderNode2 = cluster.get(resetToLeader);

        HybridTimestamp nextTimestamp = initiatorClock.now();

        sendSafeTimeSyncCommand(leaderNode2.raftClient, nextTimestamp);

        assertTrue(leaderNode2.safeTs.current().compareTo(nextTimestamp) > 0);

        assertTrue(leaderNode2.safeTs.current().compareTo(firstSafeTs) > 0);
    }

    @Test
    public void testSafeTimeReorderingOnRetry() throws Exception {
        {
            cluster = Stream.of("node1").collect(toMap(identity(), PartialNode::new));

            startCluster(cluster);
        }

        PartialNode someNode = cluster.values().iterator().next();

        RaftGroupService raftClient = someNode.raftClient;

        assertThat(raftClient.refreshLeader(), willCompleteSuccessfully());

        // Assumes stable clock on test runner.
        HybridClock initiatorClock = new TestHybridClock(() -> System.currentTimeMillis() - 100);

        sendSafeTimeSyncCommand(raftClient, initiatorClock.now());

        blockFsmThread = true;

        var fut = sendSafeTimeSyncCommandAsync(raftClient, initiatorClock.now());
        var fut2 = sendSafeTimeSyncCommandAsync(raftClient, initiatorClock.now());

        l1.await();

        // Block enough to trigger retry.
        Thread.sleep(raftConfiguration.responseTimeoutMillis().value() + 1000);

        LOG.info("Unblocking FSM thread");

        blockFsmThread = false;

        l2.countDown();

        assertThat(fut, willCompleteSuccessfully());
        assertThat(fut2, willCompleteSuccessfully());
    }

    private void startCluster(Map<String, PartialNode> cluster) throws Exception {
        for (PartialNode node : cluster.values()) {
            node.start();
        }
    }

    private class PartialNode {
        private final String nodeName;

        private final HybridClock clock;

        private ClusterService clusterService;

        private Loza raftManager;

        private LogStorageFactory partitionsLogStorageFactory;

        private RaftGroupService raftClient;

        private final SafeTimeValuesTracker safeTs = new SafeTimeValuesTracker(HybridTimestamp.MIN_VALUE);

        PartialNode(String nodeName) {
            this.nodeName = nodeName;
            this.clock = new TestHybridClock(() ->
                    nodeName.endsWith("1") ? System.currentTimeMillis()
                            : nodeName.endsWith("2") ? System.currentTimeMillis() + 200 : System.currentTimeMillis() + 400);
        }

        void start() throws Exception {
            clusterService = ClusterServiceTestUtils.clusterService(nodeName, port.getAndIncrement(), NODE_FINDER);

            assertThat(clusterService.startAsync(new ComponentContext()), willCompleteSuccessfully());

            ComponentWorkingDir workingDir = new ComponentWorkingDir(workDir.resolve(nodeName + "_loza"));

            partitionsLogStorageFactory = SharedLogStorageFactoryUtils.create(
                    "test",
                    clusterService.nodeName(),
                    workingDir.raftLogPath(),
                    raftConfiguration.fsync().value()
            );

            assertThat(partitionsLogStorageFactory.startAsync(new ComponentContext()), willCompleteSuccessfully());

            raftManager = TestLozaFactory.create(
                    clusterService,
                    raftConfiguration,
                    systemLocalConfiguration,
                    clock,
                    new RaftGroupEventsClientListener()
            );

            assertThat(raftManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

            TxManager txManagerMock = mock(TxManager.class);

            LeasePlacementDriver placementDriver = mock(LeasePlacementDriver.class);
            when(placementDriver.getCurrentPrimaryReplica(any(), any())).thenReturn(null);

            ClockService clockService = mock(ClockService.class);
            when(clockService.current()).thenReturn(clock.current());

            OutgoingSnapshotsManager outgoingSnapshotsManager = new OutgoingSnapshotsManager(
                    clusterService.nodeName(),
                    clusterService.messagingService(),
                    new NoOpFailureManager()
            );

            this.raftClient = raftManager.startRaftGroupNode(
                    new RaftNodeId(GROUP_ID, new Peer(nodeName)),
                    fromConsistentIds(cluster.keySet()),
                    new ZonePartitionRaftListener(
                            GROUP_ID,
                            mock(TxStatePartitionStorage.class),
                            txManagerMock,
                            safeTs,
                            mock(PendingIndependentComparableValuesTracker.class),
                            outgoingSnapshotsManager,
                            mock(Executor.class)
                    ) {
                        @Override
                        public void onWrite(Iterator<CommandClosure<WriteCommand>> iterator) {
                            if (blockFsmThread) {
                                LOG.info("Blocked on FSM call: safeTs=" + safeTs.current());
                                l1.countDown();

                                try {
                                    assertTrue(l2.await(30_000, TimeUnit.MILLISECONDS));
                                } catch (InterruptedException e) {
                                    fail("Unexpected interruption", e);
                                }
                            }

                            super.onWrite(iterator);
                        }
                    },
                    RaftGroupEventsListener.noopLsnr,
                    RaftGroupOptions.defaults()
                            .maxClockSkew(schemaSynchronizationConfiguration.maxClockSkewMillis().value().intValue())
                            .commandsMarshaller(new ThreadLocalPartitionCommandsMarshaller(clusterService.serializationRegistry()))
                            .serverDataPath(workingDir.metaPath())
                            .setLogStorageFactory(partitionsLogStorageFactory)
            );
        }

        void stop() throws Exception {
            closeAll(
                    raftManager == null ? null : () -> raftManager.stopRaftNodes(GROUP_ID),
                    raftManager == null ? null : raftManager::beforeNodeStop,
                    clusterService == null ? null : clusterService::beforeNodeStop,
                    raftManager == null ? null :
                            () -> assertThat(raftManager.stopAsync(new ComponentContext()), willCompleteSuccessfully()),
                    partitionsLogStorageFactory == null ? null :
                            () -> assertThat(partitionsLogStorageFactory.stopAsync(new ComponentContext()), willCompleteSuccessfully()),
                    clusterService == null ? null :
                            () -> assertThat(clusterService.stopAsync(new ComponentContext()), willCompleteSuccessfully())
            );
        }
    }
}
