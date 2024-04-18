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

package org.apache.ignite.internal.raft;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.network.utils.ClusterServiceTestUtils.findLocalAddresses;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.will;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.raft.jraft.test.TestUtils.waitForCondition;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.NodeFinder;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.network.utils.ClusterServiceTestUtils;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.TestReplicationGroupId;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Integration test methods of raft group service.
 */
@ExtendWith(MockitoExtension.class)
@ExtendWith(ConfigurationExtension.class)
public class ItRaftGroupServiceTest extends IgniteAbstractTest {
    private static final int NODES_CNT = 2;

    private static final int NODE_PORT_BASE = 20_000;

    private static final TestReplicationGroupId RAFT_GROUP_NAME = new TestReplicationGroupId("part1");

    private static final NodeFinder NODE_FINDER = new StaticNodeFinder(findLocalAddresses(NODE_PORT_BASE, NODE_PORT_BASE + NODES_CNT));

    private final List<TestNode> nodes = new ArrayList<>();

    @Mock
    private RaftGroupEventsListener eventsListener;

    @InjectConfiguration
    private RaftConfiguration raftConfiguration;

    @BeforeEach
    public void setUp(TestInfo testInfo) {
        for (int i = 0; i < NODES_CNT; i++) {
            startNode(testInfo);
        }

        PeersAndLearners configuration = nodes.stream()
                .map(TestNode::name)
                .collect(collectingAndThen(toSet(), PeersAndLearners::fromConsistentIds));

        CompletableFuture<?>[] svcFutures = nodes.stream()
                .map(node -> node.startRaftGroup(configuration))
                .toArray(CompletableFuture[]::new);

        assertThat(CompletableFuture.allOf(svcFutures), willCompleteSuccessfully());
    }

    private TestNode startNode(TestInfo testInfo) {
        var node = new TestNode(testInfo);

        node.start();

        nodes.add(node);

        return node;
    }

    @AfterEach
    public void tearDown() throws Exception {
        IgniteUtils.closeAll(nodes.parallelStream().map(node -> node::beforeNodeStop));
        IgniteUtils.closeAll(nodes.parallelStream().map(node -> node::stop));
    }

    @Test
    @Timeout(20)
    public void testTransferLeadership() {
        assertThat(nodes.get(0).raftGroupService, willCompleteSuccessfully());

        Peer leader = nodes.get(0).raftGroupService.join().leader();

        TestNode oldLeaderNode = nodes.stream()
                .filter(node -> node.name().equals(leader.consistentId()))
                .findFirst()
                .orElseThrow();

        TestNode newLeaderNode = nodes.stream()
                .filter(node -> !node.name().equals(leader.consistentId()))
                .findFirst()
                .orElseThrow();

        Peer expectedNewLeaderPeer = new Peer(newLeaderNode.name());

        CompletableFuture<Void> transferLeadership = oldLeaderNode.raftGroupService
                .thenCompose(service -> service.transferLeadership(expectedNewLeaderPeer));

        assertThat(transferLeadership, willCompleteSuccessfully());

        assertThat(oldLeaderNode.raftGroupService.thenApply(RaftGroupService::leader), willBe(expectedNewLeaderPeer));

        assertTrue(waitForCondition(() -> {
            assertThat(newLeaderNode.raftGroupService.thenCompose(RaftGroupService::refreshLeader), willCompleteSuccessfully());

            return expectedNewLeaderPeer.equals(newLeaderNode.raftGroupService.join().leader());
        }, 10_000));
    }

    @Test
    public void testChangePeersAsync(TestInfo testInfo) throws InterruptedException {
        // Start some new followers.
        List<TestNode> newFollowers = List.of(startNode(testInfo), startNode(testInfo));

        Set<String> newFollowersConfig = nodes.stream().map(TestNode::name).collect(toSet());

        // Start some new learners.
        List<TestNode> newLearners = List.of(startNode(testInfo), startNode(testInfo));

        Set<String> newLearnersConfig = newLearners.stream().map(TestNode::name).collect(toSet());

        PeersAndLearners configuration = PeersAndLearners.fromConsistentIds(newFollowersConfig, newLearnersConfig);

        // Start Raft groups on the new nodes with the new configuration.
        CompletableFuture<?>[] startedServices = Stream.concat(newFollowers.stream(), newLearners.stream())
                .map(node -> node.startRaftGroup(configuration))
                .toArray(CompletableFuture[]::new);

        assertThat(CompletableFuture.allOf(startedServices), willCompleteSuccessfully());

        // Change Raft configuration and wait until it's applied.
        var configurationComplete = new CountDownLatch(1);

        doAnswer(invocation -> {
            configurationComplete.countDown();

            return null;
        }).when(eventsListener).onNewPeersConfigurationApplied(any());

        CompletableFuture<Void> changePeersFuture = nodes.get(0).raftGroupService
                .thenCompose(service -> service.refreshAndGetLeaderWithTerm()
                        .thenCompose(l -> service.changePeersAsync(configuration, l.term()))
                );

        assertThat(changePeersFuture, willCompleteSuccessfully());

        assertTrue(configurationComplete.await(10, TimeUnit.SECONDS));

        // Check that configuration is the same on all nodes.
        for (TestNode node : nodes) {
            assertThat(
                    node.raftGroupService.thenCompose(service -> service.refreshMembers(true)),
                    willCompleteSuccessfully()
            );
            assertThat(
                    node.raftGroupService.thenApply(RaftGroupService::peers),
                    will(containsInAnyOrder(configuration.peers().toArray()))
            );
            assertThat(
                    node.raftGroupService.thenApply(RaftGroupService::learners),
                    will(containsInAnyOrder(configuration.learners().toArray()))
            );
        }
    }

    private class TestNode {
        private final ClusterService clusterService;
        private final Loza loza;
        private CompletableFuture<RaftGroupService> raftGroupService;

        TestNode(TestInfo testInfo) {
            this.clusterService = ClusterServiceTestUtils.clusterService(testInfo, NODE_PORT_BASE + nodes.size(), NODE_FINDER);
            this.loza = new Loza(clusterService, raftConfiguration, workDir.resolve("node" + nodes.size()), new HybridClockImpl());
        }

        String name() {
            return clusterService.topologyService().localMember().name();
        }

        void start() {
            clusterService.startAsync();
            loza.startAsync();
        }

        CompletableFuture<RaftGroupService> startRaftGroup(PeersAndLearners configuration) {
            String nodeName = clusterService.topologyService().localMember().name();

            Peer serverPeer = configuration.peer(nodeName);

            var nodeId = new RaftNodeId(RAFT_GROUP_NAME, serverPeer == null ? configuration.learner(nodeName) : serverPeer);

            try {
                raftGroupService = loza.startRaftGroupNodeAndWaitNodeReadyFuture(
                        nodeId, configuration, mock(RaftGroupListener.class), eventsListener
                );
            } catch (NodeStoppingException e) {
                return CompletableFuture.failedFuture(e);
            }

            return raftGroupService;
        }

        void beforeNodeStop() throws Exception {
            Stream<AutoCloseable> shutdownService = Stream.of(
                    raftGroupService == null
                            ? null
                            : (AutoCloseable) () -> raftGroupService.get(1, TimeUnit.SECONDS).shutdown()
            );

            Stream<AutoCloseable> stopRaftGroups = loza.localNodes().stream().map(id -> () -> loza.stopRaftNode(id));

            Stream<AutoCloseable> beforeNodeStop = Stream.of(loza::beforeNodeStop, clusterService::beforeNodeStop);

            IgniteUtils.closeAll(Stream.of(shutdownService, stopRaftGroups, beforeNodeStop).flatMap(Function.identity()));
        }

        void stop() throws Exception {
            IgniteUtils.closeAll(loza::stopAsync, clusterService::stopAsync);
        }
    }
}
