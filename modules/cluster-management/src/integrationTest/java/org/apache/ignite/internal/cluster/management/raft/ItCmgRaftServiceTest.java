/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.cluster.management.raft;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.will;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.utils.ClusterServiceTestUtils.clusterService;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NodeFinder;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Class with tests for the {@link CmgRaftService}.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ItCmgRaftServiceTest {
    private static final String TEST_GROUP = "test_group";

    private class Node {
        CmgRaftService raftService;

        final ClusterService clusterService;

        private final Loza raftManager;

        Node(TestInfo testInfo, NetworkAddress addr, NodeFinder nodeFinder, Path workDir) {
            this.clusterService = clusterService(testInfo, addr.port(), nodeFinder);
            this.raftManager = new Loza(clusterService, workDir);
        }

        void start() {
            clusterService.start();
            raftManager.start();
        }

        void afterNodeStart() throws Exception {
            assertTrue(waitForCondition(() -> clusterService.topologyService().allMembers().size() == cluster.size(), 1000));

            var raftStorage = new ConcurrentMapClusterStateStorage();

            raftStorage.start();

            CompletableFuture<RaftGroupService> raftService = raftManager.prepareRaftGroup(
                    TEST_GROUP,
                    List.copyOf(clusterService.topologyService().allMembers()),
                    () -> new CmgRaftGroupListener(raftStorage)
            );

            this.raftService = new CmgRaftService(raftService.get(), clusterService);
        }

        void beforeNodeStop() throws Exception {
            raftManager.stopRaftGroup(TEST_GROUP);

            raftManager.beforeNodeStop();
            clusterService.beforeNodeStop();
        }

        void stop() throws Exception {
            raftManager.stop();
            clusterService.stop();
        }

        ClusterNode localMember() {
            return clusterService.topologyService().localMember();
        }
    }

    private final List<Node> cluster = new ArrayList<>();

    @BeforeEach
    void setUp(@WorkDirectory Path workDir, TestInfo testInfo) throws Exception {
        var addr1 = new NetworkAddress("localhost", 10000);
        var addr2 = new NetworkAddress("localhost", 10001);

        var nodeFinder = new StaticNodeFinder(List.of(addr1, addr2));

        cluster.add(new Node(testInfo, addr1, nodeFinder, workDir.resolve("node1")));
        cluster.add(new Node(testInfo, addr2, nodeFinder, workDir.resolve("node2")));

        for (Node node : cluster) {
            node.start();
        }

        for (Node node : cluster) {
            node.afterNodeStart();
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        for (Node node : cluster) {
            node.beforeNodeStop();
        }

        for (Node node : cluster) {
            node.stop();
        }
    }

    /**
     * Tests the basic scenario of {@link CmgRaftService#logicalTopology} when nodes are joining and leaving.
     */
    @Test
    void testLogicalTopology() {
        Node node1 = cluster.get(0);
        Node node2 = cluster.get(1);

        ClusterNode clusterNode1 = node1.localMember();
        ClusterNode clusterNode2 = node2.localMember();

        assertThat(node1.raftService.logicalTopology(), willBe(empty()));

        assertThat(node1.raftService.startJoinCluster(), willCompleteSuccessfully());
        assertThat(node1.raftService.completeJoinCluster(), willCompleteSuccessfully());

        assertThat(node1.raftService.logicalTopology(), will(contains(clusterNode1)));

        assertThat(node2.raftService.startJoinCluster(), willCompleteSuccessfully());
        assertThat(node2.raftService.completeJoinCluster(), willCompleteSuccessfully());

        assertThat(node1.raftService.logicalTopology(), will(containsInAnyOrder(clusterNode1, clusterNode2)));

        assertThat(node1.raftService.removeFromCluster(Set.of(clusterNode1)), willCompleteSuccessfully());

        assertThat(node1.raftService.logicalTopology(), will(contains(clusterNode2)));

        assertThat(node1.raftService.removeFromCluster(Set.of(clusterNode2)), willCompleteSuccessfully());

        assertThat(node1.raftService.logicalTopology(), willBe(empty()));
    }

    /**
     * Tests that {@link CmgRaftService#startJoinCluster} and {@link CmgRaftService#removeFromCluster} methods are idempotent.
     */
    @Test
    void testLogicalTopologyIdempotence() {
        Node node1 = cluster.get(0);
        Node node2 = cluster.get(1);

        ClusterNode clusterNode1 = node1.localMember();
        ClusterNode clusterNode2 = node2.localMember();

        assertThat(node1.raftService.startJoinCluster(), willCompleteSuccessfully());
        assertThat(node2.raftService.startJoinCluster(), willCompleteSuccessfully());

        assertThat(node1.raftService.completeJoinCluster(), willCompleteSuccessfully());
        assertThat(node2.raftService.completeJoinCluster(), willCompleteSuccessfully());

        assertThat(node1.raftService.logicalTopology(), will(containsInAnyOrder(clusterNode1, clusterNode2)));

        assertThat(node1.raftService.startJoinCluster(), willCompleteSuccessfully());
        assertThat(node1.raftService.completeJoinCluster(), willCompleteSuccessfully());

        assertThat(node1.raftService.logicalTopology(), will(containsInAnyOrder(clusterNode1, clusterNode2)));

        assertThat(node1.raftService.removeFromCluster(Set.of(clusterNode1, clusterNode2)), willCompleteSuccessfully());

        assertThat(node2.raftService.logicalTopology(), willBe(empty()));

        assertThat(node1.raftService.removeFromCluster(Set.of(clusterNode1, clusterNode2)), willCompleteSuccessfully());

        assertThat(node2.raftService.logicalTopology(), willBe(empty()));
    }

    /**
     * Tests that given a set of nodes, only one {@link CmgRaftService#isCurrentNodeLeader} call returns {@code true}.
     */
    @Test
    void testIsCurrentNodeLeader() {
        CompletableFuture<Boolean> onlyOneLeader = cluster.get(0).raftService.isCurrentNodeLeader()
                .thenCombine(cluster.get(1).raftService.isCurrentNodeLeader(), Boolean::logicalXor);

        assertThat(onlyOneLeader, willBe(true));
    }

    /**
     * Tests the {@link CmgRaftService#nodeNames()} method.
     */
    @Test
    void testNodeNames() {
        String[] topology = cluster.get(0).clusterService
                .topologyService()
                .allMembers()
                .stream()
                .map(ClusterNode::name)
                .toArray(String[]::new);

        assertThat(cluster.get(0).raftService.nodeNames(), containsInAnyOrder(topology));
        assertThat(cluster.get(1).raftService.nodeNames(), containsInAnyOrder(topology));
    }

    /**
     * Tests saving and reading a {@link ClusterState}.
     */
    @Test
    void testClusterState() {
        Node node1 = cluster.get(0);
        Node node2 = cluster.get(1);

        assertThat(node1.raftService.readClusterState(), willCompleteSuccessfully());
        assertThat(node2.raftService.readClusterState(), willCompleteSuccessfully());

        ClusterState state = new ClusterState(List.of("foo"), List.of("bar"));

        assertThat(node1.raftService.writeClusterState(state), willCompleteSuccessfully());

        assertThat(node1.raftService.readClusterState(), willBe(state));
        assertThat(node2.raftService.readClusterState(), willBe(state));

        state = new ClusterState(List.of("baz"), List.of("quux"));

        assertThat(node2.raftService.writeClusterState(state), willCompleteSuccessfully());

        assertThat(node1.raftService.readClusterState(), willBe(state));
        assertThat(node2.raftService.readClusterState(), willBe(state));
    }
}
