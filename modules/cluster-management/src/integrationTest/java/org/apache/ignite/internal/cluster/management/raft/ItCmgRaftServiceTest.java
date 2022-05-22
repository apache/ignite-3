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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
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
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.cluster.management.ClusterTag;
import org.apache.ignite.internal.properties.IgniteProductVersion;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.lang.IgniteInternalException;
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

        private final ClusterStateStorage raftStorage = new ConcurrentMapClusterStateStorage();

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
            raftStorage.close();
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

        var clusterState = new ClusterState(
                node1.raftService.nodeNames(),
                node1.raftService.nodeNames(),
                IgniteProductVersion.CURRENT_VERSION,
                new ClusterTag("cluster")
        );

        assertThat(node1.raftService.initClusterState(clusterState), willCompleteSuccessfully());

        assertThat(node1.raftService.logicalTopology(), willBe(empty()));

        assertThat(joinCluster(node1, clusterState.clusterTag()), willCompleteSuccessfully());

        assertThat(node1.raftService.logicalTopology(), will(contains(clusterNode1)));

        assertThat(joinCluster(node2, clusterState.clusterTag()), willCompleteSuccessfully());

        assertThat(node1.raftService.logicalTopology(), will(containsInAnyOrder(clusterNode1, clusterNode2)));

        assertThat(node1.raftService.removeFromCluster(Set.of(clusterNode1)), willCompleteSuccessfully());

        assertThat(node1.raftService.logicalTopology(), will(contains(clusterNode2)));

        assertThat(node1.raftService.removeFromCluster(Set.of(clusterNode2)), willCompleteSuccessfully());

        assertThat(node1.raftService.logicalTopology(), willBe(empty()));
    }

    private static CompletableFuture<Void> joinCluster(Node node, ClusterTag clusterTag) {
        return node.raftService.startJoinCluster(clusterTag)
                .thenCompose(v -> node.raftService.completeJoinCluster());
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

        var clusterState = new ClusterState(
                node1.raftService.nodeNames(),
                node1.raftService.nodeNames(),
                IgniteProductVersion.CURRENT_VERSION,
                new ClusterTag("cluster")
        );

        assertThat(node1.raftService.initClusterState(clusterState), willCompleteSuccessfully());

        CompletableFuture<Void> joinFuture1 = joinCluster(node1, clusterState.clusterTag());

        CompletableFuture<Void> joinFuture2 = joinCluster(node2, clusterState.clusterTag());

        assertThat(joinFuture1, willCompleteSuccessfully());
        assertThat(joinFuture2, willCompleteSuccessfully());

        assertThat(node1.raftService.logicalTopology(), will(containsInAnyOrder(clusterNode1, clusterNode2)));

        joinFuture1 = joinCluster(node1, clusterState.clusterTag());

        assertThat(joinFuture1, willCompleteSuccessfully());

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

        ClusterState state = new ClusterState(
                List.of("foo"),
                List.of("bar"),
                IgniteProductVersion.CURRENT_VERSION,
                new ClusterTag("cluster")
        );

        assertThat(node1.raftService.initClusterState(state), willCompleteSuccessfully());

        assertThat(node1.raftService.readClusterState(), willBe(state));
        assertThat(node2.raftService.readClusterState(), willBe(state));
    }

    /**
     * Test validation of the Cluster Tag.
     */
    @Test
    void testClusterTagValidation() {
        Node node1 = cluster.get(0);
        Node node2 = cluster.get(1);

        ClusterState state = new ClusterState(
                List.of("foo"),
                List.of("bar"),
                IgniteProductVersion.CURRENT_VERSION,
                new ClusterTag("cluster")
        );

        assertThat(node1.raftService.initClusterState(state), willCompleteSuccessfully());

        // correct tag
        assertThat(node1.raftService.startJoinCluster(state.clusterTag()), willCompleteSuccessfully());

        // incorrect tag
        var incorrectTag = new ClusterTag("invalid");

        assertThrowsWithCause(
                () -> node2.raftService.startJoinCluster(incorrectTag).get(10, TimeUnit.SECONDS),
                IgniteInternalException.class,
                String.format(
                        "Join request denied, reason: Cluster tags do not match. Cluster tag: %s, cluster tag stored in CMG: %s",
                        incorrectTag, state.clusterTag()
                )
        );
    }

    /**
     * Test validation of Ignite Product Version upon join.
     */
    @Test
    void testIgniteVersionValidation() {
        CmgRaftService raftService = cluster.get(0).raftService;

        ClusterState state = new ClusterState(
                List.of("foo"),
                List.of("bar"),
                IgniteProductVersion.fromString("1.2.3"),
                new ClusterTag("cluster")
        );

        assertThat(raftService.initClusterState(state), willCompleteSuccessfully());

        assertThrowsWithCause(
                () -> raftService.startJoinCluster(state.clusterTag()).get(10, TimeUnit.SECONDS),
                IgniteInternalException.class,
                String.format(
                        "Join request denied, reason: Ignite versions do not match. Version: %s, version stored in CMG: %s",
                        IgniteProductVersion.CURRENT_VERSION, state.igniteVersion()
                )
        );
    }

    /**
     * Tests that join commands can only be executed in a sequential order: startJoinCluster -> completeJoinCluster.
     */
    @Test
    void testValidationCommandOrder() {
        CmgRaftService raftService = cluster.get(0).raftService;

        ClusterState state = new ClusterState(
                List.of("foo"),
                List.of("bar"),
                IgniteProductVersion.CURRENT_VERSION,
                new ClusterTag("cluster")
        );

        assertThat(raftService.initClusterState(state), willCompleteSuccessfully());

        // Node has not passed validation.
        String errMsg = String.format(
                "JoinReady request denied, reason: Node \"%s\" has not yet passed the validation step",
                cluster.get(0).clusterService.topologyService().localMember().id()
        );

        assertThrowsWithCause(
                () -> raftService.completeJoinCluster().get(10, TimeUnit.SECONDS),
                IgniteInternalException.class,
                errMsg
        );

        assertThat(raftService.startJoinCluster(state.clusterTag()), willCompleteSuccessfully());

        // Everything is ok after the node has passed validation.
        assertThat(raftService.completeJoinCluster(), willCompleteSuccessfully());

        // Validation state is cleared after the first successful attempt.
        assertThrowsWithCause(
                () -> raftService.completeJoinCluster().get(10, TimeUnit.SECONDS),
                IgniteInternalException.class,
                errMsg
        );
    }

    /**
     * Tests cluster state validation.
     */
    @Test
    void testClusterStateValidation() {
        CmgRaftService raftService = cluster.get(0).raftService;

        ClusterState state = new ClusterState(
                List.of("foo"),
                List.of("bar"),
                IgniteProductVersion.CURRENT_VERSION,
                new ClusterTag("cluster")
        );

        assertThat(raftService.initClusterState(state), willCompleteSuccessfully());

        // Valid state
        assertThat(raftService.initClusterState(state), willCompleteSuccessfully());

        // Invalid CMG nodes
        ClusterState invalidCmgState = new ClusterState(
                List.of("baz"),
                List.of("bar"),
                IgniteProductVersion.CURRENT_VERSION,
                new ClusterTag("cluster")
        );

        assertThrowsWithCause(
                () -> raftService.initClusterState(invalidCmgState).get(10, TimeUnit.SECONDS),
                IgniteInternalException.class,
                String.format(
                        "Init CMG request denied, reason: CMG node names do not match. CMG nodes: %s, nodes stored in CMG: %s",
                        invalidCmgState.cmgNodes(), state.cmgNodes()
                )
        );

        // Invalid MetaStorage nodes
        ClusterState invalidMsState = new ClusterState(
                List.of("foo"),
                List.of("baz"),
                IgniteProductVersion.CURRENT_VERSION,
                new ClusterTag("cluster")
        );

        assertThrowsWithCause(
                () -> raftService.initClusterState(invalidMsState).get(10, TimeUnit.SECONDS),
                IgniteInternalException.class,
                String.format(
                        "Init CMG request denied, reason: MetaStorage node names do not match. "
                                + "MetaStorage nodes: %s, nodes stored in CMG: %s",
                        invalidMsState.metaStorageNodes(), state.metaStorageNodes()
                )
        );

        // Invalid version
        ClusterState invalidVersionState = new ClusterState(
                List.of("foo"),
                List.of("bar"),
                IgniteProductVersion.fromString("1.2.3"),
                new ClusterTag("cluster")
        );

        assertThrowsWithCause(
                () -> raftService.initClusterState(invalidVersionState).get(10, TimeUnit.SECONDS),
                IgniteInternalException.class,
                String.format(
                        "Init CMG request denied, reason: Ignite versions do not match. Version: %s, version stored in CMG: %s",
                        invalidVersionState.igniteVersion(), state.igniteVersion()
                )
        );

        // Invalid tag
        ClusterState invalidTagState = new ClusterState(
                List.of("foo"),
                List.of("bar"),
                IgniteProductVersion.CURRENT_VERSION,
                new ClusterTag("invalid")
        );

        assertThrowsWithCause(
                () -> raftService.initClusterState(invalidTagState).get(10, TimeUnit.SECONDS),
                IgniteInternalException.class,
                String.format(
                        "Init CMG request denied, reason: Cluster names do not match. Cluster name: %s, cluster name stored in CMG: %s",
                        invalidTagState.clusterTag().clusterName(), state.clusterTag().clusterName()
                )
        );
    }
}
