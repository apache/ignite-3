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

package org.apache.ignite.internal.cluster.management.raft;

import static org.apache.ignite.internal.cluster.management.ClusterState.clusterState;
import static org.apache.ignite.internal.cluster.management.ClusterTag.clusterTag;
import static org.apache.ignite.internal.raft.server.RaftGroupOptions.defaults;
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
import org.apache.ignite.hlc.HybridClock;
import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.cluster.management.ClusterTag;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.internal.cluster.management.raft.commands.JoinReadyCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.JoinRequestCommand;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.properties.IgniteProductVersion;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.NodeStoppingException;
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
@ExtendWith(ConfigurationExtension.class)
public class ItCmgRaftServiceTest {
    private static final String TEST_GROUP = "test_group";

    @InjectConfiguration
    private static RaftConfiguration raftConfiguration;

    private final CmgMessagesFactory msgFactory = new CmgMessagesFactory();

    private class Node {
        CmgRaftService raftService;

        final ClusterService clusterService;

        private final Loza raftManager;

        private final ClusterStateStorage raftStorage = new TestClusterStateStorage();

        Node(TestInfo testInfo, NetworkAddress addr, NodeFinder nodeFinder, Path workDir) {
            this.clusterService = clusterService(testInfo, addr.port(), nodeFinder);
            this.raftManager = new Loza(clusterService, raftConfiguration, workDir, new HybridClock());
        }

        void start() {
            clusterService.start();
            raftManager.start();
        }

        void afterNodeStart() {
            try {
                assertTrue(waitForCondition(() -> clusterService.topologyService().allMembers().size() == cluster.size(), 1000));

                raftStorage.start();

                CompletableFuture<RaftGroupService> raftService = raftManager.prepareRaftGroup(
                        TEST_GROUP,
                        List.copyOf(clusterService.topologyService().allMembers()),
                        () -> new CmgRaftGroupListener(raftStorage),
                        defaults()
                );

                assertThat(raftService, willCompleteSuccessfully());

                this.raftService = new CmgRaftService(raftService.join(), clusterService);
            } catch (InterruptedException | NodeStoppingException e) {
                throw new RuntimeException(e);
            }
        }

        void beforeNodeStop() {
            try {
                raftManager.stopRaftGroup(TEST_GROUP);
            } catch (NodeStoppingException e) {
                throw new RuntimeException(e);
            }

            raftManager.beforeNodeStop();
            clusterService.beforeNodeStop();
        }

        void stop() {
            try {
                IgniteUtils.closeAll(
                        raftManager::stop,
                        raftStorage,
                        clusterService::stop
                );
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        ClusterNode localMember() {
            return clusterService.topologyService().localMember();
        }
    }

    private final List<Node> cluster = new ArrayList<>();

    @BeforeEach
    void setUp(@WorkDirectory Path workDir, TestInfo testInfo) {
        var addr1 = new NetworkAddress("localhost", 10000);
        var addr2 = new NetworkAddress("localhost", 10001);

        var nodeFinder = new StaticNodeFinder(List.of(addr1, addr2));

        cluster.add(new Node(testInfo, addr1, nodeFinder, workDir.resolve("node1")));
        cluster.add(new Node(testInfo, addr2, nodeFinder, workDir.resolve("node2")));

        cluster.parallelStream().forEach(Node::start);
        cluster.parallelStream().forEach(Node::afterNodeStart);
    }

    @AfterEach
    void tearDown() {
        cluster.parallelStream().forEach(Node::beforeNodeStop);
        cluster.parallelStream().forEach(Node::stop);
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

        var clusterState = clusterState(
                msgFactory,
                node1.raftService.nodeNames(),
                node1.raftService.nodeNames(),
                IgniteProductVersion.CURRENT_VERSION,
                clusterTag(msgFactory, "cluster")
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

        var clusterState = clusterState(
                msgFactory,
                node1.raftService.nodeNames(),
                node1.raftService.nodeNames(),
                IgniteProductVersion.CURRENT_VERSION,
                clusterTag(msgFactory, "cluster")
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

        ClusterState state = clusterState(
                msgFactory,
                List.of("foo"),
                List.of("bar"),
                IgniteProductVersion.CURRENT_VERSION,
                clusterTag(msgFactory, "cluster")
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

        ClusterState state = clusterState(
                msgFactory,
                List.of("foo"),
                List.of("bar"),
                IgniteProductVersion.CURRENT_VERSION,
                clusterTag(msgFactory, "cluster")
        );

        assertThat(node1.raftService.initClusterState(state), willCompleteSuccessfully());

        // correct tag
        assertThat(node1.raftService.startJoinCluster(state.clusterTag()), willCompleteSuccessfully());

        // incorrect tag
        var incorrectTag = clusterTag(msgFactory, "invalid");

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

        ClusterState state = clusterState(
                msgFactory,
                List.of("foo"),
                List.of("bar"),
                IgniteProductVersion.fromString("1.2.3"),
                clusterTag(msgFactory, "cluster")
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

        ClusterState state = clusterState(
                msgFactory,
                List.of("foo"),
                List.of("bar"),
                IgniteProductVersion.CURRENT_VERSION,
                clusterTag(msgFactory, "cluster")
        );

        assertThat(raftService.initClusterState(state), willCompleteSuccessfully());

        // Node has not passed validation.
        String errMsg = String.format(
                "JoinReady request denied, reason: Node \"%s\" has not yet passed the validation step",
                cluster.get(0).clusterService.topologyService().localMember()
        );

        assertThrowsWithCause(
                () -> raftService.completeJoinCluster().get(10, TimeUnit.SECONDS),
                IgniteInternalException.class,
                errMsg
        );

        assertThat(raftService.startJoinCluster(state.clusterTag()), willCompleteSuccessfully());

        // Everything is ok after the node has passed validation.
        assertThat(raftService.completeJoinCluster(), willCompleteSuccessfully());
    }

    /**
     * Tests cluster state validation.
     */
    @Test
    void testClusterStateValidation() {
        CmgRaftService raftService = cluster.get(0).raftService;

        ClusterState state = clusterState(
                msgFactory,
                List.of("foo"),
                List.of("bar"),
                IgniteProductVersion.CURRENT_VERSION,
                clusterTag(msgFactory, "cluster")
        );

        assertThat(raftService.initClusterState(state), willCompleteSuccessfully());

        // Valid state
        assertThat(raftService.initClusterState(state), willCompleteSuccessfully());

        // Invalid CMG nodes
        ClusterState invalidCmgState = clusterState(
                msgFactory,
                List.of("baz"),
                List.of("bar"),
                IgniteProductVersion.CURRENT_VERSION,
                clusterTag(msgFactory, "cluster")
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
        ClusterState invalidMsState = clusterState(
                msgFactory,
                List.of("foo"),
                List.of("baz"),
                IgniteProductVersion.CURRENT_VERSION,
                clusterTag(msgFactory, "cluster")
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
        ClusterState invalidVersionState = clusterState(
                msgFactory,
                List.of("foo"),
                List.of("bar"),
                IgniteProductVersion.fromString("1.2.3"),
                clusterTag(msgFactory, "cluster")
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
        ClusterState invalidTagState = clusterState(
                msgFactory,
                List.of("foo"),
                List.of("bar"),
                IgniteProductVersion.CURRENT_VERSION,
                clusterTag(msgFactory, "invalid")
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

    /**
     * Tests that {@link JoinRequestCommand} and {@link JoinReadyCommand} are idempotent.
     */
    @Test
    void testJoinCommandsIdempotence() {
        ClusterState state = clusterState(
                msgFactory,
                List.of("foo"),
                List.of("bar"),
                IgniteProductVersion.CURRENT_VERSION,
                clusterTag(msgFactory, "cluster")
        );

        assertThat(cluster.get(0).raftService.initClusterState(state), willCompleteSuccessfully());

        CmgRaftService service = cluster.get(1).raftService;

        assertThat(service.startJoinCluster(state.clusterTag()), willCompleteSuccessfully());

        assertThat(service.startJoinCluster(state.clusterTag()), willCompleteSuccessfully());

        assertThat(service.completeJoinCluster(), willCompleteSuccessfully());

        assertThat(service.completeJoinCluster(), willCompleteSuccessfully());

        assertThat(service.completeJoinCluster(), willCompleteSuccessfully());
    }
}
