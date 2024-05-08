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

package org.apache.ignite.internal.cluster.management;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.will;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cluster.management.raft.JoinDeniedException;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Integration tests for {@link ClusterManagementGroupManager}.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ItClusterManagerTest extends BaseItClusterManagementTest {
    private final List<MockNode> cluster = new ArrayList<>();

    @WorkDirectory
    private Path workDir;

    @AfterEach
    void tearDown() throws Exception {
        stopCluster();
    }

    private void startCluster(int numNodes, TestInfo testInfo) {
        cluster.addAll(createNodes(numNodes, testInfo, workDir));

        cluster.parallelStream().forEach(MockNode::start);
    }

    private void stopCluster() throws Exception {
        stopNodes(cluster);
    }

    /**
     * Tests initial cluster setup.
     */
    @Test
    void testInit(TestInfo testInfo) throws Exception {
        startCluster(2, testInfo);

        String[] cmgNodes = { cluster.get(0).name() };

        String[] metaStorageNodes = { cluster.get(1).name() };

        initCluster(metaStorageNodes, cmgNodes);

        assertThat(cluster.get(0).clusterManager().metaStorageNodes(), will(containsInAnyOrder(metaStorageNodes)));
        assertThat(cluster.get(1).clusterManager().metaStorageNodes(), will(containsInAnyOrder(metaStorageNodes)));

        LogicalNode[] expectedTopology = toLogicalNodes(currentPhysicalTopology());

        assertThat(cluster.get(0).logicalTopologyNodes(), will(containsInAnyOrder(expectedTopology)));
        assertThat(cluster.get(1).logicalTopologyNodes(), will(containsInAnyOrder(expectedTopology)));
    }

    /**
     * Tests initial cluster setup with provided configuration.
     */
    @Test
    void testInitWithProvidedConfiguration(TestInfo testInfo) throws Exception {
        startCluster(3, testInfo);

        String[] cmgNodes = { cluster.get(0).name() };

        String[] metaStorageNodes = { cluster.get(1).name() };

        String configuration = "{security: {enabled: true}}";

        initCluster(metaStorageNodes, cmgNodes, configuration);

        for (MockNode node : cluster) {
            assertThat(node.clusterManager().initialClusterConfigurationFuture(), willBe(configuration));
        }
    }

    /**
     * Tests that init fails in case some nodes cannot be found.
     */
    @Test
    void testInitDeadNodes(TestInfo testInfo) throws Exception {
        startCluster(2, testInfo);

        String[] allNodes = clusterNodeNames();

        MockNode nodeToStop = cluster.remove(0);

        stopNodes(List.of(nodeToStop));

        assertThrows(InitException.class, () -> initCluster(allNodes, allNodes));
    }

    /**
     * Tests that re-running init after a failed init attempt can succeed.
     */
    @Test
    void testInitCancel(TestInfo testInfo) throws Exception {
        startCluster(2, testInfo);

        String[] allNodes = clusterNodeNames();

        // stop a CMG node to make the init fail

        MockNode nodeToStop = cluster.remove(0);

        stopNodes(List.of(nodeToStop));

        assertThrows(InitException.class, () -> initCluster(allNodes, allNodes));

        // complete initialization with one node to check that it finishes correctly

        String[] aliveNodes = {cluster.get(0).name()};

        initCluster(aliveNodes, aliveNodes);

        assertThat(cluster.get(0).clusterManager().metaStorageNodes(), will(containsInAnyOrder(aliveNodes)));

        assertThat(cluster.get(0).logicalTopologyNodes(), will(containsInAnyOrder(toLogicalNodes(currentPhysicalTopology()))));
    }

    /**
     * Tests a scenario when a node is restarted.
     */
    @Test
    void testNodeRestart(TestInfo testInfo) throws Exception {
        startCluster(2, testInfo);

        String[] cmgNodes = {cluster.get(0).name()};

        String[] metaStorageNodes = {cluster.get(1).name()};

        initCluster(metaStorageNodes, cmgNodes);

        assertThat(cluster.get(0).clusterManager().metaStorageNodes(), will(containsInAnyOrder(metaStorageNodes)));
        assertThat(cluster.get(1).clusterManager().metaStorageNodes(), will(containsInAnyOrder(metaStorageNodes)));

        cluster.get(0).restart();

        assertThat(cluster.get(0).startFuture(), willCompleteSuccessfully());

        assertThat(cluster.get(0).clusterManager().metaStorageNodes(), will(containsInAnyOrder(metaStorageNodes)));

        waitForLogicalTopology();

        LogicalNode[] expectedTopology = toLogicalNodes(currentPhysicalTopology());

        assertThat(cluster.get(0).logicalTopologyNodes(), will(containsInAnyOrder(expectedTopology)));
        assertThat(cluster.get(1).logicalTopologyNodes(), will(containsInAnyOrder(expectedTopology)));
    }

    /**
     * Tests executing the init command with incorrect node names.
     */
    @Test
    void testInitInvalidNodes(TestInfo testInfo) throws Exception {
        startCluster(2, testInfo);

        ClusterManagementGroupManager clusterManager = cluster.get(0).clusterManager();

        // non-existent node
        assertThrowsWithCause(
                () -> clusterManager.initCluster(List.of("wrong"), List.of(), "cluster"),
                InitException.class,
                "Node \"wrong\" is not present in the physical topology"
        );

        // successful init
        clusterManager.initCluster(List.of(cluster.get(0).name()), List.of(), "cluster");

        for (MockNode node : cluster) {
            assertThat(node.clusterManager().joinFuture(), willCompleteSuccessfully());
        }

        // different node
        assertThrowsWithCause(
                () -> clusterManager.initCluster(List.of(cluster.get(1).name()), List.of(), "cluster"),
                InitException.class,
                "Init CMG request denied, reason: CMG node names do not match."
        );
    }

    /**
     * Tests a scenario, when every node in a cluster gets restarted.
     */
    @Test
    void testClusterRestart(TestInfo testInfo) throws Exception {
        startCluster(3, testInfo);

        String[] cmgNodes = {
                cluster.get(0).name(),
                cluster.get(1).name()
        };

        String[] metaStorageNodes = { cluster.get(2).name() };

        initCluster(metaStorageNodes, cmgNodes);

        for (MockNode node : cluster) {
            assertThat(node.startFuture(), willCompleteSuccessfully());
        }

        for (MockNode node : cluster) {
            node.restart();
        }

        for (MockNode node : cluster) {
            assertThat(node.startFuture(), willCompleteSuccessfully());
        }

        assertThat(cluster.get(0).logicalTopologyNodes(), will(containsInAnyOrder(toLogicalNodes(currentPhysicalTopology()))));
    }

    /**
     * Tests a scenario when a new node joins a cluster.
     */
    @Test
    void testNodeJoin(TestInfo testInfo) throws Exception {
        startCluster(2, testInfo);

        String[] cmgNodes = clusterNodeNames();

        initCluster(cmgNodes, cmgNodes);

        // create and start a new node
        MockNode node = addNodeToCluster(cluster, testInfo, workDir);

        node.start();

        assertThat(node.startFuture(), willCompleteSuccessfully());

        assertThat(node.logicalTopologyNodes(), will(containsInAnyOrder(toLogicalNodes(currentPhysicalTopology()))));
        assertThat(node.validatedNodes(), will(containsInAnyOrder(toLogicalNodes(currentPhysicalTopology()))));
    }

    /**
     * Tests a scenario when a node leaves a cluster.
     */
    @Test
    void testNodeLeave(TestInfo testInfo) throws Exception {
        startCluster(2, testInfo);

        String[] cmgNodes = { cluster.get(0).name() };

        initCluster(cmgNodes, cmgNodes);

        assertThat(cluster.get(0).logicalTopologyNodes(), will(containsInAnyOrder(toLogicalNodes(currentPhysicalTopology()))));

        MockNode nodeToStop = cluster.remove(1);

        stopNodes(List.of(nodeToStop));

        waitForLogicalTopology();

        assertThat(cluster.get(0).logicalTopologyNodes(), will(containsInAnyOrder(toLogicalNodes(currentPhysicalTopology()))));
        assertThat(cluster.get(0).validatedNodes(), will(containsInAnyOrder(toLogicalNodes(currentPhysicalTopology()))));
    }

    /**
     * Tests a scenario when a node, that participated in a cluster, tries to join a new one.
     */
    @Test
    void testJoinInvalidTag(TestInfo testInfo) throws Exception {
        // Start a cluster and initialize it
        startCluster(2, testInfo);

        MockNode firstNode = cluster.get(0);
        MockNode secondNode = cluster.get(1);

        String[] cmgNodes = { firstNode.name() };

        initCluster(cmgNodes, cmgNodes);

        // Stop the cluster
        stopCluster();

        // Remove all persistent state from the first node
        IgniteUtils.deleteIfExists(firstNode.workDir());

        // Start the nodes again
        firstNode.restart();

        // Initialize the cluster again, but with a different name. It is expected that the second node will try to join the CMG
        // and will be rejected.
        firstNode.clusterManager().initCluster(
                Arrays.asList(cmgNodes),
                Arrays.asList(cmgNodes),
                "cluster2"
        );

        assertThat(firstNode.startFuture(), willCompleteSuccessfully());

        secondNode.restart();

        assertThat(
                secondNode.startFuture(),
                willThrow(JoinDeniedException.class, "Join request denied, reason: Cluster tags do not match")
        );
    }

    /**
     * Tests a scenario when a node starts joining a cluster having a CMG leader, but finishes the join after the CMG leader changed.
     */
    @Test
    void testLeaderChangeDuringJoin(TestInfo testInfo) throws Exception {
        // Start a cluster of 3 nodes so that the CMG leader node could be stopped later.
        startCluster(3, testInfo);

        String[] cmgNodes = clusterNodeNames();

        // Start the CMG on all 3 nodes.
        initCluster(cmgNodes, cmgNodes);

        // Start a new node, but do not send the JoinReadyCommand.
        MockNode node = addNodeToCluster(cluster, testInfo, workDir);

        assertThat(node.startComponents(), willCompleteSuccessfully());

        assertThat(node.clusterManager().joinFuture(), willCompleteSuccessfully());

        // Find the CMG leader and stop it
        MockNode leaderNode = findLeaderNode(cluster).orElseThrow();

        stopNodes(List.of(leaderNode));

        // Issue the JoinReadyCommand on the joining node. It is expected that the joining node is still treated as validated.
        assertThat(node.clusterManager().onJoinReady(), willCompleteSuccessfully());
    }

    @Test
    void testLeaderChangeBeforeJoin(TestInfo testInfo) throws Exception {
        // Start a cluster of 3 nodes so that the CMG leader node could be stopped later.
        startCluster(3, testInfo);

        String[] cmgNodes = clusterNodeNames();

        // Start the CMG on all 3 nodes.
        initCluster(cmgNodes, cmgNodes);

        // Find the CMG leader and stop it
        MockNode leaderNode = findLeaderNode(cluster).orElseThrow();

        stopNodes(List.of(leaderNode));

        // Start a new node.
        MockNode node = addNodeToCluster(cluster, testInfo, workDir);

        node.start();

        assertThat(node.clusterManager().joinFuture(), willCompleteSuccessfully());
    }

    @Test
    void nonCmgMemberOfInitialTopologyGetsLogicalTopologyChanges(TestInfo testInfo) throws Exception {
        startCluster(2, testInfo);

        String[] cmgNodes = { cluster.get(0).name() };

        initCluster(cmgNodes, cmgNodes);

        MockNode nonCmgNode = cluster.get(1);
        LogicalTopologyImpl nonCmgTopology = nonCmgNode.clusterManager().logicalTopologyImpl();

        assertTrue(waitForCondition(() -> nonCmgTopology.getLogicalTopology().nodes().size() == 2, 10_000));
    }

    @Test
    void nonCmgNodeAddedLaterGetsLogicalTopologyChanges(TestInfo testInfo) throws Exception {
        startCluster(1, testInfo);

        String[] cmgNodes = { cluster.get(0).name() };

        initCluster(cmgNodes, cmgNodes);

        MockNode nonCmgNode = addNodeToCluster(cluster, testInfo, workDir);
        nonCmgNode.start();
        assertThat(nonCmgNode.startFuture(), willCompleteSuccessfully());

        LogicalTopologyImpl nonCmgTopology = nonCmgNode.clusterManager().logicalTopologyImpl();

        assertTrue(waitForCondition(() -> nonCmgTopology.getLogicalTopology().nodes().size() == 2, 10_000));
    }

    @Test
    void majority(TestInfo testInfo) throws NodeStoppingException {
        startCluster(5, testInfo);

        String[] allNodes = clusterNodeNames();

        initCluster(allNodes, allNodes);

        MockNode leaderNode = findLeaderNode(cluster).orElseThrow();

        Set<String> majority = cluster.get(0).clusterManager().majority().join();

        assertThat(majority, hasSize(3));
        assertThat(majority, hasItem(leaderNode.name()));
    }

    private Optional<MockNode> findLeaderNode(List<MockNode> cluster) {
        return cluster.stream()
                .filter(n -> {
                    CompletableFuture<Boolean> isLeader = n.clusterManager().isCmgLeader();

                    assertThat(isLeader, willCompleteSuccessfully());

                    return isLeader.join();
                })
                .findAny();
    }

    private List<ClusterNode> currentPhysicalTopology() {
        return cluster.stream()
                .map(MockNode::localMember)
                .collect(Collectors.toList());
    }

    private static LogicalNode[] toLogicalNodes(List<ClusterNode> clusterNodes) {
        return clusterNodes.stream().map(LogicalNode::new).toArray(LogicalNode[]::new);
    }

    private String[] clusterNodeNames() {
        return cluster.stream()
                .map(MockNode::name)
                .toArray(String[]::new);
    }

    private void waitForLogicalTopology() throws InterruptedException {
        assertTrue(waitForCondition(() -> {
            CompletableFuture<Set<LogicalNode>> logicalTopology = cluster.get(0).logicalTopologyNodes();

            assertThat(logicalTopology, willCompleteSuccessfully());

            return logicalTopology.join().size() == cluster.size();
        }, 10000));
    }


    private void initCluster(String[] metaStorageNodes, String[] cmgNodes) throws NodeStoppingException {
        initCluster(metaStorageNodes, cmgNodes, null);
    }

    private void initCluster(
            String[] metaStorageNodes,
            String[] cmgNodes,
            @Nullable String clusterConfiguration
    ) throws NodeStoppingException {
        cluster.get(0).clusterManager().initCluster(
                Arrays.asList(metaStorageNodes),
                Arrays.asList(cmgNodes),
                "cluster",
                clusterConfiguration
        );

        for (MockNode node : cluster) {
            assertThat(node.startFuture(), willCompleteSuccessfully());
        }
    }
}
