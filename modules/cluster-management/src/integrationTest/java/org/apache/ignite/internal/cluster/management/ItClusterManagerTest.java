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

package org.apache.ignite.internal.cluster.management;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.will;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.StaticNodeFinder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Integration tests for {@link ClusterManagementGroupManager}.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ItClusterManagerTest {
    private static final int PORT_BASE = 10000;

    private final List<MockNode> cluster = new ArrayList<>();

    @WorkDirectory
    private Path workDir;

    @AfterEach
    void tearDown() {
        stopCluster();
    }

    private void startCluster(int numNodes, TestInfo testInfo) throws IOException {
        List<NetworkAddress> addrs = IntStream.range(0, numNodes)
                .mapToObj(i -> new NetworkAddress("localhost", PORT_BASE + i))
                .collect(toList());

        StaticNodeFinder nodeFinder = new StaticNodeFinder(addrs);

        for (int i = 0; i < numNodes; ++i) {
            var node = new MockNode(testInfo, addrs.get(i), nodeFinder, workDir.resolve("node" + i));

            cluster.add(node);
        }

        for (MockNode node : cluster) {
            node.start();
        }
    }

    private void stopCluster() {
        for (MockNode node : cluster) {
            node.beforeNodeStop();
        }

        for (MockNode node : cluster) {
            node.stop();
        }
    }

    /**
     * Tests initial cluster setup.
     */
    @Test
    void testInit(TestInfo testInfo) throws Exception {
        startCluster(2, testInfo);

        String[] cmgNodes = { cluster.get(0).localMember().name() };

        String[] metaStorageNodes = { cluster.get(1).localMember().name() };

        initCluster(metaStorageNodes, cmgNodes);

        assertThat(cluster.get(0).clusterManager().metaStorageNodes(), will(containsInAnyOrder(metaStorageNodes)));
        assertThat(cluster.get(1).clusterManager().metaStorageNodes(), will(containsInAnyOrder(metaStorageNodes)));

        ClusterNode[] expectedTopology = currentPhysicalTopology();

        assertThat(cluster.get(0).clusterManager().logicalTopology(), will(containsInAnyOrder(expectedTopology)));
        assertThat(cluster.get(1).clusterManager().logicalTopology(), will(containsInAnyOrder(expectedTopology)));
    }

    /**
     * Tests that init fails in case some nodes cannot be found.
     */
    @Test
    void testInitDeadNodes(TestInfo testInfo) throws Exception {
        startCluster(2, testInfo);

        String[] allNodes = clusterNodeNames();

        MockNode nodeToStop = cluster.remove(0);

        nodeToStop.beforeNodeStop();
        nodeToStop.stop();

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

        nodeToStop.beforeNodeStop();
        nodeToStop.stop();

        assertThrows(InitException.class, () -> initCluster(allNodes, allNodes));

        // complete initialization with one node to check that it finishes correctly

        String[] aliveNodes = {cluster.get(0).localMember().name()};

        initCluster(aliveNodes, aliveNodes);

        assertThat(cluster.get(0).clusterManager().metaStorageNodes(), will(containsInAnyOrder(aliveNodes)));

        assertThat(cluster.get(0).clusterManager().logicalTopology(), will(containsInAnyOrder(currentPhysicalTopology())));
    }

    /**
     * Tests a scenario when a node is restarted.
     */
    @Test
    void testNodeRestart(TestInfo testInfo) throws Exception {
        startCluster(2, testInfo);

        String[] cmgNodes = {cluster.get(0).localMember().name()};

        String[] metaStorageNodes = {cluster.get(1).localMember().name()};

        initCluster(metaStorageNodes, cmgNodes);

        assertThat(cluster.get(0).clusterManager().metaStorageNodes(), will(containsInAnyOrder(metaStorageNodes)));
        assertThat(cluster.get(1).clusterManager().metaStorageNodes(), will(containsInAnyOrder(metaStorageNodes)));

        cluster.get(0).restart();

        assertThat(cluster.get(0).startFuture(), willCompleteSuccessfully());

        assertThat(cluster.get(0).clusterManager().metaStorageNodes(), will(containsInAnyOrder(metaStorageNodes)));

        ClusterNode[] expectedTopology = currentPhysicalTopology();

        assertThat(cluster.get(0).clusterManager().logicalTopology(), will(containsInAnyOrder(expectedTopology)));
        assertThat(cluster.get(1).clusterManager().logicalTopology(), will(containsInAnyOrder(expectedTopology)));
    }

    /**
     * Tests a scenario, when every node in a cluster gets restarted.
     */
    @Test
    void testClusterRestart(TestInfo testInfo) throws Exception {
        startCluster(3, testInfo);

        String[] cmgNodes = {
                cluster.get(0).localMember().name(),
                cluster.get(1).localMember().name()
        };

        String[] metaStorageNodes = { cluster.get(2).localMember().name() };

        initCluster(cmgNodes, metaStorageNodes);

        for (MockNode node : cluster) {
            assertThat(node.startFuture(), willCompleteSuccessfully());
        }

        for (MockNode node : cluster) {
            node.restart();
        }

        for (MockNode node : cluster) {
            assertThat(node.startFuture(), willCompleteSuccessfully());
        }

        assertThat(cluster.get(0).clusterManager().logicalTopology(), will(containsInAnyOrder(currentPhysicalTopology())));
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
        var addr = new NetworkAddress("localhost", PORT_BASE + cluster.size());

        var nodeFinder = new StaticNodeFinder(Arrays.asList(clusterNodeAddresses()));

        var node = new MockNode(testInfo, addr, nodeFinder, workDir.resolve("node" + cluster.size()));

        cluster.add(node);

        node.start();

        assertThat(node.startFuture(), willCompleteSuccessfully());

        assertThat(node.clusterManager().logicalTopology(), will(containsInAnyOrder(currentPhysicalTopology())));
    }

    /**
     * Tests a scenario when a node leaves a cluster.
     */
    @Test
    void testNodeLeave(TestInfo testInfo) throws Exception {
        startCluster(2, testInfo);

        String[] cmgNodes = { cluster.get(0).localMember().name() };

        initCluster(cmgNodes, cmgNodes);

        assertThat(cluster.get(0).clusterManager().logicalTopology(), will(containsInAnyOrder(currentPhysicalTopology())));

        MockNode nodeToStop = cluster.remove(1);

        nodeToStop.beforeNodeStop();
        nodeToStop.stop();

        waitForLogicalTopology();

        assertThat(cluster.get(0).clusterManager().logicalTopology(), will(containsInAnyOrder(currentPhysicalTopology())));
    }

    /**
     * Tests a scenario when a node, that participated in a cluster, tries to join a new one.
     */
    @Test
    void testJoinInvalidTag(TestInfo testInfo) throws Exception {
        // Start a cluster and initialize it
        startCluster(2, testInfo);

        String[] cmgNodes = { cluster.get(0).localMember().name() };

        initCluster(cmgNodes, cmgNodes);

        // Stop the cluster
        stopCluster();

        // Remove all persistent state from the first node
        IgniteUtils.deleteIfExists(workDir.resolve("node0"));

        // Start the nodes again
        for (MockNode node : cluster) {
            node.restart();
        }

        // Initialize the cluster again, but with a different name. It is expected that the second node will try to join the CMG
        // and will be rejected.
        cluster.get(0).clusterManager().initCluster(
                Arrays.asList(cmgNodes),
                Arrays.asList(cmgNodes),
                "cluster2"
        );

        assertThrowsWithCause(
                () -> cluster.get(1).clusterManager().joinFuture().get(10, TimeUnit.SECONDS),
                IgniteInternalException.class,
                "Join request denied, reason: Cluster tags do not match"
        );
    }

    /**
     * Tests a scenario when a node starts joining a cluster having a CMG leader, but finishes the join after the CMG leader changed.
     */
    @Test
    void testJoinLeaderChange(TestInfo testInfo) throws Exception {
        // Start a cluster of 3 nodes so that the CMG leader node could be stopped later.
        startCluster(3, testInfo);

        String[] cmgNodes = clusterNodeNames();

        // Start the CMG on all 3 nodes.
        initCluster(cmgNodes, cmgNodes);

        // Start a new node, but do not send the JoinReadyCommand.
        var addr = new NetworkAddress("localhost", PORT_BASE + cluster.size());

        var nodeFinder = new StaticNodeFinder(Arrays.asList(clusterNodeAddresses()));

        var node = new MockNode(testInfo, addr, nodeFinder, workDir.resolve("node" + cluster.size()));

        node.startComponents();

        assertThat(node.clusterManager().joinFuture(), willCompleteSuccessfully());

        cluster.add(node);

        // Find the CMG leader and stop it
        MockNode leaderNode = cluster.stream()
                .filter(n -> {
                    CompletableFuture<Boolean> isLeader = n.clusterManager().isCmgLeader();

                    assertThat(isLeader, willCompleteSuccessfully());

                    return isLeader.join();
                })
                .findAny()
                .orElseThrow();

        leaderNode.stop();

        // Issue the JoinReadCommand on the joining node. It is expected that the joining node is still treated as validated.
        assertThat(node.clusterManager().onJoinReady(), willCompleteSuccessfully());
    }

    private ClusterNode[] currentPhysicalTopology() {
        return cluster.stream()
                .map(MockNode::localMember)
                .toArray(ClusterNode[]::new);
    }

    private String[] clusterNodeNames() {
        return cluster.stream()
                .map(MockNode::localMember)
                .map(ClusterNode::name)
                .toArray(String[]::new);
    }

    private NetworkAddress[] clusterNodeAddresses() {
        return cluster.stream()
                .map(MockNode::localMember)
                .map(ClusterNode::address)
                .toArray(NetworkAddress[]::new);
    }

    private void waitForLogicalTopology() throws InterruptedException {
        assertTrue(waitForCondition(() -> {
            CompletableFuture<Collection<ClusterNode>> logicalTopology = cluster.get(0).clusterManager().logicalTopology();

            assertThat(logicalTopology, willCompleteSuccessfully());

            return logicalTopology.join().size() == cluster.size();
        }, 10000));
    }

    private void initCluster(String[] metaStorageNodes, String[] cmgNodes) throws NodeStoppingException {
        cluster.get(0).clusterManager().initCluster(
                Arrays.asList(metaStorageNodes),
                Arrays.asList(cmgNodes),
                "cluster"
        );

        for (MockNode node : cluster) {
            assertThat(node.startFuture(), willCompleteSuccessfully());
        }
    }
}
