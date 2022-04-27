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
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.will;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.StaticNodeFinder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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

    @BeforeEach
    void setUp(TestInfo testInfo) throws IOException {
        var addr1 = new NetworkAddress("localhost", PORT_BASE);
        var addr2 = new NetworkAddress("localhost", PORT_BASE + 1);

        var nodeFinder = new StaticNodeFinder(List.of(addr1, addr2));

        cluster.add(new MockNode(testInfo, addr1, nodeFinder, workDir.resolve("node0")));
        cluster.add(new MockNode(testInfo, addr2, nodeFinder, workDir.resolve("node1")));

        for (MockNode node : cluster) {
            node.start();
        }
    }

    @AfterEach
    void tearDown() {
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
    void testInit() throws Exception {
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
    void testInitDeadNodes() {
        String[] allNodes = { cluster.get(0).localMember().name(), cluster.get(1).localMember().name() };

        MockNode nodeToStop = cluster.remove(0);

        nodeToStop.beforeNodeStop();
        nodeToStop.stop();

        assertThrows(InitException.class, () -> initCluster(allNodes, allNodes));
    }

    /**
     * Tests that re-running init after a failed init attempt can succeed.
     */
    @Test
    void testInitCancel() throws Exception {
        String[] allNodes = { cluster.get(0).localMember().name(), cluster.get(1).localMember().name() };

        // stop a CMG node to make the init fail

        MockNode nodeToStop = cluster.remove(0);

        nodeToStop.beforeNodeStop();
        nodeToStop.stop();

        assertThrows(InitException.class, () -> initCluster(allNodes, allNodes));

        // complete initialization with one node to check that it finishes correctly

        String[] aliveNodes = { cluster.get(0).localMember().name() };

        initCluster(aliveNodes, aliveNodes);

        assertThat(cluster.get(0).clusterManager().metaStorageNodes(), will(containsInAnyOrder(aliveNodes)));

        assertThat(cluster.get(0).clusterManager().logicalTopology(), will(containsInAnyOrder(currentPhysicalTopology())));
    }

    /**
     * Tests a scenario when a node is restarted.
     */
    @Test
    void testNodeRestart() throws Exception {
        String[] cmgNodes = { cluster.get(0).localMember().name() };

        String[] metaStorageNodes = { cluster.get(1).localMember().name() };

        initCluster(metaStorageNodes, cmgNodes);

        assertThat(cluster.get(0).clusterManager().metaStorageNodes(), will(containsInAnyOrder(metaStorageNodes)));
        assertThat(cluster.get(1).clusterManager().metaStorageNodes(), will(containsInAnyOrder(metaStorageNodes)));

        cluster.get(0).restart();

        assertThat(cluster.get(0).clusterManager().metaStorageNodes(), will(containsInAnyOrder(metaStorageNodes)));

        ClusterNode[] expectedTopology = currentPhysicalTopology();

        waitForLogicalTopology();

        assertThat(cluster.get(0).clusterManager().logicalTopology(), will(containsInAnyOrder(expectedTopology)));
        assertThat(cluster.get(1).clusterManager().logicalTopology(), will(containsInAnyOrder(expectedTopology)));
    }

    /**
     * Tests a scenario when a new node joins a cluster.
     */
    @Test
    void testNodeJoin(TestInfo testInfo) throws Exception {
        String[] cmgNodes = { cluster.get(0).localMember().name() };

        initCluster(cmgNodes, cmgNodes);

        // create and start a new node
        var addr = new NetworkAddress("localhost", PORT_BASE + cluster.size());

        var nodeFinder = new StaticNodeFinder(cluster.stream().map(node -> node.localMember().address()).collect(toList()));

        var node = new MockNode(testInfo, addr, nodeFinder, workDir.resolve("node" + cluster.size()));

        cluster.add(node);

        node.start();

        waitForLogicalTopology();

        assertThat(node.clusterManager().logicalTopology(), will(containsInAnyOrder(currentPhysicalTopology())));
    }

    /**
     * Tests a scenario when a node leaves a cluster.
     */
    @Test
    void testNodeLeave() throws Exception {
        String[] cmgNodes = { cluster.get(0).localMember().name() };

        initCluster(cmgNodes, cmgNodes);

        assertThat(cluster.get(0).clusterManager().logicalTopology(), will(containsInAnyOrder(currentPhysicalTopology())));

        MockNode nodeToStop = cluster.remove(1);

        nodeToStop.beforeNodeStop();
        nodeToStop.stop();

        waitForLogicalTopology();

        assertThat(cluster.get(0).clusterManager().logicalTopology(), will(containsInAnyOrder(currentPhysicalTopology())));
    }

    private ClusterNode[] currentPhysicalTopology() {
        return cluster.stream().map(MockNode::localMember).toArray(ClusterNode[]::new);
    }

    private void waitForLogicalTopology() throws InterruptedException {
        waitForCondition(() -> {
            CompletableFuture<Collection<ClusterNode>> logicalTopology = cluster.get(0).clusterManager().logicalTopology();

            assertThat(logicalTopology, willCompleteSuccessfully());

            return logicalTopology.join().size() == cluster.size();
        }, 1000);
    }

    private void initCluster(String[] metaStorageNodes, String[] cmgNodes) throws NodeStoppingException, InterruptedException {
        cluster.get(0).clusterManager().initCluster(Arrays.asList(metaStorageNodes), Arrays.asList(cmgNodes));

        waitForLogicalTopology();
    }
}
