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

package org.apache.ignite.internal.cluster.management.topology;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.cluster.management.raft.ClusterStateStorage;
import org.apache.ignite.internal.cluster.management.raft.TestClusterStateStorage;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(WorkDirectoryExtension.class)
class LogicalTopologyImplTest {
    private final ClusterStateStorage storage = new TestClusterStateStorage();

    private LogicalTopology topologyService;

    @WorkDirectory
    protected Path workDir;

    @BeforeEach
    void setUp() {
        storage.start();

        topologyService = new LogicalTopologyImpl(storage);
    }

    @AfterEach
    void tearDown() {
        storage.close();
    }

    /**
     * Tests methods for working with the logical topology.
     */
    @Test
    void testLogicalTopology() {
        assertThat(topologyService.getLogicalTopology(), is(empty()));

        var node1 = new ClusterNode("foo", "bar", new NetworkAddress("localhost", 123));

        topologyService.putLogicalTopologyNode(node1);

        assertThat(topologyService.getLogicalTopology(), contains(node1));

        var node2 = new ClusterNode("baz", "quux", new NetworkAddress("localhost", 123));

        topologyService.putLogicalTopologyNode(node2);

        assertThat(topologyService.getLogicalTopology(), containsInAnyOrder(node1, node2));

        var node3 = new ClusterNode("lol", "boop", new NetworkAddress("localhost", 123));

        topologyService.putLogicalTopologyNode(node3);

        assertThat(topologyService.getLogicalTopology(), containsInAnyOrder(node1, node2, node3));

        topologyService.removeLogicalTopologyNodes(Set.of(node1, node2));

        assertThat(topologyService.getLogicalTopology(), contains(node3));

        topologyService.removeLogicalTopologyNodes(Set.of(node3));

        assertThat(topologyService.getLogicalTopology(), is(empty()));
    }

    /**
     * Tests that all methods for working with the logical topology are idempotent.
     */
    @Test
    void testLogicalTopologyIdempotence() {
        var node = new ClusterNode("foo", "bar", new NetworkAddress("localhost", 123));

        topologyService.putLogicalTopologyNode(node);
        topologyService.putLogicalTopologyNode(node);

        assertThat(topologyService.getLogicalTopology(), contains(node));

        topologyService.removeLogicalTopologyNodes(Set.of(node));
        topologyService.removeLogicalTopologyNodes(Set.of(node));

        assertThat(topologyService.getLogicalTopology(), is(empty()));
    }

    @Test
    void logicalTopologyAdditionUsesNameAsNodeKey() {
        topologyService.putLogicalTopologyNode(new ClusterNode("id1", "node", new NetworkAddress("host", 1000)));

        topologyService.putLogicalTopologyNode(new ClusterNode("id2", "node", new NetworkAddress("host", 1000)));

        Collection<ClusterNode> topology = topologyService.getLogicalTopology();

        assertThat(topology, hasSize(1));

        assertThat(topology.iterator().next().id(), is("id2"));
    }

    @Test
    void logicalTopologyRemovalUsesIdAsNodeKey() {
        topologyService.putLogicalTopologyNode(new ClusterNode("id1", "node", new NetworkAddress("host", 1000)));

        topologyService.removeLogicalTopologyNodes(Set.of(new ClusterNode("id2", "node", new NetworkAddress("host", 1000))));

        assertThat(topologyService.getLogicalTopology(), hasSize(1));
        assertThat(topologyService.getLogicalTopology().iterator().next().id(), is("id1"));

        topologyService.removeLogicalTopologyNodes(Set.of(new ClusterNode("id1", "another-name", new NetworkAddress("host", 1000))));

        assertThat(topologyService.getLogicalTopology(), is(empty()));
    }

    @Test
    void inLogicalTopologyTestUsesIdAsNodeKey() {
        topologyService.putLogicalTopologyNode(new ClusterNode("id1", "node", new NetworkAddress("host", 1000)));

        assertTrue(topologyService.isNodeInLogicalTopology(new ClusterNode("id1", "node", new NetworkAddress("host", 1000))));
        assertFalse(topologyService.isNodeInLogicalTopology(new ClusterNode("another-id", "node", new NetworkAddress("host", 1000))));
    }

    @Test
    void logicalTopologyIsRestoredCorrectlyWithSnapshot() throws Exception {
        Path snapshotDir = workDir.resolve("snapshot");
        Files.createDirectory(snapshotDir);

        topologyService.putLogicalTopologyNode(new ClusterNode("id1", "node", new NetworkAddress("host", 1000)));

        storage.snapshot(snapshotDir).get(10, TimeUnit.SECONDS);

        topologyService.putLogicalTopologyNode(new ClusterNode("id2", "another-node", new NetworkAddress("host", 1001)));

        storage.restoreSnapshot(snapshotDir);

        List<String> namesInTopology = topologyService.getLogicalTopology().stream()
                .map(ClusterNode::name)
                .collect(toList());
        assertThat(namesInTopology, contains("node"));
    }
}
