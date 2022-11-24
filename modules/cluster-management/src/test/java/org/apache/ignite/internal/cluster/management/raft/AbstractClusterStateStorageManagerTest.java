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
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.internal.properties.IgniteProductVersion;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for {@link RaftStorageManager}.
 */
@ExtendWith(WorkDirectoryExtension.class)
public abstract class AbstractClusterStateStorageManagerTest {
    private RaftStorageManager storageManager;

    private ClusterStateStorage storage;

    private final CmgMessagesFactory msgFactory = new CmgMessagesFactory();

    @WorkDirectory
    Path workDir;

    abstract ClusterStateStorage clusterStateStorage();

    @BeforeEach
    void setUp() {
        storage = clusterStateStorage();

        storage.start();

        storageManager = new RaftStorageManager(storage);
    }

    @AfterEach
    void tearDown() {
        storage.close();
    }

    /**
     * Tests methods for working the Cluster State.
     */
    @Test
    void testClusterState() {
        var state = clusterState(
                msgFactory,
                List.of("foo", "bar"),
                List.of("foo", "baz"),
                IgniteProductVersion.CURRENT_VERSION,
                clusterTag(msgFactory, "cluster")
        );

        assertThat(storageManager.getClusterState(), is(nullValue()));

        storageManager.putClusterState(state);

        assertThat(storageManager.getClusterState(), is(equalTo(state)));

        state = clusterState(
                msgFactory,
                List.of("foo"),
                List.of("foo"),
                IgniteProductVersion.fromString("3.3.3"),
                clusterTag(msgFactory, "new_cluster")
        );

        storageManager.putClusterState(state);

        assertThat(storageManager.getClusterState(), is(equalTo(state)));
    }

    /**
     * Tests methods for working with the logical topology.
     */
    @Test
    void testLogicalTopology() {
        assertThat(storageManager.getLogicalTopology(), is(empty()));

        var node1 = new ClusterNode("foo", "bar", new NetworkAddress("localhost", 123));

        storageManager.putLogicalTopologyNode(node1);

        assertThat(storageManager.getLogicalTopology(), contains(node1));

        var node2 = new ClusterNode("baz", "quux", new NetworkAddress("localhost", 123));

        storageManager.putLogicalTopologyNode(node2);

        assertThat(storageManager.getLogicalTopology(), containsInAnyOrder(node1, node2));

        var node3 = new ClusterNode("lol", "boop", new NetworkAddress("localhost", 123));

        storageManager.putLogicalTopologyNode(node3);

        assertThat(storageManager.getLogicalTopology(), containsInAnyOrder(node1, node2, node3));

        storageManager.removeLogicalTopologyNodes(Set.of(node1, node2));

        assertThat(storageManager.getLogicalTopology(), contains(node3));

        storageManager.removeLogicalTopologyNodes(Set.of(node3));

        assertThat(storageManager.getLogicalTopology(), is(empty()));
    }

    /**
     * Tests that all methods for working with the logical topology are idempotent.
     */
    @Test
    void testLogicalTopologyIdempotence() {
        var node = new ClusterNode("foo", "bar", new NetworkAddress("localhost", 123));

        storageManager.putLogicalTopologyNode(node);
        storageManager.putLogicalTopologyNode(node);

        assertThat(storageManager.getLogicalTopology(), contains(node));

        storageManager.removeLogicalTopologyNodes(Set.of(node));
        storageManager.removeLogicalTopologyNodes(Set.of(node));

        assertThat(storageManager.getLogicalTopology(), is(empty()));
    }

    /**
     * Tests the snapshot-related methods.
     */
    @Test
    void testSnapshot() {
        var state = clusterState(
                msgFactory,
                List.of("foo", "bar"),
                List.of("foo", "baz"),
                IgniteProductVersion.CURRENT_VERSION,
                clusterTag(msgFactory, "cluster")
        );

        storageManager.putClusterState(state);

        var node1 = new ClusterNode("foo", "bar", new NetworkAddress("localhost", 123));
        var node2 = new ClusterNode("bar", "baz", new NetworkAddress("localhost", 123));

        storageManager.putLogicalTopologyNode(node1);
        storageManager.putLogicalTopologyNode(node2);

        assertThat(storageManager.snapshot(workDir), willCompleteSuccessfully());

        var newState = clusterState(
                msgFactory,
                List.of("foo"),
                List.of("foo"),
                IgniteProductVersion.fromString("3.3.3"),
                clusterTag(msgFactory, "new_cluster")
        );

        storageManager.putClusterState(newState);

        var node3 = new ClusterNode("nonono", "nononono", new NetworkAddress("localhost", 123));

        storageManager.putLogicalTopologyNode(node3);

        storageManager.restoreSnapshot(workDir);

        assertThat(storageManager.getClusterState(), is(equalTo(state)));
        assertThat(storageManager.getLogicalTopology(), containsInAnyOrder(node1, node2));
    }

    /**
     * Tests CRUD operations on validated nodes.
     */
    @Test
    void testValidatedNodes() {
        storageManager.putValidatedNode("node1");

        storageManager.putValidatedNode("node2");

        assertThat(storageManager.isNodeValidated("node1"), is(true));
        assertThat(storageManager.isNodeValidated("node2"), is(true));
        assertThat(storageManager.isNodeValidated("node3"), is(false));

        assertThat(storageManager.getValidatedNodeIds(), containsInAnyOrder("node1", "node2"));

        storageManager.removeValidatedNode("node1");

        assertThat(storageManager.isNodeValidated("node1"), is(false));
        assertThat(storageManager.isNodeValidated("node2"), is(true));

        assertThat(storageManager.getValidatedNodeIds(), containsInAnyOrder("node2"));
    }

    @Test
    void logicalTopologyAdditionUsesNameAsNodeKey() {
        storageManager.putLogicalTopologyNode(new ClusterNode("id1", "node", new NetworkAddress("host", 1000)));

        storageManager.putLogicalTopologyNode(new ClusterNode("id2", "node", new NetworkAddress("host", 1000)));

        Collection<ClusterNode> topology = storageManager.getLogicalTopology();

        assertThat(topology, hasSize(1));

        assertThat(topology.iterator().next().id(), is("id2"));
    }

    @Test
    void logicalTopologyRemovalUsesIdAsNodeKey() {
        storageManager.putLogicalTopologyNode(new ClusterNode("id1", "node", new NetworkAddress("host", 1000)));

        storageManager.removeLogicalTopologyNodes(Set.of(new ClusterNode("id2", "node", new NetworkAddress("host", 1000))));

        assertThat(storageManager.getLogicalTopology(), hasSize(1));
        assertThat(storageManager.getLogicalTopology().iterator().next().id(), is("id1"));

        storageManager.removeLogicalTopologyNodes(Set.of(new ClusterNode("id1", "another-name", new NetworkAddress("host", 1000))));

        assertThat(storageManager.getLogicalTopology(), is(empty()));
    }

    @Test
    void inLogicalTopologyTestUsesIdAsNodeKey() {
        storageManager.putLogicalTopologyNode(new ClusterNode("id1", "node", new NetworkAddress("host", 1000)));

        assertTrue(storageManager.isNodeInLogicalTopology(new ClusterNode("id1", "node", new NetworkAddress("host", 1000))));
        assertFalse(storageManager.isNodeInLogicalTopology(new ClusterNode("another-id", "node", new NetworkAddress("host", 1000))));
    }
}
