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

import static org.apache.ignite.internal.cluster.management.ClusterTag.clusterTag;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.cluster.management.ClusterTag;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.properties.IgniteProductVersion;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Tests for {@link RaftStorageManager}.
 */
public abstract class AbstractClusterStateStorageManagerTest extends IgniteAbstractTest {
    private RaftStorageManager storageManager;

    private ClusterStateStorage storage;

    private final CmgMessagesFactory msgFactory = new CmgMessagesFactory();

    abstract ClusterStateStorage clusterStateStorage(String nodeName);

    @BeforeEach
    void setUp(TestInfo testInfo) {
        storage = clusterStateStorage(testNodeName(testInfo, 0));

        assertThat(storage.startAsync(new ComponentContext()), willCompleteSuccessfully());

        storageManager = new RaftStorageManager(storage);
    }

    @AfterEach
    void tearDown() {
        assertThat(storage.stopAsync(new ComponentContext()), willCompleteSuccessfully());
    }

    /**
     * Tests methods for working the Cluster State.
     */
    @Test
    void testClusterState() {
        ClusterTag clusterTag1 = clusterTag(msgFactory, "cluster");
        var state = msgFactory.clusterState()
                .cmgNodes(Set.copyOf(List.of("foo", "bar")))
                .metaStorageNodes(Set.copyOf(List.of("foo", "baz")))
                .version(IgniteProductVersion.CURRENT_VERSION.toString())
                .clusterTag(clusterTag1)
                .build();

        assertThat(storageManager.getClusterState(), is(nullValue()));

        storageManager.putClusterState(state);

        assertThat(storageManager.getClusterState(), is(equalTo(state)));

        IgniteProductVersion igniteVersion = IgniteProductVersion.fromString("3.3.3");
        ClusterTag clusterTag = clusterTag(msgFactory, "new_cluster");
        state = msgFactory.clusterState()
                .cmgNodes(Set.copyOf(List.of("foo")))
                .metaStorageNodes(Set.copyOf(List.of("foo")))
                .version(igniteVersion.toString())
                .clusterTag(clusterTag)
                .build();

        storageManager.putClusterState(state);

        assertThat(storageManager.getClusterState(), is(equalTo(state)));
    }

    /**
     * Tests the snapshot-related methods.
     */
    @Test
    void testSnapshot() throws IOException {
        ClusterTag clusterTag1 = clusterTag(msgFactory, "cluster");
        var state = msgFactory.clusterState()
                .cmgNodes(Set.copyOf(List.of("foo", "bar")))
                .metaStorageNodes(Set.copyOf(List.of("foo", "baz")))
                .version(IgniteProductVersion.CURRENT_VERSION.toString())
                .clusterTag(clusterTag1)
                .build();

        storageManager.putClusterState(state);

        Path snapshotDir = workDir.resolve("snapshot");
        Files.createDirectory(snapshotDir);

        assertThat(storageManager.snapshot(snapshotDir), willCompleteSuccessfully());

        IgniteProductVersion igniteVersion = IgniteProductVersion.fromString("3.3.3");
        ClusterTag clusterTag = clusterTag(msgFactory, "new_cluster");
        var newState = msgFactory.clusterState()
                .cmgNodes(Set.copyOf(List.of("foo")))
                .metaStorageNodes(Set.copyOf(List.of("foo")))
                .version(igniteVersion.toString())
                .clusterTag(clusterTag)
                .build();

        storageManager.putClusterState(newState);

        storageManager.restoreSnapshot(snapshotDir);

        assertThat(storageManager.getClusterState(), is(equalTo(state)));
    }

    /**
     * Tests CRUD operations on validated nodes.
     */
    @Test
    void testValidatedNodes() {
        var node1 = new LogicalNode("node1", "node1", new NetworkAddress("localhost", 10_000));
        var node2 = new LogicalNode("node2", "node2", new NetworkAddress("localhost", 10_001));
        var node3 = new LogicalNode("node3", "node3", new NetworkAddress("localhost", 10_002));

        storageManager.putValidatedNode(node1);

        storageManager.putValidatedNode(node2);

        assertThat(storageManager.isNodeValidated(node1), is(true));
        assertThat(storageManager.isNodeValidated(node2), is(true));
        assertThat(storageManager.isNodeValidated(node3), is(false));

        assertThat(storageManager.getValidatedNodes(), containsInAnyOrder(node1, node2));

        storageManager.removeValidatedNode(node1);

        assertThat(storageManager.isNodeValidated(node1), is(false));
        assertThat(storageManager.isNodeValidated(node2), is(true));

        assertThat(storageManager.getValidatedNodes(), containsInAnyOrder(node2));
    }
}
