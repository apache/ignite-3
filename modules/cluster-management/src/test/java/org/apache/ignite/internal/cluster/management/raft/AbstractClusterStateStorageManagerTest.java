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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import java.nio.file.Path;
import java.util.List;
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

        storageManager.restoreSnapshot(workDir);

        assertThat(storageManager.getClusterState(), is(equalTo(state)));
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
}
