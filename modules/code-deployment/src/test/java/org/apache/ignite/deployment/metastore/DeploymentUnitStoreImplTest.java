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

package org.apache.ignite.deployment.metastore;

import static org.apache.ignite.internal.deployunit.DeploymentStatus.DEPLOYED;
import static org.apache.ignite.internal.deployunit.DeploymentStatus.OBSOLETE;
import static org.apache.ignite.internal.deployunit.DeploymentStatus.REMOVING;
import static org.apache.ignite.internal.deployunit.DeploymentStatus.UPLOADING;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.nullValue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.deployunit.metastore.ClusterEventCallback;
import org.apache.ignite.internal.deployunit.metastore.ClusterStatusWatchListener;
import org.apache.ignite.internal.deployunit.metastore.DeploymentUnitStoreImpl;
import org.apache.ignite.internal.deployunit.metastore.NodeEventCallback;
import org.apache.ignite.internal.deployunit.metastore.NodeStatusWatchListener;
import org.apache.ignite.internal.deployunit.metastore.status.UnitClusterStatus;
import org.apache.ignite.internal.deployunit.metastore.status.UnitNodeStatus;
import org.apache.ignite.internal.failure.NoOpFailureProcessor;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.persistence.RocksDbKeyValueStorage;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test suite for {@link DeploymentUnitStoreImpl}.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class DeploymentUnitStoreImplTest extends BaseIgniteAbstractTest {
    private static final String LOCAL_NODE = "localNode";

    private final List<UnitNodeStatus> nodeHistory = Collections.synchronizedList(new ArrayList<>());

    private final NodeEventCallback nodeEventCallback = new NodeEventCallback() {
        @Override
        public void onUpdate(UnitNodeStatus status, List<UnitNodeStatus> holders) {
            nodeHistory.add(status);
        }
    };

    private final List<UnitClusterStatus> clusterHistory = Collections.synchronizedList(new ArrayList<>());

    private final ClusterEventCallback clusterEventCallback = new ClusterEventCallback() {
        @Override
        public void onUpdate(UnitClusterStatus status) {
            clusterHistory.add(status);
        }
    };

    private DeploymentUnitStoreImpl metastore;

    private Runnable toStop;

    @WorkDirectory
    private Path workDir;

    @BeforeEach
    public void setup() {
        nodeHistory.clear();
        clusterHistory.clear();
        KeyValueStorage storage = new RocksDbKeyValueStorage("test", workDir, new NoOpFailureProcessor("test"));

        MetaStorageManager metaStorageManager = StandaloneMetaStorageManager.create(storage);
        metastore = new DeploymentUnitStoreImpl(metaStorageManager);
        NodeStatusWatchListener nodeListener = new NodeStatusWatchListener(metastore, LOCAL_NODE, nodeEventCallback);
        metastore.registerNodeStatusListener(nodeListener);
        ClusterStatusWatchListener clusterListener = new ClusterStatusWatchListener(clusterEventCallback);
        metastore.registerClusterStatusListener(clusterListener);

        assertThat(metaStorageManager.startAsync(ForkJoinPool.commonPool()), willCompleteSuccessfully());

        toStop = () -> {
            nodeListener.stop();

            assertThat(metaStorageManager.stopAsync(), willCompleteSuccessfully());
        };

        assertThat("Watches were not deployed", metaStorageManager.deployWatches(), willCompleteSuccessfully());
    }

    @AfterEach
    public void stop() {
        toStop.run();
        toStop = null;
    }

    @Test
    public void clusterStatusTest() throws Exception {
        String id = "id1";
        Version version = Version.parseVersion("1.1.1");

        CompletableFuture<UnitClusterStatus> clusterStatusFuture = metastore.createClusterStatus(id, version, Set.of());
        assertThat(clusterStatusFuture, willCompleteSuccessfully());

        UnitClusterStatus clusterStatus = clusterStatusFuture.get();
        UUID opId = clusterStatus.opId();

        assertThat(metastore.getClusterStatus(id, version),
                willBe(new UnitClusterStatus(id, version, UPLOADING, opId, Set.of())));

        assertThat(metastore.updateClusterStatus(id, version, DEPLOYED), willBe(true));
        assertThat(metastore.getClusterStatus(id, version),
                willBe(new UnitClusterStatus(id, version, DEPLOYED, opId, Set.of())));

        assertThat(metastore.removeClusterStatus(id, version, opId), willBe(true));

        assertThat(metastore.getClusterStatus(id, version), willBe(nullValue()));
    }

    @Test
    void clusterStatusAba() {
        String id = "id1";
        Version version = Version.parseVersion("1.1.1");

        CompletableFuture<UnitClusterStatus> clusterStatusFuture1 = metastore.createClusterStatus(id, version, Set.of());
        assertThat(clusterStatusFuture1, willCompleteSuccessfully());

        UUID opId1 = clusterStatusFuture1.join().opId();

        assertThat(metastore.removeClusterStatus(id, version, opId1), willBe(true));

        // Create new cluster status with the same id and version
        CompletableFuture<UnitClusterStatus> clusterStatusFuture2 = metastore.createClusterStatus(id, version, Set.of());
        assertThat(clusterStatusFuture2, willCompleteSuccessfully());

        UUID opId2 = clusterStatusFuture2.join().opId();

        // Remove with the initial operation ID should fail
        assertThat(metastore.removeClusterStatus(id, version, opId1), willBe(false));

        // Remove with the correct operation ID should succeed
        assertThat(metastore.removeClusterStatus(id, version, opId2), willBe(true));
    }

    @Test
    void nodeStatusAba() {
        String id = "id1";
        Version version = Version.parseVersion("1.1.1");
        String node1 = "node1";

        UUID opId1 = UUID.randomUUID();
        UUID opId2 = UUID.randomUUID();

        assertThat(metastore.createNodeStatus(node1, id, version, opId1, UPLOADING), willBe(true));

        assertThat(metastore.removeNodeStatus(node1, id, version, opId1), willBe(true));

        // Create new node status with the same id and version
        assertThat(metastore.createNodeStatus(node1, id, version, opId2, UPLOADING), willBe(true));

        // Remove with the initial operation ID should fail
        assertThat(metastore.removeNodeStatus(node1, id, version, opId1), willBe(false));

        // Remove with the correct operation ID should succeed
        assertThat(metastore.removeNodeStatus(node1, id, version, opId2), willBe(true));
    }

    @Test
    public void nodeStatusTest() throws Exception {
        String id = "id2";
        Version version = Version.parseVersion("1.1.1");

        String node1 = "node1";
        String node2 = "node2";
        String node3 = "node3";

        CompletableFuture<UnitClusterStatus> clusterStatusFuture = metastore.createClusterStatus(id, version, Set.of(node1, node2, node3));
        assertThat(clusterStatusFuture, willCompleteSuccessfully());

        UnitClusterStatus clusterStatus = clusterStatusFuture.get();
        UUID opId = clusterStatus.opId();

        assertThat(metastore.getClusterStatus(id, version),
                willBe(new UnitClusterStatus(id, version, UPLOADING, opId, Set.of(node1, node2, node3))));

        assertThat(metastore.createNodeStatus(node1, id, version, opId, UPLOADING), willBe(true));
        assertThat(metastore.getNodeStatus(node1, id, version),
                willBe(new UnitNodeStatus(id, version, UPLOADING, opId, node1)));

        assertThat(metastore.updateNodeStatus(node1, id, version, DEPLOYED), willBe(true));
        assertThat(metastore.getNodeStatus(node1, id, version),
                willBe(new UnitNodeStatus(id, version, DEPLOYED, opId, node1)));

        assertThat(metastore.createNodeStatus(node2, id, version, opId, UPLOADING), willBe(true));
        assertThat(metastore.getNodeStatus(node2, id, version),
                willBe(new UnitNodeStatus(id, version, UPLOADING, opId, node2)));

        assertThat(metastore.createNodeStatus(node3, id, version, opId, UPLOADING), willBe(true));

        assertThat(metastore.updateClusterStatus(id, version, DEPLOYED), willBe(true));
        assertThat(metastore.getClusterStatus(id, version),
                willBe(new UnitClusterStatus(id, version, DEPLOYED, opId, Set.of(node1, node2, node3))));

        assertThat(metastore.getClusterStatuses(id),
                willBe(contains((new UnitClusterStatus(id, version, DEPLOYED, opId, Set.of(node1, node2, node3)))))
        );

        assertThat(metastore.removeClusterStatus(id, version, opId), willBe(true));
        assertThat(metastore.getNodeStatus(node1, id, version),
                willBe(new UnitNodeStatus(id, version, DEPLOYED, opId, node1)));

        assertThat(metastore.removeNodeStatus(node1, id, version, opId), willBe(true));
        assertThat(metastore.getNodeStatus(node1, id, version), willBe(nullValue()));
    }

    @Test
    public void testNodeEventListener() {
        String id = "id5";
        Version version = Version.parseVersion("1.1.1");
        String node1 = LOCAL_NODE;

        UUID opId = UUID.randomUUID();
        assertThat(metastore.createNodeStatus(node1, id, version, opId, UPLOADING), willBe(true));
        assertThat(metastore.updateNodeStatus(node1, id, version, DEPLOYED), willBe(true));
        assertThat(metastore.updateNodeStatus(node1, id, version, OBSOLETE), willBe(true));
        assertThat(metastore.updateNodeStatus(node1, id, version, REMOVING), willBe(true));

        await().untilAsserted(() ->
                assertThat(nodeHistory, containsInAnyOrder(
                        new UnitNodeStatus(id, version, UPLOADING, opId, node1),
                        new UnitNodeStatus(id, version, DEPLOYED, opId, node1),
                        new UnitNodeStatus(id, version, OBSOLETE, opId, node1),
                        new UnitNodeStatus(id, version, REMOVING, opId, node1)
                )));
    }

    @Test
    public void testClusterEventListener() throws Exception {
        String id = "id6";
        Version version = Version.parseVersion("1.1.1");

        CompletableFuture<UnitClusterStatus> clusterStatusFuture = metastore.createClusterStatus(id, version, Set.of());
        assertThat(clusterStatusFuture, willCompleteSuccessfully());

        UnitClusterStatus clusterStatus = clusterStatusFuture.get();
        UUID opId = clusterStatus.opId();

        assertThat(metastore.updateClusterStatus(id, version, DEPLOYED), willBe(true));
        assertThat(metastore.updateClusterStatus(id, version, OBSOLETE), willBe(true));
        assertThat(metastore.updateClusterStatus(id, version, REMOVING), willBe(true));

        await().untilAsserted(() ->
                assertThat(clusterHistory, containsInAnyOrder(
                        new UnitClusterStatus(id, version, UPLOADING, opId, Set.of()),
                        new UnitClusterStatus(id, version, DEPLOYED, opId, Set.of()),
                        new UnitClusterStatus(id, version, OBSOLETE, opId, Set.of()),
                        new UnitClusterStatus(id, version, REMOVING, opId, Set.of())
                )));
    }
}
