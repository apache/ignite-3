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

package org.apache.ignite.internal.index;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.COLUMN_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.INDEX_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.LOCAL_NODE;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.NODE_ID;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.NODE_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.TABLE_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.createTable;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.dropIndex;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.newPrimaryReplicaMeta;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogTestUtils;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.TopologyService;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/** For {@link IndexBuildingStarterController} testing. */
@ExtendWith(MockitoExtension.class)
public class IndexBuildingStarterControllerTest extends IgniteAbstractTest {
    private final HybridClock clock = new HybridClockImpl();

    private final CatalogManager catalogManager = CatalogTestUtils.createTestCatalogManager(NODE_NAME, clock);

    private final TestPlacementDriver placementDriver = new TestPlacementDriver();

    private final ClusterService clusterService = createClusterService();

    @Mock
    private IndexBuildingStarter indexBuildingStarter;

    private @Nullable IndexBuildingStarterController controller;

    @BeforeEach
    void setUp() {
        assertThat(catalogManager.start(), willCompleteSuccessfully());

        createTable(catalogManager, TABLE_NAME, COLUMN_NAME);

        controller = new IndexBuildingStarterController(catalogManager, placementDriver, clusterService, indexBuildingStarter);
    }

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(
                catalogManager::beforeNodeStop,
                catalogManager::stop,
                controller == null ? null : controller::close
        );
    }

    @Test
    void testStartTaskOnIndexCreateEvent() {
        setPrimaryReplicaLocalNode();

        verify(indexBuildingStarter, never()).scheduleTask(any());

        createIndex();

        verify(indexBuildingStarter).scheduleTask(eq(indexDescriptor()));
    }

    @Test
    void testStopTaskOnIndexDropEvent() {
        setPrimaryReplicaLocalNode();

        createIndex();

        CatalogIndexDescriptor indexDescriptor = indexDescriptor();

        dropIndex(catalogManager, INDEX_NAME);

        verify(indexBuildingStarter).stopTask(eq(indexDescriptor));
    }

    @Test
    void testStartTasksOnPrimaryReplicaElected() {
        createIndex();

        CatalogIndexDescriptor indexDescriptor = indexDescriptor();

        verify(indexBuildingStarter, never()).scheduleTask(eq(indexDescriptor));

        setPrimaryReplicaLocalNode();

        verify(indexBuildingStarter).scheduleTask(eq(indexDescriptor));
    }

    @Test
    void testStopTasksOnExpirePrimaryReplica() {
        setPrimaryReplicaLocalNode();

        createIndex();

        setPrimaryReplicaAnotherNode();

        verify(indexBuildingStarter).stopTasks(eq(tableId()));
    }

    private void createIndex() {
        TestIndexManagementUtils.createIndex(catalogManager, TABLE_NAME, INDEX_NAME, COLUMN_NAME);
    }

    private void setPrimaryReplicaLocalNode() {
        setPrimaryReplica(LOCAL_NODE);
    }

    private void setPrimaryReplicaAnotherNode() {
        setPrimaryReplica(new ClusterNodeImpl(NODE_ID + "-next", NODE_NAME + "-next", mock(NetworkAddress.class)));
    }

    private void setPrimaryReplica(ClusterNode clusterNode) {
        TablePartitionId groupId = new TablePartitionId(tableId(), 0);

        ReplicaMeta replicaMeta = newPrimaryReplicaMeta(clusterNode, groupId, HybridTimestamp.MIN_VALUE, HybridTimestamp.MAX_VALUE);

        assertThat(placementDriver.setPrimaryReplicaMeta(0, groupId, completedFuture(replicaMeta)), willCompleteSuccessfully());
    }

    private int tableId() {
        return TestIndexManagementUtils.tableId(catalogManager, TABLE_NAME, clock);
    }

    private CatalogIndexDescriptor indexDescriptor() {
        return TestIndexManagementUtils.indexDescriptor(catalogManager, INDEX_NAME, clock);
    }

    private static ClusterService createClusterService() {
        ClusterService clusterService = mock(ClusterService.class);
        TopologyService topologyService = mock(TopologyService.class);

        when(topologyService.localMember()).thenReturn(LOCAL_NODE);

        when(clusterService.topologyService()).thenReturn(topologyService);

        return clusterService;
    }
}
