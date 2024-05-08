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
import static org.apache.ignite.internal.catalog.CatalogTestUtils.createCatalogManagerWithTestUpdateLog;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.COLUMN_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.INDEX_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.LOCAL_NODE;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.NODE_ID;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.NODE_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.TABLE_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.createTable;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.newPrimaryReplicaMeta;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.closeAllManually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.commands.MakeIndexAvailableCommand;
import org.apache.ignite.internal.catalog.commands.StartBuildingIndexCommand;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
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

/** For {@link ChangeIndexStatusTaskController} testing. */
@ExtendWith(MockitoExtension.class)
public class ChangeIndexStatusTaskControllerTest extends BaseIgniteAbstractTest {
    private final HybridClock clock = new HybridClockImpl();

    private final TestPlacementDriver placementDriver = new TestPlacementDriver();

    private final ClusterService clusterService = createClusterService();

    private CatalogManager catalogManager;

    @Mock
    private ChangeIndexStatusTaskScheduler changeIndexStatusTaskScheduler;

    private @Nullable ChangeIndexStatusTaskController taskController;

    @BeforeEach
    void setUp() {
        catalogManager = createCatalogManagerWithTestUpdateLog(NODE_NAME, clock);

        assertThat(catalogManager.startAsync(), willCompleteSuccessfully());

        createTable(catalogManager, TABLE_NAME, COLUMN_NAME);

        taskController = new ChangeIndexStatusTaskController(
                catalogManager, placementDriver, clusterService, changeIndexStatusTaskScheduler
        );
    }

    @AfterEach
    void tearDown() throws Exception {
        closeAllManually(
                catalogManager::beforeNodeStop,
                () -> assertThat(catalogManager.stopAsync(), willCompleteSuccessfully()),
                taskController
        );
    }

    @Test
    void testStartTaskOnIndexCreateEvent() {
        setPrimaryReplicaLocalNode();

        verify(changeIndexStatusTaskScheduler, never()).scheduleStartBuildingTask(any());

        createIndex();

        verify(changeIndexStatusTaskScheduler).scheduleStartBuildingTask(eq(indexDescriptor()));
    }

    @Test
    void testStartTaskOnIndexDropEvent() {
        setPrimaryReplicaLocalNode();

        createIndex();

        CatalogIndexDescriptor indexDescriptor = indexDescriptor();

        int indexId = indexDescriptor.id();

        assertThat(
                catalogManager.execute(List.of(
                        StartBuildingIndexCommand.builder().indexId(indexId).build(),
                        MakeIndexAvailableCommand.builder().indexId(indexId).build())
                ),
                willCompleteSuccessfully()
        );

        verify(changeIndexStatusTaskScheduler, never()).scheduleRemoveIndexTask(any());

        dropIndex();

        verify(changeIndexStatusTaskScheduler).scheduleRemoveIndexTask(argThat(descriptor -> descriptor.id() == indexId));
    }

    @Test
    void testStopTaskOnIndexDropEvent() {
        setPrimaryReplicaLocalNode();

        createIndex();

        CatalogIndexDescriptor indexDescriptor = indexDescriptor();

        dropIndex();

        verify(changeIndexStatusTaskScheduler).stopStartBuildingTask(eq(indexDescriptor.id()));
    }

    @Test
    void testStartTasksOnPrimaryReplicaElected() {
        createIndex();

        CatalogIndexDescriptor indexDescriptor = indexDescriptor();

        verify(changeIndexStatusTaskScheduler, never()).scheduleStartBuildingTask(eq(indexDescriptor));

        setPrimaryReplicaLocalNode();

        verify(changeIndexStatusTaskScheduler).scheduleStartBuildingTask(eq(indexDescriptor));
    }

    @Test
    void testStopTasksOnExpirePrimaryReplica() {
        setPrimaryReplicaLocalNode();

        createIndex();

        setPrimaryReplicaAnotherNode();

        verify(changeIndexStatusTaskScheduler).stopTasksForTable(eq(tableId()));
    }

    private void createIndex() {
        TestIndexManagementUtils.createIndex(catalogManager, TABLE_NAME, INDEX_NAME, COLUMN_NAME);
    }

    private void dropIndex() {
        TestIndexManagementUtils.dropIndex(catalogManager, INDEX_NAME);
    }

    private void setPrimaryReplicaLocalNode() {
        setPrimaryReplica(LOCAL_NODE);
    }

    private void setPrimaryReplicaAnotherNode() {
        setPrimaryReplica(new ClusterNodeImpl(NODE_ID + "-next", NODE_NAME + "-next", mock(NetworkAddress.class)));
    }

    private void setPrimaryReplica(ClusterNode clusterNode) {
        ZonePartitionId zonePartId = new ZonePartitionId(zoneId(), tableId(), 0);

        ReplicaMeta replicaMeta = newPrimaryReplicaMeta(clusterNode, zonePartId, HybridTimestamp.MIN_VALUE, HybridTimestamp.MAX_VALUE);

        assertThat(placementDriver.setPrimaryReplicaMeta(0, zonePartId, completedFuture(replicaMeta)), willCompleteSuccessfully());
    }

    private int tableId() {
        return TestIndexManagementUtils.tableId(catalogManager, TABLE_NAME, clock);
    }

    private int zoneId() {
        return catalogManager.catalog(catalogManager.latestCatalogVersion()).table(tableId()).zoneId();
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
