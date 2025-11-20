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

import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.createCatalogManagerWithTestUpdateLog;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.COLUMN_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.INDEX_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.LOCAL_NODE;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.NODE_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.createTable;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.newPrimaryReplicaMeta;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.COLOCATION_FEATURE_FLAG;
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
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.commands.MakeIndexAvailableCommand;
import org.apache.ignite.internal.catalog.commands.StartBuildingIndexCommand;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.lowwatermark.TestLowWatermark;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.table.TableTestUtils;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.network.NetworkAddress;
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
    private static final String PRECREATED_TABLE_NAME = "precreated-table";
    private static final String NOT_PRECREATED_TABLE_NAME = "not-precreated-table";

    private final HybridClock clock = new HybridClockImpl();

    private final TestPlacementDriver placementDriver = new TestPlacementDriver();

    private final ClusterService clusterService = createClusterService();

    private final TestLowWatermark lowWatermark = new TestLowWatermark();

    private CatalogManager catalogManager;

    @Mock
    private ChangeIndexStatusTaskScheduler changeIndexStatusTaskScheduler;

    private @Nullable ChangeIndexStatusTaskController taskController;

    @BeforeEach
    void setUp() {
        catalogManager = createCatalogManagerWithTestUpdateLog(NODE_NAME, clock);

        assertThat(catalogManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

        createTable(catalogManager, PRECREATED_TABLE_NAME, COLUMN_NAME);

        taskController = new ChangeIndexStatusTaskController(
                catalogManager,
                placementDriver,
                clusterService,
                lowWatermark,
                changeIndexStatusTaskScheduler
        );

        taskController.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        closeAllManually(
                catalogManager::beforeNodeStop,
                () -> assertThat(catalogManager.stopAsync(new ComponentContext()), willCompleteSuccessfully()),
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
    void testDoNotStartTaskOnIndexCreationWithTable() {
        setPrimaryReplicaLocalNode();

        createTable(catalogManager, NOT_PRECREATED_TABLE_NAME, COLUMN_NAME);

        verify(changeIndexStatusTaskScheduler, never()).scheduleStartBuildingTask(any());
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

    @Test
    @WithSystemProperty(key = COLOCATION_FEATURE_FLAG, value = "true")
    void testScheduleStartBuildingTasksOnTableAddedToZoneWhereWeArePrimary() {
        String tableName = NOT_PRECREATED_TABLE_NAME;
        String indexName = "index2";

        setPrimaryReplicaLocalNode();

        createTable(catalogManager, tableName, COLUMN_NAME);
        createIndex(tableName, indexName);

        CatalogIndexDescriptor indexDescriptor = indexDescriptor(indexName);

        verify(changeIndexStatusTaskScheduler).scheduleStartBuildingTask(eq(indexDescriptor));
    }

    @Test
    @WithSystemProperty(key = COLOCATION_FEATURE_FLAG, value = "true")
    void testStopAllTableTasksOnTableDropFromZoneWhereWeArePrimary() {
        int tableId = tableId();

        setPrimaryReplicaLocalNode();

        dropTable();
        raiseLowWatermarkToCurrentCatalogVersion();

        verify(changeIndexStatusTaskScheduler).stopTasksForTable(eq(tableId));
    }

    private void createIndex() {
        createIndex(PRECREATED_TABLE_NAME, INDEX_NAME);
    }

    private void createIndex(String tableName, String indexName) {
        TestIndexManagementUtils.createIndex(catalogManager, tableName, indexName, COLUMN_NAME);
    }

    private void dropIndex() {
        TestIndexManagementUtils.dropIndex(catalogManager, INDEX_NAME);
    }

    private void dropTable() {
        TableTestUtils.dropTable(catalogManager, SqlCommon.DEFAULT_SCHEMA_NAME, PRECREATED_TABLE_NAME);
    }

    private void setPrimaryReplicaLocalNode() {
        setPrimaryReplica(LOCAL_NODE);
    }

    private void setPrimaryReplicaAnotherNode() {
        setPrimaryReplica(new ClusterNodeImpl(randomUUID(), NODE_NAME + "-next", mock(NetworkAddress.class)));
    }

    private void setPrimaryReplica(InternalClusterNode clusterNode) {
        ReplicationGroupId groupId = IgniteSystemProperties.colocationEnabled()
                ? new ZonePartitionId(zoneId(), 0)
                : new TablePartitionId(tableId(), 0);

        ReplicaMeta replicaMeta = newPrimaryReplicaMeta(clusterNode, groupId, HybridTimestamp.MIN_VALUE, HybridTimestamp.MAX_VALUE);

        assertThat(placementDriver.setPrimaryReplicaMeta(0, groupId, completedFuture(replicaMeta)), willCompleteSuccessfully());
    }

    private void raiseLowWatermarkToCurrentCatalogVersion() {
        Catalog currentCatalog = catalogManager.activeCatalog(HybridTimestamp.MAX_VALUE.longValue());

        lowWatermark.updateAndNotify(hybridTimestamp(currentCatalog.time()));
    }

    private int tableId() {
        return TestIndexManagementUtils.tableId(catalogManager, PRECREATED_TABLE_NAME, clock);
    }

    private int zoneId() {
        return TableTestUtils.getZoneIdByTableNameStrict(catalogManager, PRECREATED_TABLE_NAME, clock.nowLong());
    }

    private CatalogIndexDescriptor indexDescriptor() {
        return indexDescriptor(INDEX_NAME);
    }

    private CatalogIndexDescriptor indexDescriptor(String indexName) {
        return TestIndexManagementUtils.indexDescriptor(catalogManager, indexName, clock);
    }

    private static ClusterService createClusterService() {
        ClusterService clusterService = mock(ClusterService.class);
        TopologyService topologyService = mock(TopologyService.class);

        when(topologyService.localMember()).thenReturn(LOCAL_NODE);

        when(clusterService.topologyService()).thenReturn(topologyService);

        return clusterService;
    }
}
