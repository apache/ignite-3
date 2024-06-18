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
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.pkIndexName;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.COLUMN_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.INDEX_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.LOCAL_NODE;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.NODE_ID;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.NODE_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.PK_INDEX_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.TABLE_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.createTable;
import static org.apache.ignite.internal.table.TableTestUtils.createHashIndex;
import static org.apache.ignite.internal.table.TableTestUtils.getIndexIdStrict;
import static org.apache.ignite.internal.table.TableTestUtils.getIndexStrict;
import static org.apache.ignite.internal.table.TableTestUtils.getTableIdStrict;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.commands.MakeIndexAvailableCommand;
import org.apache.ignite.internal.catalog.commands.StartBuildingIndexCommand;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.placementdriver.leases.Lease;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.table.TableTestUtils;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** For {@link IndexBuildController} testing. */
public class IndexBuildControllerTest extends BaseIgniteAbstractTest {
    private static final int PARTITION_ID = 10;

    private IndexBuilder indexBuilder;

    private CatalogManager catalogManager;

    private IndexBuildController indexBuildController;

    private final TestPlacementDriver placementDriver = new TestPlacementDriver();

    private final HybridClock clock = new HybridClockImpl();

    private final ClockService clockService = new TestClockService(clock);

    private IndexManager indexManager = null;

    @BeforeEach
    void setUp() {
        indexBuilder = mock(IndexBuilder.class);

        indexManager = mock(IndexManager.class, invocation -> {
            MvTableStorage mvTableStorage = mock(MvTableStorage.class);
            MvPartitionStorage mvPartitionStorage = mock(MvPartitionStorage.class);
            IndexStorage indexStorage = mock(IndexStorage.class);

            when(mvTableStorage.getMvPartition(anyInt())).thenReturn(mvPartitionStorage);
            when(mvTableStorage.getIndex(anyInt(), anyInt())).thenReturn(indexStorage);

            return completedFuture(mvTableStorage);
        });

        ClusterService clusterService = mock(ClusterService.class, invocation -> mock(TopologyService.class, invocation1 -> LOCAL_NODE));

        catalogManager = createCatalogManagerWithTestUpdateLog(NODE_NAME, clock);
        assertThat(catalogManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

        indexBuildController = new IndexBuildController(
                indexBuilder,
                indexManager,
                catalogManager,
                clusterService,
                placementDriver,
                clockService
        );

        createTable(catalogManager, TABLE_NAME, COLUMN_NAME);
    }

    @AfterEach
    void tearDown() throws Exception {
        closeAll(
                catalogManager == null ? null :
                        () -> assertThat(catalogManager.stopAsync(new ComponentContext()), willCompleteSuccessfully()),
                indexBuildController == null ? null : indexBuildController::close
        );
    }

    @Test
    void testStartBuildIndexesOnIndexCreate() {
        setPrimaryReplicaWhichExpiresInOneSecond(PARTITION_ID, NODE_NAME, NODE_ID, clock.now());

        clearInvocations(indexBuilder);

        createIndex(INDEX_NAME);

        verify(indexBuilder, never()).scheduleBuildIndex(
                eq(tableId()),
                eq(PARTITION_ID),
                eq(indexId(INDEX_NAME)),
                any(),
                any(),
                eq(LOCAL_NODE),
                anyLong(),
                eq(indexCreationCatalogVersion(INDEX_NAME))
        );

        verify(indexBuilder, never()).scheduleBuildIndexAfterDisasterRecovery(
                eq(tableId()),
                eq(PARTITION_ID),
                eq(indexId(INDEX_NAME)),
                any(),
                any(),
                eq(LOCAL_NODE),
                anyLong(),
                eq(indexCreationCatalogVersion(INDEX_NAME))
        );
    }

    @Test
    void testStartBuildIndexesOnIndexBuilding() {
        setPrimaryReplicaWhichExpiresInOneSecond(PARTITION_ID, NODE_NAME, NODE_ID, clock.now());

        clearInvocations(indexBuilder);

        createIndex(INDEX_NAME);

        startBuildingIndex(indexId(INDEX_NAME));

        verify(indexBuilder).scheduleBuildIndex(
                eq(tableId()),
                eq(PARTITION_ID),
                eq(indexId(INDEX_NAME)),
                any(),
                any(),
                eq(LOCAL_NODE),
                anyLong(),
                eq(indexCreationCatalogVersion(INDEX_NAME))
        );

        verify(indexBuilder, never()).scheduleBuildIndexAfterDisasterRecovery(
                eq(tableId()),
                eq(PARTITION_ID),
                eq(indexId(INDEX_NAME)),
                any(),
                any(),
                eq(LOCAL_NODE),
                anyLong(),
                eq(indexCreationCatalogVersion(INDEX_NAME))
        );
    }

    @Test
    void testExceptionIsThrownOnIndexBuildingWhenStorageIsNull() {
        setPrimaryReplicaWhichExpiresInOneSecond(PARTITION_ID, NODE_NAME, NODE_ID, clock.now());

        createIndex(INDEX_NAME);

        when(indexManager.getMvTableStorage(anyLong(), anyInt())).thenReturn(nullCompletedFuture());

        assertThrows(
                ExecutionException.class,
                () -> catalogManager.execute(StartBuildingIndexCommand.builder().indexId(indexId(INDEX_NAME)).build())
                        .get(10_000, TimeUnit.MILLISECONDS),
                "Table storage for the specified table cannot be null"
        );
    }

    @Test
    void testStartBuildIndexesOnPrimaryReplicaElected() {
        createIndex(INDEX_NAME);

        startBuildingIndex(indexId(INDEX_NAME));

        setPrimaryReplicaWhichExpiresInOneSecond(PARTITION_ID, NODE_NAME, NODE_ID, clock.now());

        verify(indexBuilder).scheduleBuildIndex(
                eq(tableId()),
                eq(PARTITION_ID),
                eq(indexId(INDEX_NAME)),
                any(),
                any(),
                eq(LOCAL_NODE),
                anyLong(),
                eq(indexCreationCatalogVersion(INDEX_NAME))
        );

        verify(indexBuilder).scheduleBuildIndexAfterDisasterRecovery(
                eq(tableId()),
                eq(PARTITION_ID),
                eq(indexId(PK_INDEX_NAME)),
                any(),
                any(),
                eq(LOCAL_NODE),
                anyLong(),
                eq(indexCreationCatalogVersion(PK_INDEX_NAME))
        );
    }

    @Test
    void testExceptionIsThrownOnPrimaryReplicaElectedWhenStorageIsNull() {
        when(indexManager.getMvTableStorage(anyLong(), anyInt())).thenReturn(nullCompletedFuture());

        CompletableFuture<ReplicaMeta> replicaMetaFuture = completedFuture(replicaMetaForOneSecond(NODE_NAME, NODE_ID, clock.now()));

        assertThrows(
                ExecutionException.class,
                () -> placementDriver.setPrimaryReplicaMeta(0, replicaId(PARTITION_ID), replicaMetaFuture)
                        .get(10_000, TimeUnit.MILLISECONDS),
                "Table storage for the specified table cannot be null"
        );
    }

    @Test
    void testNotStartBuildPkIndexesForNewTable() {
        setPrimaryReplicaWhichExpiresInOneSecond(PARTITION_ID, NODE_NAME, NODE_ID, clock.now());

        String tableName = TABLE_NAME + "_new";

        createTable(catalogManager, tableName, COLUMN_NAME);

        verify(indexBuilder, never()).scheduleBuildIndex(
                eq(tableId(tableName)),
                eq(PARTITION_ID),
                eq(indexId(pkIndexName(tableName))),
                any(),
                any(),
                eq(LOCAL_NODE),
                anyLong(),
                anyInt()
        );

        verify(indexBuilder, never()).scheduleBuildIndexAfterDisasterRecovery(
                eq(tableId(tableName)),
                eq(PARTITION_ID),
                eq(indexId(pkIndexName(tableName))),
                any(),
                any(),
                eq(LOCAL_NODE),
                anyLong(),
                anyInt()
        );
    }

    @Test
    void testStopBuildIndexesOnIndexDrop() {
        createIndex(INDEX_NAME);

        int indexId = indexId(INDEX_NAME);

        dropIndex(INDEX_NAME);

        verify(indexBuilder).stopBuildingIndexes(indexId);
    }

    @Test
    void testStopBuildIndexesOnChangePrimaryReplica() {
        setPrimaryReplicaWhichExpiresInOneSecond(PARTITION_ID, NODE_NAME, NODE_ID, clock.now());
        setPrimaryReplicaWhichExpiresInOneSecond(PARTITION_ID, NODE_NAME + "_other", NODE_ID + "_other", clock.now());

        verify(indexBuilder).stopBuildingIndexes(tableId(), PARTITION_ID);
    }

    @Test
    void testStartBuildIndexesOnPrimaryReplicaElectedOnlyForBuildingIndexes() {
        createIndex(INDEX_NAME + 0);
        createIndex(INDEX_NAME + 1);

        int indexId0 = indexId(INDEX_NAME + 0);

        startBuildingIndex(indexId0);
        makeIndexAvailable(indexId0);

        setPrimaryReplicaWhichExpiresInOneSecond(PARTITION_ID, NODE_NAME, NODE_ID, clock.now());

        verify(indexBuilder, never()).scheduleBuildIndex(
                eq(tableId()),
                eq(PARTITION_ID),
                anyInt(),
                any(),
                any(),
                eq(LOCAL_NODE),
                anyLong(),
                anyInt()
        );

        verify(indexBuilder).scheduleBuildIndexAfterDisasterRecovery(
                eq(tableId()),
                eq(PARTITION_ID),
                eq(indexId0),
                any(),
                any(),
                eq(LOCAL_NODE),
                anyLong(),
                anyInt()
        );
    }

    private void createIndex(String indexName) {
        createHashIndex(catalogManager, SqlCommon.DEFAULT_SCHEMA_NAME, TABLE_NAME, indexName, List.of(COLUMN_NAME), false);
    }

    private void startBuildingIndex(int indexId) {
        assertThat(catalogManager.execute(StartBuildingIndexCommand.builder().indexId(indexId).build()), willCompleteSuccessfully());
    }

    private void makeIndexAvailable(int indexId) {
        assertThat(catalogManager.execute(MakeIndexAvailableCommand.builder().indexId(indexId).build()), willCompleteSuccessfully());
    }

    private void dropIndex(String indexName) {
        TableTestUtils.dropIndex(catalogManager, SqlCommon.DEFAULT_SCHEMA_NAME, indexName);
    }

    private void setPrimaryReplicaWhichExpiresInOneSecond(
            int partitionId,
            String leaseholder,
            String leaseholderId,
            HybridTimestamp startTime
    ) {
        CompletableFuture<ReplicaMeta> replicaMetaFuture = completedFuture(replicaMetaForOneSecond(leaseholder, leaseholderId, startTime));

        assertThat(placementDriver.setPrimaryReplicaMeta(0, replicaId(partitionId), replicaMetaFuture), willCompleteSuccessfully());
    }

    private int tableId() {
        return tableId(TABLE_NAME);
    }

    private int tableId(String tableName) {
        return getTableIdStrict(catalogManager, tableName, clock.nowLong());
    }

    private int indexId(String indexName) {
        return getIndexIdStrict(catalogManager, indexName, clock.nowLong());
    }

    private TablePartitionId replicaId(int partitionId) {
        return new TablePartitionId(tableId(), partitionId);
    }

    private ReplicaMeta replicaMetaForOneSecond(String leaseholder, String leaseholderId, HybridTimestamp startTime) {
        return new Lease(
                leaseholder,
                leaseholderId,
                startTime,
                startTime.addPhysicalTime(1_000),
                new TablePartitionId(tableId(), PARTITION_ID)
        );
    }

    private int indexCreationCatalogVersion(String indexName) {
        return getIndexStrict(catalogManager, indexName, clock.nowLong()).txWaitCatalogVersion();
    }
}
