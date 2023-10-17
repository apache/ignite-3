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
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.pkIndexName;
import static org.apache.ignite.internal.table.TableTestUtils.createHashIndex;
import static org.apache.ignite.internal.table.TableTestUtils.getIndexIdStrict;
import static org.apache.ignite.internal.table.TableTestUtils.getTableIdStrict;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogTestUtils;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.MakeIndexAvailableCommand;
import org.apache.ignite.internal.event.AbstractEventProducer;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.placementdriver.leases.Lease;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.table.TableTestUtils;
import org.apache.ignite.internal.table.distributed.index.IndexBuilder;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.TopologyService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** For {@link IndexBuildController} testing. */
public class IndexBuildControllerTest extends BaseIgniteAbstractTest {
    private static final String NODE_NAME = "test_node";

    private static final String TABLE_NAME = "test_table";

    private static final String COLUMN_NAME = "test_column";

    private static final String INDEX_NAME = "test_index";

    private static final int PARTITION_ID = 10;

    private ClusterNode localNode;

    private IndexBuilder indexBuilder;

    private CatalogManager catalogManager;

    private IndexBuildController indexBuildController;

    private final TestPlacementDriver placementDriver = new TestPlacementDriver();

    private final HybridClock clock = new HybridClockImpl();

    @BeforeEach
    void setUp() {
        localNode = mock(ClusterNode.class, invocation -> NODE_NAME);

        indexBuilder = mock(IndexBuilder.class);

        IndexManager indexManager = mock(IndexManager.class, invocation -> {
            MvTableStorage mvTableStorage = mock(MvTableStorage.class);
            MvPartitionStorage mvPartitionStorage = mock(MvPartitionStorage.class);
            IndexStorage indexStorage = mock(IndexStorage.class);

            when(mvTableStorage.getMvPartition(anyInt())).thenReturn(mvPartitionStorage);
            when(mvTableStorage.getIndex(anyInt(), anyInt())).thenReturn(indexStorage);

            return completedFuture(mvTableStorage);
        });

        ClusterService clusterService = mock(ClusterService.class, invocation -> mock(TopologyService.class, invocation1 -> localNode));

        catalogManager = CatalogTestUtils.createTestCatalogManager(NODE_NAME, clock);
        catalogManager.start();

        TableTestUtils.createTable(
                catalogManager,
                DEFAULT_SCHEMA_NAME,
                DEFAULT_ZONE_NAME,
                TABLE_NAME,
                List.of(ColumnParams.builder().name(COLUMN_NAME).type(INT32).build()),
                List.of(COLUMN_NAME)
        );

        indexBuildController = new IndexBuildController(indexBuilder, indexManager, catalogManager, clusterService, placementDriver, clock);
    }

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.stopAll(catalogManager, indexBuildController);
    }

    @Test
    void testStartBuildIndexesOnIndexCreate() {
        setPrimaryReplicaWitchExpireInOneSecond(PARTITION_ID, NODE_NAME, clock.now());

        clearInvocations(indexBuilder);

        createIndex(INDEX_NAME);

        verify(indexBuilder).scheduleBuildIndex(eq(tableId()), eq(PARTITION_ID), eq(indexId(INDEX_NAME)), any(), any(), eq(localNode));
    }

    @Test
    void testStartBuildIndexesOnPrimaryReplicaElected() {
        createIndex(INDEX_NAME);

        setPrimaryReplicaWitchExpireInOneSecond(PARTITION_ID, NODE_NAME, clock.now());

        verify(indexBuilder).scheduleBuildIndex(eq(tableId()), eq(PARTITION_ID), eq(indexId(INDEX_NAME)), any(), any(), eq(localNode));

        verify(indexBuilder).scheduleBuildIndex(
                eq(tableId()),
                eq(PARTITION_ID),
                eq(indexId(pkIndexName(TABLE_NAME))),
                any(),
                any(),
                eq(localNode)
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
        setPrimaryReplicaWitchExpireInOneSecond(PARTITION_ID, NODE_NAME, clock.now());
        setPrimaryReplicaWitchExpireInOneSecond(PARTITION_ID, NODE_NAME + "_other", clock.now());

        verify(indexBuilder).stopBuildingIndexes(tableId(), PARTITION_ID);
    }

    @Test
    void testStartBuildIndexesOnPrimaryReplicaElectedOnlyForRegisteredIndexes() {
        createIndex(INDEX_NAME);

        int indexId = indexId(INDEX_NAME);

        makeIndexAvailable(indexId);

        setPrimaryReplicaWitchExpireInOneSecond(PARTITION_ID, NODE_NAME, clock.now());

        verify(indexBuilder, never()).scheduleBuildIndex(
                eq(tableId()),
                eq(PARTITION_ID),
                eq(indexId(INDEX_NAME)),
                any(),
                any(),
                eq(localNode)
        );

        verify(indexBuilder).scheduleBuildIndex(
                eq(tableId()),
                eq(PARTITION_ID),
                eq(indexId(pkIndexName(TABLE_NAME))),
                any(),
                any(),
                eq(localNode)
        );
    }

    private void createIndex(String indexName) {
        createHashIndex(catalogManager, DEFAULT_SCHEMA_NAME, TABLE_NAME, indexName, List.of(COLUMN_NAME), false);
    }

    private void makeIndexAvailable(int indexId) {
        assertThat(catalogManager.execute(MakeIndexAvailableCommand.builder().indexId(indexId).build()), willCompleteSuccessfully());
    }

    private void dropIndex(String indexName) {
        TableTestUtils.dropIndex(catalogManager, DEFAULT_SCHEMA_NAME, indexName);
    }

    private void setPrimaryReplicaWitchExpireInOneSecond(int partitionId, String leaseholder, HybridTimestamp startTime) {
        CompletableFuture<ReplicaMeta> replicaMetaFuture = completedFuture(replicaMetaForOneSecond(leaseholder, startTime));

        assertThat(placementDriver.setPrimaryReplicaMeta(0, replicaId(partitionId), replicaMetaFuture), willCompleteSuccessfully());
    }

    private int tableId() {
        return getTableIdStrict(catalogManager, TABLE_NAME, clock.nowLong());
    }

    private int indexId(String indexName) {
        return getIndexIdStrict(catalogManager, indexName, clock.nowLong());
    }

    private TablePartitionId replicaId(int partitionId) {
        return new TablePartitionId(tableId(), partitionId);
    }

    private ReplicaMeta replicaMetaForOneSecond(String leaseholder, HybridTimestamp startTime) {
        return new Lease(leaseholder, startTime, startTime.addPhysicalTime(1_000), new TablePartitionId(tableId(), PARTITION_ID));
    }

    private static class TestPlacementDriver extends AbstractEventProducer<PrimaryReplicaEvent, PrimaryReplicaEventParameters> implements
            PlacementDriver {
        private final Map<ReplicationGroupId, CompletableFuture<ReplicaMeta>> primaryReplicaMetaFutureById = new ConcurrentHashMap<>();

        @Override
        public CompletableFuture<ReplicaMeta> awaitPrimaryReplica(
                ReplicationGroupId groupId,
                HybridTimestamp timestamp,
                long timeout,
                TimeUnit unit
        ) {
            return primaryReplicaMetaFutureById.get(groupId);
        }

        @Override
        public CompletableFuture<ReplicaMeta> getPrimaryReplica(ReplicationGroupId replicationGroupId, HybridTimestamp timestamp) {
            return primaryReplicaMetaFutureById.get(replicationGroupId);
        }

        @Override
        public CompletableFuture<Void> previousPrimaryExpired(ReplicationGroupId grpId) {
            throw new UnsupportedOperationException();
        }

        CompletableFuture<Void> setPrimaryReplicaMeta(
                long causalityToken,
                TablePartitionId replicaId,
                CompletableFuture<ReplicaMeta> replicaMetaFuture
        ) {
            primaryReplicaMetaFutureById.put(replicaId, replicaMetaFuture);

            return replicaMetaFuture.thenCompose(replicaMeta -> fireEvent(
                    PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED,
                    new PrimaryReplicaEventParameters(causalityToken, replicaId, replicaMeta.getLeaseholder())
            ));
        }
    }
}
