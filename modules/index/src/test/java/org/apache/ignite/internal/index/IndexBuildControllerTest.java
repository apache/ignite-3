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
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.pkIndexName;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.COLUMN_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.INDEX_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.LOCAL_NODE;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.NODE_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.OTHER_NODE;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.TABLE_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.createTable;
import static org.apache.ignite.internal.table.TableTestUtils.createHashIndex;
import static org.apache.ignite.internal.table.TableTestUtils.getIndexIdStrict;
import static org.apache.ignite.internal.table.TableTestUtils.getTableIdStrict;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogTestUtils;
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
import org.mockito.verification.VerificationMode;

/** For {@link IndexBuildController} testing. */
public class IndexBuildControllerTest extends BaseIgniteAbstractTest {
    private static final int PARTITION_ID = 10;

    private IndexBuilder indexBuilder;

    private CatalogManager catalogManager;

    private IndexBuildController indexBuildController;

    private final TestPlacementDriver placementDriver = new TestPlacementDriver();

    private final HybridClock clock = new HybridClockImpl();

    @BeforeEach
    void setUp() {
        indexBuilder = mock(IndexBuilder.class);

        IndexManager indexManager = mock(IndexManager.class, invocation -> {
            MvTableStorage mvTableStorage = mock(MvTableStorage.class);
            MvPartitionStorage mvPartitionStorage = mock(MvPartitionStorage.class);
            IndexStorage indexStorage = mock(IndexStorage.class);

            when(mvTableStorage.getMvPartition(anyInt())).thenReturn(mvPartitionStorage);
            when(mvTableStorage.getIndex(anyInt(), anyInt())).thenReturn(indexStorage);

            return completedFuture(mvTableStorage);
        });

        ClusterService clusterService = mock(ClusterService.class, invocation -> mock(TopologyService.class, invocation1 -> LOCAL_NODE));

        catalogManager = CatalogTestUtils.createTestCatalogManager(NODE_NAME, clock);
        catalogManager.start();

        createTable(catalogManager, TABLE_NAME, COLUMN_NAME);

        indexBuildController = new IndexBuildController(indexBuilder, indexManager, catalogManager, clusterService, placementDriver, clock);
    }

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.stopAll(catalogManager, indexBuildController);
    }

    @Test
    void testStartBuildIndexesOnIndexCreate() {
        setPrimaryReplicaWhichExpiresInOneMin(replicaId(PARTITION_ID), LOCAL_NODE, clock.now());

        clearInvocations(indexBuilder);

        createIndex(INDEX_NAME);

        verifyScheduleBuildIndex(times(1), replicaId(PARTITION_ID), indexId(INDEX_NAME), LOCAL_NODE);
    }

    @Test
    void testStartBuildIndexesOnPrimaryReplicaElected() {
        createIndex(INDEX_NAME);

        setPrimaryReplicaWhichExpiresInOneMin(replicaId(PARTITION_ID), LOCAL_NODE, clock.now());

        verifyScheduleBuildIndex(times(1), replicaId(PARTITION_ID), indexId(INDEX_NAME), LOCAL_NODE);
        verifyScheduleBuildIndex(times(1), replicaId(PARTITION_ID), indexId(pkIndexName(TABLE_NAME)), LOCAL_NODE);
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
        setPrimaryReplicaWhichExpiresInOneMin(replicaId(PARTITION_ID), LOCAL_NODE, clock.now());
        setPrimaryReplicaWhichExpiresInOneMin(replicaId(PARTITION_ID), OTHER_NODE, clock.now());

        verify(indexBuilder).stopBuildingIndexes(tableId(), PARTITION_ID);
    }

    @Test
    void testStartBuildIndexesOnPrimaryReplicaElectedOnlyForRegisteredIndexes() {
        createIndex(INDEX_NAME);

        int indexId = indexId(INDEX_NAME);

        makeIndexAvailable(indexId);

        setPrimaryReplicaWhichExpiresInOneMin(replicaId(PARTITION_ID), LOCAL_NODE, clock.now());

        verifyScheduleBuildIndex(never(), replicaId(PARTITION_ID), indexId, LOCAL_NODE);
        verifyScheduleBuildIndex(times(1), replicaId(PARTITION_ID), indexId(pkIndexName(TABLE_NAME)), LOCAL_NODE);
    }

    @Test
    void testRecoverBuildingRegisteredIndexesOnly() {
        createIndex(INDEX_NAME + 0);
        createIndex(INDEX_NAME + 1);

        int indexId0 = indexId(INDEX_NAME + 0);
        int indexId1 = indexId(INDEX_NAME + 1);

        makeIndexAvailable(indexId0);

        setPrimaryReplicaWhichExpiresInOneMin(replicaId(PARTITION_ID), LOCAL_NODE, clock.now());

        clearInvocations(indexBuilder);

        assertThat(indexBuildController.recoverBuildIndexes(0), willCompleteSuccessfully());

        verifyScheduleBuildIndex(never(), replicaId(PARTITION_ID), indexId0, LOCAL_NODE);
        verifyScheduleBuildIndex(times(1), replicaId(PARTITION_ID), indexId1, LOCAL_NODE);
    }

    @Test
    void testRecoverBuildingIndexForPartitionWhichLocalNodeIsPrimaryReplicaOnly() {
        createIndex(INDEX_NAME);

        int partitionId0 = PARTITION_ID;
        int partitionId1 = PARTITION_ID + 1;
        int partitionId2 = PARTITION_ID + 2;
        int partitionId3 = PARTITION_ID + 3; // Unknown primaryReplicaMeta.

        setPrimaryReplicaWhichExpiresInOneMin(replicaId(partitionId0), LOCAL_NODE, clock.now());
        setPrimaryReplicaWhichExpiresInOneMin(replicaId(partitionId1), OTHER_NODE, clock.now());
        setPrimaryReplicaWhichExpiresInOneMin(replicaId(partitionId1), LOCAL_NODE, clock.now().addPhysicalTime(-DAYS.toMillis(1)));

        clearInvocations(indexBuilder);

        assertThat(indexBuildController.recoverBuildIndexes(0), willCompleteSuccessfully());

        int indexId = indexId(INDEX_NAME);

        verifyScheduleBuildIndex(times(1), replicaId(partitionId0), indexId, LOCAL_NODE);

        verifyScheduleBuildIndex(never(), replicaId(partitionId1), indexId, OTHER_NODE);
        verifyScheduleBuildIndex(never(), replicaId(partitionId2), indexId, LOCAL_NODE);
        verifyScheduleBuildIndexAnyNode(never(), replicaId(partitionId3), indexId);
    }

    private void verifyScheduleBuildIndex(
            VerificationMode verificationMode,
            TablePartitionId replicaGroupId,
            int indexId,
            ClusterNode clusterNode
    ) {
        verify(indexBuilder, verificationMode).scheduleBuildIndex(
                eq(replicaGroupId.tableId()),
                eq(replicaGroupId.partitionId()),
                eq(indexId),
                any(),
                any(),
                eq(clusterNode),
                anyLong()
        );
    }

    private void verifyScheduleBuildIndexAnyNode(
            VerificationMode verificationMode,
            TablePartitionId replicaGroupId,
            int indexId
    ) {
        verify(indexBuilder, verificationMode).scheduleBuildIndex(
                eq(replicaGroupId.tableId()),
                eq(replicaGroupId.partitionId()),
                eq(indexId),
                any(),
                any(),
                any(),
                anyLong()
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

    private void setPrimaryReplicaWhichExpiresInOneMin(
            TablePartitionId replicaGroupId,
            ClusterNode clusterNode,
            HybridTimestamp startTime
    ) {
        CompletableFuture<ReplicaMeta> replicaMetaFuture = completedFuture(replicaMetaForOneMin(clusterNode, startTime, replicaGroupId));

        assertThat(placementDriver.setPrimaryReplicaMeta(0, replicaGroupId, replicaMetaFuture), willCompleteSuccessfully());
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

    private static ReplicaMeta replicaMetaForOneMin(ClusterNode clusterNode, HybridTimestamp startTime, TablePartitionId replicaGroupId) {
        return new Lease(
                clusterNode.name(),
                clusterNode.id(),
                startTime,
                startTime.addPhysicalTime(MINUTES.toMillis(1)),
                replicaGroupId
        );
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
            return primaryReplicaMetaFutureById.getOrDefault(replicationGroupId, completedFuture(null));
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
