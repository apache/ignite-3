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

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.awaitDefaultZoneCreation;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.createTestCatalogManager;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.pkIndexName;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.AVAILABLE;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.COLUMN_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.INDEX_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.NODE_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.TABLE_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.createTable;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.ArrayUtils.BYTE_EMPTY_ARRAY;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.apache.ignite.internal.util.IgniteUtils.startAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.commands.AlterZoneCommand;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.table.TableTestUtils;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.network.ClusterNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** For {@link IndexAvailabilityController} testing. */
public class IndexAvailabilityControllerTest extends BaseIgniteAbstractTest {
    private static final long ANY_ENLISTMENT_CONSISTENCY_TOKEN = 100500;

    private final HybridClock clock = new HybridClockImpl();

    private int partitions;

    private final MetaStorageManagerImpl metaStorageManager = StandaloneMetaStorageManager.create();

    private final CatalogManager catalogManager = createTestCatalogManager(NODE_NAME, clock, metaStorageManager);

    private final ExecutorService executorService = newSingleThreadExecutor();

    private final IndexBuilder indexBuilder = new IndexBuilder(
            executorService,
            mock(ReplicaService.class, invocation -> nullCompletedFuture())
    );

    private final IndexAvailabilityController indexAvailabilityController = new IndexAvailabilityController(
            catalogManager,
            metaStorageManager,
            indexBuilder
    );

    @BeforeEach
    void setUp() {
        assertThat(
                startAsync(metaStorageManager, catalogManager)
                        .thenCompose(unused -> metaStorageManager.deployWatches()),
                willCompleteSuccessfully()
        );

        awaitDefaultZoneCreation(catalogManager);

        Catalog catalog = catalogManager.catalog(catalogManager.activeCatalogVersion(clock.nowLong()));

        assert catalog != null;

        CatalogZoneDescriptor zoneDescriptor = catalog.defaultZone();

        assertNotNull(zoneDescriptor);

        partitions = zoneDescriptor.partitions();

        assertThat(partitions, greaterThan(4));

        createTable(catalogManager, TABLE_NAME, COLUMN_NAME);
    }

    @AfterEach
    void tearDown() throws Exception {
        closeAll(
                indexAvailabilityController::close,
                indexBuilder::close,
                () -> assertThat(catalogManager.stopAsync(), willCompleteSuccessfully()),
                () -> assertThat(metaStorageManager.stopAsync(), willCompleteSuccessfully()),
                () -> shutdownAndAwaitTermination(executorService, 1, TimeUnit.SECONDS)
        );
    }

    @Test
    void testMetastoreKeysAfterIndexCreate() throws Exception {
        createIndex(INDEX_NAME);

        int indexId = indexId(INDEX_NAME);

        awaitTillGlobalMetastoreRevisionIsApplied();

        assertInProgressBuildIndexKeyAbsent(indexId);

        for (int partitionId = 0; partitionId < partitions; partitionId++) {
            assertPartitionBuildIndexKeyAbsent(indexId, partitionId);
        }
    }

    @Test
    void testMetastoreKeysAfterIndexBuilding() throws Exception {
        createIndex(INDEX_NAME);

        int indexId = indexId(INDEX_NAME);

        startBuildIndex(indexId);

        awaitTillGlobalMetastoreRevisionIsApplied();

        assertInProgressBuildIndexKeyExists(indexId);

        for (int partitionId = 0; partitionId < partitions; partitionId++) {
            assertPartitionBuildIndexKeyExists(indexId, partitionId);
        }
    }

    @Test
    void testMetastoreKeysAfterTableCreate() throws Exception {
        String tableName = TABLE_NAME + "_new";

        createTable(catalogManager, tableName, COLUMN_NAME);

        int indexId = indexId(pkIndexName(tableName));

        awaitTillGlobalMetastoreRevisionIsApplied();

        assertInProgressBuildIndexKeyAbsent(indexId);

        for (int partitionId = 0; partitionId < partitions; partitionId++) {
            assertPartitionBuildIndexKeyAbsent(indexId, partitionId);
        }
    }

    @Test
    void testMetastoreKeysAfterIndexCreateForOnlyOnePartition() throws Exception {
        changePartitionCountInCatalog(1);

        testMetastoreKeysAfterIndexCreate();
    }

    @Test
    void testMetastoreKeysAfterIndexBuildingForOnlyOnePartition() throws Exception {
        changePartitionCountInCatalog(1);

        testMetastoreKeysAfterIndexBuilding();
    }

    @Test
    void testMetastoreKeysAfterFinishBuildIndexForOnePartition() throws Exception {
        createIndex(INDEX_NAME);

        int indexId = indexId(INDEX_NAME);

        startBuildIndex(indexId);

        finishBuildingIndexForPartition(indexId, 0, indexCreationCatalogVersion(INDEX_NAME));

        awaitTillGlobalMetastoreRevisionIsApplied();

        assertInProgressBuildIndexKeyExists(indexId);

        assertPartitionBuildIndexKeyAbsent(indexId, 0);

        for (int partitionId = 1; partitionId < partitions; partitionId++) {
            assertPartitionBuildIndexKeyExists(indexId, partitionId);
        }

        assertFalse(isIndexAvailable(INDEX_NAME));
    }

    @Test
    void testMetastoreKeysAfterFinishBuildIndexForAllPartitions() throws Exception {
        createIndex(INDEX_NAME);

        int indexId = indexId(INDEX_NAME);

        startBuildIndex(indexId);

        for (int partitionId = 0; partitionId < partitions; partitionId++) {
            assertThat(
                    metaStorageManager.remove(ByteArray.fromString(partitionBuildIndexKey(indexId, partitionId))),
                    willCompleteSuccessfully()
            );
        }

        awaitTillGlobalMetastoreRevisionIsApplied();

        assertInProgressBuildIndexKeyAbsent(indexId);

        for (int partitionId = 0; partitionId < partitions; partitionId++) {
            assertPartitionBuildIndexKeyAbsent(indexId, partitionId);
        }

        assertTrue(isIndexAvailable(INDEX_NAME));
    }

    @Test
    void testMetastoreKeysAfterDropIndex() throws Exception {
        createIndex(INDEX_NAME);

        int indexId = indexId(INDEX_NAME);

        dropIndex(INDEX_NAME);

        awaitTillGlobalMetastoreRevisionIsApplied();

        assertInProgressBuildIndexKeyAbsent(indexId);

        for (int partitionId = 0; partitionId < partitions; partitionId++) {
            assertPartitionBuildIndexKeyAbsent(indexId, partitionId);
        }
    }

    @Test
    void testMetastoreKeysAfterDropIndexForOnlyOnePartition() throws Exception {
        changePartitionCountInCatalog(1);

        testMetastoreKeysAfterDropIndex();
    }

    @Test
    void testMetastoreKeysAfterFinishBuildingOnePartitionAndDropIndex() throws Exception {
        createIndex(INDEX_NAME);

        int indexId = indexId(INDEX_NAME);

        finishBuildingIndexForPartition(indexId, 0, indexCreationCatalogVersion(INDEX_NAME));

        dropIndex(INDEX_NAME);

        awaitTillGlobalMetastoreRevisionIsApplied();

        assertInProgressBuildIndexKeyAbsent(indexId);

        for (int partitionId = 0; partitionId < partitions; partitionId++) {
            assertPartitionBuildIndexKeyAbsent(indexId, partitionId);
        }
    }

    @Test
    void testMetastoreKeysAfterDropIndexWithLastRemainingPartition() throws Exception {
        createIndex(INDEX_NAME);

        int indexId = indexId(INDEX_NAME);

        for (int partitionId = 1; partitionId < partitions; partitionId++) {
            finishBuildingIndexForPartition(indexId, partitionId, indexCreationCatalogVersion(INDEX_NAME));
        }

        dropIndex(INDEX_NAME);

        awaitTillGlobalMetastoreRevisionIsApplied();

        assertInProgressBuildIndexKeyAbsent(indexId);

        for (int partitionId = 0; partitionId < partitions; partitionId++) {
            assertPartitionBuildIndexKeyAbsent(indexId, partitionId);
        }
    }

    @Test
    void testMetastoreKeysAfterDropIndexWithTwoRemainingPartitions() throws Exception {
        createIndex(INDEX_NAME);

        int indexId = indexId(INDEX_NAME);

        for (int partitionId = 2; partitionId < partitions; partitionId++) {
            finishBuildingIndexForPartition(indexId, partitionId, indexCreationCatalogVersion(INDEX_NAME));
        }

        dropIndex(INDEX_NAME);

        awaitTillGlobalMetastoreRevisionIsApplied();

        assertInProgressBuildIndexKeyAbsent(indexId);

        for (int partitionId = 0; partitionId < partitions; partitionId++) {
            assertPartitionBuildIndexKeyAbsent(indexId, partitionId);
        }
    }

    private void awaitTillGlobalMetastoreRevisionIsApplied() throws Exception {
        TestIndexManagementUtils.awaitTillGlobalMetastoreRevisionIsApplied(metaStorageManager);
    }

    private void createIndex(String indexName) {
        TableTestUtils.createHashIndex(catalogManager, DEFAULT_SCHEMA_NAME, TABLE_NAME, indexName, List.of(COLUMN_NAME), false);
    }

    private void startBuildIndex(int indexId) {
        TestIndexManagementUtils.startBuildingIndex(catalogManager, indexId);
    }

    private void dropIndex(String indexName) {
        TableTestUtils.dropIndex(catalogManager, DEFAULT_SCHEMA_NAME, indexName);
    }

    private int indexId(String indexName) {
        return TableTestUtils.getIndexIdStrict(catalogManager, indexName, clock.nowLong());
    }

    private int tableId(String tableName) {
        return TableTestUtils.getTableIdStrict(catalogManager, tableName, clock.nowLong());
    }

    private int zoneId(String tableName) {
        return TableTestUtils.getTableZoneIdStrict(catalogManager, tableName, clock.nowLong());
    }

    private boolean isIndexAvailable(String indexName) {
        return TableTestUtils.getIndexStrict(catalogManager, indexName, clock.nowLong()).status() == AVAILABLE;
    }

    private void changePartitionCountInCatalog(int newPartitions) {
        Catalog catalog = catalogManager.catalog(catalogManager.activeCatalogVersion(clock.nowLong()));

        assert catalog != null;

        assertThat(
                catalogManager.execute(AlterZoneCommand.builder().zoneName(catalog.defaultZone().name()).partitions(newPartitions).build()),
                willCompleteSuccessfully()
        );

        partitions = newPartitions;
    }

    private void finishBuildingIndexForPartition(int indexId, int partitionId, int indexCreationCatalogVersion) {
        // It may look complicated, but the other method through mocking IndexBuilder seems messier.
        IndexStorage indexStorage = mock(IndexStorage.class);

        when(indexStorage.getNextRowIdToBuild())
                .thenReturn(new RowId(partitionId))
                .thenReturn(null);

        indexBuilder.scheduleBuildIndex(
                zoneId(TABLE_NAME),
                tableId(TABLE_NAME),
                partitionId,
                indexId,
                indexStorage,
                mock(MvPartitionStorage.class),
                mock(ClusterNode.class),
                ANY_ENLISTMENT_CONSISTENCY_TOKEN,
                indexCreationCatalogVersion
        );

        CompletableFuture<Void> finishBuildIndexFuture = new CompletableFuture<>();

        indexBuilder.listen(new IndexBuildCompletionListener() {
            @Override
            public void onBuildCompletion(int indexId1, int tableId1, int partitionId1) {
                if (indexId1 == indexId && partitionId1 == partitionId) {
                    finishBuildIndexFuture.complete(null);
                }
            }
        });

        assertThat(finishBuildIndexFuture, willCompleteSuccessfully());
    }

    private void assertInProgressBuildIndexKeyExists(int indexId) {
        String startBuildIndexKey = inProgressBuildIndexKey(indexId);

        assertThat(
                startBuildIndexKey,
                metaStorageManager.get(ByteArray.fromString(startBuildIndexKey)).thenApply(Entry::value),
                willBe(BYTE_EMPTY_ARRAY)
        );
    }

    private void assertInProgressBuildIndexKeyAbsent(int indexId) {
        String startBuildIndexKey = inProgressBuildIndexKey(indexId);

        assertThat(
                startBuildIndexKey,
                metaStorageManager.get(ByteArray.fromString(startBuildIndexKey)).thenApply(Entry::value),
                willBe(nullValue())
        );
    }

    private void assertPartitionBuildIndexKeyExists(int indexId, int partitionId) {
        String partitionBuildIndexKey = partitionBuildIndexKey(indexId, partitionId);

        assertThat(
                partitionBuildIndexKey,
                metaStorageManager.get(ByteArray.fromString(partitionBuildIndexKey)).thenApply(Entry::value),
                willBe(BYTE_EMPTY_ARRAY)
        );
    }

    private void assertPartitionBuildIndexKeyAbsent(int indexId, int partitionId) {
        String partitionBuildIndexKey = partitionBuildIndexKey(indexId, partitionId);

        assertThat(
                partitionBuildIndexKey,
                metaStorageManager.get(ByteArray.fromString(partitionBuildIndexKey)).thenApply(Entry::value),
                willBe(nullValue())
        );
    }

    private static String inProgressBuildIndexKey(int indexId) {
        return "indexBuild.inProgress." + indexId;
    }

    private static String partitionBuildIndexKey(int indexId, int partitionId) {
        return "indexBuild.partition." + indexId + "." + partitionId;
    }

    private int indexCreationCatalogVersion(String indexName) {
        return TableTestUtils.getIndexStrict(catalogManager, indexName, clock.nowLong()).txWaitCatalogVersion();
    }
}
