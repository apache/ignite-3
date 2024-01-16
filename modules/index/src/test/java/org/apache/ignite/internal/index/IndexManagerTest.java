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
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_DATA_REGION;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.pkIndexName;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.COLUMN_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.INDEX_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.NODE_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.PK_INDEX_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.TABLE_NAME;
import static org.apache.ignite.internal.table.TableTestUtils.createHashIndex;
import static org.apache.ignite.internal.table.TableTestUtils.getTableIdStrict;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongFunction;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogTestUtils;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.impl.MetaStorageService;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.TestRocksDbKeyValueStorage;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.TableTestUtils;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.PartitionSet;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.schema.ConstantSchemaVersions;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.sql.IgniteSql;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

/** Test class to verify {@link IndexManager}. */
@ExtendWith(WorkDirectoryExtension.class)
public class IndexManagerTest extends BaseIgniteAbstractTest {
    private static final String OTHER_TABLE_NAME = TABLE_NAME + "-other";

    private static final String PK_INDEX_NAME_OTHER_TABLE = pkIndexName(OTHER_TABLE_NAME);

    private final HybridClock clock = new HybridClockImpl();

    @WorkDirectory
    private Path workDir;

    private TableManager mockTableManager;

    private SchemaManager mockSchemaManager;

    private MetaStorageManagerImpl metaStorageManager;

    private CatalogManager catalogManager;

    private IndexManager indexManager;

    private final Map<Integer, TableViewInternal> tableViewInternalByTableId = new ConcurrentHashMap<>();

    @BeforeEach
    public void setUp() {
        mockTableManager = mock(TableManager.class);

        when(mockTableManager.tableAsync(anyLong(), anyInt())).thenAnswer(inv -> completedFuture(mockTable(inv.getArgument(1))));

        when(mockTableManager.getTable(anyInt())).thenAnswer(inv -> mockTable(inv.getArgument(0)));

        when(mockTableManager.localPartitionSetAsync(anyLong(), anyInt())).thenReturn(completedFuture(PartitionSet.EMPTY_SET));

        mockSchemaManager = mock(SchemaManager.class);

        when(mockSchemaManager.schemaRegistry(anyLong(), anyInt())).thenReturn(nullCompletedFuture());

        createAndStartComponents();

        createTable(TABLE_NAME);
    }

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.stopAll(metaStorageManager, catalogManager, indexManager);
    }

    @Test
    void testGetMvTableStorageForNonExistsTable() {
        assertThat(getMvTableStorageLatestRevision(Integer.MAX_VALUE), willBe(nullValue()));
    }

    @Test
    void testGetMvTableStorageForExistsTable() {
        assertThat(getMvTableStorageLatestRevision(tableId()), willBe(notNullValue()));
    }

    @Test
    void testGetMvTableStorageForDroppedTable() {
        dropTable(TABLE_NAME);

        assertThat(getMvTableStorageLatestRevision(Integer.MAX_VALUE), willBe(nullValue()));
    }

    @Test
    void testGetMvTableStorageForNewIndexInCatalogListener() {
        CompletableFuture<MvTableStorage> getMvTableStorageInCatalogListenerFuture = new CompletableFuture<>();

        catalogManager.listen(CatalogEvent.INDEX_CREATE, (parameters, exception) -> {
            if (exception != null) {
                getMvTableStorageInCatalogListenerFuture.completeExceptionally(exception);
            } else {
                try {
                    CompletableFuture<MvTableStorage> mvTableStorageFuture = getMvTableStorage(parameters.causalityToken(), tableId());

                    assertFalse(mvTableStorageFuture.isDone());

                    mvTableStorageFuture.whenComplete((mvTableStorage, throwable) -> {
                        if (throwable != null) {
                            getMvTableStorageInCatalogListenerFuture.completeExceptionally(throwable);
                        } else {
                            getMvTableStorageInCatalogListenerFuture.complete(mvTableStorage);
                        }
                    });
                } catch (Throwable t) {
                    getMvTableStorageInCatalogListenerFuture.completeExceptionally(t);
                }
            }

            return falseCompletedFuture();
        });

        createHashIndex(
                catalogManager,
                DEFAULT_SCHEMA_NAME,
                TABLE_NAME,
                TABLE_NAME + "_test_index",
                List.of(COLUMN_NAME),
                false
        );

        assertThat(getMvTableStorageInCatalogListenerFuture, willBe(notNullValue()));
    }

    @Test
    void testDontUnregisterIndexOnCatalogEventIndexDrop() {
        createIndex(TABLE_NAME, INDEX_NAME);
        dropIndex(INDEX_NAME);

        TableViewInternal tableViewInternal = tableViewInternalByTableId.get(tableId());

        verify(tableViewInternal, never()).unregisterIndex(anyInt());
    }

    @Test
    void testCollectIndexesForRecoveryForCreatedTables() {
        createTable(OTHER_TABLE_NAME);

        Map<CatalogTableDescriptor, Collection<CatalogIndexDescriptor>> collectedIndexes = collectIndexesForRecovery();

        int latestCatalogVersion = catalogManager.latestCatalogVersion();

        CatalogTableDescriptor table = table(latestCatalogVersion, TABLE_NAME);
        CatalogTableDescriptor otherTable = table(latestCatalogVersion, OTHER_TABLE_NAME);

        assertThat(collectedIndexes, hasKey(table));
        assertThat(collectedIndexes, hasKey(otherTable));

        assertThat(collectedIndexes.get(table), hasItems(index(latestCatalogVersion, PK_INDEX_NAME)));
        assertThat(collectedIndexes.get(otherTable), hasItems(index(latestCatalogVersion, PK_INDEX_NAME_OTHER_TABLE)));
    }

    @Test
    void testCollectIndexesForRecoveryForCreatedAndDroppedIndexes() {
        createTable(OTHER_TABLE_NAME);

        String indexName0 = INDEX_NAME + 0;
        String indexName1 = INDEX_NAME + 1;
        String indexName2 = INDEX_NAME + 2;
        String indexName3 = INDEX_NAME + 3;

        String indexNameOtherTable0 = INDEX_NAME + "-other" + 0;
        String indexNameOtherTable1 = INDEX_NAME + "-other" + 1;
        String indexNameOtherTable2 = INDEX_NAME + "-other" + 2;
        String indexNameOtherTable3 = INDEX_NAME + "-other" + 3;

        createIndexes(TABLE_NAME, indexName0, indexName1, indexName2, indexName3);
        createIndexes(OTHER_TABLE_NAME, indexNameOtherTable0, indexNameOtherTable1, indexNameOtherTable2, indexNameOtherTable3);

        makeIndexesAvailable(indexName0, indexName2, indexNameOtherTable1, indexNameOtherTable3);

        List<CatalogIndexDescriptor> droppedIndexes = dropIndexes(indexName1, indexName2);
        List<CatalogIndexDescriptor> droppedIndexesOtherTable = dropIndexes(indexNameOtherTable0, indexNameOtherTable1);

        Map<CatalogTableDescriptor, Collection<CatalogIndexDescriptor>> collectedIndexes = collectIndexesForRecovery();

        int latestCatalogVersion = catalogManager.latestCatalogVersion();

        CatalogTableDescriptor table = table(latestCatalogVersion, TABLE_NAME);
        CatalogTableDescriptor otherTable = table(latestCatalogVersion, OTHER_TABLE_NAME);

        assertThat(collectedIndexes, hasKey(table));
        assertThat(collectedIndexes, hasKey(otherTable));

        assertThat(
                collectedIndexes.get(table),
                hasItems(
                        index(latestCatalogVersion, PK_INDEX_NAME),
                        index(latestCatalogVersion, indexName0),
                        index(latestCatalogVersion, indexName3),
                        droppedIndexes.get(0),
                        droppedIndexes.get(1)
                )
        );

        assertThat(
                collectedIndexes.get(otherTable),
                hasItems(
                        index(latestCatalogVersion, PK_INDEX_NAME_OTHER_TABLE),
                        index(latestCatalogVersion, indexNameOtherTable2),
                        index(latestCatalogVersion, indexNameOtherTable3),
                        droppedIndexesOtherTable.get(0),
                        droppedIndexesOtherTable.get(1)
                )
        );
    }

    @Test
    void testCollectIndexesForDroppedTable() {
        createTable(OTHER_TABLE_NAME);
        dropTable(OTHER_TABLE_NAME);

        Map<CatalogTableDescriptor, Collection<CatalogIndexDescriptor>> collectedIndexes = collectIndexesForRecovery();

        CatalogTableDescriptor table = table(catalogManager.latestCatalogVersion(), TABLE_NAME);

        assertThat(collectedIndexes, aMapWithSize(1));
        assertThat(collectedIndexes, hasKey(table));
    }

    @Test
    void testStartAllIndexesOnNodeRecovery() throws Exception {
        String indexName0 = INDEX_NAME + 0;
        String indexName1 = INDEX_NAME + 1;
        String indexName2 = INDEX_NAME + 2;
        String indexName3 = INDEX_NAME + 3;

        createIndexes(TABLE_NAME, indexName0, indexName1, indexName2, indexName3);

        makeIndexesAvailable(indexName0, indexName2);

        dropIndexes(indexName1, indexName2);

        TableViewInternal tableViewInternal = tableViewInternalByTableId.get(tableId());

        clearInvocations(tableViewInternal);

        IgniteUtils.stopAll(indexManager, catalogManager, metaStorageManager);

        createAndStartComponents();

        ArgumentCaptor<StorageHashIndexDescriptor> captor = ArgumentCaptor.forClass(StorageHashIndexDescriptor.class);

        verify(tableViewInternal, times(5)).registerHashIndex(captor.capture(), anyBoolean(), any(), any());

        CatalogTableDescriptor table = table(catalogManager.latestCatalogVersion(), TABLE_NAME);

        assertThat(
                captor.getAllValues().stream().map(StorageHashIndexDescriptor::id).sorted().collect(toList()),
                equalTo(collectIndexesForRecovery().get(table).stream().map(CatalogObjectDescriptor::id).sorted().collect(toList()))
        );
    }

    private TableViewInternal mockTable(int tableId) {
        return tableViewInternalByTableId.computeIfAbsent(tableId, this::newMockTable);
    }

    private TableViewInternal newMockTable(int tableId) {
        CatalogZoneDescriptor zone = catalogManager.zone(DEFAULT_ZONE_NAME, clock.nowLong());

        assertNotNull(zone);

        StorageTableDescriptor storageTableDescriptor = new StorageTableDescriptor(tableId, zone.partitions(), DEFAULT_DATA_REGION);

        MvTableStorage mvTableStorage = mock(MvTableStorage.class);

        when(mvTableStorage.getTableDescriptor()).thenReturn(storageTableDescriptor);

        InternalTable internalTable = mock(InternalTable.class);

        when(internalTable.tableId()).thenReturn(tableId);
        when(internalTable.storage()).thenReturn(mvTableStorage);

        return spy(new TableImpl(internalTable, new HeapLockManager(), new ConstantSchemaVersions(1), mock(IgniteSql.class)));
    }

    private CompletableFuture<MvTableStorage> getMvTableStorageLatestRevision(int tableId) {
        return metaStorageManager.metaStorageService()
                .thenCompose(MetaStorageService::currentRevision)
                .thenCompose(latestRevision -> getMvTableStorage(latestRevision, tableId));
    }

    private CompletableFuture<MvTableStorage> getMvTableStorage(long causalityToken, int tableId) {
        return indexManager.getMvTableStorage(causalityToken, tableId);
    }

    private int tableId() {
        return getTableIdStrict(catalogManager, TABLE_NAME, clock.nowLong());
    }

    private void createAndStartComponents() {
        metaStorageManager = StandaloneMetaStorageManager.create(new TestRocksDbKeyValueStorage(NODE_NAME, workDir));

        catalogManager = CatalogTestUtils.createTestCatalogManager(NODE_NAME, clock, metaStorageManager);

        indexManager = new IndexManager(
                mockSchemaManager,
                mockTableManager,
                catalogManager,
                metaStorageManager,
                (LongFunction<CompletableFuture<?>> function) -> metaStorageManager.registerRevisionUpdateListener(function::apply)
        );

        List.of(metaStorageManager, catalogManager, indexManager).forEach(IgniteComponent::start);

        assertThat(metaStorageManager.recoveryFinishedFuture(), willCompleteSuccessfully());
        assertThat(metaStorageManager.notifyRevisionUpdateListenerOnStart(), willCompleteSuccessfully());
        assertThat(metaStorageManager.deployWatches(), willCompleteSuccessfully());
    }

    private CatalogTableDescriptor table(int catalogVersion, String tableName) {
        return CatalogTestUtils.table(catalogManager, catalogVersion, tableName);
    }

    private CatalogIndexDescriptor index(int catalogVersion, String indexName) {
        return CatalogTestUtils.index(catalogManager, catalogVersion, indexName);
    }

    private void createTable(String tableName) {
        TestIndexManagementUtils.createTable(catalogManager, tableName, COLUMN_NAME);
    }

    private void dropTable(String tableName) {
        TableTestUtils.dropTable(catalogManager, DEFAULT_SCHEMA_NAME, tableName);
    }

    private void createIndex(String tableName, String indexName) {
        createHashIndex(catalogManager, DEFAULT_SCHEMA_NAME, tableName, indexName, List.of(COLUMN_NAME), false);
    }

    private void makeIndexAvailable(String indexName) {
        CatalogIndexDescriptor index = index(catalogManager.latestCatalogVersion(), indexName);

        TestIndexManagementUtils.makeIndexAvailable(catalogManager, index.id());
    }

    private void dropIndex(String indexName) {
        TableTestUtils.dropIndex(catalogManager, DEFAULT_SCHEMA_NAME, indexName);
    }

    private void createIndexes(String tableName, String... indexNames) {
        for (String indexName : indexNames) {
            createIndex(tableName, indexName);
        }
    }

    private void makeIndexesAvailable(String... indexNames) {
        for (String indexName : indexNames) {
            makeIndexAvailable(indexName);
        }
    }

    private List<CatalogIndexDescriptor> dropIndexes(String... indexNames) {
        var res = new ArrayList<CatalogIndexDescriptor>(indexNames.length);

        for (String indexName : indexNames) {
            res.add(index(catalogManager.latestCatalogVersion(), indexName));

            dropIndex(indexName);
        }

        return res;
    }

    private Map<CatalogTableDescriptor, Collection<CatalogIndexDescriptor>> collectIndexesForRecovery() {
        return IndexManager.collectIndexesForRecovery(catalogManager);
    }
}
