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
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.createTestCatalogManager;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.COLUMN_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.INDEX_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.NODE_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.TABLE_NAME;
import static org.apache.ignite.internal.table.TableTestUtils.createHashIndex;
import static org.apache.ignite.internal.table.TableTestUtils.getIndexIdStrict;
import static org.apache.ignite.internal.table.TableTestUtils.getTableIdStrict;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.startAsync;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.lowwatermark.TestLowWatermark;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.marshaller.ReflectionMarshallersProvider;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.impl.MetaStorageRevisionListenerRegistry;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.ReadOperationForCompactionTracker;
import org.apache.ignite.internal.metastorage.server.persistence.RocksDbKeyValueStorage;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.TableTestUtils;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.PartitionSet;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.TableStatsStalenessConfiguration;
import org.apache.ignite.internal.table.distributed.schema.ConstantSchemaVersions;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.WaitDieDeadlockPreventionPolicy;
import org.apache.ignite.sql.IgniteSql;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Test class to verify {@link IndexManager}. */
@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(ExecutorServiceExtension.class)
public class IndexManagerTest extends BaseIgniteAbstractTest {
    private final HybridClock clock = new HybridClockImpl();

    @WorkDirectory
    private Path workDir;

    @InjectExecutorService
    private ScheduledExecutorService scheduledExecutorService;

    private TableManager mockTableManager;

    private SchemaManager mockSchemaManager;

    private MetaStorageManagerImpl metaStorageManager;

    private CatalogManager catalogManager;

    private IndexManager indexManager;

    private final Map<Integer, TableViewInternal> tableViewInternalByTableId = new ConcurrentHashMap<>();

    private final TestLowWatermark lowWatermark = new TestLowWatermark();

    @BeforeEach
    public void setUp() {
        mockTableManager = mock(TableManager.class);

        when(mockTableManager.tableAsync(anyLong(), anyInt())).thenAnswer(inv -> completedFuture(mockTable(inv.getArgument(1))));

        when(mockTableManager.cachedTable(anyInt())).thenAnswer(inv -> mockTable(inv.getArgument(0)));

        when(mockTableManager.localPartitionSetAsync(anyLong(), anyInt())).thenReturn(completedFuture(PartitionSet.EMPTY_SET));

        mockSchemaManager = mock(SchemaManager.class);

        when(mockSchemaManager.schemaRegistry(anyLong(), anyInt())).thenReturn(nullCompletedFuture());

        createAndStartComponents();

        createTable(TABLE_NAME);
    }

    @AfterEach
    void tearDown() {
        assertThat(stopAsync(new ComponentContext(), metaStorageManager, catalogManager, indexManager), willCompleteSuccessfully());
    }

    @Test
    void testDontUnregisterIndexOnCatalogEventIndexDrop() {
        createIndex(TABLE_NAME, INDEX_NAME);
        dropIndex(INDEX_NAME);

        TableViewInternal tableViewInternal = tableViewInternalByTableId.get(tableId());

        verify(tableViewInternal, never()).unregisterIndex(anyInt());
    }

    @Test
    void testDestroyIndex() {
        TableViewInternal tableViewInternal = tableViewInternalByTableId.get(tableId());

        when(tableViewInternal.internalTable().storage().destroyIndex(anyInt()))
                .thenReturn(nullCompletedFuture());

        createIndex(TABLE_NAME, INDEX_NAME);

        int indexId = indexId();

        dropIndex(INDEX_NAME);
        assertThat(fireDestroyEvent(), willCompleteSuccessfully());

        verify(tableViewInternal, timeout(1000)).unregisterIndex(indexId);
        verify(tableViewInternal.internalTable().storage(), timeout(1000)).destroyIndex(indexId);
    }

    @Test
    void testIndexDestroyedWithTable() {
        createIndex(TABLE_NAME, INDEX_NAME);

        int tableId = tableId();
        int indexId = indexId();

        dropTable(TABLE_NAME);
        assertThat(fireDestroyEvent(), willCompleteSuccessfully());

        TableViewInternal tableViewInternal = tableViewInternalByTableId.get(tableId);

        // Table manager is responsible to drop index partitions together with a table.
        verify(tableViewInternal, never()).unregisterIndex(indexId);
    }

    private TableViewInternal mockTable(int tableId) {
        return tableViewInternalByTableId.computeIfAbsent(tableId, this::newMockTable);
    }

    private TableViewInternal newMockTable(int tableId) {
        Catalog catalog = Objects.requireNonNull(catalogManager.activeCatalog(clock.nowLong()));
        CatalogZoneDescriptor zone = catalog.defaultZone();

        assertNotNull(zone);

        StorageTableDescriptor storageTableDescriptor = new StorageTableDescriptor(tableId, zone.partitions(), DEFAULT_STORAGE_PROFILE);

        MvTableStorage mvTableStorage = mock(MvTableStorage.class);

        when(mvTableStorage.getTableDescriptor()).thenReturn(storageTableDescriptor);

        InternalTable internalTable = mock(InternalTable.class);

        when(internalTable.tableId()).thenReturn(tableId);
        when(internalTable.storage()).thenReturn(mvTableStorage);

        CatalogTableDescriptor table = catalog.table(tableId);

        ReflectionMarshallersProvider marshallers = new ReflectionMarshallersProvider();

        return spy(new TableImpl(
                internalTable,
                lockManager(),
                new ConstantSchemaVersions(1),
                marshallers,
                mock(IgniteSql.class),
                mock(FailureProcessor.class),
                table.primaryKeyIndexId(),
                mock(TableStatsStalenessConfiguration.class)
        ));
    }

    private static LockManager lockManager() {
        HeapLockManager lockManager = HeapLockManager.smallInstance();
        lockManager.start(new WaitDieDeadlockPreventionPolicy());
        return lockManager;
    }

    private int tableId() {
        return getTableIdStrict(catalogManager, TABLE_NAME, clock.nowLong());
    }

    private int indexId() {
        return getIndexIdStrict(catalogManager, INDEX_NAME, clock.nowLong());
    }

    private void createAndStartComponents() {
        var readOperationForCompactionTracker = new ReadOperationForCompactionTracker();

        var storage = new RocksDbKeyValueStorage(
                NODE_NAME,
                workDir,
                new NoOpFailureManager(),
                readOperationForCompactionTracker,
                scheduledExecutorService
        );

        metaStorageManager = StandaloneMetaStorageManager.create(storage, clock, readOperationForCompactionTracker);

        catalogManager = createTestCatalogManager(NODE_NAME, clock, metaStorageManager);

        indexManager = new IndexManager(
                mockSchemaManager,
                mockTableManager,
                catalogManager,
                ForkJoinPool.commonPool(),
                new MetaStorageRevisionListenerRegistry(metaStorageManager),
                lowWatermark
        );

        ComponentContext context = new ComponentContext();

        assertThat(
                startAsync(context, metaStorageManager)
                        .thenCompose(unused -> metaStorageManager.recoveryFinishedFuture()),
                willCompleteSuccessfully()
        );

        assertThat(
                startAsync(context, catalogManager, indexManager)
                        .thenCompose(unused -> metaStorageManager.notifyRevisionUpdateListenerOnStart())
                        .thenCompose(unused -> metaStorageManager.deployWatches()),
                willCompleteSuccessfully()
        );

        assertThat("Catalog initialization", catalogManager.catalogInitializationFuture(), willCompleteSuccessfully());
    }

    private void createTable(String tableName) {
        TestIndexManagementUtils.createTable(catalogManager, tableName, COLUMN_NAME);
    }

    private void dropTable(String tableName) {
        TableTestUtils.dropTable(catalogManager, SqlCommon.DEFAULT_SCHEMA_NAME, tableName);
    }

    private void createIndex(String tableName, String indexName) {
        createHashIndex(catalogManager, SqlCommon.DEFAULT_SCHEMA_NAME, tableName, indexName, List.of(COLUMN_NAME), false);
    }

    private void dropIndex(String indexName) {
        TableTestUtils.dropIndex(catalogManager, SqlCommon.DEFAULT_SCHEMA_NAME, indexName);
    }

    private CompletableFuture<Void> fireDestroyEvent() {
        return lowWatermark.updateAndNotify(clock.now());
    }
}
