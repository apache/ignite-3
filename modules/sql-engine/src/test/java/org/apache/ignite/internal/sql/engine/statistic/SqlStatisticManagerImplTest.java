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

package org.apache.ignite.internal.sql.engine.statistic;

import static it.unimi.dsi.fastutil.ints.Int2ObjectMap.entry;
import static org.apache.ignite.internal.sql.engine.statistic.SqlStatisticManagerImpl.DEFAULT_TABLE_SIZE;
import static org.apache.ignite.internal.sql.engine.statistic.SqlStatisticManagerImpl.INITIAL_DELAY;
import static org.apache.ignite.internal.sql.engine.statistic.SqlStatisticManagerImpl.REFRESH_PERIOD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntList;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CreateTableEventParameters;
import org.apache.ignite.internal.catalog.events.DropTableEventParameters;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.lowwatermark.event.ChangeLowWatermarkEventParameters;
import org.apache.ignite.internal.lowwatermark.event.LowWatermarkEvent;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.sql.ColumnType;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests of SqlStatisticManagerImpl.
 */
@ExtendWith(MockitoExtension.class)
@ExtendWith(ExecutorServiceExtension.class)
class SqlStatisticManagerImplTest extends BaseIgniteAbstractTest {
    @Mock
    private TableManager tableManager;

    @Mock
    private CatalogManager catalogManager;

    @Mock
    private TableViewInternal tableViewInternal;

    @Mock
    private InternalTable internalTable;

    @Mock
    private LowWatermark lowWatermark;

    @Mock
    StatisticAggregatorImpl statAggregator;

    @InjectExecutorService
    private ScheduledExecutorService commonExecutor;

    private static final CatalogTableColumnDescriptor pkCol =
            new CatalogTableColumnDescriptor("pkCol", ColumnType.STRING, false, 0, 0, 10, null);

    @Test
    public void checkDefaultTableSize() {
        int tableId = ThreadLocalRandom.current().nextInt();
        // Preparing:
        when(catalogManager.catalog(anyInt())).thenReturn(mock(Catalog.class));

        SqlStatisticManagerImpl sqlStatisticManager =
                new SqlStatisticManagerImpl(tableManager, catalogManager, lowWatermark, commonExecutor, statAggregator);
        sqlStatisticManager.start();

        // Test:
        // return default value for unknown table.
        assertEquals(DEFAULT_TABLE_SIZE, sqlStatisticManager.tableSize(tableId));
    }

    @Test
    public void checkTableSize() {
        int tableId = ThreadLocalRandom.current().nextInt();
        long tableSize = 999_888_777L;
        // Preparing:
        prepareCatalogWithTable(tableId);

        when(tableManager.cachedTable(tableId)).thenReturn(tableViewInternal);
        when(tableViewInternal.internalTable()).thenReturn(internalTable);
        when(statAggregator.estimatedSizeWithLastUpdate(List.of(internalTable)))
                .thenReturn(CompletableFuture.completedFuture(Int2ObjectMap.ofEntries(
                        entry(tableId, new PartitionModificationInfo(tableSize, Long.MAX_VALUE)))));

        SqlStatisticManagerImpl sqlStatisticManager =
                new SqlStatisticManagerImpl(tableManager, catalogManager, lowWatermark, commonExecutor, statAggregator);
        sqlStatisticManager.start();

        long timeout = 2 * Math.max(REFRESH_PERIOD, INITIAL_DELAY);

        Awaitility.await().timeout(timeout, TimeUnit.MILLISECONDS).untilAsserted(() ->
                assertEquals(tableSize, sqlStatisticManager.tableSize(tableId))
        );

        // Second time we should obtain the same value from a cache.
        assertEquals(tableSize, sqlStatisticManager.tableSize(tableId));

        verify(statAggregator, times(1)).estimatedSizeWithLastUpdate(List.of(internalTable));
    }

    @Test
    public void checkModifyTableSize() {
        int tableId = ThreadLocalRandom.current().nextInt();
        long tableSize1 = 999_888_777L;
        long tableSize2 = 111_222_333L;

        HybridTimestamp time1 = HybridTimestamp.MAX_VALUE.subtractPhysicalTime(1000);
        HybridTimestamp time2 = HybridTimestamp.MAX_VALUE.subtractPhysicalTime(500);
        // Preparing:
        prepareCatalogWithTable(tableId);

        when(tableManager.cachedTable(tableId)).thenReturn(tableViewInternal);
        when(tableViewInternal.internalTable()).thenReturn(internalTable);

        when(statAggregator.estimatedSizeWithLastUpdate(List.of(internalTable)))
                .thenReturn(
                        CompletableFuture.completedFuture(Int2ObjectMap.ofEntries(
                                entry(tableId, new PartitionModificationInfo(tableSize1, time1.longValue())))),
                        CompletableFuture.completedFuture(Int2ObjectMap.ofEntries(
                                entry(tableId, new PartitionModificationInfo(tableSize2, time2.longValue()))))
                );

        SqlStatisticManagerImpl sqlStatisticManager =
                new SqlStatisticManagerImpl(tableManager, catalogManager, lowWatermark, commonExecutor, statAggregator);
        sqlStatisticManager.start();

        long timeout = 2 * Math.max(REFRESH_PERIOD, INITIAL_DELAY);

        Awaitility.await().timeout(timeout, TimeUnit.MILLISECONDS).untilAsserted(() ->
                assertEquals(tableSize1, sqlStatisticManager.tableSize(tableId))
        );
        // The second time we should obtain the same value from a cache.
        assertEquals(tableSize1, sqlStatisticManager.tableSize(tableId));

        // Forcibly update.
        sqlStatisticManager.forceUpdateAll();
        // Now we need obtain a fresh value of table size.
        assertEquals(tableSize2, sqlStatisticManager.tableSize(tableId));
        assertEquals(time2.longValue(), sqlStatisticManager.tableSizeMap.get(tableId).modificationCounter());

        verify(statAggregator, times(2)).estimatedSizeWithLastUpdate(List.of(internalTable));
    }

    @Test
    public void checkLoadAllTablesOnStart() throws Exception {
        int minimumCatalogVersion = 1;
        int maximumCatalogVersion = 10;
        long tableSize = 99999L;

        // Preparing:
        when(catalogManager.earliestCatalogVersion()).thenReturn(minimumCatalogVersion);
        when(catalogManager.latestCatalogVersion()).thenReturn(maximumCatalogVersion);
        for (int catalogVersion = minimumCatalogVersion; catalogVersion <= maximumCatalogVersion; catalogVersion++) {
            List<CatalogTableDescriptor> catalogDescriptors = new ArrayList<>();
            catalogDescriptors.add(CatalogTableDescriptor.builder()
                    .id(catalogVersion)
                    .schemaId(1)
                    .primaryKeyIndexId(1)
                    .name("")
                    .zoneId(1)
                    .newColumns(List.of(pkCol))
                    .primaryKeyColumns(IntList.of(0))
                    .storageProfile("")
                    .build());
            catalogDescriptors.add(CatalogTableDescriptor.builder()
                    .id(catalogVersion + 1)
                    .schemaId(1)
                    .primaryKeyIndexId(1)
                    .name("")
                    .zoneId(1)
                    .newColumns(List.of(pkCol))
                    .primaryKeyColumns(IntList.of(0))
                    .storageProfile("")
                    .build());

            Catalog catalog = mock(Catalog.class);
            when(catalogManager.catalog(catalogVersion)).thenReturn(catalog);
            when(catalog.tables()).thenReturn(catalogDescriptors);
        }

        when(tableManager.cachedTable(anyInt())).thenReturn(tableViewInternal);
        when(tableViewInternal.internalTable()).thenReturn(internalTable);
        Int2ObjectMap<PartitionModificationInfo> modifications = new Int2ObjectOpenHashMap<>();
        for (int i = minimumCatalogVersion; i <= maximumCatalogVersion + 1; ++i) {
            modifications.put(i, new PartitionModificationInfo(tableSize, Long.MAX_VALUE));
        }
        when(statAggregator.estimatedSizeWithLastUpdate(any()))
                .thenReturn(CompletableFuture.completedFuture(modifications));

        SqlStatisticManagerImpl sqlStatisticManager =
                new SqlStatisticManagerImpl(tableManager, catalogManager, lowWatermark, commonExecutor, statAggregator);
        sqlStatisticManager.start();

        sqlStatisticManager.forceUpdateAll();
        sqlStatisticManager.lastUpdateStatisticFuture().get(5_000, TimeUnit.MILLISECONDS);

        // Test:
        // For known tables we got calculated table size.
        for (int i = minimumCatalogVersion; i <= maximumCatalogVersion; i++) {
            assertEquals(tableSize, sqlStatisticManager.tableSize(i));
            assertEquals(tableSize, sqlStatisticManager.tableSize(i + 1));
        }

        // For unknown tables we got default size.
        for (int i = maximumCatalogVersion + 2; i <= maximumCatalogVersion + 10; i++) {
            assertEquals(DEFAULT_TABLE_SIZE, sqlStatisticManager.tableSize(i));
        }
    }

    @SuppressWarnings("PMD.UseNotifyAllInsteadOfNotify")
    @Test
    public void checkCleanupTableSizeCache() {
        int tableId = ThreadLocalRandom.current().nextInt();
        long tableSize = 999_888_777L;
        // Preparing:
        prepareCatalogWithTable(tableId);

        ArgumentCaptor<EventListener<DropTableEventParameters>> tableDropCapture = ArgumentCaptor.forClass(EventListener.class);
        doNothing().when(catalogManager).listen(eq(CatalogEvent.TABLE_DROP), tableDropCapture.capture());
        doNothing().when(catalogManager).listen(eq(CatalogEvent.TABLE_CREATE), any());

        ArgumentCaptor<EventListener<ChangeLowWatermarkEventParameters>> lowWatermarkCapture = ArgumentCaptor.forClass(EventListener.class);
        doNothing().when(lowWatermark).listen(eq(LowWatermarkEvent.LOW_WATERMARK_CHANGED), lowWatermarkCapture.capture());

        when(tableManager.cachedTable(tableId)).thenReturn(tableViewInternal);
        when(tableViewInternal.internalTable()).thenReturn(internalTable);
        when(statAggregator.estimatedSizeWithLastUpdate(List.of(internalTable)))
                .thenReturn(CompletableFuture.completedFuture(Int2ObjectMap.ofEntries(
                        entry(tableId, new PartitionModificationInfo(tableSize, Long.MAX_VALUE)))));

        SqlStatisticManagerImpl sqlStatisticManager =
                new SqlStatisticManagerImpl(tableManager, catalogManager, lowWatermark, commonExecutor, statAggregator);
        sqlStatisticManager.start();

        long timeout = 2 * Math.max(REFRESH_PERIOD, INITIAL_DELAY);

        Awaitility.await().timeout(timeout, TimeUnit.MILLISECONDS).untilAsserted(() ->
                assertEquals(tableSize, sqlStatisticManager.tableSize(tableId))
        );

        int catalogVersionBeforeDrop = 1;
        int catalogVersionAfterDrop = 2;

        // After table drop we still obtain the same value from a cache.
        tableDropCapture.getValue().notify(new DropTableEventParameters(-1, catalogVersionAfterDrop, tableId));
        assertEquals(tableSize, sqlStatisticManager.tableSize(tableId));
        assertEquals(1L, sqlStatisticManager.droppedTables.size());

        // After LWM has been changed data in case is removed and we get default value.
        when(catalogManager.activeCatalogVersion(catalogVersionBeforeDrop)).thenReturn(catalogVersionAfterDrop);
        lowWatermarkCapture.getValue()
                .notify(new ChangeLowWatermarkEventParameters(HybridTimestamp.hybridTimestamp(catalogVersionBeforeDrop)));

        assertEquals(DEFAULT_TABLE_SIZE, sqlStatisticManager.tableSize(tableId));
        // After LWM dropped tables are also cleared.
        assertEquals(0L, sqlStatisticManager.droppedTables.size());
    }

    @SuppressWarnings("PMD.UseNotifyAllInsteadOfNotify")
    @Test
    public void checkCacheForNewlyCreatedTable() {
        int tableId = ThreadLocalRandom.current().nextInt();
        long tableSize = 999_888_777L;
        // Preparing:
        when(catalogManager.catalog(anyInt())).thenReturn(mock(Catalog.class));

        ArgumentCaptor<EventListener<CreateTableEventParameters>> tableCreateCapture = ArgumentCaptor.forClass(EventListener.class);
        doNothing().when(catalogManager).listen(eq(CatalogEvent.TABLE_DROP), any());
        doNothing().when(catalogManager).listen(eq(CatalogEvent.TABLE_CREATE), tableCreateCapture.capture());

        when(tableManager.cachedTable(tableId)).thenReturn(tableViewInternal);
        when(tableViewInternal.internalTable()).thenReturn(internalTable);
        when(statAggregator.estimatedSizeWithLastUpdate(List.of(internalTable)))
                .thenReturn(CompletableFuture.completedFuture(Int2ObjectMap.ofEntries(
                        entry(tableId, new PartitionModificationInfo(tableSize, Long.MAX_VALUE)))));

        SqlStatisticManagerImpl sqlStatisticManager =
                new SqlStatisticManagerImpl(tableManager, catalogManager, lowWatermark, commonExecutor, statAggregator);
        sqlStatisticManager.start();

        // Test:
        // it's not created TABLE, so size will be as default value.
        assertEquals(DEFAULT_TABLE_SIZE, sqlStatisticManager.tableSize(tableId));

        CatalogTableDescriptor catalogDescriptor = CatalogTableDescriptor.builder()
                .id(tableId)
                .schemaId(1)
                .primaryKeyIndexId(1)
                .name("")
                .zoneId(1)
                .newColumns(List.of(pkCol))
                .primaryKeyColumns(IntList.of(0))
                .storageProfile("")
                .build();
        tableCreateCapture.getValue().notify(new CreateTableEventParameters(-1, 0, catalogDescriptor));
        // now table is created and size should be actual.
        long timeout = 2 * Math.max(REFRESH_PERIOD, INITIAL_DELAY);

        Awaitility.await().timeout(timeout, TimeUnit.MILLISECONDS).untilAsserted(() ->
                assertEquals(tableSize, sqlStatisticManager.tableSize(tableId))
        );
    }

    @Test
    void statisticUpdatesIfEstimationPartiallyUnavailable() {
        int tableId = ThreadLocalRandom.current().nextInt();
        long tableSize1 = 100_000L;
        long tableSize2 = 500_000L;

        HybridTimestamp time1 = HybridTimestamp.MAX_VALUE.subtractPhysicalTime(1000);
        HybridTimestamp time2 = HybridTimestamp.MAX_VALUE.subtractPhysicalTime(500);
        // Preparing:
        prepareCatalogWithTable(tableId);

        when(statAggregator.estimatedSizeWithLastUpdate(List.of(internalTable))).thenReturn(
                // stale result goes first
                CompletableFuture.completedFuture(Int2ObjectMap.ofEntries(
                        entry(tableId, new PartitionModificationInfo(tableSize1, time1.longValue())))),
                CompletableFuture.failedFuture(new RuntimeException("smth wrong")),
                CompletableFuture.completedFuture(Int2ObjectMap.ofEntries(
                        entry(tableId, new PartitionModificationInfo(tableSize2, time2.longValue()))))
        );

        when(tableManager.cachedTable(tableId)).thenReturn(tableViewInternal);
        when(tableViewInternal.internalTable()).thenReturn(internalTable);

        SqlStatisticManagerImpl sqlStatisticManager =
                new SqlStatisticManagerImpl(tableManager, catalogManager, lowWatermark, commonExecutor, statAggregator);

        sqlStatisticManager.start();
        // tableSize1
        sqlStatisticManager.forceUpdateAll();

        assertEquals(tableSize1, sqlStatisticManager.tableSize(tableId));

        // err call
        sqlStatisticManager.forceUpdateAll();

        assertEquals(tableSize1, sqlStatisticManager.tableSize(tableId));

        // tableSize2
        sqlStatisticManager.forceUpdateAll();

        assertEquals(tableSize2, sqlStatisticManager.tableSize(tableId));

        verify(statAggregator, times(3)).estimatedSizeWithLastUpdate(List.of(internalTable));
    }

    @Test
    void ensureStaleRequestsAreDiscarded() {
        int tableId = ThreadLocalRandom.current().nextInt();
        long tableSize1 = 100_000L;
        long tableSize2 = 500_000L;

        HybridTimestamp time1 = HybridTimestamp.MAX_VALUE.subtractPhysicalTime(1000);
        HybridTimestamp time2 = HybridTimestamp.MAX_VALUE.subtractPhysicalTime(500);
        // Preparing:
        prepareCatalogWithTable(tableId);

        when(tableManager.cachedTable(tableId)).thenReturn(tableViewInternal);
        when(tableViewInternal.internalTable()).thenReturn(internalTable);
        when(statAggregator.estimatedSizeWithLastUpdate(List.of(internalTable))).thenReturn(
                // stale result goes first
                CompletableFuture.completedFuture(Int2ObjectMap.ofEntries(
                        entry(tableId, new PartitionModificationInfo(tableSize1, time1.longValue())))),
                CompletableFuture.completedFuture(Int2ObjectMap.ofEntries(
                        entry(tableId, new PartitionModificationInfo(tableSize2, time2.longValue()))))
        );

        SqlStatisticManagerImpl sqlStatisticManager =
                new SqlStatisticManagerImpl(tableManager, catalogManager, lowWatermark, commonExecutor, statAggregator);
        sqlStatisticManager.start();

        // Test:
        // first call results in non-completed future, therefore default size is expected
        assertEquals(DEFAULT_TABLE_SIZE, sqlStatisticManager.tableSize(tableId));

        // get stale
        sqlStatisticManager.forceUpdateAll();
        // get tableSize2
        sqlStatisticManager.forceUpdateAll();

        long timeout = 2 * Math.max(REFRESH_PERIOD, INITIAL_DELAY);

        Awaitility.await().timeout(timeout, TimeUnit.MILLISECONDS).untilAsserted(() ->
                assertEquals(tableSize2, sqlStatisticManager.tableSize(tableId))
        );

        // Stale result must be discarded.
        assertEquals(tableSize2, sqlStatisticManager.tableSize(tableId));
    }

    private void prepareCatalogWithTable(int tableId) {
        when(catalogManager.earliestCatalogVersion()).thenReturn(1);
        when(catalogManager.latestCatalogVersion()).thenReturn(1);
        Catalog catalog = mock(Catalog.class);
        when(catalogManager.catalog(1)).thenReturn(catalog);
        CatalogTableDescriptor catalogDescriptor = CatalogTableDescriptor.builder()
                .id(tableId)
                .schemaId(1)
                .primaryKeyIndexId(1)
                .name("")
                .zoneId(1)
                .newColumns(List.of(pkCol))
                .primaryKeyColumns(IntList.of(0))
                .storageProfile("")
                .build();
        when(catalog.tables()).thenReturn(List.of(catalogDescriptor));
    }
}
