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

import it.unimi.dsi.fastutil.longs.LongObjectImmutablePair;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManager;
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

    @InjectExecutorService
    private ScheduledExecutorService commonExecutor;

    @Test
    public void checkDefaultTableSize() {
        int tableId = ThreadLocalRandom.current().nextInt();
        // Preparing:
        when(catalogManager.catalog(anyInt())).thenReturn(mock(Catalog.class));

        SqlStatisticManagerImpl sqlStatisticManager =
                new SqlStatisticManagerImpl(tableManager, catalogManager, lowWatermark, commonExecutor);
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
        when(internalTable.estimatedSizeWithLastUpdate())
                .thenReturn(CompletableFuture.completedFuture(LongObjectImmutablePair.of(tableSize, HybridTimestamp.MAX_VALUE)));

        SqlStatisticManagerImpl sqlStatisticManager =
                new SqlStatisticManagerImpl(tableManager, catalogManager, lowWatermark, commonExecutor);
        sqlStatisticManager.start();

        long timeout = 2 * Math.max(REFRESH_PERIOD, INITIAL_DELAY);

        Awaitility.await().timeout(timeout, TimeUnit.MILLISECONDS).untilAsserted(() ->
                assertEquals(tableSize, sqlStatisticManager.tableSize(tableId))
        );

        // Second time we should obtain the same value from a cache.
        assertEquals(tableSize, sqlStatisticManager.tableSize(tableId));

        verify(internalTable, times(1)).estimatedSizeWithLastUpdate();
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

        when(internalTable.estimatedSizeWithLastUpdate())
                .thenReturn(
                        CompletableFuture.completedFuture(LongObjectImmutablePair.of(tableSize1, time1)),
                        CompletableFuture.completedFuture(LongObjectImmutablePair.of(tableSize2, time2))
                );

        SqlStatisticManagerImpl sqlStatisticManager =
                new SqlStatisticManagerImpl(tableManager, catalogManager, lowWatermark, commonExecutor);
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
        assertEquals(time2, sqlStatisticManager.tableSizeMap.get(tableId).getTimestamp());

        verify(internalTable, times(2)).estimatedSizeWithLastUpdate();
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
            catalogDescriptors.add(new CatalogTableDescriptor(catalogVersion, 1, 1, "", 1, List.of(), List.of(), null, ""));
            catalogDescriptors.add(new CatalogTableDescriptor(catalogVersion + 1, 1, 1, "", 1, List.of(), List.of(), null, ""));

            Catalog catalog = mock(Catalog.class);
            when(catalogManager.catalog(catalogVersion)).thenReturn(catalog);
            when(catalog.tables()).thenReturn(catalogDescriptors);
        }

        when(tableManager.cachedTable(anyInt())).thenReturn(tableViewInternal);
        when(tableViewInternal.internalTable()).thenReturn(internalTable);
        when(internalTable.estimatedSizeWithLastUpdate())
                .thenReturn(CompletableFuture.completedFuture(LongObjectImmutablePair.of(tableSize, HybridTimestamp.MAX_VALUE)));

        SqlStatisticManagerImpl sqlStatisticManager = new SqlStatisticManagerImpl(tableManager, catalogManager, lowWatermark, commonExecutor);
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
        when(internalTable.estimatedSizeWithLastUpdate())
                .thenReturn(CompletableFuture.completedFuture(LongObjectImmutablePair.of(tableSize, HybridTimestamp.MAX_VALUE)));

        SqlStatisticManagerImpl sqlStatisticManager =
                new SqlStatisticManagerImpl(tableManager, catalogManager, lowWatermark, commonExecutor);
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

        // After LWM has been changed data in case is removed and we get default value.
        when(catalogManager.activeCatalogVersion(catalogVersionBeforeDrop)).thenReturn(catalogVersionAfterDrop);
        lowWatermarkCapture.getValue()
                .notify(new ChangeLowWatermarkEventParameters(HybridTimestamp.hybridTimestamp(catalogVersionBeforeDrop)));

        assertEquals(DEFAULT_TABLE_SIZE, sqlStatisticManager.tableSize(tableId));
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
        when(internalTable.estimatedSizeWithLastUpdate())
                .thenReturn(CompletableFuture.completedFuture(LongObjectImmutablePair.of(tableSize, HybridTimestamp.MAX_VALUE)));

        SqlStatisticManagerImpl sqlStatisticManager =
                new SqlStatisticManagerImpl(tableManager, catalogManager, lowWatermark, commonExecutor);
        sqlStatisticManager.start();

        // Test:
        // it's not created TABLE, so size will be as default value.
        assertEquals(DEFAULT_TABLE_SIZE, sqlStatisticManager.tableSize(tableId));

        CatalogTableDescriptor catalogDescriptor = new CatalogTableDescriptor(tableId, 1, 1, "", 1, List.of(), List.of(), null, "");
        tableCreateCapture.getValue().notify(new CreateTableEventParameters(-1, 0, catalogDescriptor));
        // now table is created and size should be actual.
        long timeout = 2 * Math.max(REFRESH_PERIOD, INITIAL_DELAY);

        Awaitility.await().timeout(timeout, TimeUnit.MILLISECONDS).untilAsserted(() ->
                assertEquals(tableSize, sqlStatisticManager.tableSize(tableId))
        );
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

        CompletableFuture<LongObjectImmutablePair<HybridTimestamp>> staleResultFuture = new CompletableFuture<>();
        when(tableManager.cachedTable(tableId)).thenReturn(tableViewInternal);
        when(tableViewInternal.internalTable()).thenReturn(internalTable);
        when(internalTable.estimatedSizeWithLastUpdate()).thenReturn(
                 staleResultFuture, // stale result goes first
                 CompletableFuture.completedFuture(LongObjectImmutablePair.of(tableSize2, time2))
        );

        SqlStatisticManagerImpl sqlStatisticManager =
                new SqlStatisticManagerImpl(tableManager, catalogManager, lowWatermark, commonExecutor);
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

        staleResultFuture.complete(LongObjectImmutablePair.of(tableSize1, time1));

        // Stale result must be discarded.
        assertEquals(tableSize2, sqlStatisticManager.tableSize(tableId));
    }

    private void prepareCatalogWithTable(int tableId) {
        when(catalogManager.earliestCatalogVersion()).thenReturn(1);
        when(catalogManager.latestCatalogVersion()).thenReturn(1);
        Catalog catalog = mock(Catalog.class);
        when(catalogManager.catalog(1)).thenReturn(catalog);
        CatalogTableDescriptor catalogDescriptor = new CatalogTableDescriptor(tableId, 1, 1, "", 1, List.of(), List.of(), null, "");
        when(catalog.tables()).thenReturn(List.of(catalogDescriptor));
    }
}
