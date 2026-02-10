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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntList;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CreateTableEventParameters;
import org.apache.ignite.internal.catalog.events.DropTableEventParameters;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.lowwatermark.event.ChangeLowWatermarkEventParameters;
import org.apache.ignite.internal.lowwatermark.event.LowWatermarkEvent;
import org.apache.ignite.internal.sql.configuration.distributed.StatisticsConfiguration;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mock.Strictness;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests of SqlStatisticManagerImpl.
 */
@ExtendWith(MockitoExtension.class)
@ExtendWith(ConfigurationExtension.class)
class SqlStatisticManagerImplTest extends BaseIgniteAbstractTest {

    // Should be in-sync with statisticsConfiguration
    private static final int UPDATE_INTERVAL_SECONDS = 5;

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

    @Mock(strictness = Strictness.LENIENT)
    private StatisticAggregatorImpl statAggregator;

    @Mock(strictness = Strictness.LENIENT)
    private ScheduledExecutorService scheduledExecutorService;

    @Mock(strictness = Strictness.LENIENT)
    private ScheduledFuture<Void> taskFuture;

    @InjectConfiguration("mock.autoRefresh.staleRowsCheckIntervalSeconds=5")
    private StatisticsConfiguration statisticsConfiguration;

    private static final CatalogTableColumnDescriptor pkCol =
            new CatalogTableColumnDescriptor("pkCol", ColumnType.STRING, false, 0, 0, 10, null);

    private final ArrayDeque<Runnable> scheduledTasks = new ArrayDeque<>();

    @Test
    public void checkDefaultTableSize() {
        int tableId = ThreadLocalRandom.current().nextInt();
        // Preparing:
        prepareEmptyCatalog();
        prepareTaskScheduler();

        when(statAggregator.estimatedSizeWithLastUpdate(List.of()))
                .thenReturn(CompletableFuture.completedFuture(Int2ObjectMaps.emptyMap()));

        SqlStatisticManagerImpl sqlStatisticManager = newSqlStatisticsManager();
        sqlStatisticManager.start();

        runScheduledTasks();

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
        prepareTaskScheduler();

        when(tableManager.cachedTable(tableId)).thenReturn(tableViewInternal);
        when(tableViewInternal.internalTable()).thenReturn(internalTable);
        when(statAggregator.estimatedSizeWithLastUpdate(List.of(internalTable)))
                .thenReturn(CompletableFuture.completedFuture(Int2ObjectMap.ofEntries(
                        entry(tableId, new PartitionModificationInfo(tableSize, Long.MAX_VALUE)))));

        SqlStatisticManagerImpl sqlStatisticManager = newSqlStatisticsManager();
        sqlStatisticManager.start();

        runScheduledTasks();
        assertEquals(tableSize, sqlStatisticManager.tableSize(tableId));

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
        prepareTaskScheduler();

        when(tableManager.cachedTable(tableId)).thenReturn(tableViewInternal);
        when(tableViewInternal.internalTable()).thenReturn(internalTable);

        when(statAggregator.estimatedSizeWithLastUpdate(List.of(internalTable)))
                .thenReturn(
                        CompletableFuture.completedFuture(Int2ObjectMap.ofEntries(
                                entry(tableId, new PartitionModificationInfo(tableSize1, time1.longValue())))),
                        CompletableFuture.completedFuture(Int2ObjectMap.ofEntries(
                                entry(tableId, new PartitionModificationInfo(tableSize2, time2.longValue()))))
                );

        SqlStatisticManagerImpl sqlStatisticManager = newSqlStatisticsManager();
        sqlStatisticManager.start();

        runScheduledTasks();

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

        prepareTaskScheduler();

        SqlStatisticManagerImpl sqlStatisticManager = newSqlStatisticsManager();
        sqlStatisticManager.start();

        runScheduledTasks();

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
        prepareTaskScheduler();

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

        SqlStatisticManagerImpl sqlStatisticManager = newSqlStatisticsManager();
        sqlStatisticManager.start();

        runScheduledTasks();
        assertEquals(tableSize, sqlStatisticManager.tableSize(tableId));

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

        when(statAggregator.estimatedSizeWithLastUpdate(List.of()))
                .thenReturn(CompletableFuture.completedFuture(Int2ObjectMaps.emptyMap()));

        when(statAggregator.estimatedSizeWithLastUpdate(List.of(internalTable)))
                .thenReturn(CompletableFuture.completedFuture(Int2ObjectMap.ofEntries(
                        entry(tableId, new PartitionModificationInfo(tableSize, Long.MAX_VALUE)))));

        prepareTaskScheduler();

        SqlStatisticManagerImpl sqlStatisticManager = newSqlStatisticsManager();
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

        runScheduledTasks();
        assertEquals(tableSize, sqlStatisticManager.tableSize(tableId));
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

        prepareTaskScheduler();

        SqlStatisticManagerImpl sqlStatisticManager = newSqlStatisticsManager();
        sqlStatisticManager.start();

        // tableSize1 - scheduled by the sqlStatisticManager start
        sqlStatisticManager.forceUpdateAll();
        assertEquals(tableSize1, sqlStatisticManager.tableSize(tableId));

        // Error was handled after the first call.
        runScheduledTasks();

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

        prepareTaskScheduler();

        SqlStatisticManagerImpl sqlStatisticManager = newSqlStatisticsManager();
        sqlStatisticManager.start();

        // Test:
        // first call results in non-completed future, therefore default size is expected
        assertEquals(DEFAULT_TABLE_SIZE, sqlStatisticManager.tableSize(tableId));

        // get stale
        sqlStatisticManager.forceUpdateAll();
        // get tableSize2
        sqlStatisticManager.forceUpdateAll();

        runScheduledTasks();
        assertEquals(tableSize2, sqlStatisticManager.tableSize(tableId));

        // Stale result must be discarded.
        assertEquals(tableSize2, sqlStatisticManager.tableSize(tableId));
    }

    @Test
    public void updateSchedulingInterval() {
        prepareEmptyCatalog();
        prepareTaskScheduler();

        when(statAggregator.estimatedSizeWithLastUpdate(List.of()))
                .thenReturn(CompletableFuture.completedFuture(Int2ObjectMaps.emptyMap()));

        ConfigurationValue<Integer> configurationValue = statisticsConfiguration.autoRefresh().staleRowsCheckIntervalSeconds();
        configurationValue.update(UPDATE_INTERVAL_SECONDS).join();

        SqlStatisticManagerImpl sqlStatisticManager = newSqlStatisticsManager();
        sqlStatisticManager.start();

        configurationValue.update(UPDATE_INTERVAL_SECONDS + 1).join();
        configurationValue.update(UPDATE_INTERVAL_SECONDS - 1).join();
        // Same value no effect.
        configurationValue.update(UPDATE_INTERVAL_SECONDS - 1).join();

        sqlStatisticManager.stop();

        InOrder inOrder = Mockito.inOrder(scheduledExecutorService, taskFuture);
        // Start
        inOrder.verify(scheduledExecutorService).scheduleAtFixedRate(any(Runnable.class), eq(0L), eq(5L), eq(TimeUnit.SECONDS));
        // Update 1
        inOrder.verify(taskFuture).cancel(anyBoolean());
        inOrder.verify(scheduledExecutorService).scheduleAtFixedRate(any(Runnable.class), eq(6L), eq(6L), eq(TimeUnit.SECONDS));
        // Update 2
        inOrder.verify(taskFuture).cancel(anyBoolean());
        inOrder.verify(scheduledExecutorService).scheduleAtFixedRate(any(Runnable.class), eq(4L), eq(4L), eq(TimeUnit.SECONDS));
        // Stopping cancels the task.
        inOrder.verify(taskFuture).cancel(anyBoolean());
    }

    @Test
    public void taskDoesNotTerminateAfterAnException() {
        int tableId = ThreadLocalRandom.current().nextInt();
        long tableSize = 1_000L;
        // Preparing:
        prepareCatalogWithTable(tableId);
        prepareTaskScheduler();

        when(tableManager.cachedTable(tableId)).thenReturn(tableViewInternal);
        when(tableViewInternal.internalTable()).thenReturn(internalTable);

        // The first invocation fails, subsequent succeed.
        AtomicBoolean failTheFirstTime = new AtomicBoolean(true);
        doAnswer(invocation -> {
            if (failTheFirstTime.compareAndSet(true, false)) {
                throw new RuntimeException("Unexpected error");
            } else {
                return CompletableFuture.completedFuture(Int2ObjectMap.ofEntries(
                        entry(tableId, new PartitionModificationInfo(tableSize, Long.MAX_VALUE))));
            }
        }).when(statAggregator).estimatedSizeWithLastUpdate(List.of(internalTable));

        int intervalSeconds = 2;
        SqlStatisticManagerImpl sqlStatisticManager = newSqlStatisticsManager(intervalSeconds);
        sqlStatisticManager.start();

        // The first run fails
        runScheduledTasks();
        assertEquals(1, sqlStatisticManager.tableSize(tableId));

        runScheduledTasks();
        assertEquals(tableSize, sqlStatisticManager.tableSize(tableId));
    }

    private SqlStatisticManagerImpl newSqlStatisticsManager() {
        return newSqlStatisticsManager(UPDATE_INTERVAL_SECONDS);
    }

    private SqlStatisticManagerImpl newSqlStatisticsManager(int interval) {
        ConfigurationValue<Integer> checkInterval = statisticsConfiguration.autoRefresh().staleRowsCheckIntervalSeconds();
        checkInterval.update(interval).join();

        return new SqlStatisticManagerImpl(
                tableManager,
                catalogManager,
                lowWatermark,
                scheduledExecutorService,
                statAggregator,
                checkInterval
        );
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

    private void prepareEmptyCatalog() {
        when(catalogManager.earliestCatalogVersion()).thenReturn(1);
        when(catalogManager.latestCatalogVersion()).thenReturn(1);
        Catalog catalog = mock(Catalog.class);
        when(catalogManager.catalog(1)).thenReturn(catalog);
        when(catalog.tables()).thenReturn(List.of());
    }

    private void prepareTaskScheduler() {
        doAnswer(invocation -> {
            Runnable r = invocation.getArgument(0);
            scheduledTasks.add(r);
            return taskFuture;
        }).when(scheduledExecutorService).scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));

        doAnswer(invocation -> {
            scheduledTasks.poll();
            return true;
        }).when(taskFuture).cancel(anyBoolean());
    }

    private void runScheduledTasks() {
        for (Runnable r : scheduledTasks) {
            r.run();
        }
    }
}
