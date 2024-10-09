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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests of SqlStatisticManagerImpl.
 */
@ExtendWith(MockitoExtension.class)
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

    @Test
    public void checkDefaultTableSize() {
        int tableId = ThreadLocalRandom.current().nextInt();
        when(tableManager.cachedTable(tableId)).thenReturn(null);

        SqlStatisticManagerImpl sqlStatisticManager = new SqlStatisticManagerImpl(tableManager, catalogManager, lowWatermark);

        // return default value for unknown table.
        assertEquals(1_000_000L, sqlStatisticManager.tableSize(tableId));
    }

    @Test
    public void checkMinimumTableSize() {
        int tableId = ThreadLocalRandom.current().nextInt();
        prepareMocksForStart(tableId);

        when(tableManager.cachedTable(tableId)).thenReturn(tableViewInternal);
        when(tableViewInternal.internalTable()).thenReturn(internalTable);
        when(internalTable.estimatedSize()).thenReturn(CompletableFuture.completedFuture(10L));

        SqlStatisticManagerImpl sqlStatisticManager = new SqlStatisticManagerImpl(tableManager, catalogManager, lowWatermark);
        sqlStatisticManager.start();

        // even table size is 10 it should return default minimum size
        assertEquals(1_000L, sqlStatisticManager.tableSize(tableId));
    }

    @Test
    public void checkTableSize() {
        int tableId = ThreadLocalRandom.current().nextInt();
        long tableSize = 999_888_777L;
        prepareMocksForStart(tableId);

        when(tableManager.cachedTable(tableId)).thenReturn(tableViewInternal);
        when(tableViewInternal.internalTable()).thenReturn(internalTable);
        when(internalTable.estimatedSize()).thenReturn(CompletableFuture.completedFuture(tableSize));

        SqlStatisticManagerImpl sqlStatisticManager = new SqlStatisticManagerImpl(tableManager, catalogManager, lowWatermark);
        sqlStatisticManager.start();

        assertEquals(tableSize, sqlStatisticManager.tableSize(tableId));
        // The second time we should obtain the same value from a cache.
        assertEquals(tableSize, sqlStatisticManager.tableSize(tableId));
        assertEquals(tableSize, sqlStatisticManager.tableSize(tableId));
        verify(internalTable, times(1)).estimatedSize();
    }

    @Test
    public void checkModifyTableSize() {
        int tableId = ThreadLocalRandom.current().nextInt();
        long tableSize1 = 999_888_777L;
        long tableSize2 = 111_222_333L;

        prepareMocksForStart(tableId);

        when(tableManager.cachedTable(tableId)).thenReturn(tableViewInternal);
        when(tableViewInternal.internalTable()).thenReturn(internalTable);
        when(internalTable.estimatedSize()).thenReturn(
                CompletableFuture.completedFuture(tableSize1),
                CompletableFuture.completedFuture(tableSize2));

        SqlStatisticManagerImpl sqlStatisticManager = new SqlStatisticManagerImpl(tableManager, catalogManager, lowWatermark);
        sqlStatisticManager.start();

        assertEquals(tableSize1, sqlStatisticManager.tableSize(tableId));
        // The second time we should obtain the same value from a cache.
        assertEquals(tableSize1, sqlStatisticManager.tableSize(tableId));

        // Allow to refresh value.
        sqlStatisticManager.setThresholdTimeToPostponeUpdateMs(0);
        // Now we need obtain a fresh value of table size.
        assertEquals(tableSize2, sqlStatisticManager.tableSize(tableId));
        verify(internalTable, times(2)).estimatedSize();
    }

    @Test
    public void checkLoadAllTablesOnStart() {
        int minimumCatalogVersion = 1;
        int maximumCatalogVersion = 10;
        when(catalogManager.earliestCatalogVersion()).thenReturn(minimumCatalogVersion);
        when(catalogManager.latestCatalogVersion()).thenReturn(maximumCatalogVersion);
        for (int catalogVersion = minimumCatalogVersion; catalogVersion <= maximumCatalogVersion; catalogVersion++) {
            List<CatalogTableDescriptor> catalogDescriptors = new ArrayList<>();
            catalogDescriptors.add(new CatalogTableDescriptor(catalogVersion, 1, 1, "", 1, List.of(), List.of(), null, ""));
            catalogDescriptors.add(new CatalogTableDescriptor(catalogVersion + 1, 1, 1, "", 1, List.of(), List.of(), null, ""));

            when(catalogManager.tables(catalogVersion)).thenReturn(catalogDescriptors);
        }

        when(tableManager.cachedTable(anyInt())).thenReturn(tableViewInternal);
        when(tableViewInternal.internalTable()).thenReturn(internalTable);
        when(internalTable.estimatedSize()).thenReturn(CompletableFuture.completedFuture(99999L));

        SqlStatisticManagerImpl sqlStatisticManager = new SqlStatisticManagerImpl(tableManager, catalogManager, lowWatermark);
        sqlStatisticManager.start();

        // For known tables we got calculated table size.
        for (int i = minimumCatalogVersion; i <= maximumCatalogVersion; i++) {
            assertEquals(99999L, sqlStatisticManager.tableSize(i));
            assertEquals(99999L, sqlStatisticManager.tableSize(i + 1));
        }

        // For unknown tables we got default size.
        for (int i = maximumCatalogVersion + 2; i <= maximumCatalogVersion + 10; i++) {
            assertEquals(1_000_000L, sqlStatisticManager.tableSize(i));
        }


    }

    private void prepareMocksForStart(int tableId) {
        when(catalogManager.earliestCatalogVersion()).thenReturn(1);
        when(catalogManager.latestCatalogVersion()).thenReturn(1);
        CatalogTableDescriptor catalogDescriptor = new CatalogTableDescriptor(tableId, 1, 1, "", 1, List.of(), List.of(), null, "");
        when(catalogManager.tables(1)).thenReturn(List.of(catalogDescriptor));
    }
}
