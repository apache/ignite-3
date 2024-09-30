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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.internal.catalog.CatalogManager;
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

    @Test
    public void checkDefaultTableSize() {
        int tableId = ThreadLocalRandom.current().nextInt();
        when(tableManager.cachedTable(tableId)).thenReturn(null);

        SqlStatisticManagerImpl sqlStatisticManager = new SqlStatisticManagerImpl(tableManager, catalogManager);

        assertEquals(1_000_000L, sqlStatisticManager.tableSize(tableId));
    }

    @Test
    public void checkMinimumTableSize() {
        int tableId = ThreadLocalRandom.current().nextInt();
        when(tableManager.cachedTable(tableId)).thenReturn(tableViewInternal);
        when(tableViewInternal.internalTable()).thenReturn(internalTable);
        when(internalTable.estimatedSize()).thenReturn(CompletableFuture.completedFuture(10L));

        SqlStatisticManagerImpl sqlStatisticManager = new SqlStatisticManagerImpl(tableManager, catalogManager);

        assertEquals(1_000L, sqlStatisticManager.tableSize(tableId));
    }

    @Test
    public void checkTableSize() {
        int tableId = ThreadLocalRandom.current().nextInt();
        long tableSize = 999_888_777L;
        when(tableManager.cachedTable(tableId)).thenReturn(tableViewInternal);
        when(tableViewInternal.internalTable()).thenReturn(internalTable);
        when(internalTable.estimatedSize()).thenReturn(CompletableFuture.completedFuture(tableSize));

        SqlStatisticManagerImpl sqlStatisticManager = new SqlStatisticManagerImpl(tableManager, catalogManager);

        assertEquals(tableSize, sqlStatisticManager.tableSize(tableId));
        // The second time we should obtain the same value from a cache.
        assertEquals(tableSize, sqlStatisticManager.tableSize(tableId));
        assertEquals(tableSize, sqlStatisticManager.tableSize(tableId));
        verify(tableManager, times(1)).cachedTable(tableId);
    }

    @Test
    public void checkModifyTableSize() {
        int tableId = ThreadLocalRandom.current().nextInt();
        long tableSize1 = 999_888_777L;
        long tableSize2 = 111_222_333L;
        when(tableManager.cachedTable(tableId)).thenReturn(tableViewInternal);
        when(tableViewInternal.internalTable()).thenReturn(internalTable);
        when(internalTable.estimatedSize()).thenReturn(
                CompletableFuture.completedFuture(tableSize1),
                CompletableFuture.completedFuture(tableSize2));

        SqlStatisticManagerImpl sqlStatisticManager = new SqlStatisticManagerImpl(tableManager, catalogManager);

        assertEquals(tableSize1, sqlStatisticManager.tableSize(tableId));
        // The second time we should obtain the same value from a cache.
        assertEquals(tableSize1, sqlStatisticManager.tableSize(tableId));

        // Allow to refresh value.
        sqlStatisticManager.setThresholdTimeToPostponeUpdateMs(0);
        // Now we need obtain a fresh value of table size.
        assertEquals(tableSize2, sqlStatisticManager.tableSize(tableId));
        verify(tableManager, times(2)).cachedTable(tableId);
    }
}
