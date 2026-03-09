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

package org.apache.ignite.internal.storage.pagememory;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.pagememory.mv.PersistentPageMemoryMvPartitionStorage;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

class PersistentDataRegionMetricsCalculatorTest extends BaseIgniteAbstractTest {
    private static final int PAGE_SIZE = 4096;
    private static final int TABLE_ID = 1;
    private static final int MAX_PARTITIONS = 10;

    private final PersistentDataRegionMetricsCalculator calculator = new PersistentDataRegionMetricsCalculator(PAGE_SIZE);

    @Test
    void testTotalAllocatedSizeEmptyTable() {
        PersistentPageMemoryTableStorage table = createTable();

        assertThat(calculator.totalAllocatedSize(table), is(0L));
    }

    @Test
    void testTotalAllocatedSizeSinglePartition() {
        PersistentPageMemoryTableStorage table = createTable();
        addPartition(table, 0, 10, 3, 500L);

        assertThat(calculator.totalAllocatedSize(table), is(10L * PAGE_SIZE));
    }

    @Test
    void testTotalAllocatedSizeMultiplePartitions() {
        PersistentPageMemoryTableStorage table = createTable();
        addPartition(table, 0, 5, 1, 0L);
        addPartition(table, 1, 8, 3, 0L);

        assertThat(calculator.totalAllocatedSize(table), is(13L * PAGE_SIZE));
    }

    @Test
    void testTotalAllocatedSizeMultipleTables() {
        PersistentPageMemoryTableStorage table1 = createTable(1);
        addPartition(table1, 0, 5, 0, 0L);

        PersistentPageMemoryTableStorage table2 = createTable(2);
        addPartition(table2, 0, 3, 1, 0L);

        assertThat(calculator.totalAllocatedSize(List.of(table1, table2)), is(8L * PAGE_SIZE));
    }

    @Test
    void testTotalUsedSizeEmptyTable() {
        PersistentPageMemoryTableStorage table = createTable();

        assertThat(calculator.totalUsedSize(table), is(0L));
    }

    @Test
    void testTotalUsedSizeNoEmptyPages() {
        PersistentPageMemoryTableStorage table = createTable();
        addPartition(table, 0, 10, 0, 0L);

        assertThat(calculator.totalUsedSize(table), is(10L * PAGE_SIZE));
    }

    @Test
    void testTotalUsedSizeWithEmptyPages() {
        PersistentPageMemoryTableStorage table = createTable();
        addPartition(table, 0, 10, 3, 500L);

        // Used pages = total - empty = 10 - 3 = 7
        assertThat(calculator.totalUsedSize(table), is(7L * PAGE_SIZE));
    }

    @Test
    void testTotalUsedSizeMultiplePartitions() {
        PersistentPageMemoryTableStorage table = createTable();
        addPartition(table, 0, 5, 1, 0L);
        addPartition(table, 1, 8, 3, 0L);

        // Used pages: (5-1) + (8-3) = 4 + 5 = 9
        assertThat(calculator.totalUsedSize(table), is(9L * PAGE_SIZE));
    }

    @Test
    void testTotalUsedSizeMultipleTables() {
        PersistentPageMemoryTableStorage table1 = createTable(1);
        addPartition(table1, 0, 5, 0, 0L);

        PersistentPageMemoryTableStorage table2 = createTable(2);
        addPartition(table2, 0, 3, 1, 0L);

        // Used pages: 5 + (3-1) = 5 + 2 = 7
        assertThat(calculator.totalUsedSize(List.of(table1, table2)), is(7L * PAGE_SIZE));
    }

    @Test
    void testTotalEmptySizeEmptyTable() {
        PersistentPageMemoryTableStorage table = createTable();

        assertThat(calculator.totalEmptySize(table), is(0L));
    }

    @Test
    void testTotalEmptySizeNoEmptyPages() {
        PersistentPageMemoryTableStorage table = createTable();
        addPartition(table, 0, 10, 0, 0L);

        assertThat(calculator.totalEmptySize(table), is(0L));
    }

    @Test
    void testTotalEmptySizeWithEmptyPages() {
        PersistentPageMemoryTableStorage table = createTable();
        addPartition(table, 0, 10, 3, 500L);

        assertThat(calculator.totalEmptySize(table), is(3L * PAGE_SIZE));
    }

    @Test
    void testTotalEmptySizeMultiplePartitions() {
        PersistentPageMemoryTableStorage table = createTable();
        addPartition(table, 0, 5, 1, 0L);
        addPartition(table, 1, 8, 3, 0L);

        assertThat(calculator.totalEmptySize(table), is(4L * PAGE_SIZE));
    }

    @Test
    void testTotalEmptySizeMultipleTables() {
        PersistentPageMemoryTableStorage table1 = createTable(1);
        addPartition(table1, 0, 5, 2, 0L);

        PersistentPageMemoryTableStorage table2 = createTable(2);
        addPartition(table2, 0, 3, 1, 0L);

        assertThat(calculator.totalEmptySize(List.of(table1, table2)), is(3L * PAGE_SIZE));
    }

    @Test
    void testAllocatedEqualsUsedPlusEmpty() {
        PersistentPageMemoryTableStorage table = createTable();
        addPartition(table, 0, 10, 3, 500L);
        addPartition(table, 1, 7, 2, 200L);

        long allocated = calculator.totalAllocatedSize(table);
        long used = calculator.totalUsedSize(table);
        long empty = calculator.totalEmptySize(table);

        assertThat(allocated, is(used + empty));
    }

    @Test
    void testTotalDataSizeEmptyTable() {
        PersistentPageMemoryTableStorage table = createTable();

        assertThat(calculator.totalDataSize(table), is(0L));
    }

    @Test
    void testTotalDataSizeFullPagesNoFreeSpace() {
        PersistentPageMemoryTableStorage table = createTable();
        addPartition(table, 0, 10, 0, 0L);

        // Data occupies all used page space
        assertThat(calculator.totalDataSize(table), is(10L * PAGE_SIZE));
    }

    @Test
    void testTotalDataSizeWithEmptyPagesNoFreeSpace() {
        PersistentPageMemoryTableStorage table = createTable();
        addPartition(table, 0, 10, 3, 0L);

        // Only non-empty pages count; no fragmentation within those pages
        assertThat(calculator.totalDataSize(table), is(7L * PAGE_SIZE));
    }

    @Test
    void testTotalDataSizeWithFreeSpaceInUsedPages() {
        PersistentPageMemoryTableStorage table = createTable();
        addPartition(table, 0, 10, 2, 1500L);

        // Used pages: 10 - 2 = 8. Data = 8 * pageSize - 1500
        assertThat(calculator.totalDataSize(table), is(8L * PAGE_SIZE - 1500L));
    }

    @Test
    void testTotalDataSizeMultiplePartitions() {
        PersistentPageMemoryTableStorage table = createTable();
        addPartition(table, 0, 5, 1, 500L);
        addPartition(table, 1, 8, 3, 1000L);

        // Partition 0: (5-1)*pageSize - 500 = 4*pageSize - 500
        // Partition 1: (8-3)*pageSize - 1000 = 5*pageSize - 1000
        long expected = 4L * PAGE_SIZE - 500L + 5L * PAGE_SIZE - 1000L;
        assertThat(calculator.totalDataSize(table), is(expected));
    }

    @Test
    void testTotalDataSizeMultipleTables() {
        PersistentPageMemoryTableStorage table1 = createTable(1);
        addPartition(table1, 0, 5, 0, 0L);

        PersistentPageMemoryTableStorage table2 = createTable(2);
        addPartition(table2, 0, 3, 1, 200L);

        // Table1: 5*pageSize - 0 = 5*pageSize
        // Table2: (3-1)*pageSize - 200 = 2*pageSize - 200
        long expected = 5L * PAGE_SIZE + 2L * PAGE_SIZE - 200L;
        assertThat(calculator.totalDataSize(List.of(table1, table2)), is(expected));
    }

    @Test
    void testPagesFillFactorEmptyTableReturnsZero() {
        PersistentPageMemoryTableStorage table = createTable();

        assertThat(calculator.pagesFillFactor(table), is(0.0));
        assertThat(calculator.pagesFillFactor(List.of(table)), is(0.0));
    }

    @Test
    void testPagesFillFactorAllPagesEmptyReturnsZero() {
        PersistentPageMemoryTableStorage table = createTable();
        // All pages are in the empty list, no used pages
        addPartition(table, 0, 5, 5, 0L);

        assertThat(calculator.pagesFillFactor(table), is(0.0));
    }

    @Test
    void testPagesFillFactorUsedPagesNoFreeSpaceReturnsOne() {
        PersistentPageMemoryTableStorage table = createTable();
        addPartition(table, 0, 10, 0, 0L);

        assertThat(calculator.pagesFillFactor(table), is(1.0));
        assertThat(calculator.pagesFillFactor(List.of(table)), is(1.0));
    }

    @Test
    void testPagesFillFactorUsedPagesWithFreeSpace() {
        PersistentPageMemoryTableStorage table = createTable();
        // 8 used pages, 2048 bytes free space
        addPartition(table, 0, 8, 0, 2048L);

        long usedBytes = 8L * PAGE_SIZE;
        double expected = (double) (usedBytes - 2048L) / usedBytes;
        assertThat(calculator.pagesFillFactor(table), closeTo(expected, 1.0e-10));
    }

    @Test
    void testPagesFillFactorWithEmptyAndUsedPages() {
        PersistentPageMemoryTableStorage table = createTable();
        // 10 pages: 3 empty, 7 used with 1000 bytes free
        addPartition(table, 0, 10, 3, 1000L);

        long usedBytes = 7L * PAGE_SIZE;
        double expected = (double) (usedBytes - 1000L) / usedBytes;
        assertThat(calculator.pagesFillFactor(table), closeTo(expected, 1.0e-10));
        assertThat(calculator.pagesFillFactor(List.of(table)), closeTo(expected, 1.0e-10));
    }

    @Test
    void testPagesFillFactorMultiplePartitions() {
        PersistentPageMemoryTableStorage table = createTable();
        addPartition(table, 0, 5, 1, 500L);
        addPartition(table, 1, 8, 3, 1000L);

        // Total used pages: (5-1) + (8-3) = 4 + 5 = 9
        // Total used bytes: 9 * pageSize
        // Total free space: 500 + 1000 = 1500
        long usedBytes = 9L * PAGE_SIZE;
        long freeBytes = 1500L;
        double expected = (double) (usedBytes - freeBytes) / usedBytes;
        assertThat(calculator.pagesFillFactor(table), closeTo(expected, 1.0e-10));
    }

    @Test
    void testPagesFillFactorMultipleTables() {
        PersistentPageMemoryTableStorage table1 = createTable(1);
        addPartition(table1, 0, 5, 0, 500L);

        PersistentPageMemoryTableStorage table2 = createTable(2);
        addPartition(table2, 0, 3, 1, 200L);

        // Used pages: 5 + (3-1) = 7; used bytes: 7 * pageSize; free: 500 + 200 = 700
        long usedBytes = 7L * PAGE_SIZE;
        long freeBytes = 700L;
        double expected = (double) (usedBytes - freeBytes) / usedBytes;
        assertThat(calculator.pagesFillFactor(List.of(table1, table2)), closeTo(expected, 1.0e-10));
    }

    @Test
    void testSingleTableAndCollectionOfOneGiveSameResults() {
        PersistentPageMemoryTableStorage table = createTable();
        addPartition(table, 0, 10, 3, 1500L);
        addPartition(table, 1, 7, 2, 800L);

        assertThat(calculator.totalAllocatedSize(table), is(calculator.totalAllocatedSize(List.of(table))));
        assertThat(calculator.totalUsedSize(table), is(calculator.totalUsedSize(List.of(table))));
        assertThat(calculator.totalEmptySize(table), is(calculator.totalEmptySize(List.of(table))));
        assertThat(calculator.totalDataSize(table), is(calculator.totalDataSize(List.of(table))));
        assertThat(calculator.pagesFillFactor(table), is(calculator.pagesFillFactor(List.of(table))));
    }

    @Test
    void testEmptyCollectionReturnsZeroForAllMetrics() {
        assertThat(calculator.totalAllocatedSize(List.of()), is(0L));
        assertThat(calculator.totalUsedSize(List.of()), is(0L));
        assertThat(calculator.totalEmptySize(List.of()), is(0L));
        assertThat(calculator.totalDataSize(List.of()), is(0L));
        assertThat(calculator.pagesFillFactor(List.of()), is(0.0));
    }

    private static PersistentPageMemoryTableStorage createTable() {
        return createTable(TABLE_ID);
    }

    @SuppressWarnings("DataFlowIssue")
    private static PersistentPageMemoryTableStorage createTable(int tableId) {
        return new PersistentPageMemoryTableStorage(
                new StorageTableDescriptor(tableId, MAX_PARTITIONS, "default"),
                id -> null,
                null,
                null,
                null,
                null,
                null
        );
    }

    private static void addPartition(
            PersistentPageMemoryTableStorage table,
            int partitionId,
            int pageCount,
            int emptyPages,
            long freeSpace
    ) {
        PersistentPageMemoryMvPartitionStorage partition = mock(PersistentPageMemoryMvPartitionStorage.class);
        when(partition.pageCount()).thenReturn(pageCount);
        when(partition.emptyDataPageCountInFreeList()).thenReturn(emptyPages);
        when(partition.freeSpaceInFreeList()).thenReturn(freeSpace);

        assertThat(table.mvPartitionStorages.create(partitionId, id -> partition), willCompleteSuccessfully());
    }
}
