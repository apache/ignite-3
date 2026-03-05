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

import java.util.Collection;
import java.util.stream.Stream;
import org.apache.ignite.internal.storage.pagememory.mv.PersistentPageMemoryMvPartitionStorage;

/**
 * Class containing methods to calculate various metrics related to an "aipersist" data region and tables in it.
 */
class PersistentDataRegionMetricsCalculator {
    private final int pageSize;

    PersistentDataRegionMetricsCalculator(int pageSize) {
        this.pageSize = pageSize;
    }

    long totalAllocatedSize(Collection<PersistentPageMemoryTableStorage> tables) {
        long pagesCount = tables.stream().mapToLong(PersistentDataRegionMetricsCalculator::tableAllocatedPagesCount).sum();

        return pagesCount * pageSize;
    }

    long totalAllocatedSize(PersistentPageMemoryTableStorage table) {
        return tableAllocatedPagesCount(table) * pageSize;
    }

    private static long tableAllocatedPagesCount(PersistentPageMemoryTableStorage table) {
        return allPartitions(table).mapToLong(PersistentPageMemoryMvPartitionStorage::pageCount).sum();
    }

    long totalUsedSize(Collection<PersistentPageMemoryTableStorage> tables) {
        long pagesCount = tables.stream().mapToLong(PersistentDataRegionMetricsCalculator::tableNonEmptyAllocatedPagesCount).sum();

        return pagesCount * pageSize;
    }

    long totalUsedSize(PersistentPageMemoryTableStorage table) {
        return tableNonEmptyAllocatedPagesCount(table) * pageSize;
    }

    private static long tableNonEmptyAllocatedPagesCount(PersistentPageMemoryTableStorage table) {
        return allPartitions(table)
                .mapToLong(partitionStorage -> partitionStorage.pageCount() - partitionStorage.emptyDataPageCountInFreeList())
                .sum();
    }

    long totalEmptySize(Collection<PersistentPageMemoryTableStorage> tables) {
        long pageCount = tables.stream().mapToLong(PersistentDataRegionMetricsCalculator::tableEmptyPagesCount).sum();

        return pageCount * pageSize;
    }

    long totalEmptySize(PersistentPageMemoryTableStorage table) {
        return tableEmptyPagesCount(table) * pageSize;
    }

    private static long tableEmptyPagesCount(PersistentPageMemoryTableStorage table) {
        return allPartitions(table).mapToLong(PersistentPageMemoryMvPartitionStorage::emptyDataPageCountInFreeList).sum();
    }

    long totalDataSize(Collection<PersistentPageMemoryTableStorage> tables) {
        return tables.stream().mapToLong(this::totalDataSize).sum();
    }

    long totalDataSize(PersistentPageMemoryTableStorage table) {
        return allPartitions(table).mapToLong(this::partitionFilledSpaceBytes).sum();
    }

    private long partitionFilledSpaceBytes(PersistentPageMemoryMvPartitionStorage partition) {
        int pagesCount = partition.pageCount();

        int emptyPagesCount = partition.emptyDataPageCountInFreeList();

        long pagesWithData = (long) pagesCount - emptyPagesCount;

        return pagesWithData * pageSize - partition.freeSpaceInFreeList();
    }

    /**
     * Returns the ratio of space occupied by user and system data to the size of all pages that contain this data.
     *
     * <p>This metric can help to determine how much space of a data page is occupied on average. Low fill factor can
     * indicate that data pages are very fragmented (i.e. there is a lot of empty space across all data pages).
     */
    double pagesFillFactor(Collection<PersistentPageMemoryTableStorage> tables) {
        // Number of bytes used by pages that contain at least some data.
        long totalUsedSpaceBytes = totalUsedSize(tables);

        if (totalUsedSpaceBytes == 0) {
            return 0;
        }

        // Amount of free space in these pages.
        long freeSpaceBytes = tables.stream()
                .flatMap(PersistentDataRegionMetricsCalculator::allPartitions)
                .mapToLong(PersistentPageMemoryMvPartitionStorage::freeSpaceInFreeList)
                .sum();

        // Number of bytes that contain useful data.
        long nonEmptySpaceBytes = totalUsedSpaceBytes - freeSpaceBytes;

        return (double) nonEmptySpaceBytes / totalUsedSpaceBytes;
    }

    /** Same as {@link #pagesFillFactor(Collection)} but for a single table. */
    double pagesFillFactor(PersistentPageMemoryTableStorage table) {
        long totalUsedSpaceBytes = totalUsedSize(table);

        if (totalUsedSpaceBytes == 0) {
            return 0;
        }

        long freeSpaceBytes = allPartitions(table)
                .mapToLong(PersistentPageMemoryMvPartitionStorage::freeSpaceInFreeList)
                .sum();

        long nonEmptySpaceBytes = totalUsedSpaceBytes - freeSpaceBytes;

        return (double) nonEmptySpaceBytes / totalUsedSpaceBytes;
    }

    private static Stream<PersistentPageMemoryMvPartitionStorage> allPartitions(PersistentPageMemoryTableStorage tableStorage) {
        return tableStorage.mvPartitionStorages.getAll().stream();
    }
}
