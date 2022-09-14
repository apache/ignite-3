/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.storage.pagememory.index.sorted;

import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumns;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumnsFreeList;
import org.apache.ignite.internal.storage.pagememory.index.hash.HashIndexRow;
import org.apache.ignite.internal.storage.pagememory.index.hash.InsertHashIndexRowInvokeClosure;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * Sorted index storage implementation.
 */
public class PageMemorySortedIndexStorage implements SortedIndexStorage {
    /** Index descriptor. */
    private final SortedIndexDescriptor descriptor;

    /** Free list to store index columns. */
    private final IndexColumnsFreeList freeList;

    /** Sorted index tree instance. */
    private final SortedIndexTree sortedIndexTree;

    /** Partition id. */
    private final int partitionId;

    /**
     * Constructor.
     *
     * @param descriptor Sorted index descriptor.
     * @param freeList Free list to store index columns.
     * @param sortedIndexTree Sorted index tree instance.
     */
    public PageMemorySortedIndexStorage(SortedIndexDescriptor descriptor, IndexColumnsFreeList freeList, SortedIndexTree sortedIndexTree) {
        this.descriptor = descriptor;
        this.freeList = freeList;
        this.sortedIndexTree = sortedIndexTree;
    }

    @Override
    public SortedIndexDescriptor indexDescriptor() {
        return descriptor;
    }

    @Override
    public void put(IndexRow row) {
        IndexColumns indexColumns = new IndexColumns(partitionId, row.indexColumns().byteBuffer());

        try {
            HashIndexRow hashIndexRow = new HashIndexRow(indexColumns, row.rowId());

            var insert = new InsertHashIndexRowInvokeClosure(hashIndexRow, freeList, IoStatisticsHolderNoOp.INSTANCE);

            hashIndexTree.invoke(hashIndexRow, null, insert);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Failed to put value into index", e);
        }
    }

    @Override
    public void remove(IndexRow row) {

    }

    @Override
    public Cursor<IndexRow> scan(@Nullable BinaryTuple lowerBound, @Nullable BinaryTuple upperBound, int flags) {
        return null;
    }
}
