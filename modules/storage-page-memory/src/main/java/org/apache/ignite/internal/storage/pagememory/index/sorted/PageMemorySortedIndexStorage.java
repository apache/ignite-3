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

package org.apache.ignite.internal.storage.pagememory.index.sorted;

import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumns;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumnsFreeList;
import org.apache.ignite.internal.storage.pagememory.util.TreeCursorAdapter;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteCursor;
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

    /** Lowest possible RowId according to signed long ordering. */
    private final RowId lowestRowId;

    /** Highest possible RowId according to signed long ordering. */
    private final RowId highestRowId;

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

        partitionId = sortedIndexTree.partitionId();

        lowestRowId = new RowId(partitionId, Long.MIN_VALUE, Long.MIN_VALUE);

        highestRowId = new RowId(partitionId, Long.MAX_VALUE, Long.MAX_VALUE);
    }

    @Override
    public SortedIndexDescriptor indexDescriptor() {
        return descriptor;
    }

    @Override
    public void put(IndexRow row) {
        IndexColumns indexColumns = new IndexColumns(partitionId, row.indexColumns().byteBuffer());

        try {
            SortedIndexRow sortedIndexRow = new SortedIndexRow(indexColumns, row.rowId());

            var insert = new InsertSortedIndexRowInvokeClosure(sortedIndexRow, freeList, IoStatisticsHolderNoOp.INSTANCE);

            sortedIndexTree.invoke(sortedIndexRow, null, insert);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Failed to put value into index", e);
        }
    }

    @Override
    public void remove(IndexRow row) {
        IndexColumns indexColumns = new IndexColumns(partitionId, row.indexColumns().byteBuffer());

        try {
            SortedIndexRow hashIndexRow = new SortedIndexRow(indexColumns, row.rowId());

            var remove = new RemoveSortedIndexRowInvokeClosure(hashIndexRow, freeList, IoStatisticsHolderNoOp.INSTANCE);

            sortedIndexTree.invoke(hashIndexRow, null, remove);

            // Performs actual deletion from freeList if necessary.
            remove.afterCompletion();
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Failed to remove value from index", e);
        }
    }

    @Override
    public Cursor<IndexRow> scan(@Nullable BinaryTuplePrefix lowerBound, @Nullable BinaryTuplePrefix upperBound, int flags) {
        IgniteCursor<SortedIndexRow> cursor;

        try {
            cursor = sortedIndexTree.find(
                    toSortedIndexRow(lowerBound, lowestRowId),
                    toSortedIndexRow(upperBound, highestRowId),
                    (flags & GREATER_OR_EQUAL) != 0,
                    (flags & LESS_OR_EQUAL) != 0,
                    null,
                    null
            );
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Failed to create scan cursor", e);
        }

        // TODO: IGNITE-17320 надо бы тут сделать по нормальному

        return Cursor.fromIterator(new TreeCursorAdapter<>(cursor, sortedIndexRow -> new IndexRow() {
            @Override
            public BinaryTuple indexColumns() {
                return new BinaryTuple(descriptor.binaryTupleSchema(), sortedIndexRow.indexColumns().valueBuffer());
            }

            @Override
            public RowId rowId() {
                return sortedIndexRow.rowId();
            }
        }));
    }

    private @Nullable SortedIndexRow toSortedIndexRow(@Nullable BinaryTuplePrefix binaryTuple, RowId rowId) {
        return binaryTuple == null ? null : new SortedIndexRow(new IndexColumns(partitionId, binaryTuple.byteBuffer()), rowId);
    }
}
