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

import static org.apache.ignite.internal.util.CursorUtils.map;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.binarytuple.BinaryTupleCommon;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.IndexRowImpl;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumns;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumnsFreeList;
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

        partitionId = sortedIndexTree.partitionId();
    }

    @Override
    public SortedIndexDescriptor indexDescriptor() {
        return descriptor;
    }

    @Override
    public Cursor<RowId> get(BinaryTuple key) throws StorageException {
        SortedIndexRowKey lowerBound = createLowerBound(BinaryTuplePrefix.fromBinaryTuple(key), true);

        SortedIndexRowKey upperBound = createUpperBound(BinaryTuplePrefix.fromBinaryTuple(key), true);

        try {
            return map(sortedIndexTree.find(lowerBound, upperBound), SortedIndexRow::rowId);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Failed to create scan cursor", e);
        }
    }

    @Override
    public void put(IndexRow row) {
        IndexColumns indexColumns = new IndexColumns(partitionId, row.indexColumns().byteBuffer());

        try {
            SortedIndexRow sortedIndexRow = new SortedIndexRow(indexColumns, row.rowId());

            var insert = new InsertSortedIndexRowInvokeClosure(sortedIndexRow, freeList);

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

            var remove = new RemoveSortedIndexRowInvokeClosure(hashIndexRow, freeList);

            sortedIndexTree.invoke(hashIndexRow, null, remove);

            // Performs actual deletion from freeList if necessary.
            remove.afterCompletion();
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Failed to remove value from index", e);
        }
    }

    @Override
    public Cursor<IndexRow> scan(@Nullable BinaryTuplePrefix lowerBound, @Nullable BinaryTuplePrefix upperBound, int flags) {
        boolean includeLower = (flags & GREATER_OR_EQUAL) != 0;
        boolean includeUpper = (flags & LESS_OR_EQUAL) != 0;

        SortedIndexRowKey lower = createLowerBound(lowerBound, includeLower);

        SortedIndexRowKey upper = createUpperBound(upperBound, includeUpper);

        try {
            Cursor<SortedIndexRow> cursor = sortedIndexTree.find(lower, upper, includeLower, includeUpper, null, null);

            return map(cursor, this::toIndexRowImpl);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Failed to create scan cursor", e);
        }
    }

    private static void setEqualityFlag(BinaryTuplePrefix prefix) {
        ByteBuffer buffer = prefix.byteBuffer();

        byte flags = buffer.get(0);

        buffer.put(0, (byte) (flags | BinaryTupleCommon.EQUALITY_FLAG));
    }

    @Nullable
    private SortedIndexRowKey createLowerBound(@Nullable BinaryTuplePrefix bound, boolean include) {
        if (bound == null) {
            return null;
        }

        // Skip the lower bound, if needed.
        if (!include) {
            setEqualityFlag(bound);
        }

        return toSortedIndexRowKey(bound);
    }

    @Nullable
    private SortedIndexRowKey createUpperBound(@Nullable BinaryTuplePrefix bound, boolean include) {
        // Upper bound is skipped by default, so we need to invert the condition
        return createLowerBound(bound, !include);
    }

    private SortedIndexRowKey toSortedIndexRowKey(BinaryTuplePrefix binaryTuple) {
        return new SortedIndexRowKey(new IndexColumns(partitionId, binaryTuple.byteBuffer()));
    }

    private IndexRowImpl toIndexRowImpl(SortedIndexRow sortedIndexRow) {
        return new IndexRowImpl(
                new BinaryTuple(descriptor.binaryTupleSchema(), sortedIndexRow.indexColumns().valueBuffer()),
                sortedIndexRow.rowId()
        );
    }
}
