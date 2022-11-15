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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import org.apache.ignite.internal.binarytuple.BinaryTupleCommon;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageClosedException;
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
 * Implementation of Sorted index storage using Page Memory.
 */
public class PageMemorySortedIndexStorage implements SortedIndexStorage {
    private static final VarHandle STARTED;

    static {
        try {
            STARTED = MethodHandles.lookup().findVarHandle(PageMemorySortedIndexStorage.class, "started", boolean.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

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

    private volatile boolean started = true;

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
    public Cursor<RowId> get(BinaryTuple key) throws StorageException {
        checkClosed();

        SortedIndexRowKey lowerBound = toSortedIndexRow(key, lowestRowId);

        SortedIndexRowKey upperBound = toSortedIndexRow(key, highestRowId);

        try {
            Cursor<SortedIndexRow> cursor = sortedIndexTree.find(lowerBound, upperBound);

            return new Cursor<>() {
                @Override
                public void close() throws Exception {
                    cursor.close();
                }

                @Override
                public boolean hasNext() {
                    checkClosed();

                    return cursor.hasNext();
                }

                @Override
                public RowId next() {
                    checkClosed();

                    return cursor.next().rowId();
                }
            };
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Failed to create scan cursor", e);
        }
    }

    @Override
    public void put(IndexRow row) {
        checkClosed();

        try {
            SortedIndexRow sortedIndexRow = toSortedIndexRow(row.indexColumns(), row.rowId());

            var insert = new InsertSortedIndexRowInvokeClosure(sortedIndexRow, freeList, sortedIndexTree.inlineSize());

            sortedIndexTree.invoke(sortedIndexRow, null, insert);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Failed to put value into index", e);
        }
    }

    @Override
    public void remove(IndexRow row) {
        checkClosed();

        try {
            SortedIndexRow sortedIndexRow = toSortedIndexRow(row.indexColumns(), row.rowId());

            var remove = new RemoveSortedIndexRowInvokeClosure(sortedIndexRow, freeList);

            sortedIndexTree.invoke(sortedIndexRow, null, remove);

            // Performs actual deletion from freeList if necessary.
            remove.afterCompletion();
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Failed to remove value from index", e);
        }
    }

    @Override
    public Cursor<IndexRow> scan(@Nullable BinaryTuplePrefix lowerBound, @Nullable BinaryTuplePrefix upperBound, int flags) {
        checkClosed();

        boolean includeLower = (flags & GREATER_OR_EQUAL) != 0;
        boolean includeUpper = (flags & LESS_OR_EQUAL) != 0;

        SortedIndexRowKey lower = createBound(lowerBound, !includeLower);

        SortedIndexRowKey upper = createBound(upperBound, includeUpper);

        try {
            Cursor<SortedIndexRow> cursor = sortedIndexTree.find(lower, upper);

            return new Cursor<>() {
                @Override
                public void close() throws Exception {
                    cursor.close();
                }

                @Override
                public boolean hasNext() {
                    checkClosed();

                    return cursor.hasNext();
                }

                @Override
                public IndexRow next() {
                    checkClosed();

                    return toIndexRowImpl(cursor.next());
                }
            };
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Failed to create scan cursor", e);
        }
    }

    @Nullable
    private SortedIndexRowKey createBound(@Nullable BinaryTuplePrefix bound, boolean setEqualityFlag) {
        if (bound == null) {
            return null;
        }

        ByteBuffer buffer = bound.byteBuffer();

        if (setEqualityFlag) {
            byte flags = buffer.get(0);

            buffer.put(0, (byte) (flags | BinaryTupleCommon.EQUALITY_FLAG));
        }

        return new SortedIndexRowKey(new IndexColumns(partitionId, buffer));
    }

    private SortedIndexRow toSortedIndexRow(BinaryTuple tuple, RowId rowId) {
        return new SortedIndexRow(new IndexColumns(partitionId, tuple.byteBuffer()), rowId);
    }

    private IndexRowImpl toIndexRowImpl(SortedIndexRow sortedIndexRow) {
        return new IndexRowImpl(
                new BinaryTuple(descriptor.binaryTupleSchema(), sortedIndexRow.indexColumns().valueBuffer()),
                sortedIndexRow.rowId()
        );
    }

    /**
     * Destroys the sorted index storage and its data in it.
     */
    public void destroy() {
        close0(true);
    }

    /**
     * Closes the sorted index storage.
     */
    public void close() {
        close0(false);
    }

    /**
     * Closes the sorted index storage.
     *
     * @param destroy Whether to destroy data in storage.
     */
    private void close0(boolean destroy) {
        if (!STARTED.compareAndSet(this, true, false)) {
            return;
        }

        sortedIndexTree.close();

        if (destroy) {
            //TODO IGNITE-17626 Implement.
        }
    }

    private void checkClosed() {
        if (!started) {
            throw new StorageClosedException("Storage is already closed");
        }
    }
}
