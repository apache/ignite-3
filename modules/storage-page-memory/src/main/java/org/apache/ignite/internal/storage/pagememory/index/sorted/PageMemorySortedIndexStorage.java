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
import java.util.NoSuchElementException;
import java.util.function.Function;
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
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of Sorted index storage using Page Memory.
 */
public class PageMemorySortedIndexStorage implements SortedIndexStorage {
    private static final VarHandle CLOSED;

    static {
        try {
            CLOSED = MethodHandles.lookup().findVarHandle(PageMemorySortedIndexStorage.class, "closed", boolean.class);
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

    /** Busy lock for synchronous closing. */
    private final IgniteSpinBusyLock closeBusyLock = new IgniteSpinBusyLock();

    /** To avoid double closure. */
    @SuppressWarnings("unused")
    private volatile boolean closed;

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
        if (!closeBusyLock.enterBusy()) {
            throwStorageClosedException();
        }

        try {
            SortedIndexRowKey lowerBound = toSortedIndexRow(key, lowestRowId);

            SortedIndexRowKey upperBound = toSortedIndexRow(key, highestRowId);

            return convertCursor(sortedIndexTree.find(lowerBound, upperBound), SortedIndexRow::rowId);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Failed to create scan cursor", e);
        } finally {
            closeBusyLock.leaveBusy();
        }
    }

    @Override
    public void put(IndexRow row) {
        if (!closeBusyLock.enterBusy()) {
            throwStorageClosedException();
        }

        try {
            SortedIndexRow sortedIndexRow = toSortedIndexRow(row.indexColumns(), row.rowId());

            var insert = new InsertSortedIndexRowInvokeClosure(sortedIndexRow, freeList, sortedIndexTree.inlineSize());

            sortedIndexTree.invoke(sortedIndexRow, null, insert);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Failed to put value into index", e);
        } finally {
            closeBusyLock.leaveBusy();
        }
    }

    @Override
    public void remove(IndexRow row) {
        if (!closeBusyLock.enterBusy()) {
            throwStorageClosedException();
        }

        try {
            SortedIndexRow sortedIndexRow = toSortedIndexRow(row.indexColumns(), row.rowId());

            var remove = new RemoveSortedIndexRowInvokeClosure(sortedIndexRow, freeList);

            sortedIndexTree.invoke(sortedIndexRow, null, remove);

            // Performs actual deletion from freeList if necessary.
            remove.afterCompletion();
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Failed to remove value from index", e);
        } finally {
            closeBusyLock.leaveBusy();
        }
    }

    @Override
    public Cursor<IndexRow> scan(@Nullable BinaryTuplePrefix lowerBound, @Nullable BinaryTuplePrefix upperBound, int flags) {
        if (!closeBusyLock.enterBusy()) {
            throwStorageClosedException();
        }

        try {
            boolean includeLower = (flags & GREATER_OR_EQUAL) != 0;
            boolean includeUpper = (flags & LESS_OR_EQUAL) != 0;

            SortedIndexRowKey lower = createBound(lowerBound, !includeLower);

            SortedIndexRowKey upper = createBound(upperBound, includeUpper);

            return new ScanCursor(lower, upper);
        } finally {
            closeBusyLock.leaveBusy();
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
     * Closes the sorted index storage.
     */
    public void close() {
        if (!CLOSED.compareAndSet(this, false, true)) {
            return;
        }

        closeBusyLock.block();

        sortedIndexTree.close();
    }

    /**
     * Throws an exception that the storage is already closed.
     */
    private void throwStorageClosedException() {
        throw new StorageClosedException("Storage is already closed");
    }

    /**
     * Returns a new cursor that converts elements to another type, and also throws {@link StorageClosedException} on
     * {@link Cursor#hasNext()} and {@link Cursor#next()} when the sorted index storage is {@link #close()}.
     *
     * @param cursor Cursor.
     * @param mapper Conversion function.
     */
    private <T, R> Cursor<R> convertCursor(Cursor<T> cursor, Function<T, R> mapper) {
        return new Cursor<>() {
            @Override
            public void close() {
                cursor.close();
            }

            @Override
            public boolean hasNext() {
                if (!closeBusyLock.enterBusy()) {
                    throwStorageClosedException();
                }

                try {
                    return cursor.hasNext();
                } finally {
                    closeBusyLock.leaveBusy();
                }
            }

            @Override
            public R next() {
                if (!closeBusyLock.enterBusy()) {
                    throwStorageClosedException();
                }

                try {
                    return mapper.apply(cursor.next());
                } finally {
                    closeBusyLock.leaveBusy();
                }
            }
        };
    }

    private class ScanCursor implements Cursor<IndexRow> {
        @Nullable
        private Boolean hasNext;

        @Nullable
        private final SortedIndexRowKey lower;

        @Nullable
        private final SortedIndexRowKey upper;

        @Nullable
        private SortedIndexRow treeRow;

        private ScanCursor(@Nullable SortedIndexRowKey lower, @Nullable SortedIndexRowKey upper) {
            this.lower = lower;
            this.upper = upper;
        }

        @Override
        public void close() {
            // No-op.
        }

        @Override
        public boolean hasNext() {
            if (!closeBusyLock.enterBusy()) {
                throwStorageClosedException();
            }

            try {
                advanceIfNeeded();

                return hasNext;
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException("Error while advancing the cursor", e);
            } finally {
                closeBusyLock.leaveBusy();
            }
        }

        @Override
        public IndexRow next() {
            if (!closeBusyLock.enterBusy()) {
                throwStorageClosedException();
            }

            try {
                advanceIfNeeded();

                boolean hasNext = this.hasNext;

                this.hasNext = null;

                if (!hasNext) {
                    throw new NoSuchElementException();
                }

                return toIndexRowImpl(treeRow);
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException("Error while advancing the cursor", e);
            } finally {
                closeBusyLock.leaveBusy();
            }
        }

        private void advanceIfNeeded() throws IgniteInternalCheckedException {
            if (hasNext != null) {
                return;
            }

            if (treeRow == null) {
                treeRow = lower == null ? sortedIndexTree.findFirst() : sortedIndexTree.findNext(lower, true);
            } else {
                SortedIndexRow next = sortedIndexTree.findNext(treeRow, false);

                if (next == null) {
                    hasNext = false;

                    return;
                } else {
                    treeRow = next;
                }
            }

            hasNext = treeRow != null && (upper == null || compareRows(treeRow, upper) < 0);
        }

        private int compareRows(SortedIndexRowKey key1, SortedIndexRowKey key2) {
            return sortedIndexTree.getBinaryTupleComparator().compare(
                    key1.indexColumns().valueBuffer(),
                    key2.indexColumns().valueBuffer()
            );
        }
    }
}
