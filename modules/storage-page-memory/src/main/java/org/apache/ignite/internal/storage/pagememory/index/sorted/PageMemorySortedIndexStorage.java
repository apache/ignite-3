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

import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionIfStorageInProgressOfRebalance;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionIfStorageNotInCleanupOrRebalancedState;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.function.Function;
import org.apache.ignite.internal.binarytuple.BinaryTupleCommon;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.pagememory.util.GradualTaskExecutor;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageClosedException;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.IndexRowImpl;
import org.apache.ignite.internal.storage.index.PeekCursor;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.pagememory.index.AbstractPageMemoryIndexStorage;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumns;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumnsFreeList;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMeta;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMetaTree;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of Sorted index storage using Page Memory.
 */
public class PageMemorySortedIndexStorage extends AbstractPageMemoryIndexStorage implements SortedIndexStorage {
    private static final IgniteLogger LOG = Loggers.forClass(PageMemorySortedIndexStorage.class);

    /** Index descriptor. */
    private final SortedIndexDescriptor descriptor;

    /** Sorted index tree instance. */
    private volatile SortedIndexTree sortedIndexTree;

    /**
     * Constructor.
     *
     * @param indexMeta Index meta.
     * @param descriptor Sorted index descriptor.
     * @param freeList Free list to store index columns.
     * @param sortedIndexTree Sorted index tree instance.
     * @param indexMetaTree Index meta tree instance.
     */
    public PageMemorySortedIndexStorage(
            IndexMeta indexMeta,
            SortedIndexDescriptor descriptor,
            IndexColumnsFreeList freeList,
            SortedIndexTree sortedIndexTree,
            IndexMetaTree indexMetaTree
    ) {
        super(indexMeta, sortedIndexTree.partitionId(), freeList, indexMetaTree);

        this.descriptor = descriptor;
        this.sortedIndexTree = sortedIndexTree;
    }

    @Override
    public SortedIndexDescriptor indexDescriptor() {
        return descriptor;
    }

    @Override
    public Cursor<RowId> get(BinaryTuple key) throws StorageException {
        return busy(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            try {
                SortedIndexRowKey lowerBound = toSortedIndexRow(key, lowestRowId);

                SortedIndexRowKey upperBound = toSortedIndexRow(key, highestRowId);

                return convertCursor(sortedIndexTree.find(lowerBound, upperBound), SortedIndexRow::rowId);
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException("Failed to create scan cursor", e);
            }
        });
    }

    @Override
    public void put(IndexRow row) {
        busy(() -> {
            try {
                SortedIndexRow sortedIndexRow = toSortedIndexRow(row.indexColumns(), row.rowId());

                var insert = new InsertSortedIndexRowInvokeClosure(sortedIndexRow, freeList, sortedIndexTree.inlineSize());

                sortedIndexTree.invoke(sortedIndexRow, null, insert);

                return null;
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException("Failed to put value into index", e);
            }
        });
    }

    @Override
    public void remove(IndexRow row) {
        busy(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            try {
                SortedIndexRow sortedIndexRow = toSortedIndexRow(row.indexColumns(), row.rowId());

                var remove = new RemoveSortedIndexRowInvokeClosure(sortedIndexRow, freeList);

                sortedIndexTree.invoke(sortedIndexRow, null, remove);

                // Performs actual deletion from freeList if necessary.
                remove.afterCompletion();

                return null;
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException("Failed to remove value from index", e);
            }
        });
    }

    @Override
    public PeekCursor<IndexRow> scan(@Nullable BinaryTuplePrefix lowerBound, @Nullable BinaryTuplePrefix upperBound, int flags) {
        return busy(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            boolean includeLower = (flags & GREATER_OR_EQUAL) != 0;
            boolean includeUpper = (flags & LESS_OR_EQUAL) != 0;

            SortedIndexRowKey lower = createBound(lowerBound, !includeLower);

            SortedIndexRowKey upper = createBound(upperBound, includeUpper);

            return new ScanCursor(lower, upper);
        });
    }

    private @Nullable SortedIndexRowKey createBound(@Nullable BinaryTuplePrefix bound, boolean setEqualityFlag) {
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

    private @Nullable IndexRowImpl toIndexRowImpl(@Nullable SortedIndexRow sortedIndexRow) {
        return sortedIndexRow == null ? null : new IndexRowImpl(
                new BinaryTuple(descriptor.binaryTupleSchema().elementCount(), sortedIndexRow.indexColumns().valueBuffer()),
                sortedIndexRow.rowId()
        );
    }

    @Override
    public void closeStructures() {
        sortedIndexTree.close();
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
                return busy(() -> {
                    throwExceptionIfStorageInProgressOfRebalance(state.get(), PageMemorySortedIndexStorage.this::createStorageInfo);

                    return cursor.hasNext();
                });
            }

            @Override
            public R next() {
                return busy(() -> {
                    throwExceptionIfStorageInProgressOfRebalance(state.get(), PageMemorySortedIndexStorage.this::createStorageInfo);

                    return mapper.apply(cursor.next());
                });
            }
        };
    }

    /** Constant that represents the absence of value in {@link ScanCursor}. */
    private static final SortedIndexRow NO_INDEX_ROW = new SortedIndexRow(null, null);

    private class ScanCursor implements PeekCursor<IndexRow> {
        @Nullable
        private Boolean hasNext;

        @Nullable
        private final SortedIndexRowKey lower;

        @Nullable
        private final SortedIndexRowKey upper;

        @Nullable
        private SortedIndexRow treeRow;

        @Nullable
        private SortedIndexRow peekedRow = NO_INDEX_ROW;

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
            return busy(() -> {
                try {
                    return advanceIfNeeded();
                } catch (IgniteInternalCheckedException e) {
                    throw new StorageException("Error while advancing the cursor", e);
                }
            });
        }

        @Override
        public IndexRow next() {
            return busy(() -> {
                try {
                    if (!advanceIfNeeded()) {
                        throw new NoSuchElementException();
                    }

                    this.hasNext = null;

                    return toIndexRowImpl(treeRow);
                } catch (IgniteInternalCheckedException e) {
                    throw new StorageException("Error while advancing the cursor", e);
                }
            });
        }

        @Override
        public @Nullable IndexRow peek() {
            return busy(() -> {
                throwExceptionIfStorageInProgressOfRebalance(state.get(), PageMemorySortedIndexStorage.this::createStorageInfo);

                try {
                    return toIndexRowImpl(peekBusy());
                } catch (IgniteInternalCheckedException e) {
                    throw new StorageException("Error when peeking next element", e);
                }
            });
        }

        private @Nullable SortedIndexRow peekBusy() throws IgniteInternalCheckedException {
            if (hasNext != null) {
                return treeRow;
            }

            if (treeRow == null) {
                peekedRow = lower == null ? sortedIndexTree.findFirst() : sortedIndexTree.findNext(lower, true);
            } else {
                peekedRow = sortedIndexTree.findNext(treeRow, false);
            }

            if (peekedRow == null || (upper != null && compareRows(peekedRow, upper) >= 0)) {
                peekedRow = null;
            }

            return peekedRow;
        }

        private boolean advanceIfNeeded() throws IgniteInternalCheckedException {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), PageMemorySortedIndexStorage.this::createStorageInfo);

            if (hasNext != null) {
                return hasNext;
            }

            treeRow = (peekedRow == NO_INDEX_ROW) ? peekBusy() : peekedRow;
            peekedRow = NO_INDEX_ROW;

            hasNext = treeRow != null;
            return hasNext;
        }

        private int compareRows(SortedIndexRowKey key1, SortedIndexRowKey key2) {
            return sortedIndexTree.getBinaryTupleComparator().compare(
                    key1.indexColumns().valueBuffer(),
                    key2.indexColumns().valueBuffer()
            );
        }
    }

    /**
     * Updates the internal data structures of the storage on rebalance or cleanup.
     *
     * @param freeList Free list to store index columns.
     * @param sortedIndexTree Sorted index tree instance.
     * @throws StorageException If failed.
     */
    public void updateDataStructures(IndexColumnsFreeList freeList, SortedIndexTree sortedIndexTree) {
        throwExceptionIfStorageNotInCleanupOrRebalancedState(state.get(), this::createStorageInfo);

        this.freeList = freeList;
        this.sortedIndexTree = sortedIndexTree;
    }

    /**
     * Starts destruction of the data stored by this index partition.
     *
     * @param executor {@link GradualTaskExecutor} on which to destroy.
     * @throws StorageException If something goes wrong.
     */
    public void startDestructionOn(GradualTaskExecutor executor) throws StorageException {
        try {
            executor.execute(
                    sortedIndexTree.startGradualDestruction(rowKey -> removeIndexColumns((SortedIndexRow) rowKey), false)
            ).whenComplete((res, ex) -> {
                if (ex != null) {
                    LOG.error("Sorted index " + descriptor.id() + " destruction has failed", ex);
                }
            });
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Cannot destroy sorted index " + indexDescriptor().id(), e);
        }
    }

    private void removeIndexColumns(SortedIndexRow indexRow) {
        if (indexRow.indexColumns().link() != PageIdUtils.NULL_LINK) {
            try {
                freeList.removeDataRowByLink(indexRow.indexColumns().link());
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException("Cannot destroy sorted index " + indexDescriptor().id(), e);
            }

            indexRow.indexColumns().link(PageIdUtils.NULL_LINK);
        }
    }
}
