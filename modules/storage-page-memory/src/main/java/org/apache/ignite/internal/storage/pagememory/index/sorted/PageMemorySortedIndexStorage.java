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

import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionDependingOnStorageState;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionDependingOnStorageStateOnRebalance;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionIfStorageInProgressOfRebalance;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionIfStorageNotInCleanupOrRebalancedState;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
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
import org.apache.ignite.internal.storage.StorageRebalanceException;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.IndexRowImpl;
import org.apache.ignite.internal.storage.index.PeekCursor;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumns;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumnsFreeList;
import org.apache.ignite.internal.storage.util.StorageState;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of Sorted index storage using Page Memory.
 */
public class PageMemorySortedIndexStorage implements SortedIndexStorage {
    private static final IgniteLogger LOG = Loggers.forClass(PageMemorySortedIndexStorage.class);

    /** Index descriptor. */
    private final SortedIndexDescriptor descriptor;

    /** Free list to store index columns. */
    private volatile IndexColumnsFreeList freeList;

    /** Sorted index tree instance. */
    private volatile SortedIndexTree sortedIndexTree;

    /** Partition id. */
    private final int partitionId;

    /** Lowest possible RowId according to signed long ordering. */
    private final RowId lowestRowId;

    /** Highest possible RowId according to signed long ordering. */
    private final RowId highestRowId;

    /** Busy lock. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Current state of the storage. */
    private final AtomicReference<StorageState> state = new AtomicReference<>(StorageState.RUNNABLE);

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

        lowestRowId = RowId.lowestRowId(partitionId);

        highestRowId = RowId.highestRowId(partitionId);
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
        if (!state.compareAndSet(StorageState.RUNNABLE, StorageState.CLOSED)) {
            StorageState state = this.state.get();

            assert state == StorageState.CLOSED : state;

            return;
        }

        busyLock.block();

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

    private class ScanCursor implements PeekCursor<IndexRow> {
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
            return busy(() -> {
                try {
                    advanceIfNeeded();

                    return hasNext;
                } catch (IgniteInternalCheckedException e) {
                    throw new StorageException("Error while advancing the cursor", e);
                }
            });
        }

        @Override
        public IndexRow next() {
            return busy(() -> {
                try {
                    advanceIfNeeded();

                    boolean hasNext = this.hasNext;

                    if (!hasNext) {
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

                if (hasNext != null) {
                    if (hasNext) {
                        return toIndexRowImpl(treeRow);
                    }

                    return null;
                }

                try {
                    SortedIndexRow nextTreeRow;

                    if (treeRow == null) {
                        nextTreeRow = lower == null ? sortedIndexTree.findFirst() : sortedIndexTree.findNext(lower, true);
                    } else {
                        nextTreeRow = sortedIndexTree.findNext(treeRow, false);
                    }

                    if (nextTreeRow == null || (upper != null && compareRows(nextTreeRow, upper) >= 0)) {
                        return null;
                    }

                    return toIndexRowImpl(nextTreeRow);
                } catch (IgniteInternalCheckedException e) {
                    throw new StorageException("Error when peeking next element", e);
                }
            });
        }

        private void advanceIfNeeded() throws IgniteInternalCheckedException {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), PageMemorySortedIndexStorage.this::createStorageInfo);

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

    private <V> V busy(Supplier<V> supplier) {
        if (!busyLock.enterBusy()) {
            throwExceptionDependingOnStorageState(state.get(), createStorageInfo());
        }

        try {
            return supplier.get();
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Prepares storage for rebalancing.
     *
     * @throws StorageRebalanceException If there was an error when starting the rebalance.
     */
    public void startRebalance() {
        if (!state.compareAndSet(StorageState.RUNNABLE, StorageState.REBALANCE)) {
            throwExceptionDependingOnStorageStateOnRebalance(state.get(), createStorageInfo());
        }

        // Changed storage states and expect all storage operations to stop soon.
        busyLock.block();

        try {
            this.sortedIndexTree.close();
        } finally {
            busyLock.unblock();
        }
    }

    /**
     * Completes the rebalancing of the storage.
     *
     * @throws StorageRebalanceException If there is an error while completing the storage rebalance.
     */
    public void completeRebalance() {
        if (!state.compareAndSet(StorageState.REBALANCE, StorageState.RUNNABLE)) {
            throwExceptionDependingOnStorageStateOnRebalance(state.get(), createStorageInfo());
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

    private String createStorageInfo() {
        return IgniteStringFormatter.format("indexId={}, partitionId={}", descriptor.id(), partitionId);
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

    /**
     * Prepares the storage for cleanup.
     *
     * <p>After cleanup (successful or not), method {@link #finishCleanup()} must be called.
     */
    public void startCleanup() {
        if (!state.compareAndSet(StorageState.RUNNABLE, StorageState.CLEANUP)) {
            throwExceptionDependingOnStorageState(state.get(), createStorageInfo());
        }

        // Changed storage states and expect all storage operations to stop soon.
        busyLock.block();

        try {
            sortedIndexTree.close();
        } finally {
            busyLock.unblock();
        }
    }

    /**
     * Finishes cleanup up the storage.
     */
    public void finishCleanup() {
        state.compareAndSet(StorageState.CLEANUP, StorageState.RUNNABLE);
    }
}
