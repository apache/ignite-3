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


package org.apache.ignite.internal.storage.pagememory.index;

import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionDependingOnStorageState;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionDependingOnStorageStateOnRebalance;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionIfStorageInProgressOfRebalance;

import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.StorageRebalanceException;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.storage.index.PeekCursor;
import org.apache.ignite.internal.storage.pagememory.index.common.IndexRowKey;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumnsFreeList;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMeta;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMetaKey;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMetaTree;
import org.apache.ignite.internal.storage.pagememory.index.meta.UpdateLastRowIdUuidToBuiltInvokeClosure;
import org.apache.ignite.internal.storage.util.StorageState;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract index storage based on Page Memory.
 */
public abstract class AbstractPageMemoryIndexStorage<K extends IndexRowKey, V extends K> implements IndexStorage {
    /** Index ID. */
    private final int indexId;

    /** Partition id. */
    protected final int partitionId;

    /** Lowest possible RowId according to signed long ordering. */
    protected final RowId lowestRowId;

    /** Highest possible RowId according to signed long ordering. */
    protected final RowId highestRowId;

    /** Free list to store index columns. */
    protected volatile IndexColumnsFreeList freeList;

    /** Busy lock. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Current state of the storage. */
    protected final AtomicReference<StorageState> state = new AtomicReference<>(StorageState.RUNNABLE);

    /** Index meta tree instance. */
    private final IndexMetaTree indexMetaTree;

    /** Row ID for which the index needs to be built, {@code null} means that the index building has completed. */
    private volatile @Nullable RowId nextRowIdToBuilt;

    protected AbstractPageMemoryIndexStorage(
            IndexMeta indexMeta,
            int partitionId,
            IndexColumnsFreeList freeList,
            IndexMetaTree indexMetaTree
    ) {
        this.indexId = indexMeta.indexId();
        this.partitionId = partitionId;
        this.freeList = freeList;
        this.indexMetaTree = indexMetaTree;

        lowestRowId = RowId.lowestRowId(partitionId);

        highestRowId = RowId.highestRowId(partitionId);

        nextRowIdToBuilt = indexMeta.nextRowIdUuidToBuild() == null ? null : new RowId(partitionId, indexMeta.nextRowIdUuidToBuild());
    }

    @Override
    public @Nullable RowId getNextRowIdToBuild() {
        return busy(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            return nextRowIdToBuilt;
        });
    }

    @Override
    public void setNextRowIdToBuild(@Nullable RowId rowId) {
        busy(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            UUID rowIdUuid = rowId == null ? null : rowId.uuid();

            try {
                indexMetaTree.invoke(new IndexMetaKey(indexId), null, new UpdateLastRowIdUuidToBuiltInvokeClosure(rowIdUuid));
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException("Error updating last row ID uuid to built: [{}, rowId={}]", e, createStorageInfo(), rowId);
            }

            nextRowIdToBuilt = rowId;

            return null;
        });
    }

    /**
     * Closes the hash index storage.
     */
    public void close() {
        if (!state.compareAndSet(StorageState.RUNNABLE, StorageState.CLOSED)) {
            StorageState state = this.state.get();

            assert state == StorageState.CLOSED : state;

            return;
        }

        busyLock.block();

        closeStructures();
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
            closeStructures();
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
            closeStructures();
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

    /** Constant that represents the absence of value in {@link ScanCursor}. Not equivalent to {@code null} value. */
    private static final IndexRowKey NO_INDEX_ROW = () -> null;

    /**
     * Cursor that always returns up-to-date next element.
     *
     * @param <R> Type of the returned value.
     */
    protected abstract class ScanCursor<R> implements PeekCursor<R> {
        private final BplusTree<K, V> indexTree;

        private final @Nullable K lower;

        private @Nullable Boolean hasNext;

        /**
         * Last row used in mapping in the {@link #next()} call.
         * {@code null} upon cursor creation or after {@link #hasNext()} returned {@code null}.
         */
        private @Nullable V treeRow;

        /**
         * Row used in the mapping of the latest {@link #peek()} call, that was performed after the last {@link #next()} call.
         * {@link #NO_INDEX_ROW} if there was no such call.
         */
        private @Nullable V peekedRow = (V) NO_INDEX_ROW;

        protected ScanCursor(@Nullable K lower, BplusTree<K, V> indexTree) {
            this.lower = lower;
            this.indexTree = indexTree;
        }

        /**
         * Maps value from the index tree into the required result.
         */
        protected abstract R map(V value);

        /**
         * Check whether the passed value exceeds the upper bound for the scan.
         */
        protected abstract boolean exceedsUpperBound(V value);

        @Override
        public void close() {
            // No-op.
        }

        @Override
        public boolean hasNext() {
            return busy(() -> {
                try {
                    return advanceIfNeededBusy();
                } catch (IgniteInternalCheckedException e) {
                    throw new StorageException("Error while advancing the cursor", e);
                }
            });
        }

        @Override
        public R next() {
            return busy(() -> {
                try {
                    if (!advanceIfNeededBusy()) {
                        throw new NoSuchElementException();
                    }

                    this.hasNext = null;

                    return map(treeRow);
                } catch (IgniteInternalCheckedException e) {
                    throw new StorageException("Error while advancing the cursor", e);
                }
            });
        }

        @Override
        public @Nullable R peek() {
            return busy(() -> {
                throwExceptionIfStorageInProgressOfRebalance(state.get(), AbstractPageMemoryIndexStorage.this::createStorageInfo);

                try {
                    return map(peekBusy());
                } catch (IgniteInternalCheckedException e) {
                    throw new StorageException("Error when peeking next element", e);
                }
            });
        }

        private @Nullable V peekBusy() throws IgniteInternalCheckedException {
            if (hasNext != null) {
                return treeRow;
            }

            if (treeRow == null) {
                peekedRow = lower == null ? indexTree.findFirst() : indexTree.findNext(lower, true);
            } else {
                peekedRow = indexTree.findNext(treeRow, false);
            }

            if (peekedRow != null && exceedsUpperBound(peekedRow)) {
                peekedRow = null;
            }

            return peekedRow;
        }

        private boolean advanceIfNeededBusy() throws IgniteInternalCheckedException {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), AbstractPageMemoryIndexStorage.this::createStorageInfo);

            if (hasNext != null) {
                return hasNext;
            }

            treeRow = (peekedRow == NO_INDEX_ROW) ? peekBusy() : peekedRow;
            peekedRow = (V) NO_INDEX_ROW;

            hasNext = treeRow != null;
            return hasNext;
        }
    }

    protected <V> V busy(Supplier<V> supplier) {
        if (!busyLock.enterBusy()) {
            throwExceptionDependingOnStorageState(state.get(), createStorageInfo());
        }

        try {
            return supplier.get();
        } finally {
            busyLock.leaveBusy();
        }
    }

    protected String createStorageInfo() {
        return IgniteStringFormatter.format("indexId={}, partitionId={}", indexId, partitionId);
    }

    /**
     * Closes internal structures.
     */
    protected abstract void closeStructures();
}
