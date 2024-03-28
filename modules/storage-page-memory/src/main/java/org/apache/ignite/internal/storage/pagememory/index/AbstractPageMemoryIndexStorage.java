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

import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionDependingOnIndexStorageState;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionDependingOnStorageState;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionDependingOnStorageStateOnRebalance;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionIfStorageInProgressOfRebalance;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionIfStorageNotInCleanupOrRebalancedState;

import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.util.GradualTask;
import org.apache.ignite.internal.pagememory.util.GradualTaskExecutor;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.StorageRebalanceException;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.storage.index.PeekCursor;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine;
import org.apache.ignite.internal.storage.pagememory.VolatilePageMemoryStorageEngine;
import org.apache.ignite.internal.storage.pagememory.index.common.IndexRowKey;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumnsFreeList;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMeta;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMetaKey;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMetaTree;
import org.apache.ignite.internal.storage.pagememory.index.meta.UpdateLastRowIdUuidToBuildInvokeClosure;
import org.apache.ignite.internal.storage.util.StorageState;
import org.apache.ignite.internal.storage.util.StorageUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract index storage based on Page Memory.
 */
public abstract class AbstractPageMemoryIndexStorage<K extends IndexRowKey, V extends K, TreeT extends BplusTree<K, V>>
        implements IndexStorage {
    private static final IgniteLogger LOG = Loggers.forClass(AbstractPageMemoryIndexStorage.class);

    /** Partition id. */
    protected final int partitionId;

    /** Lowest possible RowId according to signed long ordering. */
    protected final RowId lowestRowId;

    /** Highest possible RowId according to signed long ordering. */
    protected final RowId highestRowId;

    /** Current state of the storage. */
    protected final AtomicReference<StorageState> state = new AtomicReference<>(StorageState.RUNNABLE);

    /** Index meta tree instance. */
    private volatile IndexMetaTree indexMetaTree;

    /** Free list to store index columns. */
    protected volatile IndexColumnsFreeList freeList;

    /** Index tree instance. */
    protected volatile TreeT indexTree;

    /** Row ID for which the index needs to be built, {@code null} means that the index building has completed. */
    private volatile @Nullable RowId nextRowIdToBuild;

    /** Busy lock. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Index ID. */
    private final int indexId;

    private final boolean isVolatile;

    protected AbstractPageMemoryIndexStorage(
            IndexMeta indexMeta,
            int partitionId,
            TreeT indexTree,
            IndexColumnsFreeList freeList,
            IndexMetaTree indexMetaTree,
            boolean isVolatile
    ) {
        this.indexId = indexMeta.indexId();
        this.partitionId = partitionId;
        this.indexTree = indexTree;
        this.freeList = freeList;
        this.indexMetaTree = indexMetaTree;
        this.isVolatile = isVolatile;

        lowestRowId = RowId.lowestRowId(partitionId);

        highestRowId = RowId.highestRowId(partitionId);

        nextRowIdToBuild = getNextRowIdToBuild(indexMeta);
    }

    private @Nullable RowId getNextRowIdToBuild(IndexMeta indexMeta) {
        UUID uuid = indexMeta.nextRowIdUuidToBuild();

        return uuid == null ? null : new RowId(partitionId, uuid);
    }

    @Override
    public @Nullable RowId getNextRowIdToBuild() {
        return busyNonDataRead(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            return nextRowIdToBuild;
        });
    }

    @Override
    public void setNextRowIdToBuild(@Nullable RowId rowId) {
        busyNonDataRead(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            UUID rowIdUuid = rowId == null ? null : rowId.uuid();

            try {
                indexMetaTree.invoke(new IndexMetaKey(indexId), null, new UpdateLastRowIdUuidToBuildInvokeClosure(rowIdUuid));
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException("Error updating last row ID uuid to build: [{}, rowId={}]", e, createStorageInfo(), rowId);
            }

            nextRowIdToBuild = rowId;

            return null;
        });
    }

    /** Closes the index storage. */
    public void close() {
        if (!transitionToTerminalState(StorageState.CLOSED)) {
            return;
        }

        busyLock.block();

        closeStructures();
    }

    /**
     * If not already in a terminal state, transitions to the supplied state and returns {@code true}, otherwise just returns {@code false}.
     */
    private boolean transitionToTerminalState(StorageState targetState) {
        return StorageUtils.transitionToTerminalState(targetState, state);
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

    /**
     * Starts destruction of the data stored by this index partition.
     *
     * @param executor {@link GradualTaskExecutor} on which to destroy.
     * @return Future that gets completed when the destruction operation finishes.
     * @throws StorageException If something goes wrong.
     */
    public final CompletableFuture<Void> startDestructionOn(GradualTaskExecutor executor) throws StorageException {
        try {
            int maxWorkUnits = isVolatile
                    ? VolatilePageMemoryStorageEngine.MAX_DESTRUCTION_WORK_UNITS
                    : PersistentPageMemoryStorageEngine.MAX_DESTRUCTION_WORK_UNITS;

            return executor.execute(createDestructionTask(maxWorkUnits))
                    .whenComplete((res, e) -> {
                        if (e != null) {
                            LOG.error("Unable to destroy index {}", e, indexId);
                        }
                    });
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Unable to destroy index " + indexId, e);
        }
    }

    /**
     * Transitions this storage to the {@link StorageState#DESTROYED} state. Blocks the busy lock, but does not
     * close the structures (they will have to be closed by calling {@link #closeStructures()}).
     *
     * @return {@code true} if this call actually made the transition and, hence, the caller must call {@link #closeStructures()}.
     */
    public boolean transitionToDestroyedState() {
        if (!transitionToTerminalState(StorageState.DESTROYED)) {
            return false;
        }

        busyLock.block();

        return true;
    }

    protected abstract GradualTask createDestructionTask(int maxWorkUnits) throws IgniteInternalCheckedException;

    protected String createStorageInfo() {
        return IgniteStringFormatter.format("indexId={}, partitionId={}", indexId, partitionId);
    }

    /**
     * Closes internal structures.
     */
    public void closeStructures() {
        indexTree.close();

        nextRowIdToBuild = null;
    }

    /**
     * Updates the internal data structures of the storage on rebalance or cleanup.
     *
     * @param freeList Free list to store index columns.
     * @param indexTree Hash index tree instance.
     * @throws StorageException If failed.
     */
    public void updateDataStructures(IndexMetaTree indexMetaTree, IndexColumnsFreeList freeList, TreeT indexTree) {
        throwExceptionIfStorageNotInCleanupOrRebalancedState(state.get(), this::createStorageInfo);

        this.indexMetaTree = indexMetaTree;
        this.freeList = freeList;
        this.indexTree = indexTree;

        try {
            IndexMeta indexMeta = indexMetaTree.findOne(new IndexMetaKey(indexId), null);

            assert indexMeta != null : indexId;

            this.nextRowIdToBuild = getNextRowIdToBuild(indexMeta);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error reading next row ID uuid to build: [{}]", e, createStorageInfo());
        }
    }

    /** Constant that represents the absence of value in {@link ScanCursor}. Not equivalent to {@code null} value. */
    private static final IndexRowKey NO_INDEX_ROW = () -> null;

    /**
     * Invoke a supplier that performs an operation that is not a data read.
     *
     * @param supplier Operation closure.
     * @return Whatever the supplier returns.
     */
    protected <T> T busyNonDataRead(Supplier<T> supplier) {
        return busy(supplier, false);
    }

    /**
     * Invoke a supplier that performs an operation that is a data read.
     *
     * @param supplier Operation closure.
     * @return Whatever the supplier returns.
     */
    protected <T> T busyDataRead(Supplier<T> supplier) {
        return busy(supplier, true);
    }

    private <T> T busy(Supplier<T> supplier, boolean read) {
        if (!busyLock.enterBusy()) {
            throwExceptionDependingOnIndexStorageState(state.get(), read, createStorageInfo());
        }

        try {
            return supplier.get();
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Cursor that always returns up-to-date next element.
     *
     * @param <R> Type of the returned value.
     */
    protected abstract class ScanCursor<R> implements PeekCursor<R> {
        protected final TreeT localTree = indexTree;

        private final @Nullable K lower;

        private @Nullable Boolean hasNext;

        /**
         * Last row used in mapping in the {@link #next()} call. {@code null} upon cursor creation or after {@link #hasNext()} returned
         * {@code null}.
         */
        private @Nullable V treeRow;

        /**
         * Row used in the mapping of the latest {@link #peek()} call, that was performed after the last {@link #next()} call.
         * {@link #NO_INDEX_ROW} if there was no such call.
         */
        private @Nullable V peekedRow = (V) NO_INDEX_ROW;

        protected ScanCursor(@Nullable K lower) {
            this.lower = lower;
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
            return busyDataRead(() -> {
                try {
                    return advanceIfNeededBusy();
                } catch (IgniteInternalCheckedException e) {
                    throw new StorageException("Error while advancing the cursor", e);
                }
            });
        }

        @Override
        public R next() {
            return busyDataRead(() -> {
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
            return busyDataRead(() -> {
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
                peekedRow = lower == null ? localTree.findFirst() : localTree.findNext(lower, true);
            } else {
                peekedRow = localTree.findNext(treeRow, false);
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
}
