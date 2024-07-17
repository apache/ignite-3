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

package org.apache.ignite.internal.storage.rocksdb.index;

import static org.apache.ignite.internal.storage.rocksdb.RocksDbStorageUtils.INDEX_ID_SIZE;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbStorageUtils.KEY_BYTE_ORDER;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbStorageUtils.PARTITION_ID_SIZE;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbStorageUtils.TABLE_ID_SIZE;
import static org.apache.ignite.internal.storage.util.StorageUtils.initialRowIdToBuild;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionDependingOnIndexStorageState;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionDependingOnStorageState;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionDependingOnStorageStateOnRebalance;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionIfStorageInProgressOfRebalance;
import static org.apache.ignite.internal.storage.util.StorageUtils.transitionToTerminalState;
import static org.apache.ignite.internal.util.ArrayUtils.BYTE_EMPTY_ARRAY;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.rocksdb.RocksUtils;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.StorageRebalanceException;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.storage.index.PeekCursor;
import org.apache.ignite.internal.storage.rocksdb.PartitionDataHelper;
import org.apache.ignite.internal.storage.rocksdb.RocksDbMetaStorage;
import org.apache.ignite.internal.storage.util.StorageState;
import org.apache.ignite.internal.storage.util.StorageUtils;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteBatchWithIndex;

/**
 * Abstract index storage base on RocksDB.
 */
public abstract class AbstractRocksDbIndexStorage implements IndexStorage {
    /** Common prefix for keys in all index storages, containing IDs of different entities. */
    public static final int PREFIX_WITH_IDS_LENGTH = TABLE_ID_SIZE + INDEX_ID_SIZE + PARTITION_ID_SIZE;

    private final int tableId;

    protected final int indexId;

    protected final int partitionId;

    private final boolean pk;

    private final RocksDbMetaStorage indexMetaStorage;

    /** Busy lock. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Current state of the storage. */
    protected final AtomicReference<StorageState> state = new AtomicReference<>(StorageState.RUNNABLE);

    /** Row ID for which the index needs to be built, {@code null} means that the index building has completed. */
    private volatile @Nullable RowId nextRowIdToBuild;

    AbstractRocksDbIndexStorage(int tableId, int indexId, int partitionId, RocksDbMetaStorage indexMetaStorage, boolean pk) {
        this.tableId = tableId;
        this.indexId = indexId;
        this.indexMetaStorage = indexMetaStorage;
        this.partitionId = partitionId;
        this.pk = pk;

        nextRowIdToBuild = indexMetaStorage.getNextRowIdToBuild(tableId, indexId, partitionId, pk);
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

            WriteBatchWithIndex writeBatch = PartitionDataHelper.requireWriteBatch();

            indexMetaStorage.putNextRowIdToBuild(writeBatch, tableId, indexId, partitionId, rowId);

            nextRowIdToBuild = rowId;

            return null;
        });
    }

    /**
     * Closes the hash index storage.
     */
    public void close() {
        if (!transitionToTerminalState(StorageState.CLOSED, state)) {
            return;
        }

        busyLock.block();
    }

    /**
     * Transitions the storage to the {@link StorageState#DESTROYED} state and blocks the busy lock.
     */
    public void transitionToDestroyedState() {
        if (!transitionToTerminalState(StorageState.DESTROYED, state)) {
            return;
        }

        busyLock.block();
    }

    /**
     * Prepares the storage for rebalancing.
     *
     * @throws StorageRebalanceException If there was an error when starting the rebalance.
     */
    public void startRebalance(WriteBatch writeBatch) {
        if (!state.compareAndSet(StorageState.RUNNABLE, StorageState.REBALANCE)) {
            throwExceptionDependingOnStorageStateOnRebalance(state.get(), createStorageInfo());
        }

        // Changed storage states and expect all storage operations to stop soon.
        busyLock.block();

        try {
            destroyData(writeBatch);
        } catch (RocksDBException e) {
            throw new StorageRebalanceException("Error when trying to start rebalancing storage: " + createStorageInfo(), e);
        } finally {
            busyLock.unblock();
        }
    }

    /**
     * Aborts storage rebalancing.
     *
     * @throws StorageRebalanceException If there was an error when aborting the rebalance.
     */
    public void abortRebalance(WriteBatch writeBatch) {
        if (!state.compareAndSet(StorageState.REBALANCE, StorageState.RUNNABLE)) {
            throwExceptionDependingOnStorageStateOnRebalance(state.get(), createStorageInfo());
        }

        try {
            destroyData(writeBatch);
        } catch (RocksDBException e) {
            throw new StorageRebalanceException("Error when trying to abort rebalancing storage: " + createStorageInfo(), e);
        }
    }

    /**
     * Completes storage rebalancing.
     *
     * @throws StorageRebalanceException If there was an error when finishing the rebalance.
     */
    public void finishRebalance() {
        if (!state.compareAndSet(StorageState.REBALANCE, StorageState.RUNNABLE)) {
            throwExceptionDependingOnStorageStateOnRebalance(state.get(), createStorageInfo());
        }
    }

    /**
     * Prepares the storage  for cleanup.
     *
     * <p>After cleanup (successful or not), method {@link #finishCleanup()} must be called.
     */
    public void startCleanup(WriteBatch writeBatch) throws RocksDBException {
        if (!state.compareAndSet(StorageState.RUNNABLE, StorageState.CLEANUP)) {
            throwExceptionDependingOnStorageState(state.get(), createStorageInfo());
        }

        // Changed storage states and expect all storage operations to stop soon.
        busyLock.block();

        destroyData(writeBatch);
    }

    /**
     * Finishes cleanup up the storage.
     */
    public void finishCleanup() {
        if (state.compareAndSet(StorageState.CLEANUP, StorageState.RUNNABLE)) {
            busyLock.unblock();
        }
    }

    /**
     * Invoke a supplier that performs an operation that is not a data read.
     *
     * @param supplier Operation closure.
     * @return Whatever the supplier returns.
     */
    <V> V busyNonDataRead(Supplier<V> supplier) {
        return busy(supplier, false);
    }

    /**
     * Invoke a supplier that performs an operation that is a data read.
     *
     * @param supplier Operation closure.
     * @return Whatever the supplier returns.
     */
    <V> V busyDataRead(Supplier<V> supplier) {
        return busy(supplier, true);
    }

    private <V> V busy(Supplier<V> supplier, boolean read) {
        if (!busyLock.enterBusy()) {
            throwExceptionDependingOnIndexStorageState(state.get(), read, createStorageInfo());
        }

        try {
            return supplier.get();
        } finally {
            busyLock.leaveBusy();
        }
    }

    String createStorageInfo() {
        return IgniteStringFormatter.format("indexId={}, partitionId={}", indexId, partitionId);
    }

    /**
     * Deletes the data associated with the index, using passed write batch for the operation.
     *
     * @throws RocksDBException If failed to delete data.
     */
    public final void destroyData(WriteBatch writeBatch) throws RocksDBException {
        clearIndex(writeBatch);

        indexMetaStorage.removeNextRowIdToBuild(writeBatch, tableId, indexId, partitionId);

        nextRowIdToBuild = pk ? null : initialRowIdToBuild(partitionId);
    }

    /** Method that needs to be overridden by the inheritors to remove all implementation specific data for this index. */
    abstract void clearIndex(WriteBatch writeBatch) throws RocksDBException;

    /**
     * Cursor that always returns up-to-date next element.
     */
    protected abstract class UpToDatePeekCursor<T> implements PeekCursor<T> {
        private final Slice upperBoundSlice;
        private final byte[] lowerBound;

        private final ReadOptions options;
        private final RocksIterator it;

        private @Nullable Boolean hasNext;

        /**
         * Last key used in mapping in the {@link #next()} call.
         * {@code null} upon cursor creation or after {@link #hasNext()} returned {@code null}.
         */
        private byte @Nullable [] key;

        /**
         * Row used in the mapping of the latest {@link #peek()} call, that was performed after the last {@link #next()} call.
         * {@link ArrayUtils#BYTE_EMPTY_ARRAY} if there was no such call.
         */
        private byte @Nullable [] peekedKey = BYTE_EMPTY_ARRAY;

        UpToDatePeekCursor(byte[] upperBound, ColumnFamily indexCf, byte[] lowerBound) {
            this.lowerBound = lowerBound;
            upperBoundSlice = new Slice(upperBound);
            options = new ReadOptions().setIterateUpperBound(upperBoundSlice);
            it = indexCf.newIterator(options);
        }

        /**
         * Maps the key from the index into the required result.
         */
        protected abstract T map(ByteBuffer byteBuffer);

        @Override
        public void close() {
            try {
                closeAll(it, options, upperBoundSlice);
            } catch (Exception e) {
                throw new StorageException("Error closing cursor", e);
            }
        }

        @Override
        public boolean hasNext() {
            return busyDataRead(this::advanceIfNeededBusy);
        }

        @Override
        public T next() {
            return busyDataRead(() -> {
                if (!advanceIfNeededBusy()) {
                    throw new NoSuchElementException();
                }

                this.hasNext = null;

                return map(ByteBuffer.wrap(key).order(KEY_BYTE_ORDER));
            });
        }

        @Override
        public @Nullable T peek() {
            return busyDataRead(() -> {
                throwExceptionIfStorageInProgressOfRebalance(state.get(), AbstractRocksDbIndexStorage.this::createStorageInfo);

                byte[] res = peekBusy();

                if (res == null) {
                    return null;
                } else {
                    return map(ByteBuffer.wrap(res).order(KEY_BYTE_ORDER));
                }
            });
        }

        private byte @Nullable [] peekBusy() {
            if (hasNext != null) {
                return key;
            }

            refreshAndPrepareRocksIteratorBusy();

            if (!it.isValid()) {
                RocksUtils.checkIterator(it);

                peekedKey = null;
            } else {
                peekedKey = it.key();
            }

            return peekedKey;
        }

        private boolean advanceIfNeededBusy() throws StorageException {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), AbstractRocksDbIndexStorage.this::createStorageInfo);

            //noinspection ArrayEquality
            key = (peekedKey == BYTE_EMPTY_ARRAY) ? peekBusy() : peekedKey;
            peekedKey = BYTE_EMPTY_ARRAY;

            hasNext = key != null;
            return hasNext;
        }

        private void refreshAndPrepareRocksIteratorBusy() {
            try {
                it.refresh();
            } catch (RocksDBException e) {
                throw new StorageException("Error refreshing an iterator", e);
            }

            if (key == null) {
                it.seek(lowerBound);
            } else {
                it.seekForPrev(key);

                if (it.isValid()) {
                    it.next();
                } else {
                    RocksUtils.checkIterator(it);

                    it.seek(lowerBound);
                }
            }
        }
    }

    protected void throwExceptionIfIndexNotBuilt() {
        StorageUtils.throwExceptionIfIndexIsNotBuilt(nextRowIdToBuild, this::createStorageInfo);
    }
}
