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

import static org.apache.ignite.internal.storage.rocksdb.RocksDbStorageUtils.KEY_BYTE_ORDER;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionDependingOnStorageState;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionDependingOnStorageStateOnRebalance;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionIfStorageInProgressOfRebalance;
import static org.apache.ignite.internal.util.ArrayUtils.BYTE_EMPTY_ARRAY;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
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
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.IgniteStringFormatter;
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
abstract class AbstractRocksDbIndexStorage implements IndexStorage {
    private final int indexId;

    final PartitionDataHelper helper;

    private final RocksDbMetaStorage indexMetaStorage;

    /** Busy lock. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Current state of the storage. */
    protected final AtomicReference<StorageState> state = new AtomicReference<>(StorageState.RUNNABLE);

    /** Row ID for which the index needs to be built, {@code null} means that the index building has completed. */
    private volatile @Nullable RowId nextRowIdToBuilt;

    AbstractRocksDbIndexStorage(int indexId, PartitionDataHelper helper, RocksDbMetaStorage indexMetaStorage) {
        this.indexId = indexId;
        this.helper = helper;
        this.indexMetaStorage = indexMetaStorage;

        int partitionId = helper.partitionId();

        nextRowIdToBuilt = indexMetaStorage.getNextRowIdToBuilt(indexId, partitionId, RowId.lowestRowId(partitionId));
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

            WriteBatchWithIndex writeBatch = PartitionDataHelper.requireWriteBatch();

            indexMetaStorage.putNextRowIdToBuilt(writeBatch, indexId, helper.partitionId(), rowId);

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
    public void abortReblance(WriteBatch writeBatch) {
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

    <V> V busy(Supplier<V> supplier) {
        if (!busyLock.enterBusy()) {
            throwExceptionDependingOnStorageState(state.get(), createStorageInfo());
        }

        try {
            return supplier.get();
        } finally {
            busyLock.leaveBusy();
        }
    }

    String createStorageInfo() {
        return IgniteStringFormatter.format("indexId={}, partitionId={}", indexId, helper.partitionId());
    }

    /**
     * Deletes the data associated with the index, using passed write batch for the operation.
     *
     * @throws RocksDBException If failed to delete data.
     */
    abstract void destroyData(WriteBatch writeBatch) throws RocksDBException;

    /**
     * Cursor that always returns up-to-date next element.
     */
    protected final class UpToDatePeekCursor<T> implements PeekCursor<T> {
        private final Slice upperBoundSlice;
        private final byte[] lowerBound;

        private final ReadOptions options;
        private final RocksIterator it;
        private final Function<ByteBuffer, T> mapper;

        @Nullable
        private Boolean hasNext;

        private byte @Nullable [] key;

        private byte @Nullable [] peekedKey = BYTE_EMPTY_ARRAY;

        UpToDatePeekCursor(byte[] upperBound, ColumnFamily indexCf, Function<ByteBuffer, T> mapper, byte[] lowerBound) {
            this.lowerBound = lowerBound;
            upperBoundSlice = new Slice(upperBound);
            options = new ReadOptions().setIterateUpperBound(upperBoundSlice);
            it = indexCf.newIterator(options);

            this.mapper = mapper;
        }

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
            return busy(this::advanceIfNeeded);
        }

        @Override
        public T next() {
            return busy(() -> {
                if (!advanceIfNeeded()) {
                    throw new NoSuchElementException();
                }

                this.hasNext = null;

                return mapper.apply(ByteBuffer.wrap(key).order(KEY_BYTE_ORDER));
            });
        }

        @Override
        public @Nullable T peek() {
            return busy(() -> {
                throwExceptionIfStorageInProgressOfRebalance(state.get(), AbstractRocksDbIndexStorage.this::createStorageInfo);

                byte[] res = peek0();

                if (res == null) {
                    return null;
                } else {
                    return mapper.apply(ByteBuffer.wrap(res).order(KEY_BYTE_ORDER));
                }
            });
        }

        private byte @Nullable [] peek0() {
            if (hasNext != null) {
                return key;
            }

            refreshAndPrepareRocksIterator();

            if (!it.isValid()) {
                RocksUtils.checkIterator(it);

                peekedKey = null;
            } else {
                peekedKey = it.key();
            }

            return peekedKey;
        }

        private boolean advanceIfNeeded() throws StorageException {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), AbstractRocksDbIndexStorage.this::createStorageInfo);

            //noinspection ArrayEquality
            key = (peekedKey == BYTE_EMPTY_ARRAY) ? peek0() : peekedKey;
            peekedKey = BYTE_EMPTY_ARRAY;

            hasNext = key != null;
            return hasNext;
        }

        private void refreshAndPrepareRocksIterator() {
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
}
