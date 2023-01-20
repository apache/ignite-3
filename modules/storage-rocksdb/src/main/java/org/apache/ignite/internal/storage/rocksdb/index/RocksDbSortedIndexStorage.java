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

import static org.apache.ignite.internal.rocksdb.RocksUtils.incrementArray;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionDependingOnStorageState;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionDependingOnStorageStateOnRebalance;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionIfStorageInProgressOfRebalance;
import static org.apache.ignite.internal.util.ArrayUtils.BYTE_EMPTY_ARRAY;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.internal.binarytuple.BinaryTupleCommon;
import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.rocksdb.RocksUtils;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.StorageRebalanceException;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.IndexRowImpl;
import org.apache.ignite.internal.storage.index.PeekCursor;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.rocksdb.RocksDbMvPartitionStorage;
import org.apache.ignite.internal.storage.util.StorageState;
import org.apache.ignite.internal.util.Cursor;
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
 * {@link SortedIndexStorage} implementation based on RocksDB.
 *
 * <p>This storage uses the following format for keys:
 * <pre>
 * Partition ID - 2 bytes
 * Tuple value - variable length
 * Row ID (UUID) - 16 bytes
 * </pre>
 *
 * <p>We use an empty array as values, because all required information can be extracted from the key.
 */
public class RocksDbSortedIndexStorage implements SortedIndexStorage {
    private static final int ROW_ID_SIZE = Long.BYTES * 2;

    private static final ByteOrder ORDER = ByteOrder.BIG_ENDIAN;

    private final SortedIndexDescriptor descriptor;

    private final ColumnFamily indexCf;

    private final RocksDbMvPartitionStorage partitionStorage;

    /** Busy lock. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Current state of the storage. */
    private final AtomicReference<StorageState> state = new AtomicReference<>(StorageState.RUNNABLE);

    /**
     * Creates a storage.
     *
     * @param descriptor Sorted Index descriptor.
     * @param indexCf Column family that stores the index data.
     * @param partitionStorage Partition storage of the corresponding index.
     */
    public RocksDbSortedIndexStorage(
            SortedIndexDescriptor descriptor,
            ColumnFamily indexCf,
            RocksDbMvPartitionStorage partitionStorage
    ) {
        this.descriptor = descriptor;
        this.indexCf = indexCf;
        this.partitionStorage = partitionStorage;
    }

    @Override
    public SortedIndexDescriptor indexDescriptor() {
        return descriptor;
    }

    @Override
    public Cursor<RowId> get(BinaryTuple key) throws StorageException {
        return busy(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            BinaryTuplePrefix keyPrefix = BinaryTuplePrefix.fromBinaryTuple(key);

            return scan(keyPrefix, keyPrefix, true, true, this::decodeRowId);
        });
    }

    @Override
    public void put(IndexRow row) {
        busy(() -> {
            try {
                WriteBatchWithIndex writeBatch = partitionStorage.currentWriteBatch();

                writeBatch.put(indexCf.handle(), rocksKey(row), BYTE_EMPTY_ARRAY);

                return null;
            } catch (RocksDBException e) {
                throw new StorageException("Unable to insert data into sorted index. Index ID: " + descriptor.id(), e);
            }
        });
    }

    @Override
    public void remove(IndexRow row) {
        busy(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            try {
                WriteBatchWithIndex writeBatch = partitionStorage.currentWriteBatch();

                writeBatch.delete(indexCf.handle(), rocksKey(row));

                return null;
            } catch (RocksDBException e) {
                throw new StorageException("Unable to remove data from sorted index. Index ID: " + descriptor.id(), e);
            }
        });
    }

    @Override
    public PeekCursor<IndexRow> scan(@Nullable BinaryTuplePrefix lowerBound, @Nullable BinaryTuplePrefix upperBound, int flags) {
        return busy(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            boolean includeLower = (flags & GREATER_OR_EQUAL) != 0;
            boolean includeUpper = (flags & LESS_OR_EQUAL) != 0;

            return scan(lowerBound, upperBound, includeLower, includeUpper, this::decodeRow);
        });
    }

    private <T> PeekCursor<T> scan(
            @Nullable BinaryTuplePrefix lowerBound,
            @Nullable BinaryTuplePrefix upperBound,
            boolean includeLower,
            boolean includeUpper,
            Function<ByteBuffer, T> mapper
    ) {
        byte[] lowerBoundBytes;

        if (lowerBound == null) {
            lowerBoundBytes = null;
        } else {
            lowerBoundBytes = rocksPrefix(lowerBound);

            // Skip the lower bound, if needed (RocksDB includes the lower bound by default).
            if (!includeLower) {
                setEqualityFlag(lowerBoundBytes);
            }
        }

        byte[] upperBoundBytes;

        if (upperBound == null) {
            upperBoundBytes = null;
        } else {
            upperBoundBytes = rocksPrefix(upperBound);

            // Include the upper bound, if needed (RocksDB excludes the upper bound by default).
            if (includeUpper) {
                setEqualityFlag(upperBoundBytes);
            }
        }

        return createScanCursor(lowerBoundBytes, upperBoundBytes, mapper);
    }

    private <T> PeekCursor<T> createScanCursor(
            byte @Nullable [] lowerBound,
            byte @Nullable [] upperBound,
            Function<ByteBuffer, T> mapper
    ) {
        Slice upperBoundSlice = upperBound == null ? new Slice(partitionStorage.partitionEndPrefix()) : new Slice(upperBound);

        ReadOptions options = new ReadOptions().setIterateUpperBound(upperBoundSlice);

        RocksIterator it = indexCf.newIterator(options);

        return new PeekCursor<>() {
            @Nullable
            private Boolean hasNext;

            private byte @Nullable [] key;

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
                return busy(() -> {
                    advanceIfNeeded();

                    return hasNext;
                });
            }

            @Override
            public T next() {
                return busy(() -> {
                    advanceIfNeeded();

                    boolean hasNext = this.hasNext;

                    if (!hasNext) {
                        throw new NoSuchElementException();
                    }

                    this.hasNext = null;

                    return mapper.apply(ByteBuffer.wrap(key).order(ORDER));
                });
            }

            @Override
            public @Nullable T peek() {
                return busy(() -> {
                    throwExceptionIfStorageInProgressOfRebalance(state.get(), RocksDbSortedIndexStorage.this::createStorageInfo);

                    if (hasNext != null) {
                        if (hasNext) {
                            return mapper.apply(ByteBuffer.wrap(key).order(ORDER));
                        }

                        return null;
                    }

                    refreshAndPrepareRocksIterator();

                    if (!it.isValid()) {
                        RocksUtils.checkIterator(it);

                        return null;
                    } else {
                        return mapper.apply(ByteBuffer.wrap(it.key()).order(ORDER));
                    }
                });
            }

            private void advanceIfNeeded() throws StorageException {
                throwExceptionIfStorageInProgressOfRebalance(state.get(), RocksDbSortedIndexStorage.this::createStorageInfo);

                if (hasNext != null) {
                    return;
                }

                refreshAndPrepareRocksIterator();

                if (!it.isValid()) {
                    RocksUtils.checkIterator(it);

                    hasNext = false;
                } else {
                    key = it.key();

                    hasNext = true;
                }
            }

            private void refreshAndPrepareRocksIterator() {
                try {
                    it.refresh();
                } catch (RocksDBException e) {
                    throw new StorageException("Error refreshing an iterator", e);
                }

                if (key == null) {
                    it.seek(lowerBound == null ? partitionStorage.partitionStartPrefix() : lowerBound);
                } else {
                    it.seekForPrev(key);

                    if (it.isValid()) {
                        it.next();
                    } else {
                        RocksUtils.checkIterator(it);

                        it.seek(lowerBound == null ? partitionStorage.partitionStartPrefix() : lowerBound);
                    }
                }
            }
        };
    }

    private static void setEqualityFlag(byte[] prefix) {
        // Flags start after the partition ID.
        byte flags = prefix[Short.BYTES];

        prefix[Short.BYTES] = (byte) (flags | BinaryTupleCommon.EQUALITY_FLAG);
    }

    private IndexRow decodeRow(ByteBuffer bytes) {
        assert bytes.getShort(0) == partitionStorage.partitionId();

        var tuple = new BinaryTuple(descriptor.binaryTupleSchema(), binaryTupleSlice(bytes));

        return new IndexRowImpl(tuple, decodeRowId(bytes));
    }

    private RowId decodeRowId(ByteBuffer bytes) {
        // RowId UUID is located at the last 16 bytes of the key
        long mostSignificantBits = bytes.getLong(bytes.limit() - Long.BYTES * 2);
        long leastSignificantBits = bytes.getLong(bytes.limit() - Long.BYTES);

        return new RowId(partitionStorage.partitionId(), mostSignificantBits, leastSignificantBits);
    }

    private byte[] rocksPrefix(BinaryTuplePrefix prefix) {
        ByteBuffer bytes = prefix.byteBuffer();

        return ByteBuffer.allocate(Short.BYTES + bytes.remaining())
                .order(ORDER)
                .putShort((short) partitionStorage.partitionId())
                .put(bytes)
                .array();
    }

    private byte[] rocksKey(IndexRow row) {
        ByteBuffer bytes = row.indexColumns().byteBuffer();

        return ByteBuffer.allocate(Short.BYTES + bytes.remaining() + ROW_ID_SIZE)
                .order(ORDER)
                .putShort((short) partitionStorage.partitionId())
                .put(bytes)
                .putLong(row.rowId().mostSignificantBits())
                .putLong(row.rowId().leastSignificantBits())
                .array();
    }

    private static ByteBuffer binaryTupleSlice(ByteBuffer key) {
        return key.duplicate()
                // Discard partition ID.
                .position(Short.BYTES)
                // Discard row ID.
                .limit(key.limit() - ROW_ID_SIZE)
                .slice()
                .order(ByteOrder.LITTLE_ENDIAN);
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
    }

    /**
     * Deletes the data associated with the index, using passed write batch for the operation.
     *
     * @throws RocksDBException If failed to delete data.
     */
    public void destroyData(WriteBatch writeBatch) throws RocksDBException {
        byte[] constantPrefix = ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN).putShort((short) partitionStorage.partitionId()).array();

        byte[] rangeEnd = incrementArray(constantPrefix);

        assert rangeEnd != null;

        writeBatch.deleteRange(indexCf.handle(), constantPrefix, rangeEnd);
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

    private String createStorageInfo() {
        return IgniteStringFormatter.format("indexId={}, partitionId={}", descriptor.id(), partitionStorage.partitionId());
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
}
