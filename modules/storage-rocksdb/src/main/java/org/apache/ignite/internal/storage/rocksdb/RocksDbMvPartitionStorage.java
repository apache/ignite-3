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

package org.apache.ignite.internal.storage.rocksdb;

import static java.lang.ThreadLocal.withInitial;
import static java.nio.ByteBuffer.allocate;
import static java.nio.ByteBuffer.allocateDirect;
import static java.util.Arrays.copyOf;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.storage.rocksdb.PartitionDataHelper.DATA_ID_SIZE;
import static org.apache.ignite.internal.storage.rocksdb.PartitionDataHelper.DATA_ID_WITH_TX_STATE_SIZE;
import static org.apache.ignite.internal.storage.rocksdb.PartitionDataHelper.MAX_KEY_SIZE;
import static org.apache.ignite.internal.storage.rocksdb.PartitionDataHelper.ROW_ID_OFFSET;
import static org.apache.ignite.internal.storage.rocksdb.PartitionDataHelper.ROW_PREFIX_SIZE;
import static org.apache.ignite.internal.storage.rocksdb.PartitionDataHelper.THREAD_LOCAL_STATE;
import static org.apache.ignite.internal.storage.rocksdb.PartitionDataHelper.deserializeRow;
import static org.apache.ignite.internal.storage.rocksdb.PartitionDataHelper.getFromBatchAndDb;
import static org.apache.ignite.internal.storage.rocksdb.PartitionDataHelper.isTombstone;
import static org.apache.ignite.internal.storage.rocksdb.PartitionDataHelper.putTimestampDesc;
import static org.apache.ignite.internal.storage.rocksdb.PartitionDataHelper.readTimestampDesc;
import static org.apache.ignite.internal.storage.rocksdb.PartitionDataHelper.requireWriteBatch;
import static org.apache.ignite.internal.storage.rocksdb.PartitionDataHelper.setLastBit;
import static org.apache.ignite.internal.storage.rocksdb.PartitionDataHelper.wrapIterator;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbMetaStorage.ESTIMATED_SIZE_PREFIX;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbMetaStorage.LEASE_PREFIX;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbMetaStorage.PARTITION_CONF_PREFIX;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbMetaStorage.PARTITION_META_PREFIX;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbStorageUtils.KEY_BYTE_ORDER;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbStorageUtils.createKey;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbStorageUtils.normalize;
import static org.apache.ignite.internal.storage.rocksdb.instance.SharedRocksDbInstance.DFLT_WRITE_OPTS;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionDependingOnStorageState;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionDependingOnStorageStateOnRebalance;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionIfStorageInProgressOfRebalance;
import static org.apache.ignite.internal.storage.util.StorageUtils.transitionToTerminalState;
import static org.apache.ignite.internal.util.ByteUtils.bytesToLong;
import static org.apache.ignite.internal.util.ByteUtils.longToBytes;
import static org.apache.ignite.internal.util.ByteUtils.putLongToBytes;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.rocksdb.RocksIteratorAdapter;
import org.apache.ignite.internal.rocksdb.RocksUtils;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.PartitionTimestampCursor;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.StorageRebalanceException;
import org.apache.ignite.internal.storage.TxIdMismatchException;
import org.apache.ignite.internal.storage.gc.GcEntry;
import org.apache.ignite.internal.storage.rocksdb.GarbageCollector.AddResult;
import org.apache.ignite.internal.storage.util.LocalLocker;
import org.apache.ignite.internal.storage.util.StorageState;
import org.apache.ignite.internal.tx.TransactionIds;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.AbstractWriteBatch;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteBatchWithIndex;

/**
 * Multi-versioned partition storage implementation based on RocksDB.
 *
 * <p>It uses two RocksDB Column Families to store partition data: one for managing transaction state of rows and the other for
 * storing actual row data. We call these Column Families "Partition CF" and "Data CF" respectfully.
 *
 * <p>Partition Column Family has the following format:
 *
 * <p>Key:
 * <pre>{@code
 * For write-intents
 * | Table ID (4 bytes, BE) | Partition ID (2 bytes, BE) | Row ID (16 bytes, BE) |
 *
 * For committed rows
 * | Table ID (4 bytes, BE) | Partition ID (2 bytes, BE) | Row ID (16 bytes, BE) | Commit Timestamp (8 bytes, DESC) |
 * }</pre>
 *
 * <p>Value:
 *
 * <pre>{@code
 * For write-intents
 * | Data ID (24 bytes, BE) | Commit Table ID (16 bytes) | Commit Partition ID (2 bytes) |
 *
 * For committed rows
 * | Data ID (24 bytes, BE) |
 * }</pre>
 *
 * <p>Each Data ID is then used as a key inside the Data Column Family and uniquely identifies actual row data.
 *
 * <p>Key:
 * <pre>{@code
 * | Data ID (24 bytes, BE) |
 * }</pre>
 *
 * <p>Value:
 *
 * <pre>{@code
 * | Row data |
 * }</pre>
 *
 * <p>BE means Big Endian, meaning that lexicographical bytes order matches a natural order of partitions.
 *
 * <p>DESC means that timestamps are sorted from newest to oldest (N2O).
 * Please refer to {@link PartitionDataHelper#putTimestampDesc(ByteBuffer, HybridTimestamp)} to see how it's achieved. Missing timestamp
 * could be interpreted as a moment infinitely far away in the future.
 */
public class RocksDbMvPartitionStorage implements MvPartitionStorage {
    /** Thread-local on-heap byte buffer instance to use for key manipulations. */
    private static final ThreadLocal<ByteBuffer> HEAP_COMMITTED_DATA_ID_KEY_BUFFER =
            withInitial(() -> allocate(MAX_KEY_SIZE).order(KEY_BYTE_ORDER));

    private static final ThreadLocal<ByteBuffer> HEAP_DATA_ID_KEY_BUFFER =
            withInitial(() -> allocate(ROW_PREFIX_SIZE).order(KEY_BYTE_ORDER));

    private static final ThreadLocal<ByteBuffer> DIRECT_DATA_ID_KEY_BUFFER =
            withInitial(() -> allocateDirect(MAX_KEY_SIZE).order(KEY_BYTE_ORDER));

    private static final ThreadLocal<ByteBuffer> TX_STATE_BUFFER =
            withInitial(() -> allocate(DATA_ID_WITH_TX_STATE_SIZE).order(KEY_BYTE_ORDER));

    /** Table storage instance. */
    private final RocksDbTableStorage tableStorage;

    /**
     * Partition ID (should be treated as an unsigned short).
     *
     * <p>Partition IDs are always stored in the big endian order, since they need to be compared lexicographically.
     */
    private final int partitionId;

    private final int tableId;

    /** RocksDb instance. */
    private final RocksDB db;

    /** Helper for the rocksdb partition. */
    private final PartitionDataHelper helper;

    /** Garbage collector. */
    private final GarbageCollector gc;

    /** Meta column family. */
    private final ColumnFamilyHandle meta;

    /** Read options for regular reads. */
    private final ReadOptions readOpts = new ReadOptions();

    /** Key to store applied index value in meta. */
    private final byte[] lastAppliedIndexAndTermKey;

    /** Key to store group config in meta. */
    private final byte[] lastGroupConfigKey;

    /** Key to store the lease start time. */
    private final byte[] leaseKey;

    private final byte[] estimatedSizeKey;

    /** On-heap-cached last applied index value. */
    private volatile long lastAppliedIndex;

    /** On-heap-cached last applied term value. */
    private volatile long lastAppliedTerm;

    /** On-heap-cached lease start time value. */
    private volatile long leaseStartTime;

    /** On-heap-cached last committed group configuration. */
    private volatile byte @Nullable [] lastGroupConfig;

    private volatile long estimatedSize;

    /** Busy lock. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Current state of the storage. */
    private final AtomicReference<StorageState> state = new AtomicReference<>(StorageState.RUNNABLE);

    /**
     * Constructor.
     *
     * @param tableStorage Table storage.
     * @param partitionId Partition id.
     */
    public RocksDbMvPartitionStorage(RocksDbTableStorage tableStorage, int partitionId) {
        this.tableStorage = tableStorage;
        this.partitionId = partitionId;
        tableId = tableStorage.getTableId();

        db = tableStorage.db();
        meta = tableStorage.metaCfHandle();

        int tableId = tableStorage.getTableId();
        helper = new PartitionDataHelper(tableId, partitionId, tableStorage.partitionCfHandle(), tableStorage.dataCfHandle());
        gc = new GarbageCollector(helper, db, readOpts, tableStorage.gcQueueHandle());

        lastAppliedIndexAndTermKey = createKey(PARTITION_META_PREFIX, tableId, partitionId);
        lastGroupConfigKey = createKey(PARTITION_CONF_PREFIX, tableId, partitionId);
        leaseKey = createKey(LEASE_PREFIX, tableId, partitionId);
        estimatedSizeKey = createKey(ESTIMATED_SIZE_PREFIX, tableId, partitionId);

        try {
            byte[] indexAndTerm = db.get(meta, readOpts, lastAppliedIndexAndTermKey);
            ByteBuffer buf = indexAndTerm == null ? null : ByteBuffer.wrap(indexAndTerm).order(ByteOrder.LITTLE_ENDIAN);

            lastAppliedIndex = buf == null ? 0 : buf.getLong();
            lastAppliedTerm = buf == null ? 0 : buf.getLong();

            lastGroupConfig = db.get(meta, readOpts, lastGroupConfigKey);

            byte[] leaseStartTimeBytes = db.get(meta, readOpts, leaseKey);
            ByteBuffer leaseStartTimeBuf = leaseStartTimeBytes == null
                    ? null
                    : ByteBuffer.wrap(leaseStartTimeBytes).order(ByteOrder.LITTLE_ENDIAN);

            leaseStartTime = leaseStartTimeBuf == null ? HybridTimestamp.MIN_VALUE.longValue() : leaseStartTimeBuf.getLong();

            byte[] estimatedSizeBytes = db.get(meta, readOpts, estimatedSizeKey);

            estimatedSize = estimatedSizeBytes == null ? 0 : bytesToLong(estimatedSizeBytes);
        } catch (RocksDBException e) {
            throw new StorageException(e);
        }
    }

    public PartitionDataHelper helper() {
        return helper;
    }

    @Override
    public <V> V runConsistently(WriteClosure<V> closure) throws StorageException {
        ThreadLocalState existingState = THREAD_LOCAL_STATE.get();

        if (existingState != null) {
            return closure.execute(existingState.locker);
        } else {
            return busy(() -> {
                LocalLocker locker = new LocalLocker(helper.lockByRowId);

                try (var writeBatch = new WriteBatchWithIndex()) {
                    var state = new ThreadLocalState(writeBatch, locker);
                    THREAD_LOCAL_STATE.set(state);

                    long oldAppliedIndex = lastAppliedIndex;
                    byte[] oldGroupConfig = lastGroupConfig;

                    state.pendingAppliedIndex = lastAppliedIndex;
                    state.pendingAppliedTerm = lastAppliedTerm;
                    state.pendingGroupConfig = lastGroupConfig;

                    try {
                        V res = closure.execute(locker);

                        if (writeBatch.count() > 0) {
                            // Check if the current thread's modifications have affected the estimated size. If they have,
                            // we need to use synchronization in order to atomically update and persist the new estimated size.
                            if (state.pendingEstimatedSizeDiff != 0) {
                                synchronized (this) {
                                    long newEstimatedSize = estimatedSize + state.pendingEstimatedSizeDiff;

                                    writeBatch.put(meta, estimatedSizeKey, longToBytes(newEstimatedSize));

                                    db.write(DFLT_WRITE_OPTS, writeBatch);

                                    estimatedSize = newEstimatedSize;
                                }
                            } else {
                                db.write(DFLT_WRITE_OPTS, writeBatch);
                            }

                            // Here we assume that no two threads would try to update these values concurrently.
                            if (oldAppliedIndex != state.pendingAppliedIndex) {
                                lastAppliedIndex = state.pendingAppliedIndex;
                                lastAppliedTerm = state.pendingAppliedTerm;
                            }
                            //noinspection ArrayEquality
                            if (oldGroupConfig != state.pendingGroupConfig) {
                                lastGroupConfig = state.pendingGroupConfig;
                            }
                        }

                        return res;
                    } catch (RocksDBException e) {
                        throw new StorageException("Unable to apply a write batch to RocksDB instance.", e);
                    } finally {
                        locker.unlockAll();
                    }
                } finally {
                    THREAD_LOCAL_STATE.set(null);
                }
            });
        }
    }

    @Override
    public CompletableFuture<Void> flush(boolean trigger) {
        return busy(() -> tableStorage.awaitFlush(trigger));
    }

    /**
     * Returns the partition ID of the partition that this storage is responsible for.
     */
    public int partitionId() {
        return partitionId;
    }

    @Override
    public long lastAppliedIndex() {
        return busy(() -> {
            ThreadLocalState state = THREAD_LOCAL_STATE.get();

            return state == null ? lastAppliedIndex : state.pendingAppliedIndex;
        });
    }

    @Override
    public long lastAppliedTerm() {
        return busy(() -> {
            ThreadLocalState state = THREAD_LOCAL_STATE.get();

            return state == null ? lastAppliedTerm : state.pendingAppliedTerm;
        });
    }

    @Override
    public void lastApplied(long lastAppliedIndex, long lastAppliedTerm) throws StorageException {
        busy(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            try {
                savePendingLastApplied(requireWriteBatch(), lastAppliedIndex, lastAppliedTerm);

                return null;
            } catch (RocksDBException e) {
                throw new StorageException(e);
            }
        });
    }

    private void savePendingLastApplied(
            AbstractWriteBatch writeBatch,
            long lastAppliedIndex,
            long lastAppliedTerm
    ) throws RocksDBException {
        writeBatch.put(meta, lastAppliedIndexAndTermKey, longPairToBytes(lastAppliedIndex, lastAppliedTerm));

        ThreadLocalState state = THREAD_LOCAL_STATE.get();

        // TODO Complicated code.
        if (state != null) {
            state.pendingAppliedIndex = lastAppliedIndex;
            state.pendingAppliedTerm = lastAppliedTerm;
        }
    }

    private static byte[] longPairToBytes(long index, long term) {
        ByteBuffer buf = allocate(2 * Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);

        buf.putLong(index);
        buf.putLong(term);

        return buf.array();
    }

    @Override
    public byte @Nullable [] committedGroupConfiguration() {
        byte[] array = busy(() -> {
            ThreadLocalState state = THREAD_LOCAL_STATE.get();

            return state == null ? lastGroupConfig : state.pendingGroupConfig;
        });

        return array == null ? null : array.clone();
    }

    @Override
    public void committedGroupConfiguration(byte[] config) {
        busy(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            try {
                saveGroupConfiguration(requireWriteBatch(), config);

                return null;
            } catch (RocksDBException e) {
                throw new StorageException(e);
            }
        });
    }

    private void saveGroupConfiguration(AbstractWriteBatch writeBatch, byte[] config) throws RocksDBException {
        writeBatch.put(meta, lastGroupConfigKey, config);

        ThreadLocalState state = THREAD_LOCAL_STATE.get();

        // TODO Complicated code.
        if (state != null) {
            state.pendingGroupConfig = config.clone();
        }
    }

    @Override
    public @Nullable BinaryRow addWrite(RowId rowId, @Nullable BinaryRow row, UUID txId, int commitTableId, int commitPartitionId)
            throws TxIdMismatchException, StorageException {
        return busy(() -> {
            @SuppressWarnings("resource") WriteBatchWithIndex writeBatch = requireWriteBatch();

            assert rowIsLocked(rowId);

            try {
                // Check concurrent transaction data.
                byte[] uncommittedDataIdKey = createUncommittedDataIdKey(rowId);

                byte[] previousTxState = writeBatch.getFromBatchAndDB(db, helper.partCf, readOpts, uncommittedDataIdKey);

                // Previous value must belong to the same transaction.
                if (previousTxState != null) {
                    ByteBuffer previousTxStateBuffer = ByteBuffer.wrap(previousTxState);

                    validateTxId(previousTxStateBuffer, txId);

                    ByteBuffer dataId = readDataIdFromTxState(previousTxStateBuffer);

                    byte[] payloadKey = helper.createPayloadKey(dataId);

                    BinaryRow previousRow = null;

                    boolean isOldValueTombstone = isTombstone(dataId);

                    if (!isOldValueTombstone) {
                        byte[] previousRowBytes = writeBatch.getFromBatchAndDB(db, helper.dataCf, readOpts, payloadKey);

                        previousRow = deserializeRow(previousRowBytes);
                    }

                    // We need to flip the tombstone bit in case we are overwriting a previous Write Intent with a different
                    // tombstone bit.
                    if (isOldValueTombstone ^ (row == null)) {
                        setLastBit(previousTxState, DATA_ID_SIZE - 1, row == null);

                        writeBatch.put(helper.partCf, uncommittedDataIdKey, previousTxState);
                    }

                    // No need to update the Data ID key because it should be the same as already in the storage.
                    if (row != null) {
                        writeBatch.put(helper.dataCf, payloadKey, serializeBinaryRow(row));
                    }

                    return previousRow;
                } else {
                    ByteBuffer txState = createTxState(rowId, txId, commitTableId, commitPartitionId, row == null);

                    ByteBuffer dataId = readDataIdFromTxState(txState);

                    writeBatch.put(helper.partCf, uncommittedDataIdKey, txState.array());

                    if (row != null) {
                        writeBatch.put(helper.dataCf, helper.createPayloadKey(dataId), serializeBinaryRow(row));
                    }

                    return null;
                }
            } catch (RocksDBException e) {
                throw new StorageException("Failed to update a row in storage: " + createStorageInfo(), e);
            }
        });
    }

    private static ByteBuffer createDataId(RowId rowId, HybridTimestamp txTimestamp, boolean isTombstone) {
        ByteBuffer buffer = allocate(DATA_ID_SIZE).order(KEY_BYTE_ORDER);

        putDataId(buffer, rowId, txTimestamp, isTombstone);

        return buffer.rewind();
    }

    private static ByteBuffer createTxState(RowId rowId, UUID txId, int commitTableId, int commitPartitionId, boolean isTombstone) {
        ByteBuffer buffer = TX_STATE_BUFFER.get().clear();

        putDataId(buffer, rowId, TransactionIds.beginTimestamp(txId), isTombstone);

        return buffer
                .putLong(txId.getMostSignificantBits())
                .putLong(txId.getLeastSignificantBits())
                .putInt(commitTableId)
                .putShort((short) commitPartitionId)
                .rewind();
    }

    private static void putDataId(ByteBuffer buffer, RowId rowId, HybridTimestamp txTimestamp, boolean isTombstone) {
        long timestamp = txTimestamp.longValue();

        // We use the sign bit from the timestamp (which is always zero, because timestamp is always positive) to indicate whether
        // Data ID points to a tombstone. This can help to avoid indirect reads for tombstones.
        long timestampWithTombstoneFlag = timestamp << 1 | (isTombstone ? 1 : 0);

        buffer
                .putLong(rowId.mostSignificantBits())
                .putLong(rowId.leastSignificantBits())
                .putLong(timestampWithTombstoneFlag);
    }

    private static ByteBuffer readDataIdFromTxState(ByteBuffer txState) {
        int prevLimit = txState.limit();

        ByteBuffer dataId = txState
                .limit(DATA_ID_SIZE)
                .slice()
                .order(KEY_BYTE_ORDER);

        txState.position(txState.position() + DATA_ID_SIZE).limit(prevLimit);

        return dataId;
    }

    private static byte[] serializeBinaryRow(BinaryRow row) {
        return allocate(Short.BYTES + row.tupleSliceLength())
                .order(KEY_BYTE_ORDER)
                .putShort((short) row.schemaVersion())
                .put(row.tupleSlice())
                .array();
    }

    @Override
    public @Nullable BinaryRow abortWrite(RowId rowId) throws StorageException {
        return busy(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            @SuppressWarnings("resource") WriteBatchWithIndex writeBatch = requireWriteBatch();

            assert rowIsLocked(rowId);

            byte[] uncommittedDataIdKey = createUncommittedDataIdKey(rowId);

            try {
                byte[] dataIdWithTxState = writeBatch.getFromBatchAndDB(db, helper.partCf, readOpts, uncommittedDataIdKey);

                if (dataIdWithTxState == null) {
                    // The chain doesn't contain an uncommitted write intent.
                    return null;
                }

                ByteBuffer dataId = readDataIdFromTxState(ByteBuffer.wrap(dataIdWithTxState));

                byte[] payloadKey = helper.createPayloadKey(dataId);

                BinaryRow row = null;

                if (!isTombstone(dataId)) {
                    byte[] rowBytes = writeBatch.getFromBatchAndDB(db, helper.dataCf, readOpts, payloadKey);

                    row = deserializeRow(rowBytes);
                }

                // Perform unconditional remove for the key without associated timestamp.
                writeBatch.delete(helper.partCf, uncommittedDataIdKey);

                writeBatch.delete(helper.dataCf, payloadKey);

                return row;
            } catch (RocksDBException e) {
                throw new StorageException("Failed to roll back insert/update", e);
            }
        });
    }

    private static boolean rowIsLocked(RowId rowId) {
        ThreadLocalState state = THREAD_LOCAL_STATE.get();

        return state != null && state.locker.isLocked(rowId);
    }

    @Override
    public void commitWrite(RowId rowId, HybridTimestamp timestamp) throws StorageException {
        busy(() -> {
            WriteBatchWithIndex writeBatch = requireWriteBatch();

            assert rowIsLocked(rowId);

            byte[] dataIdKey = createCommittedDataIdKey(rowId, timestamp);

            byte[] uncommittedDataIdKey = copyOf(dataIdKey, ROW_PREFIX_SIZE);

            try {
                // Read a value associated with pending write.
                byte[] txState = writeBatch.getFromBatchAndDB(db, helper.partCf, readOpts, uncommittedDataIdKey);

                if (txState == null) {
                    // The chain doesn't contain an uncommitted write intent.
                    return null;
                }

                byte[] dataId = copyOf(txState, DATA_ID_SIZE);

                boolean isNewValueTombstone = isTombstone(dataId);

                AddResult addResult = gc.tryAddToGcQueue(writeBatch, rowId, timestamp, isNewValueTombstone);

                // Delete pending write.
                writeBatch.delete(helper.partCf, uncommittedDataIdKey);

                // We only write tombstone if the previous value for the same row id was not a tombstone.
                // So there won't be consecutive tombstones for the same row id.
                if (isNewValueTombstone && addResult != AddResult.WAS_VALUE) {
                    return null;
                }

                // Add timestamp to the key, and put the value back into the storage.
                writeBatch.put(helper.partCf, dataIdKey, dataId);

                updateEstimatedSize(isNewValueTombstone, addResult);

                return null;
            } catch (RocksDBException e) {
                throw new StorageException("Failed to commit row into storage", e);
            }
        });
    }

    @Override
    public void addWriteCommitted(RowId rowId, @Nullable BinaryRow row, HybridTimestamp commitTimestamp) throws StorageException {
        busy(() -> {
            WriteBatchWithIndex writeBatch = requireWriteBatch();

            assert rowIsLocked(rowId);

            boolean isNewValueTombstone = row == null;

            try {
                AddResult addResult = gc.tryAddToGcQueue(writeBatch, rowId, commitTimestamp, isNewValueTombstone);

                // We only write tombstone if the previous value for the same row id was not a tombstone.
                // So there won't be consecutive tombstones for the same row id.
                if (isNewValueTombstone && addResult != AddResult.WAS_VALUE) {
                    return null;
                }

                byte[] dataIdKey = createCommittedDataIdKey(rowId, commitTimestamp);

                ByteBuffer dataId = createDataId(rowId, commitTimestamp, isNewValueTombstone);

                writeBatch.put(helper.partCf, dataIdKey, dataId.array());

                // TODO IGNITE-16913 Add proper way to write row bytes into array without allocations.
                if (row != null) {
                    writeBatch.put(helper.dataCf, helper.createPayloadKey(dataId), serializeBinaryRow(row));
                }

                updateEstimatedSize(isNewValueTombstone, addResult);

                return null;
            } catch (RocksDBException e) {
                throw new StorageException("Failed to update a row in storage: " + createStorageInfo(), e);
            }
        });
    }

    private static void updateEstimatedSize(boolean isNewValueTombstone, AddResult gcQueueAddResult) throws RocksDBException {
        if (isNewValueTombstone) {
            if (gcQueueAddResult == AddResult.WAS_VALUE) {
                ThreadLocalState state = THREAD_LOCAL_STATE.get();

                state.pendingEstimatedSizeDiff -= 1;
            }
        } else {
            if (gcQueueAddResult != AddResult.WAS_VALUE) {
                ThreadLocalState state = THREAD_LOCAL_STATE.get();

                state.pendingEstimatedSizeDiff += 1;
            }
        }
    }

    @Override
    public ReadResult read(RowId rowId, HybridTimestamp timestamp) throws StorageException {
        return busy(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            if (rowId.partitionId() != partitionId) {
                throw new IllegalArgumentException(
                        String.format("RowId partition [%d] is not equal to storage partition [%d].", rowId.partitionId(), partitionId));
            }

            try (
                    // Set next partition as an upper bound.
                    RocksIterator baseIterator = db.newIterator(helper.partCf, helper.upperBoundReadOpts);
                    RocksIterator seekIterator = wrapIterator(baseIterator, helper.partCf)
            ) {
                if (lookingForLatestVersions(timestamp)) {
                    return readLatestVersion(rowId, seekIterator);
                } else {
                    return readByTimestamp(seekIterator, rowId, timestamp);
                }
            }
        });
    }

    private static boolean lookingForLatestVersions(HybridTimestamp timestamp) {
        return timestamp == HybridTimestamp.MAX_VALUE;
    }

    private ReadResult readLatestVersion(RowId rowId, RocksIterator seekIterator) {
        ByteBuffer dataIdKeyPrefix = prepareDirectDataIdKeyBuf(rowId)
                .position(0)
                .limit(ROW_PREFIX_SIZE);

        // Seek to the first appearance of row id if timestamp isn't set.
        // Since timestamps are sorted from newest to oldest, first occurrence will always be the latest version.
        seekIterator.seek(dataIdKeyPrefix);

        if (invalid(seekIterator)) {
            // No data at all.
            return ReadResult.empty(rowId);
        }

        ByteBuffer dataIdKey = DIRECT_DATA_ID_KEY_BUFFER.get().clear();

        int keyLength = seekIterator.key(dataIdKey);

        dataIdKey.position(0).limit(keyLength);

        if (!matches(rowId, dataIdKey)) {
            // It is already a different row, so no version exists for our rowId.
            return ReadResult.empty(rowId);
        }

        boolean isWriteIntent = keyLength == ROW_PREFIX_SIZE;

        ByteBuffer valueBytes = ByteBuffer.wrap(seekIterator.value());

        return readResultFromKeyAndValue(isWriteIntent, dataIdKey, valueBytes);
    }

    private ReadResult readResultFromKeyAndValue(
            boolean isWriteIntent,
            ByteBuffer dataIdKey,
            ByteBuffer valueBytes
    ) {
        RowId rowId = getRowId(dataIdKey);

        if (!isWriteIntent) {
            // There is no write-intent, return latest committed row.
            return wrapCommittedValue(rowId, valueBytes, readTimestampDesc(dataIdKey));
        }

        return wrapUncommittedValue(rowId, valueBytes, null);
    }

    /**
     * Finds a row by timestamp. See {@link MvPartitionStorage#read(RowId, HybridTimestamp)} for details.
     *
     * @param seekIterator Newly created seek iterator.
     * @param rowId Row id.
     * @param timestamp Timestamp.
     * @return Read result.
     */
    private ReadResult readByTimestamp(RocksIterator seekIterator, RowId rowId, HybridTimestamp timestamp) {
        byte[] committedDataIdKey = createCommittedDataIdKey(rowId, timestamp);

        // This seek will either find a key with timestamp that's less or equal than required value, or a different key whatsoever.
        // It is guaranteed by descending order of timestamps.
        seekIterator.seek(committedDataIdKey);

        return handleReadByTimestampIterator(seekIterator, rowId, timestamp, committedDataIdKey);
    }

    /**
     * Walks "version chain" via the iterator to find a row by timestamp.
     *
     * <p>See {@link MvPartitionStorage#read(RowId, HybridTimestamp)} for details.
     *
     * @param seekIterator Iterator, on which seek operation was already performed.
     * @param rowId Row id.
     * @param timestamp Timestamp.
     * @param committedDataIdKey Key for a committed entry: partition id + row id + timestamp.
     * @return Read result.
     */
    private ReadResult handleReadByTimestampIterator(
            RocksIterator seekIterator,
            RowId rowId,
            HybridTimestamp timestamp,
            byte[] committedDataIdKey
    ) {
        // There's no guarantee that required key even exists. If it doesn't, then "seek" will point to a different key.
        // To avoid returning its value, we have to check that actual key matches what we need.
        // Here we prepare direct buffer to read key without timestamp. Shared direct buffer is used to avoid extra memory allocations.
        ByteBuffer foundKeyBuf = DIRECT_DATA_ID_KEY_BUFFER.get().clear();

        int keyLength = 0;

        if (!invalid(seekIterator)) {
            keyLength = seekIterator.key(foundKeyBuf);
        }

        if (invalid(seekIterator) || !matches(rowId, foundKeyBuf)) {
            // There is no record older than timestamp.
            // There might be a write-intent which we should return.
            // Seek to *just* row id.
            seekIterator.seek(copyOf(committedDataIdKey, ROW_PREFIX_SIZE));

            if (invalid(seekIterator)) {
                // There are no writes with row id.
                return ReadResult.empty(rowId);
            }

            foundKeyBuf.clear();
            keyLength = seekIterator.key(foundKeyBuf);

            if (!matches(rowId, foundKeyBuf)) {
                // There are no writes with row id.
                return ReadResult.empty(rowId);
            }

            boolean isWriteIntent = keyLength == ROW_PREFIX_SIZE;

            if (isWriteIntent) {
                ByteBuffer valueBytes = ByteBuffer.wrap(seekIterator.value());

                // Let's check if there is a committed write.
                seekIterator.next();

                if (invalid(seekIterator)) {
                    // There are no committed writes, we can safely return write-intent.
                    return wrapUncommittedValue(rowId, valueBytes, null);
                }

                foundKeyBuf.clear();
                seekIterator.key(foundKeyBuf);

                if (!matches(rowId, foundKeyBuf)) {
                    // There are no committed writes, we can safely return write-intent.
                    return wrapUncommittedValue(rowId, valueBytes, null);
                }
            }

            // There is a committed write, but it's more recent than our timestamp (because we didn't find it with first seek).
            return ReadResult.empty(rowId);
        } else {
            // Should not be write-intent, as we were seeking with the timestamp.
            assert keyLength == MAX_KEY_SIZE;

            HybridTimestamp rowTimestamp = readTimestampDesc(foundKeyBuf);

            ByteBuffer valueBytes = ByteBuffer.wrap(seekIterator.value());

            if (rowTimestamp.equals(timestamp)) {
                // This is exactly the row we are looking for.
                return wrapCommittedValue(rowId, valueBytes, rowTimestamp);
            }

            // Let's check if there is more recent write. If it is a write-intent, then return write-intent.
            // If it is a committed write, then we are already in a right range.
            seekIterator.prev();

            if (invalid(seekIterator)) {
                // There is no more recent commits or write-intents.
                return wrapCommittedValue(rowId, valueBytes, rowTimestamp);
            }

            foundKeyBuf.clear();
            keyLength = seekIterator.key(foundKeyBuf);

            if (!matches(rowId, foundKeyBuf)) {
                // There is no more recent commits or write-intents under this row id.
                return wrapCommittedValue(rowId, valueBytes, rowTimestamp);
            }

            boolean isWriteIntent = keyLength == ROW_PREFIX_SIZE;

            if (isWriteIntent) {
                return wrapUncommittedValue(rowId, ByteBuffer.wrap(seekIterator.value()), rowTimestamp);
            }

            return wrapCommittedValue(rowId, valueBytes, readTimestampDesc(foundKeyBuf));
        }
    }

    /**
     * Checks if row id matches the one written in the key buffer. Note: this operation changes the position in the buffer.
     *
     * @param rowId Row id.
     * @param dataIdKey Key buffer.
     * @return {@code true} if row id matches the key buffer, {@code false} otherwise.
     */
    private static boolean matches(RowId rowId, ByteBuffer dataIdKey) {
        // Comparison starts from the position of the row id.
        dataIdKey.position(ROW_ID_OFFSET);

        return rowId.mostSignificantBits() == normalize(dataIdKey.getLong())
                && rowId.leastSignificantBits() == normalize(dataIdKey.getLong());
    }

    @Override
    public Cursor<ReadResult> scanVersions(RowId rowId) throws StorageException {
        return busy(() -> {
            assert rowIsLocked(rowId);

            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            ByteBuffer prefix = prepareDirectDataIdKeyBuf(rowId)
                    .position(0)
                    .limit(ROW_PREFIX_SIZE);

            var options = new ReadOptions().setPrefixSameAsStart(true);

            RocksIterator it = db.newIterator(helper.partCf, options);

            it = wrapIterator(it, helper.partCf);

            it.seek(prefix);

            return new RocksIteratorAdapter<ReadResult>(it) {
                @Override
                protected ReadResult decodeEntry(byte[] key, byte[] value) {
                    int keyLength = key.length;

                    boolean isWriteIntent = keyLength == ROW_PREFIX_SIZE;

                    return readResultFromKeyAndValue(
                            isWriteIntent,
                            ByteBuffer.wrap(key).order(KEY_BYTE_ORDER),
                            ByteBuffer.wrap(value)
                    );
                }

                @Override
                public boolean hasNext() {
                    return busy(() -> {
                        assert rowIsLocked(rowId) : "rowId=" + rowId + ", " + createStorageInfo();

                        return super.hasNext();
                    });
                }

                @Override
                public ReadResult next() {
                    return busy(() -> {
                        assert rowIsLocked(rowId) : "rowId=" + rowId + ", " + createStorageInfo();

                        return super.next();
                    });
                }

                @Override
                public void close() {
                    assert rowIsLocked(rowId);

                    super.close();

                    RocksUtils.closeAll(options);
                }
            };
        });
    }

    @Override
    public PartitionTimestampCursor scan(HybridTimestamp timestamp) throws StorageException {
        Objects.requireNonNull(timestamp, "timestamp is null");

        return busy(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            if (lookingForLatestVersions(timestamp)) {
                return new ScanLatestVersionsCursor();
            } else {
                return new ScanByTimestampCursor(timestamp);
            }
        });
    }

    private static void setKeyBuffer(ByteBuffer keyBuf, RowId rowId, @Nullable HybridTimestamp timestamp) {
        keyBuf.putLong(ROW_ID_OFFSET, normalize(rowId.mostSignificantBits()));
        keyBuf.putLong(ROW_ID_OFFSET + Long.BYTES, normalize(rowId.leastSignificantBits()));

        if (timestamp != null) {
            putTimestampDesc(keyBuf.position(ROW_PREFIX_SIZE), timestamp);
        }

        keyBuf.rewind();
    }

    @Override
    public @Nullable RowId closestRowId(RowId lowerBound) throws StorageException {
        return busy(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            ByteBuffer keyBuf = prepareDirectDataIdKeyBuf(lowerBound)
                    .position(0)
                    .limit(ROW_PREFIX_SIZE);

            try (RocksIterator it = db.newIterator(helper.partCf, helper.scanReadOpts)) {
                it.seek(keyBuf);

                if (!it.isValid()) {
                    RocksUtils.checkIterator(it);

                    return null;
                }

                keyBuf.rewind();

                it.key(keyBuf);

                return getRowId(keyBuf);
            }
        });
    }

    private static void incrementRowId(ByteBuffer buf) {
        long lsb = 1 + buf.getLong(ROW_ID_OFFSET + Long.BYTES);

        buf.putLong(ROW_ID_OFFSET + Long.BYTES, lsb);

        if (lsb != 0L) {
            return;
        }

        long msb = 1 + buf.getLong(ROW_ID_OFFSET);

        buf.putLong(ROW_ID_OFFSET, msb);

        if (msb != 0L) {
            return;
        }

        short partitionId = (short) (1 + buf.getShort(0));

        assert partitionId != 0;

        buf.putShort(0, partitionId);

        buf.rewind();
    }

    private RowId getRowId(ByteBuffer keyBuffer) {
        return helper.getRowId(keyBuffer, ROW_ID_OFFSET);
    }

    @Override
    public void updateLease(long leaseStartTime) {
        busy(() -> {
            if (leaseStartTime <= this.leaseStartTime) {
                return null;
            }

            AbstractWriteBatch writeBatch = requireWriteBatch();

            try {
                byte[] leaseBytes = new byte[Long.BYTES];

                putLongToBytes(leaseStartTime, leaseBytes, 0);

                writeBatch.put(meta, leaseKey, leaseBytes);

                this.leaseStartTime = leaseStartTime;
            } catch (RocksDBException e) {
                throw new StorageException(e);
            }

            return null;
        });
    }

    @Override
    public long leaseStartTime() {
        return busy(() -> leaseStartTime);
    }

    /**
     * Deletes partition data from the storage, using write batch to perform the operation.
     */
    void destroyData(WriteBatch writeBatch) throws RocksDBException {
        writeBatch.delete(meta, lastAppliedIndexAndTermKey);
        writeBatch.delete(meta, lastGroupConfigKey);
        writeBatch.delete(meta, leaseKey);

        writeBatch.deleteRange(helper.partCf, helper.partitionStartPrefix(), helper.partitionEndPrefix());
        writeBatch.deleteRange(helper.dataCf, helper.partitionStartPrefix(), helper.partitionEndPrefix());

        gc.deleteQueue(writeBatch);
    }

    @Override
    public @Nullable GcEntry peek(HybridTimestamp lowWatermark) {
        WriteBatchWithIndex batch = requireWriteBatch();

        // No busy lock required, we're already in "runConsistently" closure.
        throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

        return gc.peek(batch, lowWatermark);
    }

    @Override
    public @Nullable BinaryRow vacuum(GcEntry entry) {
        WriteBatchWithIndex batch = requireWriteBatch();

        // No busy lock required, we're already in "runConsistently" closure.
        throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

        try {
            return gc.vacuum(batch, entry);
        } catch (RocksDBException e) {
            throw new StorageException("Failed to collect garbage: " + createStorageInfo(), e);
        }
    }

    @Override
    public long estimatedSize() {
        return estimatedSize;
    }

    @Override
    public void close() {
        transitionToDestroyedOrClosedState(StorageState.CLOSED);
    }

    private void transitionToDestroyedOrClosedState(StorageState targetState) {
        if (!transitionToTerminalState(targetState, state)) {
            return;
        }

        busyLock.block();

        readOpts.close();
        helper.close();
    }

    /**
     * Transitions this storage to the {@link StorageState#DESTROYED} state.
     */
    public void transitionToDestroyedState() {
        transitionToDestroyedOrClosedState(StorageState.DESTROYED);
    }

    private byte[] createUncommittedDataIdKey(RowId rowId) {
        ByteBuffer uncommittedDataIdKeyBuf = HEAP_DATA_ID_KEY_BUFFER.get().clear();

        writeRowPrefix(uncommittedDataIdKeyBuf, rowId);

        return uncommittedDataIdKeyBuf.array();
    }

    private byte[] createCommittedDataIdKey(RowId rowId, HybridTimestamp timestamp) {
        ByteBuffer keyBuf = HEAP_COMMITTED_DATA_ID_KEY_BUFFER.get().clear();

        helper.putCommittedDataIdKey(keyBuf, rowId, timestamp);

        return keyBuf.array();
    }

    private ByteBuffer prepareDirectDataIdKeyBuf(RowId rowId) {
        ByteBuffer keyBuf = DIRECT_DATA_ID_KEY_BUFFER.get().clear();

        writeRowPrefix(keyBuf, rowId);

        return keyBuf;
    }

    private void writeRowPrefix(ByteBuffer buffer, RowId rowId) {
        assert buffer.order() == KEY_BYTE_ORDER;
        assert rowId.partitionId() == partitionId : rowId;

        buffer.putInt(tableStorage.getTableId());
        buffer.putShort((short) rowId.partitionId());

        helper.putRowId(buffer, rowId);
    }

    private static void validateTxId(ByteBuffer dataIdWithTxState, UUID txId) {
        dataIdWithTxState.position(DATA_ID_SIZE);

        long msb = dataIdWithTxState.getLong();
        long lsb = dataIdWithTxState.getLong();

        dataIdWithTxState.rewind();

        if (txId.getMostSignificantBits() != msb || txId.getLeastSignificantBits() != lsb) {
            throw new TxIdMismatchException(txId, new UUID(msb, lsb));
        }
    }

    /**
     * Checks iterator validity, including both finished iteration and occurred exception.
     */
    static boolean invalid(RocksIterator it) {
        boolean invalid = !it.isValid();

        if (invalid) {
            // Check the status first. This operation is guaranteed to throw if an internal error has occurred during
            // the iteration. Otherwise, we've exhausted the data range.
            try {
                it.status();
            } catch (RocksDBException e) {
                throw new StorageException("Failed to read data from storage", e);
            }
        }

        return invalid;
    }

    /**
     * Converts raw byte array representation of the write-intent value into a read result adding newest commit timestamp if it is not
     * {@code null}.
     *
     * @param rowId ID of the corresponding row.
     * @param transactionState Transaction state, including the Data ID.
     * @param newestCommitTs Commit timestamp of the most recent committed write of this value.
     * @return Read result instance.
     */
    private ReadResult wrapUncommittedValue(RowId rowId, ByteBuffer transactionState, @Nullable HybridTimestamp newestCommitTs) {
        assert transactionState.order() == KEY_BYTE_ORDER;

        ByteBuffer dataId = readDataIdFromTxState(transactionState);

        UUID txId = new UUID(transactionState.getLong(), transactionState.getLong());

        int commitTableId = transactionState.getInt();

        int commitPartitionId = Short.toUnsignedInt(transactionState.getShort());

        transactionState.rewind();

        BinaryRow row = readRowByDataId(dataId);

        return ReadResult.createFromWriteIntent(rowId, row, txId, commitTableId, commitPartitionId, newestCommitTs);
    }

    /**
     * Converts raw byte array representation of the value into a read result.
     *
     * @param rowId ID of the corresponding row.
     * @param dataId Data ID pointing to row data.
     * @param rowCommitTimestamp Timestamp with which the row was committed.
     * @return Read result instance or {@code null} if value is a tombstone.
     */
    private ReadResult wrapCommittedValue(RowId rowId, ByteBuffer dataId, HybridTimestamp rowCommitTimestamp) {
        return ReadResult.createFromCommitted(rowId, readRowByDataId(dataId), rowCommitTimestamp);
    }

    @Nullable
    private BinaryRow readRowByDataId(ByteBuffer dataId) {
        if (isTombstone(dataId)) {
            return null;
        }

        try {
            byte[] payloadKey = helper.createPayloadKey(dataId);

            byte[] rowBytes = getFromBatchAndDb(db, helper.dataCf, readOpts, payloadKey);

            return rowBytes == null ? null : deserializeRow(rowBytes);
        } catch (RocksDBException e) {
            throw new StorageException(e);
        }
    }

    private abstract class BasePartitionTimestampCursor implements PartitionTimestampCursor {
        protected final RocksIterator it = db.newIterator(helper.partCf, helper.scanReadOpts);

        // Here's seek buffer itself. Originally it contains a valid partition id, row id payload that's filled with zeroes, and maybe
        // a timestamp value. Zero row id guarantees that it's lexicographically less than or equal to any other row id stored in the
        // partition.
        // Byte buffer from a thread-local field can't be used here, because of two reasons:
        //  - no one guarantees that there will only be a single cursor;
        //  - no one guarantees that returned cursor will not be used by other threads.
        // The thing is, we need this buffer to preserve its content between invocations of "hasNext" method.
        final ByteBuffer seekKeyBuf = allocate(MAX_KEY_SIZE).order(KEY_BYTE_ORDER).putInt(tableId).putShort((short) partitionId);

        RowId currentRowId;

        /** Cached value for {@link #next()} method. Also optimizes the code of {@link #hasNext()}. */
        protected ReadResult next;

        protected abstract boolean hasNextBusy();

        @Override
        public boolean hasNext() {
            return busy(() -> {
                throwExceptionIfStorageInProgressOfRebalance(state.get(), RocksDbMvPartitionStorage.this::createStorageInfo);

                return hasNextBusy();
            });
        }

        @Override
        public @Nullable BinaryRow committed(HybridTimestamp timestamp) {
            return busy(() -> {
                throwExceptionIfStorageInProgressOfRebalance(state.get(), RocksDbMvPartitionStorage.this::createStorageInfo);

                Objects.requireNonNull(timestamp, "timestamp is null");

                if (currentRowId == null) {
                    throw new IllegalStateException("currentRowId is null");
                }

                setKeyBuffer(seekKeyBuf, currentRowId, timestamp);

                it.seek(seekKeyBuf.array());

                ReadResult readResult = handleReadByTimestampIterator(it, currentRowId, timestamp, seekKeyBuf.array());

                if (readResult.isEmpty()) {
                    return null;
                }

                return readResult.binaryRow();
            });
        }

        @SuppressWarnings("IteratorNextCanNotThrowNoSuchElementException") // It can.
        @Override
        public final ReadResult next() {
            return busy(() -> {
                throwExceptionIfStorageInProgressOfRebalance(state.get(), RocksDbMvPartitionStorage.this::createStorageInfo);

                if (!hasNextBusy()) {
                    throw new NoSuchElementException();
                }

                ReadResult res = next;

                next = null;

                return res;
            });
        }

        @Override
        public final void close() {
            it.close();
        }
    }

    private final class ScanLatestVersionsCursor extends BasePartitionTimestampCursor {
        @Override
        public boolean hasNextBusy() {
            // Fast-path for consecutive invocations.
            if (next != null) {
                return true;
            }

            if (currentRowId != null) {
                setKeyBuffer(seekKeyBuf, currentRowId, null);
                incrementRowId(seekKeyBuf);
            }

            currentRowId = null;

            // Prepare direct buffer slice to read keys from the iterator.
            ByteBuffer currentKeyBuffer = DIRECT_DATA_ID_KEY_BUFFER.get();

            while (true) {
                // At this point, seekKeyBuf should contain row id that's above the one we already scanned, but not greater than any
                // other row id in partition. When we start, row id is filled with zeroes. Value during the iteration is described later
                // in this code. Now let's describe what we'll find, assuming that iterator found something:
                //  - if timestamp is null:
                //      - this seek will find the newest version of the next row in iterator. Exactly what we need.
                //  - if timestamp is not null:
                //      - suppose that seek key buffer has the following value: "| P0 | R0 | T0 |" (partition, row id, timestamp)
                //        and iterator finds something, let's denote it as "| P0 | R1 | T1 |" (partition must match). Again, there are
                //        few possibilities here:
                //          - R1 == R0, this means a match. By the rules of ordering we derive that T1 >= T0. Timestamps are stored in
                //            descending order, this means that we found exactly what's needed.
                //          - R1 > R0, this means that we found next row and T1 is either missing (pending row) or represents the latest
                //            version of the row. It doesn't matter in this case, because this row id will be reused to find its value
                //            at time T0. Additional "seek" will be required to do it.
                // TODO IGNITE-18201 Remove copying.
                it.seek(copyOf(seekKeyBuf.array(), ROW_PREFIX_SIZE));

                // Finish scan if nothing was found.
                if (invalid(it)) {
                    return false;
                }

                // Read the actual key into a direct buffer.
                int keyLength = it.key(currentKeyBuffer.clear());

                currentKeyBuffer.position(0).limit(keyLength);

                boolean isWriteIntent = keyLength == ROW_PREFIX_SIZE;

                RowId rowId = getRowId(currentKeyBuffer);

                // Copy actual row id into a "seekKeyBuf" buffer.
                seekKeyBuf.putLong(ROW_ID_OFFSET, normalize(rowId.mostSignificantBits()));
                seekKeyBuf.putLong(ROW_ID_OFFSET + Long.BYTES, normalize(rowId.leastSignificantBits()));

                // This one might look tricky. We finished processing next row. There are three options:
                //  - "found" flag is false - there's no fitting version of the row. We'll continue to next iteration;
                //  - value is empty, we found a tombstone. We'll continue to next iteration as well;
                //  - value is not empty and everything's good. We'll cache it and return from method.
                // In all three cases we need to prepare the value of "seekKeyBuf" so that it has not-yet-scanned row id in it.
                // the only valid way to do so is to treat row id payload as one big unsigned integer in Big Endian and increment it.
                // It's important to note that increment may overflow. In this case "carry flag" will go into incrementing partition id.
                // This is fine for three reasons:
                //  - iterator has an upper bound, following "seek" will result in invalid iterator state.
                //  - partition id itself cannot be overflown, because it's limited with a constant less than 0xFFFF.
                // It's something like 65500, I think.
                //  - "seekKeyBuf" buffer value will not be used after that, so it's ok if we corrupt its data (in every other instance,
                //    buffer starts with a valid partition id, which is set during buffer's initialization).
                incrementRowId(seekKeyBuf);

                // Cache row and return "true" if it's found and not a tombstone.
                byte[] valueBytes = it.value();

                HybridTimestamp nextCommitTimestamp = null;

                if (isWriteIntent) {
                    it.next();

                    if (!invalid(it)) {
                        ByteBuffer key = ByteBuffer.wrap(it.key()).order(KEY_BYTE_ORDER);

                        if (matches(rowId, key)) {
                            // This is a next version of current row.
                            nextCommitTimestamp = readTimestampDesc(key);
                        }
                    }
                }

                assert valueBytes != null;

                ByteBuffer valueBuffer = ByteBuffer.wrap(valueBytes);

                ReadResult readResult = isWriteIntent
                        ? wrapUncommittedValue(rowId, valueBuffer, nextCommitTimestamp)
                        // There is no write-intent, return latest committed row.
                        : wrapCommittedValue(rowId, valueBuffer, readTimestampDesc(currentKeyBuffer));

                if (!readResult.isEmpty() || readResult.isWriteIntent()) {
                    next = readResult;
                    currentRowId = rowId;

                    return true;
                }
            }
        }
    }

    private final class ScanByTimestampCursor extends BasePartitionTimestampCursor {
        private final HybridTimestamp timestamp;

        private ScanByTimestampCursor(HybridTimestamp timestamp) {
            this.timestamp = timestamp;
        }

        @Override
        public boolean hasNextBusy() {
            // Fast-path for consecutive invocations.
            if (next != null) {
                return true;
            }

            if (currentRowId != null) {
                setKeyBuffer(seekKeyBuf, currentRowId, timestamp);
                incrementRowId(seekKeyBuf);
            }

            currentRowId = null;

            // Prepare direct buffer slice to read keys from the iterator.
            ByteBuffer directBuffer = DIRECT_DATA_ID_KEY_BUFFER.get();

            while (true) {
                // TODO IGNITE-18201 Remove copying.
                it.seek(copyOf(seekKeyBuf.array(), ROW_PREFIX_SIZE));

                if (invalid(it)) {
                    return false;
                }

                // We need to figure out what current row id is.
                it.key(directBuffer.clear());

                RowId rowId = getRowId(directBuffer);

                setKeyBuffer(seekKeyBuf, rowId, timestamp);

                // Seek to current row id + timestamp.
                it.seek(seekKeyBuf.array());

                ReadResult readResult = handleReadByTimestampIterator(it, rowId, timestamp, seekKeyBuf.array());

                if (readResult.isEmpty() && !readResult.isWriteIntent()) {
                    // Seek to next row id as we found nothing that matches.
                    incrementRowId(seekKeyBuf);

                    continue;
                }

                next = readResult;
                currentRowId = rowId;

                return true;
            }
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
     * Creates a summary info of the storage in the format "table=user, partitionId=1".
     */
    String createStorageInfo() {
        return format("tableId={}, partitionId={}", tableStorage.getTableId(), partitionId);
    }

    /**
     * Prepares the storage for rebalancing.
     *
     * @throws StorageRebalanceException If there was an error when starting the rebalance.
     */
    void startRebalance(WriteBatch writeBatch) {
        if (!state.compareAndSet(StorageState.RUNNABLE, StorageState.REBALANCE)) {
            throwExceptionDependingOnStorageStateOnRebalance(state.get(), createStorageInfo());
        }

        // Change storage states and expect all storage operations to stop soon.
        busyLock.block();

        try {
            clearStorage(writeBatch, REBALANCE_IN_PROGRESS, REBALANCE_IN_PROGRESS);
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
    void abortRebalance(WriteBatch writeBatch) {
        if (!state.compareAndSet(StorageState.REBALANCE, StorageState.RUNNABLE)) {
            throwExceptionDependingOnStorageStateOnRebalance(state.get(), createStorageInfo());
        }

        try {
            clearStorage(writeBatch, 0, 0);
        } catch (RocksDBException e) {
            throw new StorageRebalanceException("Error when trying to abort rebalancing storage: " + createStorageInfo(), e);
        }
    }

    /**
     * Completes storage rebalancing.
     *
     * @throws StorageRebalanceException If there was an error when finishing the rebalance.
     */
    void finishRebalance(WriteBatch writeBatch, long lastAppliedIndex, long lastAppliedTerm, byte[] groupConfig) {
        if (!state.compareAndSet(StorageState.REBALANCE, StorageState.RUNNABLE)) {
            throwExceptionDependingOnStorageStateOnRebalance(state.get(), createStorageInfo());
        }

        try {
            saveLastApplied(writeBatch, lastAppliedIndex, lastAppliedTerm);

            saveGroupConfigurationOnRebalance(writeBatch, groupConfig);
        } catch (RocksDBException e) {
            throw new StorageRebalanceException("Error when trying to abort rebalancing storage: " + createStorageInfo(), e);
        }
    }

    private void clearStorage(WriteBatch writeBatch, long lastAppliedIndex, long lastAppliedTerm) throws RocksDBException {
        saveLastApplied(writeBatch, lastAppliedIndex, lastAppliedTerm);

        lastGroupConfig = null;
        estimatedSize = 0;

        writeBatch.delete(meta, lastGroupConfigKey);
        writeBatch.delete(meta, leaseKey);
        writeBatch.delete(meta, estimatedSizeKey);

        writeBatch.deleteRange(helper.partCf, helper.partitionStartPrefix(), helper.partitionEndPrefix());
        writeBatch.deleteRange(helper.dataCf, helper.partitionStartPrefix(), helper.partitionEndPrefix());

        gc.deleteQueue(writeBatch);
    }

    private void saveLastApplied(WriteBatch writeBatch, long lastAppliedIndex, long lastAppliedTerm) throws RocksDBException {
        savePendingLastApplied(writeBatch, lastAppliedIndex, lastAppliedTerm);

        this.lastAppliedIndex = lastAppliedIndex;
        this.lastAppliedTerm = lastAppliedTerm;
    }

    private void saveGroupConfigurationOnRebalance(WriteBatch writeBatch, byte[] config) throws RocksDBException {
        saveGroupConfiguration(writeBatch, config);

        this.lastGroupConfig = config.clone();
    }

    /**
     * Prepares the storage for cleanup.
     *
     * <p>After cleanup (successful or not), method {@link #finishCleanup()} must be called.
     */
    void startCleanup(WriteBatch writeBatch) throws RocksDBException {
        if (!state.compareAndSet(StorageState.RUNNABLE, StorageState.CLEANUP)) {
            throwExceptionDependingOnStorageState(state.get(), createStorageInfo());
        }

        // Changed storage states and expect all storage operations to stop soon.
        busyLock.block();

        clearStorage(writeBatch, 0, 0);
    }

    /**
     * Finishes cleanup up the storage.
     */
    void finishCleanup() {
        if (state.compareAndSet(StorageState.CLEANUP, StorageState.RUNNABLE)) {
            busyLock.unblock();
        }
    }
}
