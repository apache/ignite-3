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

package org.apache.ignite.internal.tx.storage.state.rocksdb;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.util.ByteUtils.bytesToLong;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.apache.ignite.internal.util.ByteUtils.putLongToBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_STATE_STORAGE_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_STATE_STORAGE_REBALANCE_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_STATE_STORAGE_STOPPED_ERR;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.rocksdb.BusyRocksIteratorAdapter;
import org.apache.ignite.internal.rocksdb.RocksUtils;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.AbstractNativeReference;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

/**
 * Tx state storage implementation based on RocksDB.
 */
public class TxStateRocksDbStorage implements TxStateStorage {
    private static final VarHandle STATE;

    private static final VarHandle REBALANCE_FUTURE;

    static {
        try {
            STATE = MethodHandles.lookup().findVarHandle(TxStateRocksDbStorage.class, "state", StorageState.class);

            REBALANCE_FUTURE = MethodHandles.lookup().findVarHandle(
                    TxStateRocksDbStorage.class,
                    "rebalanceFuture",
                    CompletableFuture.class
            );
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /** RocksDB database. */
    private final RocksDB db;

    /** Write options. */
    private final WriteOptions writeOptions;

    /** Read options for regular reads. */
    private final ReadOptions readOptions;

    /** Read options for reading persisted data. */
    private final ReadOptions persistedTierReadOptions;

    /** Partition id. */
    private final int partitionId;

    /** Transaction state table storage. */
    private final TxStateRocksDbTableStorage tableStorage;

    /** Collection of opened RocksDB iterators. */
    private final Set<RocksIterator> iterators = ConcurrentHashMap.newKeySet();

    /** Busy lock. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Database key for the last applied index+term. */
    private final byte[] lastAppliedIndexAndTermKey;

    /** On-heap-cached last applied index value. */
    private volatile long lastAppliedIndex;

    /** On-heap-cached last applied term value. */
    private volatile long lastAppliedTerm;

    /** The value of {@link #lastAppliedIndex} persisted to the device at this moment. */
    private volatile long persistedIndex;

    /** Current state of the storage. */
    private volatile StorageState state = StorageState.RUNNABLE;

    @Nullable
    @SuppressWarnings("unused")
    private volatile CompletableFuture<Void> rebalanceFuture;

    /**
     * The constructor.
     *
     * @param db Database..
     * @param partitionId Partition id.
     * @param tableStorage Table storage.
     */
    TxStateRocksDbStorage(
            RocksDB db,
            WriteOptions writeOptions,
            ReadOptions readOptions,
            ReadOptions persistedTierReadOptions,
            int partitionId,
            TxStateRocksDbTableStorage tableStorage
    ) {
        this.db = db;
        this.writeOptions = writeOptions;
        this.readOptions = readOptions;
        this.persistedTierReadOptions = persistedTierReadOptions;
        this.partitionId = partitionId;
        this.tableStorage = tableStorage;
        this.lastAppliedIndexAndTermKey = ByteBuffer.allocate(Short.BYTES).order(ByteOrder.BIG_ENDIAN)
                .putShort((short) partitionId)
                .array();

        initLastApplied();
    }

    @Override
    @Nullable
    public TxMeta get(UUID txId) {
        if (!busyLock.enterBusy()) {
            throwExceptionIfStorageClosedOrRebalance();
        }

        try {
            throwExceptionIfStorageInProgressOfRebalance();

            byte[] txMetaBytes = db.get(txIdToKey(txId));

            return txMetaBytes == null ? null : fromBytes(txMetaBytes);
        } catch (RocksDBException e) {
            throw new IgniteInternalException(
                    TX_STATE_STORAGE_ERR,
                    "Failed to get a value from the transaction state storage, partition " + partitionId
                            + " of table " + tableStorage.configuration().value().name(),
                    e
            );
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public void put(UUID txId, TxMeta txMeta) {
        if (!busyLock.enterBusy()) {
            throwExceptionIfStorageClosedOrRebalance();
        }

        try {
            db.put(txIdToKey(txId), toBytes(txMeta));
        } catch (RocksDBException e) {
            throw new IgniteInternalException(
                TX_STATE_STORAGE_ERR,
                "Failed to put a value into the transaction state storage, partition " + partitionId
                    + " of table " + tableStorage.configuration().value().name(),
                e
            );
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public boolean compareAndSet(UUID txId, @Nullable TxState txStateExpected, TxMeta txMeta, long commandIndex, long commandTerm) {
        if (!busyLock.enterBusy()) {
            throwExceptionIfStorageClosedOrRebalance();
        }

        try (WriteBatch writeBatch = new WriteBatch()) {
            byte[] txIdBytes = txIdToKey(txId);

            byte[] txMetaExistingBytes = db.get(readOptions, txIdToKey(txId));

            boolean result;

            if (txMetaExistingBytes == null && txStateExpected == null) {
                writeBatch.put(txIdBytes, toBytes(txMeta));

                result = true;
            } else {
                if (txMetaExistingBytes != null) {
                    TxMeta txMetaExisting = fromBytes(txMetaExistingBytes);

                    if (txMetaExisting.txState() == txStateExpected) {
                        writeBatch.put(txIdBytes, toBytes(txMeta));

                        result = true;
                    } else {
                        result = txMetaExisting.txState() == txMeta.txState()
                                && Objects.equals(txMetaExisting.commitTimestamp(), txMeta.commitTimestamp());
                    }
                } else {
                    result = false;
                }
            }

            if (state != StorageState.REBALANCE) {
                writeBatch.put(lastAppliedIndexAndTermKey, indexAndTermToBytes(commandIndex, commandTerm));

                lastAppliedIndex = commandIndex;
                lastAppliedTerm = commandTerm;
            }

            db.write(writeOptions, writeBatch);

            return result;
        } catch (RocksDBException e) {
            throw new IgniteInternalException(
                TX_STATE_STORAGE_ERR,
                "Failed perform CAS operation over a value in transaction state storage, partition " + partitionId
                        + " of table " + tableStorage.configuration().value().name(),
                e
            );
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public void remove(UUID txId) {
        if (!busyLock.enterBusy()) {
            throwExceptionIfStorageClosedOrRebalance();
        }

        try {
            throwExceptionIfStorageInProgressOfRebalance();

            db.delete(txIdToKey(txId));
        } catch (RocksDBException e) {
            throw new IgniteInternalException(
                TX_STATE_STORAGE_ERR,
                "Failed to remove a value from the transaction state storage, partition " + partitionId
                    + " of table " + tableStorage.configuration().value().name(),
                e
            );
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public Cursor<IgniteBiTuple<UUID, TxMeta>> scan() {
        if (!busyLock.enterBusy()) {
            throwExceptionIfStorageClosedOrRebalance();
        }

        try {
            throwExceptionIfStorageInProgressOfRebalance();

            byte[] lowerBound = ByteBuffer.allocate(Short.BYTES + 1).putShort((short) partitionId).put((byte) 0).array();
            byte[] upperBound = partitionEndPrefix();

            RocksIterator rocksIterator = db.newIterator(new ReadOptions().setIterateUpperBound(new Slice(upperBound)));

            iterators.add(rocksIterator);

            try {
                // Skip applied index value.
                rocksIterator.seek(lowerBound);
            } catch (Exception e) {
                // Unlikely, but what if...
                iterators.remove(rocksIterator);

                rocksIterator.close();

                throw e;
            }

            return new BusyRocksIteratorAdapter<>(busyLock, rocksIterator) {
                @Override
                protected IgniteBiTuple<UUID, TxMeta> decodeEntry(byte[] keyBytes, byte[] valueBytes) {
                    UUID key = keyToTxId(keyBytes);
                    TxMeta txMeta = fromBytes(valueBytes);

                    return new IgniteBiTuple<>(key, txMeta);
                }

                @Override
                protected void handleBusyFail() {
                    throwExceptionIfStorageClosedOrRebalance();
                }

                @Override
                protected void handeBusySuccess() {
                    throwExceptionIfStorageInProgressOfRebalance();
                }

                @Override
                public void close() {
                    iterators.remove(rocksIterator);

                    super.close();
                }
            };
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public CompletableFuture<Void> flush() {
        return tableStorage.awaitFlush(true);
    }

    @Override
    public long lastAppliedIndex() {
        return lastAppliedIndex;
    }

    @Override
    public long lastAppliedTerm() {
        return lastAppliedTerm;
    }

    @Override
    public void lastApplied(long lastAppliedIndex, long lastAppliedTerm) {
        if (!busyLock.enterBusy()) {
            throwExceptionIfStorageClosedOrRebalance();
        }

        try {
            throwExceptionIfStorageInProgressOfRebalance();

            db.put(lastAppliedIndexAndTermKey, indexAndTermToBytes(lastAppliedIndex, lastAppliedTerm));

            this.lastAppliedIndex = lastAppliedIndex;
            this.lastAppliedTerm = lastAppliedTerm;
        } catch (RocksDBException e) {
            throw new IgniteInternalException(
                    TX_STATE_STORAGE_ERR,
                    "Failed to write applied index value to transaction state storage, partition " + partitionId
                            + " of table " + tableStorage.configuration().value().name(),
                    e
            );
        } finally {
            busyLock.leaveBusy();
        }
    }

    private static byte[] indexAndTermToBytes(long lastAppliedIndex, long lastAppliedTerm) {
        byte[] bytes = new byte[2 * Long.BYTES];

        putLongToBytes(lastAppliedIndex, bytes, 0);
        putLongToBytes(lastAppliedTerm, bytes, Long.BYTES);

        return bytes;
    }

    @Override
    public long persistedIndex() {
        return persistedIndex;
    }

    void refreshPersistedIndex() {
        if (!busyLock.enterBusy()) {
            return;
        }

        try {
            persistedIndex = readLastAppliedIndex(persistedTierReadOptions);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Reads the value of {@link #lastAppliedIndex} from the storage.
     *
     * @param readOptions Read options to be used for reading.
     * @return The value of last applied index.
     */
    private long readLastAppliedIndex(ReadOptions readOptions) {
        byte[] bytes = readLastAppliedIndexAndTerm(readOptions);

        if (bytes == null) {
            return 0;
        }

        return bytesToLong(bytes);
    }

    private byte @Nullable [] readLastAppliedIndexAndTerm(ReadOptions readOptions) {
        try {
            return db.get(readOptions, lastAppliedIndexAndTermKey);
        } catch (RocksDBException e) {
            throw new IgniteInternalException(
                    TX_STATE_STORAGE_ERR,
                    "Failed to read applied term value from transaction state storage, partition " + partitionId
                            + " of table " + tableStorage.configuration().value().name(),
                    e
            );
        }
    }

    @Override
    public void destroy() {
        if (!close0()) {
            return;
        }

        try {
            try (WriteBatch writeBatch = new WriteBatch()) {
                writeBatch.deleteRange(partitionStartPrefix(), partitionEndPrefix());

                writeBatch.delete(lastAppliedIndexAndTermKey);

                db.write(writeOptions, writeBatch);
            }
        } catch (Exception e) {
            throw new IgniteInternalException(
                    TX_STATE_STORAGE_ERR,
                    "Failed to destroy partition " + partitionId + " of table " + tableStorage.configuration().name(),
                    e
            );
        }
    }

    private byte[] partitionStartPrefix() {
        return ByteBuffer.allocate(Short.BYTES).order(ByteOrder.BIG_ENDIAN)
            .putShort((short) (partitionId))
            .array();
    }

    private byte[] partitionEndPrefix() {
        return ByteBuffer.allocate(Short.BYTES).order(ByteOrder.BIG_ENDIAN)
            .putShort((short) (partitionId + 1))
            .array();
    }

    private byte[] txIdToKey(UUID txId) {
        return ByteBuffer.allocate(Short.BYTES + 2 * Long.BYTES).order(ByteOrder.BIG_ENDIAN)
                .putShort((short) partitionId)
                .putLong(txId.getMostSignificantBits())
                .putLong(txId.getLeastSignificantBits())
                .array();
    }

    private UUID keyToTxId(byte[] bytes) {
        long msb = bytesToLong(bytes, Short.BYTES);
        long lsb = bytesToLong(bytes, Short.BYTES + Long.BYTES);

        return new UUID(msb, lsb);
    }

    @Override
    public void close() {
        close0();
    }

    @Override
    public CompletableFuture<Void> startRebalance() {
        if (!STATE.compareAndSet(this, StorageState.RUNNABLE, StorageState.REBALANCE)) {
            throwExceptionIfStorageClosedOrRebalance();
        }

        busyLock.block();

        try (WriteBatch writeBatch = new WriteBatch()) {
            writeBatch.deleteRange(partitionStartPrefix(), partitionEndPrefix());
            writeBatch.put(lastAppliedIndexAndTermKey, indexAndTermToBytes(REBALANCE_IN_PROGRESS, REBALANCE_IN_PROGRESS));

            db.write(writeOptions, writeBatch);

            lastAppliedIndex = REBALANCE_IN_PROGRESS;
            lastAppliedTerm = REBALANCE_IN_PROGRESS;
            persistedIndex = REBALANCE_IN_PROGRESS;

            CompletableFuture<Void> rebalanceFuture = completedFuture(null);

            this.rebalanceFuture = rebalanceFuture;

            return rebalanceFuture;
        } catch (Exception e) {
            throw new IgniteInternalException(
                    TX_STATE_STORAGE_REBALANCE_ERR,
                    IgniteStringFormatter.format("Failed to clear storage for partition {} of table {}", partitionId, getTableName()),
                    e
            );
        } finally {
            busyLock.unblock();
        }
    }

    @Override
    public CompletableFuture<Void> abortRebalance() {
        CompletableFuture<Void> rebalanceFuture = (CompletableFuture<Void>) REBALANCE_FUTURE.getAndSet(this, null);

        if (rebalanceFuture == null) {
            return completedFuture(null);
        }

        return rebalanceFuture
                .thenAccept(unused -> {
                    try (WriteBatch writeBatch = new WriteBatch()) {
                        writeBatch.deleteRange(partitionStartPrefix(), partitionEndPrefix());
                        writeBatch.delete(lastAppliedIndexAndTermKey);

                        db.write(writeOptions, writeBatch);

                        lastAppliedIndex = 0;
                        lastAppliedTerm = 0;
                        persistedIndex = 0;

                        state = StorageState.RUNNABLE;
                    } catch (Exception e) {
                        throw new IgniteInternalException(
                                TX_STATE_STORAGE_REBALANCE_ERR,
                                IgniteStringFormatter.format(
                                        "Failed to clear storage for partition {} of table {}",
                                        partitionId,
                                        getTableName()
                                ),
                                e
                        );
                    }
                });
    }

    @Override
    public CompletableFuture<Void> finishRebalance(long lastAppliedIndex, long lastAppliedTerm) {
        CompletableFuture<Void> rebalanceFuture = (CompletableFuture<Void>) REBALANCE_FUTURE.getAndSet(this, null);

        if (rebalanceFuture == null) {
            throw new IgniteInternalException(TX_STATE_STORAGE_REBALANCE_ERR, "Rebalancing has not started");
        }

        return rebalanceFuture
                .thenAccept(unused -> {
                    try (WriteBatch writeBatch = new WriteBatch()) {
                        writeBatch.put(lastAppliedIndexAndTermKey, indexAndTermToBytes(REBALANCE_IN_PROGRESS, REBALANCE_IN_PROGRESS));

                        db.write(writeOptions, writeBatch);

                        this.lastAppliedIndex = lastAppliedIndex;
                        this.lastAppliedTerm = lastAppliedTerm;
                        this.persistedIndex = lastAppliedIndex;

                        state = StorageState.RUNNABLE;
                    } catch (Exception e) {
                        throw new IgniteInternalException(
                                TX_STATE_STORAGE_REBALANCE_ERR,
                                IgniteStringFormatter.format(
                                        "Failed to finish rebalance for partition {} of table {}",
                                        partitionId,
                                        getTableName()
                                ),
                                e
                        );
                    }
                });
    }

    private void initLastApplied() {
        byte[] indexAndTermBytes = readLastAppliedIndexAndTerm(readOptions);

        if (indexAndTermBytes != null) {
            long lastAppliedIndex = bytesToLong(indexAndTermBytes);

            if (lastAppliedIndex == REBALANCE_IN_PROGRESS) {
                try (WriteBatch writeBatch = new WriteBatch()) {
                    writeBatch.deleteRange(partitionStartPrefix(), partitionEndPrefix());
                    writeBatch.delete(lastAppliedIndexAndTermKey);

                    db.write(writeOptions, writeBatch);
                } catch (Exception e) {
                    throw new IgniteInternalException(
                            TX_STATE_STORAGE_REBALANCE_ERR,
                            IgniteStringFormatter.format(
                                    "Failed to clear storage for partition {} of table {}",
                                    partitionId,
                                    getTableName()
                            ),
                            e
                    );
                }
            } else {
                this.lastAppliedIndex = lastAppliedIndex;
                persistedIndex = lastAppliedIndex;

                lastAppliedTerm = bytesToLong(indexAndTermBytes, Long.BYTES);
            }
        }
    }

    private boolean close0() {
        if (!STATE.compareAndSet(this, StorageState.RUNNABLE, StorageState.CLOSED)) {
            StorageState state = this.state;

            assert state == StorageState.CLOSED : state;

            return false;
        }

        busyLock.block();

        List<AbstractNativeReference> resources = new ArrayList<>(iterators);

        RocksUtils.closeAll(resources);

        iterators.clear();

        return true;
    }

    private void throwExceptionIfStorageClosedOrRebalance() {
        StorageState state = this.state;

        switch (state) {
            case CLOSED:
                throw createStorageClosedException();
            case REBALANCE:
                throw createStorageInProgressOfRebalanceException();
            default:
                throw createUnexpectedStorageStateException(state);
        }
    }

    private void throwExceptionIfStorageInProgressOfRebalance() {
        if (state == StorageState.REBALANCE) {
            throw createStorageInProgressOfRebalanceException();
        }
    }

    private IgniteInternalException createStorageClosedException() {
        return new IgniteInternalException(TX_STATE_STORAGE_STOPPED_ERR, "Transaction state storage is stopped");
    }

    private IgniteInternalException createStorageInProgressOfRebalanceException() {
        return new IgniteInternalException(TX_STATE_STORAGE_REBALANCE_ERR, "Storage is in the process of being rebalanced");
    }

    private IgniteInternalException createUnexpectedStorageStateException(StorageState state) {
        return new IgniteInternalException(TX_STATE_STORAGE_ERR, "Unexpected state: " + state);
    }

    private String getTableName() {
        return tableStorage.configuration().name().value();
    }

    /**
     * Storage states.
     */
    private enum StorageState {
        /** Storage is running. */
        RUNNABLE,

        /** Storage is in the process of being closed or has already closed. */
        CLOSED,

        /** Storage is in the process of being rebalanced. */
        REBALANCE
    }
}
