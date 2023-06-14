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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.ignite.internal.rocksdb.RocksIteratorAdapter;
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
    private final AtomicReference<StorageState> state = new AtomicReference<>(StorageState.RUNNABLE);

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
    }

    /**
     * Starts the storage.
     *
     * @throws IgniteInternalException In case when the operation has failed.
     */
    public void start() {
        busy(() -> {
            byte[] indexAndTermBytes = readLastAppliedIndexAndTerm(readOptions);

            if (indexAndTermBytes != null) {
                lastAppliedIndex = bytesToLong(indexAndTermBytes);
                lastAppliedTerm = bytesToLong(indexAndTermBytes, Long.BYTES);

                persistedIndex = lastAppliedIndex;
            }

            return null;
        });
    }

    @Override
    public @Nullable TxMeta get(UUID txId) {
        return busy(() -> {
            try {
                throwExceptionIfStorageInProgressOfRebalance();

                byte[] txMetaBytes = db.get(txIdToKey(txId));

                return txMetaBytes == null ? null : fromBytes(txMetaBytes);
            } catch (RocksDBException e) {
                throw new IgniteInternalException(
                        TX_STATE_STORAGE_ERR,
                        IgniteStringFormatter.format("Failed to get a value from storage: [{}]", createStorageInfo()),
                        e
                );
            }
        });
    }

    @Override
    public void put(UUID txId, TxMeta txMeta) {
        busy(() -> {
            try {
                db.put(txIdToKey(txId), toBytes(txMeta));

                return null;
            } catch (RocksDBException e) {
                throw new IgniteInternalException(
                        TX_STATE_STORAGE_ERR,
                        IgniteStringFormatter.format("Failed to put a value into storage: [{}]", createStorageInfo()),
                        e
                );
            }
        });
    }

    @Override
    public boolean compareAndSet(UUID txId, @Nullable TxState txStateExpected, TxMeta txMeta, long commandIndex, long commandTerm) {
        return busy(() -> {
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

                // If the store is in the process of rebalancing, then there is no need to update lastAppliedIndex and lastAppliedTerm.
                // This is necessary to prevent a situation where, in the middle of the rebalance, the node will be restarted and we will
                // have non-consistent storage. They will be updated by either #abortRebalance() or #finishRebalance(long, long).
                if (state.get() != StorageState.REBALANCE) {
                    updateLastApplied(writeBatch, commandIndex, commandTerm);
                }

                db.write(writeOptions, writeBatch);

                return result;
            } catch (RocksDBException e) {
                throw new IgniteInternalException(
                        TX_STATE_STORAGE_ERR,
                        IgniteStringFormatter.format("Failed perform CAS operation over a value in storage: [{}]", createStorageInfo()),
                        e
                );
            }
        });
    }

    @Override
    public void remove(UUID txId) {
        busy(() -> {
            try {
                throwExceptionIfStorageInProgressOfRebalance();

                db.delete(txIdToKey(txId));

                return null;
            } catch (RocksDBException e) {
                throw new IgniteInternalException(
                        TX_STATE_STORAGE_ERR,
                        IgniteStringFormatter.format("Failed to remove a value from storage: [{}]", createStorageInfo()),
                        e
                );
            }
        });
    }

    @Override
    public Cursor<IgniteBiTuple<UUID, TxMeta>> scan() {
        return busy(() -> {
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

            return new RocksIteratorAdapter<IgniteBiTuple<UUID, TxMeta>>(rocksIterator) {
                @Override
                protected IgniteBiTuple<UUID, TxMeta> decodeEntry(byte[] keyBytes, byte[] valueBytes) {
                    UUID key = keyToTxId(keyBytes);
                    TxMeta txMeta = fromBytes(valueBytes);

                    return new IgniteBiTuple<>(key, txMeta);
                }

                @Override
                public boolean hasNext() {
                    return busy(() -> {
                        throwExceptionIfStorageInProgressOfRebalance();

                        return super.hasNext();
                    });
                }

                @Override
                public IgniteBiTuple<UUID, TxMeta> next() {
                    return busy(() -> {
                        throwExceptionIfStorageInProgressOfRebalance();

                        return super.next();
                    });
                }

                @Override
                public void close() {
                    iterators.remove(rocksIterator);

                    super.close();
                }
            };
        });
    }

    @Override
    public CompletableFuture<Void> flush() {
        return busy(() -> tableStorage.awaitFlush(true));
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
        busy(() -> {
            try {
                throwExceptionIfStorageInProgressOfRebalance();

                db.put(lastAppliedIndexAndTermKey, indexAndTermToBytes(lastAppliedIndex, lastAppliedTerm));

                this.lastAppliedIndex = lastAppliedIndex;
                this.lastAppliedTerm = lastAppliedTerm;

                return null;
            } catch (RocksDBException e) {
                throw new IgniteInternalException(
                        TX_STATE_STORAGE_ERR,
                        IgniteStringFormatter.format("Failed to write applied index value to storage: [{}]", createStorageInfo()),
                        e
                );
            }
        });
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
                    IgniteStringFormatter.format("Failed to read applied term value from storage: [{}]", createStorageInfo()),
                    e
            );
        }
    }

    @Override
    public void destroy() {
        if (!tryToCloseStorageAndResources()) {
            return;
        }

        try (WriteBatch writeBatch = new WriteBatch()) {
            clearStorageData(writeBatch);

            writeBatch.delete(lastAppliedIndexAndTermKey);

            db.write(writeOptions, writeBatch);
        } catch (Exception e) {
            throw new IgniteInternalException(
                    TX_STATE_STORAGE_ERR,
                    IgniteStringFormatter.format("Failed to destroy storage: [{}]", createStorageInfo()),
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
        tryToCloseStorageAndResources();
    }

    @Override
    public CompletableFuture<Void> startRebalance() {
        if (!state.compareAndSet(StorageState.RUNNABLE, StorageState.REBALANCE)) {
            throwExceptionDependingOnStorageState();
        }

        busyLock.block();

        try (WriteBatch writeBatch = new WriteBatch()) {
            clearStorageData(writeBatch);

            updateLastAppliedAndPersistedIndex(writeBatch, REBALANCE_IN_PROGRESS, REBALANCE_IN_PROGRESS);

            db.write(writeOptions, writeBatch);

            return completedFuture(null);
        } catch (Exception e) {
            throw new IgniteInternalException(
                    TX_STATE_STORAGE_REBALANCE_ERR,
                    IgniteStringFormatter.format("Failed to start rebalance: [{}]", createStorageInfo()),
                    e
            );
        } finally {
            busyLock.unblock();
        }
    }

    @Override
    public CompletableFuture<Void> abortRebalance() {
        if (state.get() != StorageState.REBALANCE) {
            return completedFuture(null);
        }

        try (WriteBatch writeBatch = new WriteBatch()) {
            clearStorageData(writeBatch);

            writeBatch.delete(lastAppliedIndexAndTermKey);

            db.write(writeOptions, writeBatch);

            lastAppliedIndex = 0;
            lastAppliedTerm = 0;
            persistedIndex = 0;

            state.set(StorageState.RUNNABLE);
        } catch (Exception e) {
            throw new IgniteInternalException(
                    TX_STATE_STORAGE_REBALANCE_ERR,
                    IgniteStringFormatter.format("Failed to abort rebalance: [{}]", createStorageInfo()),
                    e
            );
        }

        return completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> finishRebalance(long lastAppliedIndex, long lastAppliedTerm) {
        if (state.get() != StorageState.REBALANCE) {
            throw new IgniteInternalException(
                    TX_STATE_STORAGE_REBALANCE_ERR,
                    IgniteStringFormatter.format("Rebalancing has not started: [{}]", createStorageInfo())
            );
        }

        try (WriteBatch writeBatch = new WriteBatch()) {
            updateLastAppliedAndPersistedIndex(writeBatch, lastAppliedIndex, lastAppliedTerm);

            db.write(writeOptions, writeBatch);

            state.set(StorageState.RUNNABLE);
        } catch (Exception e) {
            throw new IgniteInternalException(
                    TX_STATE_STORAGE_REBALANCE_ERR,
                    IgniteStringFormatter.format("Failed to finish rebalance: [{}]", createStorageInfo()),
                    e
            );
        }

        return completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> clear() {
        if (!state.compareAndSet(StorageState.RUNNABLE, StorageState.CLEANUP)) {
            throwExceptionDependingOnStorageState();
        }

        // We changed the status and wait for all current operations (together with cursors) with the storage to be completed.
        busyLock.block();

        try (WriteBatch writeBatch = new WriteBatch()) {
            clearStorageData(writeBatch);

            updateLastAppliedAndPersistedIndex(writeBatch, 0, 0);

            db.write(writeOptions, writeBatch);

            return completedFuture(null);
        } catch (RocksDBException e) {
            throw new IgniteInternalException(
                    TX_STATE_STORAGE_ERR,
                    IgniteStringFormatter.format("Failed to cleanup storage: [{}]", createStorageInfo()),
                    e
            );
        } finally {
            state.set(StorageState.RUNNABLE);

            busyLock.unblock();
        }
    }

    private void clearStorageData(WriteBatch writeBatch) throws RocksDBException {
        writeBatch.deleteRange(partitionStartPrefix(), partitionEndPrefix());
    }

    private void updateLastApplied(WriteBatch writeBatch, long lastAppliedIndex, long lastAppliedTerm) throws RocksDBException {
        writeBatch.put(lastAppliedIndexAndTermKey, indexAndTermToBytes(lastAppliedIndex, lastAppliedTerm));

        this.lastAppliedIndex = lastAppliedIndex;
        this.lastAppliedTerm = lastAppliedTerm;
    }

    private void updateLastAppliedAndPersistedIndex(
            WriteBatch writeBatch,
            long lastAppliedIndex,
            long lastAppliedTerm
    ) throws RocksDBException {
        updateLastApplied(writeBatch, lastAppliedIndex, lastAppliedTerm);

        persistedIndex = lastAppliedIndex;
    }

    /**
     * Tries to close the storage with resources if it hasn't already been closed.
     *
     * @return {@code True} if the storage was successfully closed, otherwise the storage has already been closed.
     */
    private boolean tryToCloseStorageAndResources() {
        if (!state.compareAndSet(StorageState.RUNNABLE, StorageState.CLOSED)) {
            StorageState state = this.state.get();

            assert state == StorageState.CLOSED : state;

            return false;
        }

        busyLock.block();

        RocksUtils.closeAll(iterators);

        iterators.clear();

        return true;
    }

    private void throwExceptionIfStorageInProgressOfRebalance() {
        if (state.get() == StorageState.REBALANCE) {
            throw createStorageInProgressOfRebalanceException();
        }
    }

    private IgniteInternalException createStorageInProgressOfRebalanceException() {
        return new IgniteInternalException(
                TX_STATE_STORAGE_REBALANCE_ERR,
                IgniteStringFormatter.format("Storage is in the process of rebalance: [{}]", createStorageInfo())
        );
    }

    private void throwExceptionDependingOnStorageState() {
        StorageState state = this.state.get();

        switch (state) {
            case CLOSED:
                throw new IgniteInternalException(
                        TX_STATE_STORAGE_STOPPED_ERR,
                        IgniteStringFormatter.format("Transaction state storage is stopped: [{}]", createStorageInfo())
                );
            case REBALANCE:
                throw createStorageInProgressOfRebalanceException();
            case CLEANUP:
                throw new IgniteInternalException(
                        TX_STATE_STORAGE_ERR,
                        IgniteStringFormatter.format("Storage is in the process of cleanup: [{}]", createStorageInfo())
                );
            default:
                throw new IgniteInternalException(
                        TX_STATE_STORAGE_ERR,
                        IgniteStringFormatter.format("Unexpected state: [{}, state={}]", createStorageInfo(), state)
                );
        }
    }

    private String createStorageInfo() {
        return "table=" + tableStorage.id + ", partitionId=" + partitionId;
    }

    private <V> V busy(Supplier<V> supplier) {
        if (!busyLock.enterBusy()) {
            throwExceptionDependingOnStorageState();
        }

        try {
            return supplier.get();
        } finally {
            busyLock.leaveBusy();
        }
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
        REBALANCE,

        /** Storage is in the process of cleanup. */
        CLEANUP
    }
}
