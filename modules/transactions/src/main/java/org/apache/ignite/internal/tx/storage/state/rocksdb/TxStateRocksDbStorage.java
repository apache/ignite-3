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

import static java.nio.ByteOrder.BIG_ENDIAN;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.tx.storage.state.rocksdb.TxStateRocksDbTableStorage.TABLE_PREFIX_SIZE_BYTES;
import static org.apache.ignite.internal.util.ByteUtils.bytesToLong;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.apache.ignite.internal.util.ByteUtils.putLongToBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_STATE_STORAGE_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_STATE_STORAGE_REBALANCE_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_STATE_STORAGE_STOPPED_ERR;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.rocksdb.RocksIteratorAdapter;
import org.apache.ignite.internal.rocksdb.RocksUtils;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.WriteBatch;

/**
 * Tx state storage implementation based on RocksDB.
 */
public class TxStateRocksDbStorage implements TxStateStorage {
    /** Prefix length for the payload. Consists of tableId (4 bytes) and partitionId (2 bytes), both in Big Endian. */
    private static final int PREFIX_SIZE_BYTES = TABLE_PREFIX_SIZE_BYTES + Short.BYTES;

    /** Size of the key in the storage. Consists of {@link #PREFIX_SIZE_BYTES} and a UUID (2x {@link Long#BYTES}. */
    private static final int FULL_KEY_SIZE_BYES = PREFIX_SIZE_BYTES + 2 * Long.BYTES;

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

    /** Shared TX state storage. */
    private final TxStateRocksDbSharedStorage sharedStorage;

    /** Table ID. */
    private final int tableId;

    /** On-heap-cached last applied index value. */
    private volatile long lastAppliedIndex;

    /** On-heap-cached last applied term value. */
    private volatile long lastAppliedTerm;

    /** Current state of the storage. */
    private final AtomicReference<StorageState> state = new AtomicReference<>(StorageState.RUNNABLE);

    /**
     * The constructor.
     *
     * @param partitionId Partition id.
     * @param tableStorage Table storage.
     */
    TxStateRocksDbStorage(
            int partitionId,
            TxStateRocksDbTableStorage tableStorage
    ) {
        this.partitionId = partitionId;
        this.tableStorage = tableStorage;
        this.sharedStorage = tableStorage.sharedStorage;
        this.tableId = tableStorage.id;
        this.lastAppliedIndexAndTermKey = ByteBuffer.allocate(PREFIX_SIZE_BYTES).order(BIG_ENDIAN)
                .putInt(tableId)
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
            byte[] indexAndTermBytes = readLastAppliedIndexAndTerm(sharedStorage.readOptions);

            if (indexAndTermBytes != null) {
                lastAppliedIndex = bytesToLong(indexAndTermBytes);
                lastAppliedTerm = bytesToLong(indexAndTermBytes, Long.BYTES);
            }

            return null;
        });
    }

    @Override
    public @Nullable TxMeta get(UUID txId) {
        return busy(() -> {
            try {
                throwExceptionIfStorageInProgressOfRebalance();

                byte[] txMetaBytes = sharedStorage.db().get(txIdToKey(txId));

                return txMetaBytes == null ? null : fromBytes(txMetaBytes);
            } catch (RocksDBException e) {
                throw new IgniteInternalException(
                        TX_STATE_STORAGE_ERR,
                        format("Failed to get a value from storage: [{}]", createStorageInfo()),
                        e
                );
            }
        });
    }

    @Override
    public void put(UUID txId, TxMeta txMeta) {
        busy(() -> {
            try {
                sharedStorage.db().put(txIdToKey(txId), toBytes(txMeta));

                return null;
            } catch (RocksDBException e) {
                throw new IgniteInternalException(
                        TX_STATE_STORAGE_ERR,
                        format("Failed to put a value into storage: [{}]", createStorageInfo()),
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

                byte[] txMetaExistingBytes = sharedStorage.db().get(sharedStorage.readOptions, txIdToKey(txId));

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

                sharedStorage.db().write(sharedStorage.writeOptions, writeBatch);

                return result;
            } catch (RocksDBException e) {
                throw new IgniteInternalException(
                        TX_STATE_STORAGE_ERR,
                        format("Failed perform CAS operation over a value in storage: [{}]", createStorageInfo()),
                        e
                );
            }
        });
    }

    @Override
    public void remove(UUID txId, long commandIndex, long commandTerm) {
        busy(() -> {
            try (WriteBatch writeBatch = new WriteBatch()) {
                throwExceptionIfStorageInProgressOfRebalance();

                writeBatch.delete(txIdToKey(txId));

                // If the store is in the process of rebalancing, then there is no need to update lastAppliedIndex and lastAppliedTerm.
                // This is necessary to prevent a situation where, in the middle of the rebalance, the node will be restarted and we will
                // have non-consistent storage. They will be updated by either #abortRebalance() or #finishRebalance(long, long).
                if (state.get() != StorageState.REBALANCE) {
                    updateLastApplied(writeBatch, commandIndex, commandTerm);
                }

                sharedStorage.db().write(sharedStorage.writeOptions, writeBatch);

                return null;
            } catch (RocksDBException e) {
                throw new IgniteInternalException(
                        TX_STATE_STORAGE_ERR,
                        format("Failed to remove a value from storage: [{}]", createStorageInfo()),
                        e
                );
            }
        });
    }

    @Override
    public Cursor<IgniteBiTuple<UUID, TxMeta>> scan() {
        return busy(() -> {
            throwExceptionIfStorageInProgressOfRebalance();

            // This lower bound is the lowest possible key that goes after "lastAppliedIndexAndTermKey".
            byte[] lowerBound = ByteBuffer.allocate(PREFIX_SIZE_BYTES + 1).order(BIG_ENDIAN)
                    .putInt(tableId)
                    .putShort((short) partitionId)
                    .put((byte) 0)
                    .array();
            byte[] upperBound = partitionEndPrefix();

            ReadOptions readOptions = new ReadOptions().setIterateUpperBound(new Slice(upperBound));

            RocksIterator rocksIterator = sharedStorage.db().newIterator(readOptions);

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
                    readOptions.close();

                    super.close();
                }
            };
        });
    }

    @Override
    public CompletableFuture<Void> flush() {
        return busy(() -> sharedStorage.awaitFlush(true));
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

                sharedStorage.db().put(lastAppliedIndexAndTermKey, indexAndTermToBytes(lastAppliedIndex, lastAppliedTerm));

                this.lastAppliedIndex = lastAppliedIndex;
                this.lastAppliedTerm = lastAppliedTerm;

                return null;
            } catch (RocksDBException e) {
                throw new IgniteInternalException(
                        TX_STATE_STORAGE_ERR,
                        format("Failed to write applied index value to storage: [{}]", createStorageInfo()),
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
            return sharedStorage.db().get(readOptions, lastAppliedIndexAndTermKey);
        } catch (RocksDBException e) {
            throw new IgniteInternalException(
                    TX_STATE_STORAGE_ERR,
                    format("Failed to read applied term value from storage: [{}]", createStorageInfo()),
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

            sharedStorage.db().write(sharedStorage.writeOptions, writeBatch);
        } catch (Exception e) {
            throw new IgniteInternalException(TX_STATE_STORAGE_ERR, format("Failed to destroy storage: [{}]", createStorageInfo()), e);
        }
    }

    private byte[] partitionStartPrefix() {
        return ByteBuffer.allocate(PREFIX_SIZE_BYTES).order(BIG_ENDIAN)
                .putInt(tableId)
                .putShort((short) (partitionId))
                .array();
    }

    private byte[] partitionEndPrefix() {
        return ByteBuffer.allocate(PREFIX_SIZE_BYTES).order(BIG_ENDIAN)
                .putInt(tableId)
                .putShort((short) (partitionId + 1))
                .array();
    }

    private byte[] txIdToKey(UUID txId) {
        return ByteBuffer.allocate(FULL_KEY_SIZE_BYES).order(BIG_ENDIAN)
                .putInt(tableId)
                .putShort((short) partitionId)
                .putLong(txId.getMostSignificantBits())
                .putLong(txId.getLeastSignificantBits())
                .array();
    }

    private UUID keyToTxId(byte[] bytes) {
        long msb = bytesToLong(bytes, PREFIX_SIZE_BYTES);
        long lsb = bytesToLong(bytes, PREFIX_SIZE_BYTES + Long.BYTES);

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

            updateLastApplied(writeBatch, REBALANCE_IN_PROGRESS, REBALANCE_IN_PROGRESS);

            sharedStorage.db().write(sharedStorage.writeOptions, writeBatch);

            return nullCompletedFuture();
        } catch (Exception e) {
            throw new IgniteInternalException(
                    TX_STATE_STORAGE_REBALANCE_ERR,
                    format("Failed to start rebalance: [{}]", createStorageInfo()),
                    e
            );
        } finally {
            busyLock.unblock();
        }
    }

    @Override
    public CompletableFuture<Void> abortRebalance() {
        if (state.get() != StorageState.REBALANCE) {
            return nullCompletedFuture();
        }

        try (WriteBatch writeBatch = new WriteBatch()) {
            clearStorageData(writeBatch);

            writeBatch.delete(lastAppliedIndexAndTermKey);

            sharedStorage.db().write(sharedStorage.writeOptions, writeBatch);

            lastAppliedIndex = 0;
            lastAppliedTerm = 0;

            state.set(StorageState.RUNNABLE);
        } catch (Exception e) {
            throw new IgniteInternalException(
                    TX_STATE_STORAGE_REBALANCE_ERR,
                    format("Failed to abort rebalance: [{}]", createStorageInfo()),
                    e
            );
        }

        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> finishRebalance(long lastAppliedIndex, long lastAppliedTerm) {
        if (state.get() != StorageState.REBALANCE) {
            throw new IgniteInternalException(
                    TX_STATE_STORAGE_REBALANCE_ERR,
                    format("Rebalancing has not started: [{}]", createStorageInfo())
            );
        }

        try (WriteBatch writeBatch = new WriteBatch()) {
            updateLastApplied(writeBatch, lastAppliedIndex, lastAppliedTerm);

            sharedStorage.db().write(sharedStorage.writeOptions, writeBatch);

            state.set(StorageState.RUNNABLE);
        } catch (Exception e) {
            throw new IgniteInternalException(
                    TX_STATE_STORAGE_REBALANCE_ERR,
                    format("Failed to finish rebalance: [{}]", createStorageInfo()),
                    e
            );
        }

        return nullCompletedFuture();
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

            updateLastApplied(writeBatch, 0, 0);

            sharedStorage.db().write(sharedStorage.writeOptions, writeBatch);

            return nullCompletedFuture();
        } catch (RocksDBException e) {
            throw new IgniteInternalException(
                    TX_STATE_STORAGE_ERR,
                    format("Failed to cleanup storage: [{}]", createStorageInfo()),
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
                format("Storage is in the process of rebalance: [{}]", createStorageInfo())
        );
    }

    private void throwExceptionDependingOnStorageState() {
        StorageState state = this.state.get();

        switch (state) {
            case CLOSED:
                throw new IgniteInternalException(
                        TX_STATE_STORAGE_STOPPED_ERR,
                        format("Transaction state storage is stopped: [{}]", createStorageInfo())
                );
            case REBALANCE:
                throw createStorageInProgressOfRebalanceException();
            case CLEANUP:
                throw new IgniteInternalException(
                        TX_STATE_STORAGE_ERR,
                        format("Storage is in the process of cleanup: [{}]", createStorageInfo())
                );
            default:
                throw new IgniteInternalException(
                        TX_STATE_STORAGE_ERR,
                        format("Unexpected state: [{}, state={}]", createStorageInfo(), state)
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
