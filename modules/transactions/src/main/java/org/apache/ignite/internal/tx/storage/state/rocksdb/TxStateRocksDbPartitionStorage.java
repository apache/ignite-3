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

import static java.util.Objects.requireNonNull;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.storage.util.StorageUtils.transitionToDestroyedState;
import static org.apache.ignite.internal.tx.storage.state.rocksdb.TxStateRocksDbSharedStorage.BYTE_ORDER;
import static org.apache.ignite.internal.tx.storage.state.rocksdb.TxStateRocksDbStorage.ZONE_PREFIX_SIZE_BYTES;
import static org.apache.ignite.internal.util.ByteUtils.bytesToLong;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_STATE_STORAGE_ERR;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.rocksdb.RocksIteratorAdapter;
import org.apache.ignite.internal.rocksdb.RocksUtils;
import org.apache.ignite.internal.storage.engine.MvPartitionMeta;
import org.apache.ignite.internal.storage.lease.LeaseInfo;
import org.apache.ignite.internal.storage.util.StorageState;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxMetaSerializer;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateStorageClosedException;
import org.apache.ignite.internal.tx.storage.state.TxStateStorageDestroyedException;
import org.apache.ignite.internal.tx.storage.state.TxStateStorageException;
import org.apache.ignite.internal.tx.storage.state.TxStateStorageRebalanceException;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.WriteBatch;

/**
 * Tx state storage implementation based on RocksDB.
 */
public class TxStateRocksDbPartitionStorage implements TxStatePartitionStorage {
    /** Prefix length for the payload. Consists of tableId/zoneId (4 bytes) and partitionId (2 bytes), both in Big Endian. */
    public static final int PREFIX_SIZE_BYTES = ZONE_PREFIX_SIZE_BYTES + Short.BYTES;

    /** Size of the key in the storage. Consists of {@link #PREFIX_SIZE_BYTES} and a UUID (2x {@link Long#BYTES}. */
    private static final int FULL_KEY_SIZE_BYES = PREFIX_SIZE_BYTES + 2 * Long.BYTES;

    /** Partition id. */
    private final int partitionId;

    /** Storage for meta information. */
    private final TxStateMetaRocksDbPartitionStorage metaStorage;

    /** Collection of opened RocksDB iterators. */
    private final Set<RocksIterator> iterators = ConcurrentHashMap.newKeySet();

    /** Busy lock. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Shared TX state storage. */
    private final TxStateRocksDbSharedStorage sharedStorage;

    /** Zone ID. */
    private final int zoneId;

    /** Current state of the storage. */
    private final AtomicReference<StorageState> state = new AtomicReference<>(StorageState.RUNNABLE);

    /** Column Family with TX data for this partition. */
    private final ColumnFamily dataColumnFamily;

    /**
     * The constructor.
     *
     * @param partitionId Partition id.
     * @param parentStorage Parent storage.
     */
    TxStateRocksDbPartitionStorage(int partitionId, TxStateRocksDbStorage parentStorage) {
        this.partitionId = partitionId;
        this.sharedStorage = parentStorage.sharedStorage;
        this.zoneId = parentStorage.id;
        this.dataColumnFamily = parentStorage.sharedStorage.txStateColumnFamily();

        this.metaStorage = new TxStateMetaRocksDbPartitionStorage(
                parentStorage.sharedStorage.txStateMetaColumnFamily(),
                zoneId,
                partitionId
        );
    }

    private static short shortPartitionId(int intValue) {
        //noinspection NumericCastThatLosesPrecision
        return (short) intValue;
    }

    /**
     * Starts the storage.
     *
     * @throws TxStateStorageException In case when the operation has failed.
     */
    public void start() {
        busy(() -> {
            try {
                metaStorage.start();
            } catch (RocksDBException e) {
                throw new TxStateStorageException(
                        TX_STATE_STORAGE_ERR,
                        format("Failed to start storage: [{}]", createStorageInfo()),
                        e
                );
            }

            return null;
        });
    }

    @Override
    public @Nullable TxMeta get(UUID txId) {
        return busy(() -> {
            try {
                throwExceptionIfStorageInProgressOfRebalance();

                byte[] txMetaBytes = dataColumnFamily.get(txIdToKey(txId));

                return txMetaBytes == null ? null : deserializeTxMeta(txMetaBytes);
            } catch (RocksDBException e) {
                throw new TxStateStorageException(
                        TX_STATE_STORAGE_ERR,
                        format("Failed to get a value from storage: [{}]", createStorageInfo()),
                        e
                );
            }
        });
    }

    private static TxMeta deserializeTxMeta(byte[] txMetaBytes) {
        return VersionedSerialization.fromBytes(txMetaBytes, TxMetaSerializer.INSTANCE);
    }

    @Override
    public void putForRebalance(UUID txId, TxMeta txMeta) {
        busy(() -> {
            try {
                dataColumnFamily.put(txIdToKey(txId), serializeTxMeta(txMeta));

                return null;
            } catch (RocksDBException e) {
                throw new TxStateStorageException(
                        TX_STATE_STORAGE_ERR,
                        format("Failed to put a value into storage: [{}]", createStorageInfo()),
                        e
                );
            }
        });
    }

    private static byte[] serializeTxMeta(TxMeta txMeta) {
        return VersionedSerialization.toBytes(txMeta, TxMetaSerializer.INSTANCE);
    }

    @Override
    public boolean compareAndSet(UUID txId, @Nullable TxState txStateExpected, TxMeta txMeta, long commandIndex, long commandTerm) {
        return updateData(writeBatch -> {
            byte[] txIdBytes = txIdToKey(txId);

            byte[] txMetaExistingBytes = dataColumnFamily.get(txIdToKey(txId));

            boolean result;

            if (txMetaExistingBytes == null && txStateExpected == null) {
                writeBatch.put(txIdBytes, serializeTxMeta(txMeta));

                result = true;
            } else {
                if (txMetaExistingBytes != null) {
                    TxMeta txMetaExisting = deserializeTxMeta(txMetaExistingBytes);

                    if (txMetaExisting.txState() == txStateExpected) {
                        writeBatch.put(txIdBytes, serializeTxMeta(txMeta));

                        result = true;
                    } else {
                        result = txMetaExisting.txState() == txMeta.txState()
                                && Objects.equals(txMetaExisting.commitTimestamp(), txMeta.commitTimestamp());
                    }
                } else {
                    result = false;
                }
            }

            return result;
        }, commandIndex, commandTerm);
    }

    @Override
    public void remove(UUID txId, long commandIndex, long commandTerm) {
        updateData(writeBatch -> {
            writeBatch.delete(txIdToKey(txId));

            return null;
        }, commandIndex, commandTerm);
    }

    @Override
    public void removeAll(Collection<UUID> txIds, long commandIndex, long commandTerm) {
        requireNonNull(txIds, "Collection of the transaction IDs intended for removal cannot be null.");

        updateData(writeBatch -> {
            for (UUID txId : txIds) {
                writeBatch.delete(txIdToKey(txId));
            }

            return null;
        }, commandIndex, commandTerm);
    }

    private <T> T updateData(WriteClosure<?> writeClosure, long commandIndex, long commandTerm) {
        return (T) busy(() -> {
            throwExceptionIfStorageInProgressOfRebalance();

            try (WriteBatch writeBatch = new WriteBatch()) {
                Object result = writeClosure.apply(writeBatch);

                // If the store is in the process of rebalancing, then there is no need to update lastAppliedIndex and lastAppliedTerm.
                // This is necessary to prevent a situation where, in the middle of the rebalance, the node will be restarted and we will
                // have non-consistent storage. They will be updated by either #abortRebalance() or #finishRebalance(long, long).
                if (state.get() != StorageState.REBALANCE) {
                    metaStorage.updateLastApplied(writeBatch, commandIndex, commandTerm);
                }

                sharedStorage.db().write(sharedStorage.writeOptions, writeBatch);

                return result;
            } catch (RocksDBException e) {
                throw new TxStateStorageException(
                        TX_STATE_STORAGE_ERR,
                        format("Failed to update data in the storage: [{}]", createStorageInfo()),
                        e
                );
            }
        });
    }

    // TODO: https://issues.apache.org/jira/browse/IGNITE-26175
    @Override
    @SuppressWarnings("PMD.UseDiamondOperator")
    public Cursor<IgniteBiTuple<UUID, TxMeta>> scan() {
        return busy(() -> {
            throwExceptionIfStorageInProgressOfRebalance();

            // This lower bound is the lowest possible key that goes after "lastAppliedIndexAndTermKey".
            byte[] lowerBound = ByteBuffer.allocate(PREFIX_SIZE_BYTES + 1).order(BYTE_ORDER)
                    .putInt(zoneId)
                    .putShort(shortPartitionId(partitionId))
                    .put((byte) 0)
                    .array();
            byte[] upperBound = partitionEndPrefix();

            ReadOptions readOptions = new ReadOptions().setIterateUpperBound(new Slice(upperBound));

            RocksIterator rocksIterator = dataColumnFamily.newIterator(readOptions);

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
                    TxMeta txMeta = deserializeTxMeta(valueBytes);

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
        return metaStorage.lastAppliedIndex();
    }

    @Override
    public long lastAppliedTerm() {
        return metaStorage.lastAppliedTerm();
    }

    @Override
    public void lastApplied(long lastAppliedIndex, long lastAppliedTerm) {
        updateData(writeBatch -> null, lastAppliedIndex, lastAppliedTerm);
    }

    @Override
    public void destroy() {
        transitionToDestroyedState(state);

        closeStorageAndResources();

        try (WriteBatch writeBatch = new WriteBatch()) {
            clearStorageData(writeBatch);

            sharedStorage.db().write(sharedStorage.writeOptions, writeBatch);
        } catch (Exception e) {
            throw new TxStateStorageException(TX_STATE_STORAGE_ERR, format("Failed to destroy storage: [{}]", createStorageInfo()), e);
        }
    }

    private byte[] partitionStartPrefix() {
        return ByteBuffer.allocate(PREFIX_SIZE_BYTES).order(BYTE_ORDER)
                .putInt(zoneId)
                .putShort(shortPartitionId(partitionId))
                .array();
    }

    private byte[] partitionEndPrefix() {
        return ByteBuffer.allocate(PREFIX_SIZE_BYTES).order(BYTE_ORDER)
                .putInt(zoneId)
                .putShort(shortPartitionId(partitionId + 1))
                .array();
    }

    private byte[] txIdToKey(UUID txId) {
        return ByteBuffer.allocate(FULL_KEY_SIZE_BYES).order(BYTE_ORDER)
                .putInt(zoneId)
                .putShort(shortPartitionId(partitionId))
                .putLong(txId.getMostSignificantBits())
                .putLong(txId.getLeastSignificantBits())
                .array();
    }

    private static UUID keyToTxId(byte[] bytes) {
        long msb = bytesToLong(bytes, PREFIX_SIZE_BYTES);
        long lsb = bytesToLong(bytes, PREFIX_SIZE_BYTES + Long.BYTES);

        return new UUID(msb, lsb);
    }

    @Override
    public void close() {
        StorageState prevState = state.compareAndExchange(StorageState.RUNNABLE, StorageState.CLOSED);

        // Storage may have been destroyed.
        if (prevState.isTerminal()) {
            return;
        }

        if (prevState != StorageState.RUNNABLE) {
            throwExceptionDependingOnStorageState(prevState);
        }

        closeStorageAndResources();
    }

    @Override
    public CompletableFuture<Void> startRebalance() {
        transitionFromRunningStateTo(StorageState.REBALANCE);

        busyLock.block();

        try (WriteBatch writeBatch = new WriteBatch()) {
            clearStorageData(writeBatch);

            metaStorage.updateLastApplied(writeBatch, REBALANCE_IN_PROGRESS, REBALANCE_IN_PROGRESS);

            sharedStorage.db().write(sharedStorage.writeOptions, writeBatch);

            return nullCompletedFuture();
        } catch (Exception e) {
            throw new TxStateStorageRebalanceException(
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

            sharedStorage.db().write(sharedStorage.writeOptions, writeBatch);

            state.set(StorageState.RUNNABLE);
        } catch (Exception e) {
            throw new TxStateStorageRebalanceException(
                    format("Failed to abort rebalance: [{}]", createStorageInfo()),
                    e
            );
        }

        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> finishRebalance(MvPartitionMeta partitionMeta) {
        if (state.get() != StorageState.REBALANCE) {
            throw new TxStateStorageRebalanceException(
                    format("Rebalancing has not started: [{}]", createStorageInfo())
            );
        }

        try (WriteBatch writeBatch = new WriteBatch()) {
            metaStorage.updateLastApplied(writeBatch, partitionMeta.lastAppliedIndex(), partitionMeta.lastAppliedTerm());

            metaStorage.updateConfiguration(writeBatch, partitionMeta.groupConfig());

            LeaseInfo leaseInfo = partitionMeta.leaseInfo();

            if (leaseInfo != null) {
                metaStorage.updateLease(writeBatch, leaseInfo);
            }

            metaStorage.updateSnapshotInfo(writeBatch, partitionMeta.snapshotInfo());

            sharedStorage.db().write(sharedStorage.writeOptions, writeBatch);

            state.set(StorageState.RUNNABLE);
        } catch (Exception e) {
            throw new TxStateStorageRebalanceException(
                    format("Failed to finish rebalance: [{}]", createStorageInfo()),
                    e
            );
        }

        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> clear() {
        transitionFromRunningStateTo(StorageState.CLEANUP);

        // We changed the status and wait for all current operations (together with cursors) with the storage to be completed.
        busyLock.block();

        try (WriteBatch writeBatch = new WriteBatch()) {
            clearStorageData(writeBatch);

            sharedStorage.db().write(sharedStorage.writeOptions, writeBatch);

            return nullCompletedFuture();
        } catch (RocksDBException e) {
            throw new TxStateStorageException(
                    TX_STATE_STORAGE_ERR,
                    format("Failed to cleanup storage: [{}]", createStorageInfo()),
                    e
            );
        } finally {
            state.set(StorageState.RUNNABLE);

            busyLock.unblock();
        }
    }

    private void transitionFromRunningStateTo(StorageState targetState) {
        StorageState prevState = state.compareAndExchange(StorageState.RUNNABLE, targetState);

        if (prevState != StorageState.RUNNABLE) {
            throwExceptionDependingOnStorageState(prevState);
        }
    }

    @Override
    public void committedGroupConfiguration(byte[] config, long index, long term) {
        updateData(writeBatch -> {
            metaStorage.updateConfiguration(writeBatch, config);

            return null;
        }, index, term);
    }

    @Override
    public byte @Nullable [] committedGroupConfiguration() {
        return metaStorage.configuration();
    }

    @Override
    public void leaseInfo(LeaseInfo leaseInfo, long index, long term) {
        updateData(writeBatch -> {
            metaStorage.updateLease(writeBatch, leaseInfo);

            return null;
        }, index, term);
    }

    @Override
    public LeaseInfo leaseInfo() {
        return metaStorage.leaseInfo();
    }

    @Override
    public void snapshotInfo(byte[] snapshotInfo) {
        busy(() -> {
            throwExceptionIfStorageInProgressOfRebalance();

            try (WriteBatch writeBatch = new WriteBatch()) {
                metaStorage.updateSnapshotInfo(writeBatch, snapshotInfo);

                sharedStorage.db().write(sharedStorage.writeOptions, writeBatch);
            } catch (RocksDBException e) {
                throw new TxStateStorageException(
                        TX_STATE_STORAGE_ERR,
                        format("Failed to update data in the storage: [{}]", createStorageInfo()),
                        e
                );
            }

            return null;
        });
    }

    @Override
    public byte @Nullable [] snapshotInfo() {
        try {
            return metaStorage.snapshotInfo();
        } catch (RocksDBException e) {
            throw new TxStateStorageRebalanceException(
                    format("Failed to get snapshot info: [{}]", createStorageInfo()),
                    e
            );
        }
    }

    private void clearStorageData(WriteBatch writeBatch) throws RocksDBException {
        writeBatch.deleteRange(partitionStartPrefix(), partitionEndPrefix());

        metaStorage.clear(writeBatch);
    }

    private void closeStorageAndResources() {
        busyLock.block();

        RocksUtils.closeAll(iterators);

        iterators.clear();
    }

    private void throwExceptionIfStorageInProgressOfRebalance() {
        if (state.get() == StorageState.REBALANCE) {
            throw createStorageInProgressOfRebalanceException();
        }
    }

    private TxStateStorageException createStorageInProgressOfRebalanceException() {
        return new TxStateStorageRebalanceException(
                format("Storage is in the process of rebalance: [{}]", createStorageInfo())
        );
    }

    private void throwExceptionDependingOnStorageState(StorageState state) {
        switch (state) {
            case CLOSED:
                throw new TxStateStorageClosedException(
                        format("Transaction state storage is stopped: [{}]", createStorageInfo())
                );
            case REBALANCE:
                throw createStorageInProgressOfRebalanceException();
            case CLEANUP:
                throw new TxStateStorageException(
                        TX_STATE_STORAGE_ERR,
                        format("Storage is in the process of cleanup: [{}]", createStorageInfo())
                );
            case DESTROYED:
                throw new TxStateStorageDestroyedException(
                        TX_STATE_STORAGE_ERR,
                        format("Storage has been destroyed: [{}]", createStorageInfo())
                );
            default:
                throw new TxStateStorageException(
                        TX_STATE_STORAGE_ERR,
                        format("Unexpected state: [{}, state={}]", createStorageInfo(), state)
                );
        }
    }

    private String createStorageInfo() {
        return "zone=" + zoneId + ", partitionId=" + partitionId;
    }

    private <V> V busy(Supplier<V> supplier) {
        if (!busyLock.enterBusy()) {
            throwExceptionDependingOnStorageState(state.get());
        }

        try {
            return supplier.get();
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Write closure.
     */
    @FunctionalInterface
    private interface WriteClosure<T> {
        T apply(WriteBatch writeBatch) throws RocksDBException;
    }
}
