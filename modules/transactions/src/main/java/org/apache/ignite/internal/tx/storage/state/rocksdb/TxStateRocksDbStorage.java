/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import static org.apache.ignite.internal.util.ByteUtils.bytesToLong;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.apache.ignite.internal.util.ByteUtils.longToBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_STATE_STORAGE_CREATE_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_STATE_STORAGE_DESTROY_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_STATE_STORAGE_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_STATE_STORAGE_STOPPED_ERR;
import static org.rocksdb.ReadTier.PERSISTED_TIER;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.rocksdb.RocksIteratorAdapter;
import java.util.function.IntSupplier;
import org.apache.ignite.internal.rocksdb.BusyRocksIteratorAdapter;
import org.apache.ignite.internal.rocksdb.flush.RocksDbFlusher;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Transaction;
import org.rocksdb.TransactionDB;
import org.rocksdb.TransactionDBOptions;
import org.rocksdb.WriteOptions;

/**
 * Tx state storage implementation based on RocksDB.
 */
public class TxStateRocksDbStorage implements TxStateStorage {
    static {
        RocksDB.loadLibrary();
    }

    /** Key for the applied index. */
    private static final byte[] APPLIED_INDEX_KEY = ArrayUtils.BYTE_EMPTY_ARRAY;

    /** Database path. */
    private final Path dbPath;

    /** RocksDB flusher instance. */
    private volatile RocksDbFlusher flusher;

    /** RocksDB database. */
    private volatile TransactionDB db;

    /** RockDB options. */
    @Nullable
    private volatile Options options;

    /** Database options. */
    private volatile TransactionDBOptions txDbOptions;

    /** Write options. */
    private final WriteOptions writeOptions = new WriteOptions().setDisableWAL(true);

    /** Read options for regular reads. */
    private final ReadOptions readOptions = new ReadOptions();

    /** Read options for reading persisted data. */
    private final ReadOptions persistedTierReadOptions = new ReadOptions().setReadTier(PERSISTED_TIER);

    /** Scheduled pool for {@link RocksDbFlusher}. */
    private final ScheduledExecutorService scheduledPool;

    /** Thread-pool for snapshot operations execution. */
    private final ExecutorService threadPool;

    /** Delay supplier for {@link RocksDbFlusher}. */
    private final IntSupplier delaySupplier;

    /** On-heap-cached last applied index value. */
    private volatile long lastAppliedIndex;

    /** The value of {@link #lastAppliedIndex} persisted to the device at this moment. */
    private volatile long persistedIndex;

    /** Whether is started. */
    private boolean isStarted;

    /** Collection of opened RocksDB iterators. */
    private final Set<RocksIterator> iterators = ConcurrentHashMap.newKeySet();

    /** Busy lock to stop synchronously. */
    final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /**
     * The constructor.
     *
     * @param dbPath Database path.
     * @param scheduledPool Scheduled thread pool.
     * @param threadPool Thread pool.
     * @param delaySupplier Supplier of delay values to batch independent flush requests. Please refer to {@link RocksDbFlusher} for
     *      details.
     */
    public TxStateRocksDbStorage(
            Path dbPath,
            ScheduledExecutorService scheduledPool,
            ExecutorService threadPool,
            IntSupplier delaySupplier
    ) {
        this.dbPath = dbPath;
        this.scheduledPool = scheduledPool;
        this.threadPool = threadPool;
        this.delaySupplier = delaySupplier;
    }

    /** {@inheritDoc} */
    @Override public void start() {
        try {
            flusher = new RocksDbFlusher(
                    busyLock,
                    scheduledPool,
                    threadPool,
                    delaySupplier,
                    this::refreshPersistedIndex
            );

            options = new Options()
                    .setCreateIfMissing(true)
                    .setAtomicFlush(true)
                    .setListeners(List.of(flusher.listener()));

            txDbOptions = new TransactionDBOptions();

            db = TransactionDB.open(options, txDbOptions, dbPath.toString());

            lastAppliedIndex = readLastAppliedIndex(readOptions);

            persistedIndex = lastAppliedIndex;

            isStarted = true;
        } catch (RocksDBException e) {
            throw new IgniteInternalException(TX_STATE_STORAGE_CREATE_ERR, "Failed to start transaction state storage", e);
        }
    }

    private void refreshPersistedIndex() {
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
        byte[] appliedIndexBytes;

        try {
            appliedIndexBytes = db.get(readOptions, APPLIED_INDEX_KEY);
        } catch (RocksDBException e) {
            throw new IgniteInternalException(TX_STATE_STORAGE_ERR, "Failed to read applied index value from transaction state storage", e);
        }

        return appliedIndexBytes == null ? 0 : bytesToLong(appliedIndexBytes);
    }

    /** {@inheritDoc} */
    @Override public boolean isStarted() {
        return isStarted;
    }

    /** {@inheritDoc} */
    @Override public void stop() throws Exception {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        List<AutoCloseable> closeables = new ArrayList<>(iterators);

        closeables.add(persistedTierReadOptions);
        closeables.add(readOptions);
        closeables.add(writeOptions);
        closeables.add(options);
        closeables.add(txDbOptions);
        closeables.add(db);

        IgniteUtils.closeAll(closeables);

        db = null;
        options = null;
        txDbOptions = null;

        isStarted = false;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> flush() {
        return flusher.awaitFlush(true);
    }

    /** {@inheritDoc} */
    @Override
    public long lastAppliedIndex() {
        return lastAppliedIndex;
    }

    /** {@inheritDoc} */
    @Override
    public long persistedIndex() {
        return persistedIndex;
    }

    /** {@inheritDoc} */
    @Override public TxMeta get(UUID txId) {
        if (!busyLock.enterBusy()) {
            throwStorageStoppedException();
        }

        try {
            byte[] txMetaBytes = db.get(uuidToBytes(txId));

            return txMetaBytes == null ? null : (TxMeta) fromBytes(txMetaBytes);
        } catch (RocksDBException e) {
            throw new IgniteInternalException(TX_STATE_STORAGE_ERR, "Failed to get a value from the transaction state storage", e);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public void put(UUID txId, TxMeta txMeta) {
        if (!busyLock.enterBusy()) {
            throwStorageStoppedException();
        }

        try {
            db.put(uuidToBytes(txId), toBytes(txMeta));
        } catch (RocksDBException e) {
            throw new IgniteInternalException(TX_STATE_STORAGE_ERR, "Failed to put a value into the transaction state storage", e);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean compareAndSet(UUID txId, TxState txStateExpected, TxMeta txMeta, long commandIndex) {
        requireNonNull(txStateExpected);
        requireNonNull(txMeta);

        if (!busyLock.enterBusy()) {
            throwStorageStoppedException();
        }

        byte[] txIdBytes = uuidToBytes(txId);

        try (Transaction rocksTx = db.beginTransaction(writeOptions)) {
            byte[] txMetaExistingBytes = rocksTx.get(readOptions, uuidToBytes(txId));
            TxMeta txMetaExisting = fromBytes(txMetaExistingBytes);

            boolean result;

            if (txMetaExisting.txState() == txStateExpected) {
                rocksTx.put(txIdBytes, toBytes(txMeta));

                result = true;
            } else {
                result = false;
            }

            rocksTx.put(APPLIED_INDEX_KEY, longToBytes(commandIndex));

            rocksTx.commit();

            return result;
        } catch (RocksDBException e) {
            throw new IgniteInternalException(
                TX_STATE_STORAGE_ERR,
                "Failed perform CAS operation over a value in transaction state storage",
                e
            );
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public void remove(UUID txId) {
        if (!busyLock.enterBusy()) {
            throwStorageStoppedException();
        }

        try {
            db.delete(uuidToBytes(txId));
        } catch (RocksDBException e) {
            throw new IgniteInternalException(TX_STATE_STORAGE_ERR, "Failed to remove a value from the transaction state storage", e);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public Cursor<IgniteBiTuple<UUID, TxMeta>> scan() {
        if (!busyLock.enterBusy()) {
            throwStorageStoppedException();
        }

        try {
            RocksIterator rocksIterator = db.newIterator();

            iterators.add(rocksIterator);

            try {
                // Skip applied index value.
                rocksIterator.seek(new byte[1]);
            } catch (Exception e) {
                // Unlikely, but what if...
                iterators.remove(rocksIterator);

                rocksIterator.close();

                throw e;
            }

            RocksIteratorAdapter<IgniteBiTuple<UUID, TxMeta>> iteratorAdapter = new BusyRocksIteratorAdapter<>(busyLock, rocksIterator) {
                @Override protected IgniteBiTuple<UUID, TxMeta> decodeEntry(byte[] keyBytes, byte[] valueBytes) {
                    UUID key = bytesToUuid(keyBytes);
                    TxMeta txMeta = fromBytes(valueBytes);

                    return new IgniteBiTuple<>(key, txMeta);
                }

                @Override
                protected void handleBusy() {
                    throwStorageStoppedException();
                }

                @Override
                public void close() throws Exception {
                    iterators.remove(rocksIterator);

                    super.close();
                }
            };

            return Cursor.fromIterator(iteratorAdapter);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public void destroy() {
        try (Options options = new Options()) {
            close();

            RocksDB.destroyDB(dbPath.toString(), options);
        } catch (Exception e) {
            throw new IgniteInternalException(TX_STATE_STORAGE_DESTROY_ERR, "Failed to destroy the transaction state storage", e);
        }
    }

    private static void throwStorageStoppedException() {
        throw new IgniteInternalException(TX_STATE_STORAGE_STOPPED_ERR, "Transaction state storage is stopped");
    }

    private byte[] uuidToBytes(UUID uuid) {
        return ByteBuffer.allocate(2 * Long.BYTES).order(ByteOrder.BIG_ENDIAN)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .array();
    }

    private UUID bytesToUuid(byte[] bytes) {
        long msb = bytesToLong(bytes, 0);
        long lsb = bytesToLong(bytes, Long.BYTES);

        return new UUID(msb, lsb);
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        stop();
    }
}
