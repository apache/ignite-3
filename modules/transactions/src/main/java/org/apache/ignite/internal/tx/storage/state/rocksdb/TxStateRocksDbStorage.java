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
import static org.apache.ignite.internal.rocksdb.RocksUtils.checkIterator;
import static org.apache.ignite.internal.util.ByteUtils.bytesToLong;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.apache.ignite.internal.util.ByteUtils.longToBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_STATE_STORAGE_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_STATE_STORAGE_STOPPED_ERR;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInternalException;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Transaction;
import org.rocksdb.TransactionDB;
import org.rocksdb.WriteOptions;

/**
 * Tx state storage implementation based on RocksDB.
 */
public class TxStateRocksDbStorage implements TxStateStorage {
    /** RocksDB database. */
    private volatile TransactionDB db;

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

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double closing the component. */
    private final AtomicBoolean closeGuard = new AtomicBoolean();

    /** On-heap-cached last applied index value. */
    private volatile long lastAppliedIndex;

    /** The value of {@link #lastAppliedIndex} persisted to the device at this moment. */
    private volatile long persistedIndex;

    /** Database key for the last applied index. */
    private final byte[] lastAppliedIndexKey;

    /**
     * The constructor.
     *
     * @param db Database..
     * @param partitionId Partition id.
     * @param tableStorage Table storage.
     */
    public TxStateRocksDbStorage(
            TransactionDB db,
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
        this.lastAppliedIndexKey = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN)
                .putInt(partitionId)
                .array();

        lastAppliedIndex = readLastAppliedIndex(readOptions);

        persistedIndex = lastAppliedIndex;
    }

    /** {@inheritDoc} */
    @Override public TxMeta get(UUID txId) {
        if (!busyLock.enterBusy()) {
            throwStorageStoppedException();
        }

        try {
            byte[] txMetaBytes = db.get(txIdToKey(txId));

            return txMetaBytes == null ? null : (TxMeta) fromBytes(txMetaBytes);
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

    /** {@inheritDoc} */
    @Override public void put(UUID txId, TxMeta txMeta) {
        if (!busyLock.enterBusy()) {
            throwStorageStoppedException();
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

    /** {@inheritDoc} */
    @Override public boolean compareAndSet(UUID txId, TxState txStateExpected, TxMeta txMeta, long commandIndex) {
        requireNonNull(txStateExpected);
        requireNonNull(txMeta);

        if (!busyLock.enterBusy()) {
            throwStorageStoppedException();
        }

        byte[] txIdBytes = txIdToKey(txId);

        try (Transaction rocksTx = db.beginTransaction(writeOptions)) {
            byte[] txMetaExistingBytes = rocksTx.get(readOptions, txIdToKey(txId));
            TxMeta txMetaExisting = fromBytes(txMetaExistingBytes);

            boolean result;

            if (txMetaExisting.txState() == txStateExpected) {
                rocksTx.put(txIdBytes, toBytes(txMeta));

                result = true;
            } else {
                result = false;
            }

            rocksTx.put(lastAppliedIndexKey, longToBytes(commandIndex));

            rocksTx.commit();

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

    /** {@inheritDoc} */
    @Override public void remove(UUID txId) {
        if (!busyLock.enterBusy()) {
            throwStorageStoppedException();
        }

        try {
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
                rocksIterator.seek(lastAppliedIndexKey);
            } catch (Exception e) {
                // Unlikely, but what if...
                iterators.remove(rocksIterator);

                rocksIterator.close();

                throw e;
            }

            StorageIterator iter = new StorageIterator(busyLock, rocksIterator);

            return Cursor.fromIterator(iter);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> flush() {
        return tableStorage.awaitFlush(true);
    }

    /** {@inheritDoc} */
    @Override public long lastAppliedIndex() {
        return lastAppliedIndex;
    }

    /** {@inheritDoc} */
    @Override public long persistedIndex() {
        return persistedIndex;
    }

    void refreshPersistedIndex() {
        persistedIndex = readLastAppliedIndex(persistedTierReadOptions);
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
            appliedIndexBytes = db.get(readOptions, lastAppliedIndexKey);
        } catch (RocksDBException e) {
            throw new IgniteInternalException(
                TX_STATE_STORAGE_ERR,
                "Failed to read applied index value from transaction state storage, partition " + partitionId
                    + " of table " + tableStorage.configuration().value().name(),
                e
            );
        }

        return appliedIndexBytes == null ? 0 : bytesToLong(appliedIndexBytes);
    }

    /** {@inheritDoc} */
    @Override public void destroy() {
        // No-op.
    }

    private static void throwStorageStoppedException() {
        throw new IgniteInternalException(TX_STATE_STORAGE_STOPPED_ERR, "Transaction state storage is stopped");
    }

    private byte[] txIdToKey(UUID txId) {
        return ByteBuffer.allocate(Integer.BYTES + 2 * Long.BYTES).order(ByteOrder.BIG_ENDIAN)
                .putInt(partitionId)
                .putLong(txId.getMostSignificantBits())
                .putLong(txId.getLeastSignificantBits())
                .array();
    }

    private UUID keyToTxId(byte[] bytes) {
        long msb = bytesToLong(bytes, Integer.BYTES);
        long lsb = bytesToLong(bytes, Integer.BYTES + Long.BYTES);

        return new UUID(msb, lsb);
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        if (!closeGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        List<AutoCloseable> resources = new ArrayList<>(iterators);

        IgniteUtils.closeAll(resources);
    }

    /**
     * Iterator for partition's transaction state storage that has shared underlying database.
     */
    private class StorageIterator implements Iterator<IgniteBiTuple<UUID, TxMeta>>, AutoCloseable {
        /** Busy lock. */
        private final IgniteSpinBusyLock busyLock;

        /** Rocks iterator. */
        private final RocksIterator rocksIterator;

        /** Current value. */
        private IgniteBiTuple<UUID, TxMeta> cur;

        /** Whether this iterator is valid. It becomes invalid when reaches data that belongs to abother partition. */
        private boolean isValid = true;

        /**
         * Constructor.
         *
         * @param busyLock Busy lock.
         * @param rocksIterator RocksDB iterator.
         */
        private StorageIterator(IgniteSpinBusyLock busyLock, RocksIterator rocksIterator) {
            this.busyLock = busyLock;
            this.rocksIterator = rocksIterator;
        }

        /** {@inheritDoc} */
        @Override
        public boolean hasNext() {
            if (!busyLock.enterBusy()) {
                throwStorageStoppedException();
            }

            try {
                if (!isValid) {
                    return false;
                }

                advance();

                return cur != null;
            } finally {
                busyLock.leaveBusy();
            }
        }

        /** {@inheritDoc} */
        @Override
        public IgniteBiTuple<UUID, TxMeta> next() {
            if (!busyLock.enterBusy()) {
                throwStorageStoppedException();
            }

            try {
                if (!isValid) {
                    throw new NoSuchElementException();
                }

                advance();

                if (cur != null) {
                    IgniteBiTuple<UUID, TxMeta> temp = cur;

                    cur = null;

                    return temp;
                } else {
                    throw new NoSuchElementException();
                }
            } finally {
                busyLock.leaveBusy();
            }
        }

        private void advance() {
            if (cur != null) {
                return;
            }

            boolean isRocksIteratorValid = rocksIterator.isValid();

            if (!isRocksIteratorValid) {
                checkIterator(rocksIterator);

                return;
            }

            while (true) {
                rocksIterator.next();

                byte[] keyBytes = rocksIterator.key();
                byte[] valueBytes = rocksIterator.value();

                if (keyBytes.length == Integer.BYTES) {
                    int p = ByteBuffer.wrap(keyBytes).getInt();

                    if (p == partitionId) {
                        // Skip this entry, it's last applied index.
                        continue;
                    } else {
                        // The data range is over, this is last applied index of the next partition.
                        isValid = false;

                        break;
                    }
                }

                UUID key = keyToTxId(keyBytes);
                TxMeta txMeta = fromBytes(valueBytes);

                cur = new IgniteBiTuple<>(key, txMeta);

                break;
            }
        }

        /** {@inheritDoc} */
        @Override
        public void close() throws Exception {
            iterators.remove(rocksIterator);

            rocksIterator.close();
        }
    }
}
