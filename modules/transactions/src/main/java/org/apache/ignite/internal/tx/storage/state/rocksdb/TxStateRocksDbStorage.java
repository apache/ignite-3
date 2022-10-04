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
import static org.apache.ignite.internal.util.ByteUtils.bytesToLong;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.apache.ignite.internal.util.ByteUtils.longToBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_STATE_STORAGE_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_STATE_STORAGE_STOPPED_ERR;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.configuration.storage.StorageException;
import org.apache.ignite.internal.rocksdb.BusyRocksIteratorAdapter;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInternalException;
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
    private volatile RocksDB db;

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
        this.lastAppliedIndexKey = ByteBuffer.allocate(Short.BYTES).order(ByteOrder.BIG_ENDIAN)
                .putShort((short) partitionId)
                .array();

        lastAppliedIndex = readLastAppliedIndex(readOptions);

        persistedIndex = lastAppliedIndex;
    }

    /** {@inheritDoc} */
    @Override
    public TxMeta get(UUID txId) {
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
    @Override
    public void put(UUID txId, TxMeta txMeta) {
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
    @Override
    public boolean compareAndSet(UUID txId, TxState txStateExpected, TxMeta txMeta, long commandIndex) {
        requireNonNull(txMeta);

        if (!busyLock.enterBusy()) {
            throwStorageStoppedException();
        }

        byte[] txIdBytes = txIdToKey(txId);

        try (WriteBatch writeBatch = new WriteBatch()) {
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
                    } else if (txMetaExisting.equals(txMeta)) {
                        result = true;
                    } else {
                        result = false;
                    }
                } else {
                    result = false;
                }
            }

            writeBatch.put(lastAppliedIndexKey, longToBytes(commandIndex));

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

    /** {@inheritDoc} */
    @Override
    public void remove(UUID txId) {
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
    @Override
    public Cursor<IgniteBiTuple<UUID, TxMeta>> scan() {
        if (!busyLock.enterBusy()) {
            throwStorageStoppedException();
        }

        try {
            byte[] lowerBound = ByteBuffer.allocate(Short.BYTES + 1).putShort((short) partitionId).put((byte) 0).array();
            byte[] upperBound = partitionEndPrefix();

            RocksIterator rocksIterator = db.newIterator(
                    new ReadOptions()
                            .setIterateUpperBound(new Slice(upperBound))
            );

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
                @Override protected IgniteBiTuple<UUID, TxMeta> decodeEntry(byte[] keyBytes, byte[] valueBytes) {
                    UUID key = keyToTxId(keyBytes);
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
    @Override
    public long lastAppliedIndex() {
        return lastAppliedIndex;
    }

    /** {@inheritDoc} */
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
    @Override
    public void destroy() {
        try (WriteBatch writeBatch = new WriteBatch()) {
            close();

            writeBatch.deleteRange(partitionStartPrefix(), partitionEndPrefix());

            db.write(writeOptions, writeBatch);
        } catch (Exception e) {
            throw new StorageException("Failed to destroy partition " + partitionId + " of table " + tableStorage.configuration().name(),
                    e);
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

    private static void throwStorageStoppedException() {
        throw new IgniteInternalException(TX_STATE_STORAGE_STOPPED_ERR, "Transaction state storage is stopped");
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

    /** {@inheritDoc} */
    @Override
    public void close() throws Exception {
        if (!closeGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        List<AutoCloseable> resources = new ArrayList<>(iterators);

        IgniteUtils.closeAll(resources);
    }
}
