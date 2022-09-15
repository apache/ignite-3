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
import static org.apache.ignite.internal.tx.storage.state.rocksdb.TxnStateRocksDbTableStorage.APPLIED_INDEX_KEY;
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
import org.apache.ignite.internal.rocksdb.BusyRocksIteratorAdapter;
import org.apache.ignite.internal.rocksdb.RocksIteratorAdapter;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.storage.state.TxnStateStorage;
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
public class TxnStateRocksDbStorage implements TxnStateStorage {
    /** RocksDB database. */
    private volatile TransactionDB db;

    /** Write options. */
    private final WriteOptions writeOptions;

    /** Read options for regular reads. */
    private final ReadOptions readOptions;

    /** Partition id. */
    private final int partitionId;

    /** Transaction state table storage. */
    private final TxnStateRocksDbTableStorage tableStorage;

    /** Collection of opened RocksDB iterators. */
    private final Set<RocksIterator> iterators = ConcurrentHashMap.newKeySet();

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock;

    /**
     * The constructor.
     *
     * @param db Database..
     * @param busyLock Busy lock, provided by table storage.
     * @param partitionId Partition id.
     * @param tableStorage Table storage.
     */
    public TxnStateRocksDbStorage(
            TransactionDB db,
            WriteOptions writeOptions,
            ReadOptions readOptions,
            IgniteSpinBusyLock busyLock,
            int partitionId,
            TxnStateRocksDbTableStorage tableStorage
    ) {
        this.db = db;
        this.writeOptions = writeOptions;
        this.readOptions = readOptions;
        this.busyLock = busyLock;
        this.partitionId = partitionId;
        this.tableStorage = tableStorage;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> flush() {
        return tableStorage.awaitFlush(true);
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
            db.put(uuidToBytes(txId), toBytes(txMeta));
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
            db.delete(uuidToBytes(txId));
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
        // No-op.
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
        List<AutoCloseable> resources = new ArrayList<>(iterators);

        IgniteUtils.closeAll(resources);
    }
}
