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
import static org.apache.ignite.internal.rocksdb.snapshot.ColumnFamilyRange.fullRange;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_STATE_STORAGE_CREATE_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_STATE_STORAGE_DESTROY_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_STATE_STORAGE_ERR;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.rocksdb.RocksIteratorAdapter;
import org.apache.ignite.internal.rocksdb.snapshot.RocksSnapshotManager;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.NotNull;
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
public class TxStateRocksDbStorage implements TxStateStorage, AutoCloseable {
    /** Database path. */
    private final Path dbPath;

    /** RocksDB database. */
    private volatile TransactionDB db;

    /** RockDB options. */
    @Nullable
    private volatile Options options;

    /** Database options. */
    private volatile TransactionDBOptions txDbOptions;

    /** Thread-pool for snapshot operations execution. */
    private final ExecutorService snapshotExecutor = Executors.newFixedThreadPool(2);

    /** Snapshot manager. */
    private volatile RocksSnapshotManager snapshotManager;

    /** Snapshot restore lock. */
    private final Object snapshotRestoreLock = new Object();

    /** Whether is started. */
    private boolean isStarted;

    /**
     * The constructor.
     *
     * @param dbPath Database path.
     */
    public TxStateRocksDbStorage(Path dbPath) {
        this.dbPath = dbPath;
    }

    /** {@inheritDoc} */
    @Override public void start() {
        try {
            this.options = new Options().setCreateIfMissing(true);

            this.txDbOptions = new TransactionDBOptions();

            this.db = TransactionDB.open(options, txDbOptions, dbPath.toString());

            ColumnFamily defaultCf = ColumnFamily.wrap(db, db.getDefaultColumnFamily());

            snapshotManager = new RocksSnapshotManager(db, List.of(fullRange(defaultCf)), snapshotExecutor);

            isStarted = true;
        } catch (RocksDBException e) {
            throw new IgniteInternalException(TX_STATE_STORAGE_CREATE_ERR, "Failed to start transaction state storage", e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isStarted() {
        return isStarted;
    }

    /** {@inheritDoc} */
    @Override public void stop() throws Exception {
        IgniteUtils.shutdownAndAwaitTermination(snapshotExecutor, 10, TimeUnit.SECONDS);

        IgniteUtils.closeAll(options, txDbOptions, db);

        db = null;
        options = null;
        txDbOptions = null;

        isStarted = false;
    }

    /** {@inheritDoc} */
    @Override public TxMeta get(UUID txId) {
        try {
            byte[] txMetaBytes = db.get(toBytes(txId));

            return txMetaBytes == null ? null : (TxMeta) fromBytes(txMetaBytes);
        } catch (RocksDBException e) {
            throw new IgniteInternalException(TX_STATE_STORAGE_ERR, "Failed to get a value from the transaction state storage", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void put(UUID txId, TxMeta txMeta) {
        try {
            db.put(toBytes(txId), toBytes(txMeta));
        } catch (RocksDBException e) {
            throw new IgniteInternalException(TX_STATE_STORAGE_ERR, "Failed to put a value into the transaction state storage", e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean compareAndSet(UUID txId, TxState txStateExpected, @NotNull TxMeta txMeta) {
        requireNonNull(txMeta);

        byte[] txIdBytes = toBytes(txId);

        try (Transaction rocksTx = db.beginTransaction(new WriteOptions())) {
            byte[] txMetaExistingBytes = rocksTx.get(new ReadOptions(), toBytes(txId));

            if (txStateExpected == null) {
                if (txMetaExistingBytes == null) {
                    rocksTx.put(txIdBytes, toBytes(txMeta));

                    rocksTx.commit();

                    return true;
                } else {
                    return false;
                }
            }

            TxMeta txMetaExisting = (TxMeta) fromBytes(txMetaExistingBytes);

            if (txMetaExisting.txState() == txStateExpected) {
                rocksTx.put(txIdBytes, toBytes(txMeta));

                rocksTx.commit();

                return true;
            } else {
                rocksTx.rollback();

                return false;
            }
        } catch (RocksDBException e) {
            throw new IgniteInternalException(
                TX_STATE_STORAGE_ERR,
                "Failed perform CAS operation over a value in transaction state storage",
                e
            );
        }
    }

    /** {@inheritDoc} */
    @Override public void remove(UUID txId) {
        try {
            db.delete(toBytes(txId));
        } catch (RocksDBException e) {
            throw new IgniteInternalException(TX_STATE_STORAGE_ERR, "Failed to remove a value from the transaction state storage", e);
        }
    }

    /** {@inheritDoc} */
    @Override public Cursor<IgniteBiTuple<UUID, TxMeta>> scan() {
        RocksIterator rocksIterator = db.newIterator();
        rocksIterator.seekToFirst();

        RocksIteratorAdapter<IgniteBiTuple<UUID, TxMeta>> iteratorAdapter = new RocksIteratorAdapter<>(rocksIterator) {
            @Override protected IgniteBiTuple<UUID, TxMeta> decodeEntry(byte[] keyBytes, byte[] valueBytes) {
                UUID key = (UUID) fromBytes(keyBytes);
                TxMeta txMeta = (TxMeta) fromBytes(valueBytes);

                return new IgniteBiTuple<>(key, txMeta);
            }
        };

        return Cursor.fromIterator(iteratorAdapter);
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

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> snapshot(Path snapshotPath) {
        return snapshotManager.createSnapshot(snapshotPath);
    }

    /** {@inheritDoc} */
    @Override public void restoreSnapshot(Path snapshotPath) {
        synchronized (snapshotRestoreLock) {
            destroy();

            start();

            snapshotManager.restoreSnapshot(snapshotPath);
        }
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        stop();
    }
}
