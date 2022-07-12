/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.tx.storage.state;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.rocksdb.snapshot.RocksSnapshotManager;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;
import org.rocksdb.TransactionDB;
import org.rocksdb.TransactionDBOptions;
import org.rocksdb.WriteOptions;

import static org.apache.ignite.internal.rocksdb.snapshot.ColumnFamilyRange.fullRange;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_STATE_STORAGE_CREATE_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_STATE_STORAGE_ERR;

public class TxMetaRocksDbStorage implements TxMetaStorage, AutoCloseable {
    private final Path dbPath;

    private volatile TransactionDB db;

    /** RockDB options. */
    @Nullable
    private volatile Options options;

    private volatile TransactionDBOptions txDbOptions;

    /** Thread-pool for snapshot operations execution. */
    private final ExecutorService snapshotExecutor;

    private volatile RocksSnapshotManager snapshotManager;

    private final Object snapshotRestoreLock = new Object();

    public TxMetaRocksDbStorage(Path dbPath, ExecutorService snapshotExecutor) {
        this.dbPath = dbPath;
        this.snapshotExecutor = snapshotExecutor;
    }

    /** {@inheritDoc} */
    @Override public void start() {
        try {
            this.options = new Options().setCreateIfMissing(true);

            this.txDbOptions = new TransactionDBOptions();

            this.db = TransactionDB.open(options, txDbOptions, dbPath.toString());

            ColumnFamily defaultCf = ColumnFamily.wrap(db, db.getDefaultColumnFamily());

            snapshotManager = new RocksSnapshotManager(db, List.of(fullRange(defaultCf)), snapshotExecutor);
        } catch (RocksDBException e) {
            throw new IgniteInternalException(TX_STATE_STORAGE_CREATE_ERR, "Failed to start the storage", e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isStarted() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void stop() throws Exception {
        IgniteUtils.closeAll(options, txDbOptions, db);

        db = null;
        options = null;
        txDbOptions = null;
    }

    /** {@inheritDoc} */
    @Override public TxMeta get(UUID txId) {
        try {
            byte[] txMetaBytes = db.get(toBytes(txId));

            return txMetaBytes == null ? null : (TxMeta) fromBytes(txMetaBytes);
        }
        catch (RocksDBException e) {
            throw new IgniteInternalException(TX_STATE_STORAGE_ERR, e);
        }
    }

    /** {@inheritDoc} */
    @Override public void put(UUID txId, TxMeta txMeta) {
        try {
            db.put(toBytes(txId), toBytes(txMeta));
        }
        catch (RocksDBException e) {
            throw new IgniteInternalException(TX_STATE_STORAGE_ERR, e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean compareAndSet(UUID txId, TxMeta txMetaExpected, Object txMeta) {
        byte[] txMetaExpectedBytes = toBytes(txMetaExpected);
        byte[] txIdBytes = toBytes(txId);

        try (Transaction rocksTx = db.beginTransaction(new WriteOptions())) {
            byte[] txMetaExisting = rocksTx.get(new ReadOptions(), toBytes(txId));

            if (Arrays.equals(txMetaExpectedBytes, txMetaExisting)) {
                rocksTx.put(txIdBytes, toBytes(txMeta));

                rocksTx.commit();

                return true;
            } else {
                rocksTx.rollback();

                return false;
            }
        }
        catch (RocksDBException e) {
            throw new IgniteInternalException(TX_STATE_STORAGE_ERR, e);
        }
    }

    /** {@inheritDoc} */
    @Override public void remove(UUID txId) {
        try {
            db.delete(toBytes(txId));
        }
        catch (RocksDBException e) {
            throw new IgniteInternalException(TX_STATE_STORAGE_ERR, e);
        }
    }

    @Override
    public void destroy() {
        try (Options options = new Options()) {
            close();

            RocksDB.destroyDB(dbPath.toString(), options);
        } catch (Exception e) {
            throw new IgniteInternalException("Unable to clear RocksDB instance", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> snapshot(Path snapshotPath) {
        return snapshotManager.createSnapshot(snapshotPath);
    }

    /** {@inheritDoc} */
    @Override
    public void restoreSnapshot(Path snapshotPath) {
        synchronized (snapshotRestoreLock) {
            destroy();

            start();

            snapshotManager.restoreSnapshot(snapshotPath);
        }
    }

    @Override public void close() throws Exception {
        stop();
    }
}
