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

package org.apache.ignite.internal.cluster.management.raft;

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.rocksdb.snapshot.ColumnFamilyRange.fullRange;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.rocksdb.RocksIteratorAdapter;
import org.apache.ignite.internal.rocksdb.RocksUtils;
import org.apache.ignite.internal.rocksdb.snapshot.RocksSnapshotManager;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

/**
 * {@link ClusterStateStorage} implementation based on RocksDB.
 */
public class RocksDbClusterStateStorage implements ClusterStateStorage {
    private static final IgniteLogger LOG = Loggers.forClass(RocksDbClusterStateStorage.class);

    /** Thread-pool for snapshot operations execution. */
    private final ExecutorService snapshotExecutor;

    /** Path to the rocksdb database. */
    private final Path dbPath;

    /** RockDB options. */
    private final Options options = new Options().setCreateIfMissing(true);

    /** RocksDb instance. */
    @Nullable
    private volatile RocksDB db;

    private volatile RocksSnapshotManager snapshotManager;

    private final Object snapshotRestoreLock = new Object();

    /**
     * Creates a new instance.
     *
     * @param dbPath Path to the database.
     * @param nodeName Ignite node name.
     */
    public RocksDbClusterStateStorage(Path dbPath, String nodeName) {
        this.dbPath = dbPath;
        this.snapshotExecutor = Executors.newSingleThreadExecutor(
                NamedThreadFactory.create(nodeName, "cluster-state-snapshot-executor", LOG)
        );
    }

    @Override
    public CompletableFuture<Void> start() {
        try {
            // Delete existing data, relying on log playback.
            RocksDB.destroyDB(dbPath.toString(), options);

            init();
        } catch (RocksDBException e) {
            return failedFuture(new CmgStorageException("Failed to start the storage", e));
        } catch (CmgStorageException e) {
            return failedFuture(e);
        }

        return nullCompletedFuture();
    }

    private void init() {
        try {
            RocksDB db = RocksDB.open(options, dbPath.toString());

            ColumnFamily defaultCf = ColumnFamily.wrap(db, db.getDefaultColumnFamily());

            snapshotManager = new RocksSnapshotManager(db, List.of(fullRange(defaultCf)), snapshotExecutor);

            this.db = db;
        } catch (RocksDBException e) {
            throw new CmgStorageException("Failed to start the storage", e);
        }
    }

    @Override
    public byte @Nullable [] get(byte[] key) {
        try {
            return db.get(key);
        } catch (RocksDBException e) {
            throw new CmgStorageException("Unable to get data from Rocks DB", e);
        }
    }

    @Override
    public void put(byte[] key, byte[] value) {
        try {
            db.put(key, value);
        } catch (RocksDBException e) {
            throw new CmgStorageException("Unable to put data into Rocks DB", e);
        }
    }

    @Override
    public void replaceAll(byte[] prefix, byte[] key, byte[] value) {
        try (
                var batch = new WriteBatch();
                var options = new WriteOptions();
        ) {
            byte[] endKey = RocksUtils.incrementPrefix(prefix);

            assert endKey != null : Arrays.toString(prefix);

            batch.deleteRange(prefix, endKey);

            batch.put(key, value);

            db.write(options, batch);
        } catch (RocksDBException e) {
            throw new CmgStorageException("Unable to replace data in Rocks DB", e);
        }
    }

    @Override
    public void remove(byte[] key) {
        try {
            db.delete(key);
        } catch (RocksDBException e) {
            throw new CmgStorageException("Unable to remove data from Rocks DB", e);
        }
    }

    @Override
    public void removeAll(Collection<byte[]> keys) {
        try (
                var batch = new WriteBatch();
                var options = new WriteOptions();
        ) {
            for (byte[] key : keys) {
                batch.delete(key);
            }

            db.write(options, batch);
        } catch (RocksDBException e) {
            throw new CmgStorageException("Unable to remove data from Rocks DB", e);
        }
    }

    @Override
    public <T> Cursor<T> getWithPrefix(byte[] prefix, BiFunction<byte[], byte[], T> entryTransformer) {
        byte[] upperBound = RocksUtils.incrementPrefix(prefix);

        Slice upperBoundSlice = upperBound == null ? null : new Slice(upperBound);

        ReadOptions readOptions = new ReadOptions().setIterateUpperBound(upperBoundSlice);

        RocksIterator it = db.newIterator(readOptions);

        it.seek(prefix);

        return new RocksIteratorAdapter<>(it) {
            @Override
            protected T decodeEntry(byte[] key, byte[] value) {
                return entryTransformer.apply(key, value);
            }

            @Override
            public void close() {
                super.close();

                RocksUtils.closeAll(readOptions, upperBoundSlice);
            }
        };
    }

    @Override
    public CompletableFuture<Void> snapshot(Path snapshotPath) {
        return snapshotManager.createSnapshot(snapshotPath);
    }

    @Override
    public void restoreSnapshot(Path snapshotPath) {
        synchronized (snapshotRestoreLock) {
            destroyDb();

            init();

            snapshotManager.restoreSnapshot(snapshotPath);
        }
    }

    @Override
    public void destroy() {
        IgniteUtils.shutdownAndAwaitTermination(snapshotExecutor, 10, TimeUnit.SECONDS);

        destroyDb();

        options.close();
    }

    @Override
    public void stop() {
        IgniteUtils.shutdownAndAwaitTermination(snapshotExecutor, 10, TimeUnit.SECONDS);

        RocksUtils.closeAll(db, options);
    }

    private void destroyDb() {
        db.close();

        try {
            RocksDB.destroyDB(dbPath.toString(), options);
        } catch (RocksDBException e) {
            throw new CmgStorageException("Unable to stop the RocksDB instance", e);
        }
    }
}
