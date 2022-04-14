/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.cluster.management.raft;

import static org.apache.ignite.internal.rocksdb.snapshot.ColumnFamilyRange.fullRange;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.rocksdb.RocksIteratorAdapter;
import org.apache.ignite.internal.rocksdb.snapshot.RocksSnapshotManager;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteInternalException;
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
    /** Thread-pool for snapshot operations execution. */
    private final ExecutorService snapshotExecutor = Executors.newFixedThreadPool(2);

    /** Path to the rocksdb database. */
    private final Path dbPath;

    /** RockDB options. */
    @Nullable
    private volatile Options options;

    /** RocksDb instance. */
    @Nullable
    private volatile RocksDB db;

    private volatile RocksSnapshotManager snapshotManager;

    private final Object snapshotRestoreLock = new Object();

    public RocksDbClusterStateStorage(Path dbPath) {
        this.dbPath = dbPath;
    }

    @Override
    public void start() {
        options = new Options().setCreateIfMissing(true);

        try {
            db = RocksDB.open(options, dbPath.toString());

            ColumnFamily defaultCf = ColumnFamily.wrap(db, db.getDefaultColumnFamily());

            snapshotManager = new RocksSnapshotManager(db, List.of(fullRange(defaultCf)), snapshotExecutor);
        } catch (RocksDBException e) {
            throw new IgniteInternalException("Failed to start the storage", e);
        }
    }

    @Override
    public boolean isStarted() {
        return db != null;
    }

    @Override
    public byte @Nullable [] get(byte[] key) {
        try {
            return db.get(key);
        } catch (RocksDBException e) {
            throw new IgniteInternalException("Unable to get data from Rocks DB", e);
        }
    }

    @Override
    public void put(byte[] key, byte[] value) {
        try {
            db.put(key, value);
        } catch (RocksDBException e) {
            throw new IgniteInternalException("Unable to put data into Rocks DB", e);
        }
    }

    @Override
    public void remove(byte[] key) {
        try {
            db.delete(key);
        } catch (RocksDBException e) {
            throw new IgniteInternalException("Unable to remove data from Rocks DB", e);
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
            throw new IgniteInternalException("Unable to remove data from Rocks DB", e);
        }
    }

    @Override
    public <T> Cursor<T> getWithPrefix(byte[] prefix, BiFunction<byte[], byte[], T> entryTransformer) {
        byte[] upperBound = prefix.clone();

        // using 0xFF as max value since RocksDB uses unsigned byte comparison
        assert upperBound[upperBound.length - 1] != (byte) 0xFF;

        upperBound[upperBound.length - 1] += 1;

        Slice upperBoundSlice = new Slice(upperBound);

        ReadOptions readOptions = new ReadOptions().setIterateUpperBound(upperBoundSlice);

        RocksIterator it = db.newIterator(readOptions);

        it.seek(prefix);

        return new RocksIteratorAdapter<>(it) {
            @Override
            protected T decodeEntry(byte[] key, byte[] value) {
                return entryTransformer.apply(key, value);
            }

            @Override
            public void close() throws Exception {
                super.close();

                IgniteUtils.closeAll(readOptions, upperBoundSlice);
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
            destroy();

            start();

            snapshotManager.restoreSnapshot(snapshotPath);
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

    @Override
    public void close() throws Exception {
        IgniteUtils.shutdownAndAwaitTermination(snapshotExecutor, 10, TimeUnit.SECONDS);

        IgniteUtils.closeAll(options, db);

        db = null;

        options = null;
    }
}
