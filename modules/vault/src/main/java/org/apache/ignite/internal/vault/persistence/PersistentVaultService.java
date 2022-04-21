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

package org.apache.ignite.internal.vault.persistence;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.ignite.internal.future.InFlightFutures;
import org.apache.ignite.internal.rocksdb.RocksIteratorAdapter;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultService;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.CompactionPriority;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

/**
 * Vault Service implementation based on <a href="https://github.com/facebook/rocksdb">RocksDB</a>.
 */
public class PersistentVaultService implements VaultService {
    static {
        RocksDB.loadLibrary();
    }

    private final ExecutorService threadPool = Executors.newFixedThreadPool(4, new NamedThreadFactory("vault"));

    private final InFlightFutures futureTracker = new InFlightFutures();

    private final Options options = options();

    private volatile RocksDB db;

    /** Base path for RocksDB. */
    private final Path path;

    /**
     * Creates persistent vault service.
     *
     * @param path base path for RocksDB
     */
    public PersistentVaultService(Path path) {
        this.path = path;
    }

    private static Options options() {
        // using the recommended options from https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning
        return new Options()
                .setCreateIfMissing(true)
                .setCompressionType(CompressionType.LZ4_COMPRESSION)
                .setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION)
                .setLevelCompactionDynamicLevelBytes(true)
                .setBytesPerSync(1024 * 1024)
                .setCompactionPriority(CompactionPriority.MinOverlappingRatio)
                .setTableFormatConfig(
                        new BlockBasedTableConfig()
                                .setBlockSize(16 * 1024)
                                .setCacheIndexAndFilterBlocks(true)
                                .setPinL0FilterAndIndexBlocksInCache(true)
                                .setFormatVersion(5)
                                .setFilterPolicy(new BloomFilter(10, false))
                                .setOptimizeFiltersForMemory(true)
                );
    }

    @Override
    public void start() {
        try {
            db = RocksDB.open(options, path.toString());
        } catch (RocksDBException e) {
            throw new IgniteInternalException(e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws Exception {
        IgniteUtils.shutdownAndAwaitTermination(threadPool, 10, TimeUnit.SECONDS);

        futureTracker.cancelInFlightFutures();

        IgniteUtils.closeAll(options, db);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<VaultEntry> get(ByteArray key) {
        return supplyAsync(() -> {
            try {
                byte[] value = db.get(key.bytes());

                return value == null ? null : new VaultEntry(key, value);
            } catch (RocksDBException e) {
                throw new IgniteInternalException("Unable to read data from RocksDB", e);
            }
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> put(ByteArray key, byte @Nullable [] val) {
        return runAsync(() -> {
            try {
                if (val == null) {
                    db.delete(key.bytes());
                } else {
                    db.put(key.bytes(), val);
                }
            } catch (RocksDBException e) {
                throw new IgniteInternalException("Unable to write data to RocksDB", e);
            }
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> remove(ByteArray key) {
        return runAsync(() -> {
            try {
                db.delete(key.bytes());
            } catch (RocksDBException e) {
                throw new IgniteInternalException("Unable to remove data to RocksDB", e);
            }
        });
    }

    /** {@inheritDoc} */
    @Override
    public Cursor<VaultEntry> range(ByteArray fromKey, ByteArray toKey) {
        var readOpts = new ReadOptions();

        var upperBound = new Slice(toKey.bytes());

        readOpts.setIterateUpperBound(upperBound);

        RocksIterator it = db.newIterator(readOpts);

        it.seek(fromKey.bytes());

        return new RocksIteratorAdapter<>(it) {
            @Override
            protected VaultEntry decodeEntry(byte[] key, byte[] value) {
                return new VaultEntry(new ByteArray(key), value);
            }

            @Override
            public void close() throws Exception {
                super.close();

                IgniteUtils.closeAll(upperBound, readOpts);
            }
        };
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> putAll(Map<ByteArray, byte[]> vals) {
        return runAsync(() -> {
            try (
                    var writeBatch = new WriteBatch();
                    var writeOpts = new WriteOptions()
            ) {
                for (var entry : vals.entrySet()) {
                    if (entry.getValue() == null) {
                        writeBatch.delete(entry.getKey().bytes());
                    } else {
                        writeBatch.put(entry.getKey().bytes(), entry.getValue());
                    }
                }

                db.write(writeOpts, writeBatch);
            } catch (RocksDBException e) {
                throw new IgniteInternalException("Unable to write data to RocksDB", e);
            }
        });
    }

    private <T> CompletableFuture<T> supplyAsync(Supplier<T> supplier) {
        CompletableFuture<T> future = CompletableFuture.supplyAsync(supplier, threadPool);

        futureTracker.registerFuture(future);

        return future;
    }

    private CompletableFuture<Void> runAsync(Runnable runnable) {
        CompletableFuture<Void> future = CompletableFuture.runAsync(runnable, threadPool);

        futureTracker.registerFuture(future);

        return future;
    }
}
