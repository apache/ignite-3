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

package org.apache.ignite.internal.raft.storage.impl;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.unmodifiableSet;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.raft.storage.impl.RocksDbSharedLogStorageUtils.raftNodeStorageEndPrefix;
import static org.apache.ignite.internal.raft.storage.impl.RocksDbSharedLogStorageUtils.raftNodeStorageStartPrefix;
import static org.apache.ignite.internal.rocksdb.RocksUtils.incrementPrefix;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.rocksdb.RocksDB.DEFAULT_COLUMN_FAMILY;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.components.NoOpLogSyncer;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.internal.rocksdb.LoggingRocksDbFlushListener;
import org.apache.ignite.internal.rocksdb.RocksUtils;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.apache.ignite.raft.jraft.util.ExecutorServiceHelper;
import org.apache.ignite.raft.jraft.util.Platform;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.rocksdb.AbstractEventListener;
import org.rocksdb.AbstractNativeReference;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.Env;
import org.rocksdb.Priority;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.rocksdb.util.SizeUnit;

/** Implementation of the {@link LogStorageFactory} that creates {@link RocksDbSharedLogStorage}s. */
public class DefaultLogStorageFactory implements LogStorageFactory {
    private static final IgniteLogger LOG = Loggers.forClass(DefaultLogStorageFactory.class);

    static final byte[] FINISHED_META_MIGRATION_META_KEY = {0};

    static final byte[] STORAGE_CREATED_META_PREFIX = {1};

    /** Name of the log factory, will be used in logs. */
    private final String factoryName;

    /** Node name. */
    private final String nodeName;

    /** Path to the log storage. */
    private final Path logPath;

    /** Executor for shared storages. */
    private final ExecutorService executorService;

    /** Database instance shared across log storages. */
    private RocksDB db;

    /** Database options. */
    private DBOptions dbOptions;

    /** Write options to use in writes to database. */
    private WriteOptions writeOptions;

    private ColumnFamilyHandle metaHandle;

    /** Configuration column family handle. */
    private ColumnFamilyHandle confHandle;

    /** Data column family handle. */
    private ColumnFamilyHandle dataHandle;

    private ColumnFamilyOptions cfOption;

    private AbstractEventListener flushListener;

    private final boolean fsync;

    /**
     * Thread-local batch instance, used by {@link RocksDbSharedLogStorage#appendEntriesToBatch(List)} and
     * {@link RocksDbSharedLogStorage#commitWriteBatch()}.
     * <br>
     * Shared between instances to provide more efficient way of executing batch updates.
     */
    @SuppressWarnings("ThreadLocalNotStaticFinal")
    private final ThreadLocal<WriteBatch> threadLocalWriteBatch = new ThreadLocal<>();

    /**
     * Constructor.
     *
     * @param path Path to the storage.
     */
    @TestOnly
    public DefaultLogStorageFactory(Path path) {
        this("test", "test", path, true);
    }

    /**
     * Constructor.
     *
     * @param factoryName Name of the log factory, will be used in logs.
     * @param nodeName Node name.
     * @param logPath Function to get path to the log storage.
     * @param fsync If should fsync after each write to database.
     */
    public DefaultLogStorageFactory(String factoryName, String nodeName, Path logPath, boolean fsync) {
        this.factoryName = factoryName;
        this.logPath = logPath;
        this.fsync = fsync;
        this.nodeName = nodeName;

        executorService = Executors.newSingleThreadExecutor(
                IgniteThreadFactory.create(nodeName, "raft-shared-log-storage-pool", LOG)
        );
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        // This is effectively a sync implementation.
        try {
            start();

            return nullCompletedFuture();
        } catch (Exception ex) {
            return failedFuture(ex);
        }
    }

    private void start() throws Exception {
        try {
            Files.createDirectories(logPath);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create directory: " + logPath, e);
        }

        List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

        this.dbOptions = createDbOptions();

        this.writeOptions = new WriteOptions().setSync(dbOptions.useFsync());

        this.cfOption = createColumnFamilyOptions();

        this.flushListener = new LoggingRocksDbFlushListener(factoryName, nodeName);

        List<ColumnFamilyDescriptor> columnFamilyDescriptors = List.of(
                // Column family to store metadata.
                new ColumnFamilyDescriptor("Meta".getBytes(UTF_8), cfOption),
                // Column family to store configuration log entry.
                new ColumnFamilyDescriptor("Configuration".getBytes(UTF_8), cfOption),
                // Default column family to store user data log entry.
                new ColumnFamilyDescriptor(DEFAULT_COLUMN_FAMILY, cfOption)
        );

        try {
            dbOptions.setListeners(List.of(flushListener));

            this.db = RocksDB.open(this.dbOptions, logPath.toString(), columnFamilyDescriptors, columnFamilyHandles);

            // Setup rocks thread pools to utilize all the available cores as the database is shared among
            // all the raft groups
            Env env = db.getEnv();
            // Setup background flushes pool
            env.setBackgroundThreads(Runtime.getRuntime().availableProcessors(), Priority.HIGH);
            // Setup background compactions pool
            env.setBackgroundThreads(Runtime.getRuntime().availableProcessors(), Priority.LOW);

            assert (columnFamilyHandles.size() == 3);
            this.metaHandle = columnFamilyHandles.get(0);
            this.confHandle = columnFamilyHandles.get(1);
            this.dataHandle = columnFamilyHandles.get(2);

            MetadataMigration metadataMigration = metadataMigration();
            metadataMigration.migrateIfNeeded();
        } catch (Exception e) {
            closeRocksResources();

            throw e;
        }
    }

    MetadataMigration metadataMigration() {
        return new MetadataMigration(db, writeOptions, metaHandle, confHandle, dataHandle);
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        ExecutorServiceHelper.shutdownAndAwaitTermination(executorService);

        try {
            closeRocksResources();
        } catch (RuntimeException ex) {
            return failedFuture(ex);
        }

        return nullCompletedFuture();
    }

    private void closeRocksResources() {
        // RocksUtils will handle nulls so we are good.
        List<AbstractNativeReference> closables = new ArrayList<>();
        closables.add(confHandle);
        closables.add(dataHandle);
        closables.add(db);
        closables.add(dbOptions);
        closables.add(cfOption);
        closables.add(flushListener);
        closables.add(writeOptions);

        RocksUtils.closeAll(closables);
    }

    @Override
    public LogStorage createLogStorage(String raftNodeStorageId, RaftOptions raftOptions) {
        // raftOptions is ignored as fsync status is passed via dbOptions.

        return new RocksDbSharedLogStorage(this, db, metaHandle, confHandle, dataHandle, raftNodeStorageId, writeOptions, executorService);
    }

    @Override
    public void destroyLogStorage(String uri) {
        try (WriteBatch writeBatch = new WriteBatch()) {
            RocksDbSharedLogStorage.destroyAllEntriesBetween(
                    writeBatch,
                    confHandle,
                    dataHandle,
                    raftNodeStorageStartPrefix(uri),
                    raftNodeStorageEndPrefix(uri)
            );

            writeBatch.delete(metaHandle, RocksDbSharedLogStorage.storageCreatedKey(uri));

            db.write(this.writeOptions, writeBatch);
        } catch (RocksDBException e) {
            throw new LogStorageException("Fail to destroy the log storage for " + uri, e);
        }
    }

    @Override
    public LogSyncer logSyncer() {
        return fsync ? new NoOpLogSyncer() : () -> db.syncWal();
    }

    /**
     * Returns or creates a thread-local {@link WriteBatch} instance, attached to current factory, for appending data
     * from multiple storages at the same time.
     */
    WriteBatch getOrCreateThreadLocalWriteBatch() {
        WriteBatch writeBatch = threadLocalWriteBatch.get();

        if (writeBatch == null) {
            writeBatch = new WriteBatch();

            threadLocalWriteBatch.set(writeBatch);
        }

        return writeBatch;
    }

    /**
     * Returns a thread-local {@link WriteBatch} instance, attached to current factory, for appending append data from multiple storages
     * at the same time.
     *
     * @return {@link WriteBatch} instance or {@code null} if it was never created.
     */
    @Nullable
    WriteBatch getThreadLocalWriteBatch() {
        return threadLocalWriteBatch.get();
    }

    /**
     * Clears {@link WriteBatch} returned by {@link #getOrCreateThreadLocalWriteBatch()}.
     */
    void clearThreadLocalWriteBatch(WriteBatch writeBatch) {
        writeBatch.close();

        threadLocalWriteBatch.remove();
    }

    /**
     * Creates database options.
     *
     * @return Default database options.
     */
    protected DBOptions createDbOptions() {
        return new DBOptions()
                .setMaxBackgroundJobs(Runtime.getRuntime().availableProcessors() * 2)
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setUseFsync(fsync);
    }

    /** Returns current {@link DBOptions} (or {@code null} if the factory is not started yet).. */
    @TestOnly
    public @Nullable DBOptions dbOptions() {
        return dbOptions;
    }

    /** Returns current {@link WriteOptions} (or {@code null} if the factory is not started yet). */
    @TestOnly
    public @Nullable WriteOptions writeOptions() {
        return writeOptions;
    }

    /**
     * Creates column family options.
     *
     * @return Default column family options.
     */
    private static ColumnFamilyOptions createColumnFamilyOptions() {
        var opts = new ColumnFamilyOptions();

        opts.setWriteBufferSize(64 * SizeUnit.MB);
        opts.setMaxWriteBufferNumber(5);
        opts.setMinWriteBufferNumberToMerge(1);
        opts.setLevel0FileNumCompactionTrigger(50);
        opts.setLevel0SlowdownWritesTrigger(100);
        opts.setLevel0StopWritesTrigger(200);
        // Size of level 0 which is (in stable state) equal to
        // WriteBufferSize * MinWriteBufferNumberToMerge * Level0FileNumCompactionTrigger
        opts.setMaxBytesForLevelBase(3200 * SizeUnit.MB);
        opts.setTargetFileSizeBase(320 * SizeUnit.MB);

        if (!Platform.isWindows()) {
            opts.setCompressionType(CompressionType.LZ4_COMPRESSION)
                    .setCompactionStyle(CompactionStyle.LEVEL)
                    .optimizeLevelStyleCompaction();
        }

        return opts;
    }

    @Override
    public Set<String> raftNodeStorageIdsOnDisk() {
        var groupIdsForStorage = new HashSet<String>();

        try (
                Slice upperBoundSlice = new Slice(incrementPrefix(STORAGE_CREATED_META_PREFIX));
                ReadOptions readOptions = new ReadOptions().setIterateUpperBound(upperBoundSlice);
                RocksIterator iterator = db.newIterator(metaHandle, readOptions)
        ) {
            iterator.seek(STORAGE_CREATED_META_PREFIX);

            while (iterator.isValid()) {
                byte[] key = iterator.key();

                String idForStorage = new String(
                        key,
                        STORAGE_CREATED_META_PREFIX.length,
                        key.length - STORAGE_CREATED_META_PREFIX.length,
                        UTF_8
                );
                groupIdsForStorage.add(idForStorage);

                iterator.next();
            }

            // Doing this to make an exception thrown if the iteration was stopped due to an error and not due to exhausting
            // the iteration space.
            iterator.status();
        } catch (RocksDBException e) {
            throw new IgniteInternalException(INTERNAL_ERR, "Cannot get group storage IDs", e);
        }

        return unmodifiableSet(groupIdsForStorage);
    }

    @TestOnly
    RocksDB db() {
        return db;
    }

    @TestOnly
    ColumnFamilyHandle metaColumnFamilyHandle() {
        return metaHandle;
    }

    @TestOnly
    ColumnFamilyHandle confColumnFamilyHandle() {
        return confHandle;
    }

    @TestOnly
    ColumnFamilyHandle dataColumnFamilyHandle() {
        return dataHandle;
    }
}
