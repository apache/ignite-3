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
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.rocksdb.RocksDB.DEFAULT_COLUMN_FAMILY;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.internal.rocksdb.RocksUtils;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.apache.ignite.raft.jraft.util.ExecutorServiceHelper;
import org.apache.ignite.raft.jraft.util.Platform;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.rocksdb.AbstractNativeReference;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.Env;
import org.rocksdb.Priority;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.util.SizeUnit;

/** Implementation of the {@link LogStorageFactory} that creates {@link RocksDbSharedLogStorage}s. */
public class DefaultLogStorageFactory implements LogStorageFactory {
    private static final IgniteLogger LOG = Loggers.forClass(DefaultLogStorageFactory.class);

    /** Function to get path to the log storage. */
    private final Supplier<Path> logPathSupplier;

    /** Executor for shared storages. */
    private final ExecutorService executorService;

    /** Database instance shared across log storages. */
    private RocksDB db;

    /** Database options. */
    private DBOptions dbOptions;

    /** Configuration column family handle. */
    private ColumnFamilyHandle confHandle;

    /** Data column family handle. */
    private ColumnFamilyHandle dataHandle;

    private ColumnFamilyOptions cfOption;

    protected List<AbstractNativeReference> additionalDbClosables = new ArrayList<>();

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
        this("test", () -> path);
    }

    /**
     * Constructor.
     *
     * @param logPathSupplier Function to get path to the log storage.
     */
    public DefaultLogStorageFactory(String nodeName, Supplier<Path> logPathSupplier) {
        this.logPathSupplier = logPathSupplier;

        executorService = Executors.newSingleThreadExecutor(
                NamedThreadFactory.create(nodeName, "raft-shared-log-storage-pool", LOG)
        );
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        // This is effectively a sync implementation.
        try {
            start();
            return nullCompletedFuture();
        } catch (RuntimeException ex) {
            return failedFuture(ex);
        }
    }

    private void start() {
        Path logPath = logPathSupplier.get();

        try {
            Files.createDirectories(logPath);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create directory: " + logPath, e);
        }

        List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

        this.dbOptions = createDbOptions();

        this.cfOption = createColumnFamilyOptions();

        List<ColumnFamilyDescriptor> columnFamilyDescriptors = List.of(
                // Column family to store configuration log entry.
                new ColumnFamilyDescriptor("Configuration".getBytes(UTF_8), cfOption),
                // Default column family to store user data log entry.
                new ColumnFamilyDescriptor(DEFAULT_COLUMN_FAMILY, cfOption)
        );

        try {
            this.db = RocksDB.open(this.dbOptions, logPath.toString(), columnFamilyDescriptors, columnFamilyHandles);

            // Setup rocks thread pools to utilize all the available cores as the database is shared among
            // all the raft groups
            Env env = db.getEnv();
            // Setup background flushes pool
            env.setBackgroundThreads(Runtime.getRuntime().availableProcessors(), Priority.HIGH);
            // Setup background  compactions pool
            env.setBackgroundThreads(Runtime.getRuntime().availableProcessors(), Priority.LOW);

            assert (columnFamilyHandles.size() == 2);
            this.confHandle = columnFamilyHandles.get(0);
            this.dataHandle = columnFamilyHandles.get(1);
        } catch (Exception e) {
            closeRocksResources();
            throw new RuntimeException(e);
        }
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
        closables.addAll(additionalDbClosables);
        closables.add(cfOption);

        RocksUtils.closeAll(closables);
    }

    /** {@inheritDoc} */
    @Override
    public LogStorage createLogStorage(String groupId, RaftOptions raftOptions) {
        return new RocksDbSharedLogStorage(this, db, confHandle, dataHandle, groupId, raftOptions, executorService);
    }

    @Override
    public void sync() throws RocksDBException {
        if (!dbOptions.useFsync()) {
            db.syncWal();
        }
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
            .setCreateMissingColumnFamilies(true);
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
}
