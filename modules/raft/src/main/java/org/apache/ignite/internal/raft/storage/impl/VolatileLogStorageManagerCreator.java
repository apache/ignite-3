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

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.rocksdb.RocksUtils.closeAll;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.rocksdb.RocksDB.DEFAULT_COLUMN_FAMILY;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.raft.configuration.LogStorageBudgetView;
import org.apache.ignite.internal.raft.storage.LogStorageManager;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.raft.jraft.util.ExecutorServiceHelper;
import org.apache.ignite.raft.jraft.util.Platform;
import org.jetbrains.annotations.TestOnly;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.Env;
import org.rocksdb.Options;
import org.rocksdb.Priority;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileManager;
import org.rocksdb.util.SizeUnit;

/**
 * {@link LogStorageManagerCreator} for volatile log storage.
 */
public class VolatileLogStorageManagerCreator implements LogStorageManagerCreator, IgniteComponent {
    private static final IgniteLogger LOG = Loggers.forClass(VolatileLogStorageManagerCreator.class);

    /** Database path. */
    private final Path spillOutPath;

    private Env env;

    private SstFileManager sstFileManager;

    /** Database options. */
    private DBOptions dbOptions;

    private ColumnFamilyOptions cfOption;

    /** Shared db instance. */
    private RocksDB db;

    /** Shared data column family handle. */
    private ColumnFamilyHandle columnFamily;

    /** Executor for spill-out RocksDB tasks. */
    private final ExecutorService executorService;

    private RocksDbSizeCalculator sizeCalculator;

    /**
     * Create a new instance.
     *
     * @param spillOutPath Path at which to put spill-out data.
     */
    public VolatileLogStorageManagerCreator(String nodeName, Path spillOutPath) {
        this.spillOutPath = Objects.requireNonNull(spillOutPath);

        executorService = Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors() * 2,
                IgniteThreadFactory.create(nodeName, "raft-volatile-log-rocksdb-spillout-pool", LOG)
        );
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        try {
            Files.createDirectories(spillOutPath);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create directory: " + this.spillOutPath, e);
        }

        wipeOutDb();

        env = createEnv();

        dbOptions = createDbOptions();
        cfOption = createColumnFamilyOptions();

        List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

        List<ColumnFamilyDescriptor> columnFamilyDescriptors = List.of(
                new ColumnFamilyDescriptor(DEFAULT_COLUMN_FAMILY, cfOption)
        );

        try {
            sstFileManager = new SstFileManager(env);

            dbOptions.setEnv(env);
            dbOptions.setSstFileManager(sstFileManager);

            db = RocksDB.open(this.dbOptions, this.spillOutPath.toString(), columnFamilyDescriptors, columnFamilyHandles);

            // Setup rocks thread pools to utilize all the available cores as the database is shared among
            // all the raft groups
            Env dbEnv = db.getEnv();
            // Setup background flushes pool
            dbEnv.setBackgroundThreads(Runtime.getRuntime().availableProcessors(), Priority.HIGH);
            // Setup background compactions pool
            dbEnv.setBackgroundThreads(Runtime.getRuntime().availableProcessors(), Priority.LOW);

            assert (columnFamilyHandles.size() == 1);
            this.columnFamily = columnFamilyHandles.get(0);
        } catch (Exception e) {
            closeRocksResources();

            throw new RuntimeException(e);
        }

        sizeCalculator = new RocksDbSizeCalculator(db, sstFileManager);

        return nullCompletedFuture();
    }

    protected Env createEnv() {
        return Env.getDefault();
    }

    private void wipeOutDb() {
        try (var options = new Options()) {
            RocksDB.destroyDB(spillOutPath.toString(), options);
        } catch (RocksDBException e) {
            throw new IgniteInternalException("Cannot destroy spill-out RocksDB at " + spillOutPath, e);
        }
    }

    /**
     * Creates database options.
     *
     * @return Default database options.
     */
    private static DBOptions createDbOptions() {
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

        // TODO: IGNITE-17560 - parameterize via configuration

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
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        ExecutorServiceHelper.shutdownAndAwaitTermination(executorService);

        try {
            closeRocksResources();
        } catch (Exception e) {
            return failedFuture(e);
        }

        return nullCompletedFuture();
    }

    private void closeRocksResources() {
        // This class obtains a default env which is not necessary to be closed, but the closure call is tolerated.
        // But future subclasses might override how they create env, so we still close it explicitly.
        closeAll(columnFamily, db, dbOptions, cfOption, sstFileManager, env);
    }

    @Override
    public LogStorageManager manager(LogStorageBudgetView budgetView) {
        return new VolatileLogStorageManager(budgetView, db, columnFamily, executorService);
    }

    @TestOnly
    RocksDB db() {
        return db;
    }

    /**
     * Returns total number of bytes occupied on disk by the log storages managed by this object.
     */
    public long totalBytesOnDisk() {
        return sizeCalculator.totalBytesOnDisk();
    }
}
