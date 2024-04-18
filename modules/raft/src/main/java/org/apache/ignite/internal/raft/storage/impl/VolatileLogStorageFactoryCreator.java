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
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.raft.configuration.LogStorageBudgetView;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.raft.jraft.util.ExecutorServiceHelper;
import org.apache.ignite.raft.jraft.util.Platform;
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
import org.rocksdb.util.SizeUnit;

/**
 * {@link LogStorageFactoryCreator} for volatile log storage.
 */
public class VolatileLogStorageFactoryCreator implements LogStorageFactoryCreator, IgniteComponent {
    private static final IgniteLogger LOG = Loggers.forClass(VolatileLogStorageFactoryCreator.class);

    /** Database path. */
    private final Path spillOutPath;

    /** Database options. */
    private DBOptions dbOptions;

    /** Shared db instance. */
    private RocksDB db;

    /** Shared data column family handle. */
    private ColumnFamilyHandle columnFamily;

    /** Executor for spill-out RocksDB tasks. */
    private final ExecutorService executorService;

    /**
     * Create a new instance.
     *
     * @param spillOutPath Path at which to put spill-out data.
     */
    public VolatileLogStorageFactoryCreator(String nodeName, Path spillOutPath) {
        this.spillOutPath = Objects.requireNonNull(spillOutPath);

        executorService = Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors() * 2,
                NamedThreadFactory.create(nodeName, "raft-volatile-log-rocksdb-spillout-pool", LOG)
        );
    }

    @Override
    public CompletableFuture<Void> startAsync() {
        try {
            Files.createDirectories(spillOutPath);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create directory: " + this.spillOutPath, e);
        }

        wipeOutDb();

        dbOptions = createDbOptions();
        ColumnFamilyOptions cfOption = createColumnFamilyOptions();

        List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

        List<ColumnFamilyDescriptor> columnFamilyDescriptors = List.of(
                new ColumnFamilyDescriptor(DEFAULT_COLUMN_FAMILY, cfOption)
        );

        try {
            db = RocksDB.open(this.dbOptions, this.spillOutPath.toString(), columnFamilyDescriptors, columnFamilyHandles);

            // Setup rocks thread pools to utilize all the available cores as the database is shared among
            // all the raft groups
            Env env = db.getEnv();
            // Setup background flushes pool
            env.setBackgroundThreads(Runtime.getRuntime().availableProcessors(), Priority.HIGH);
            // Setup background compactions pool
            env.setBackgroundThreads(Runtime.getRuntime().availableProcessors(), Priority.LOW);

            assert (columnFamilyHandles.size() == 1);
            this.columnFamily = columnFamilyHandles.get(0);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return nullCompletedFuture();
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
    public CompletableFuture<Void> stopAsync() {
        ExecutorServiceHelper.shutdownAndAwaitTermination(executorService);

        try {
            IgniteUtils.closeAll(columnFamily, db, dbOptions);
        } catch (Exception e) {
            return failedFuture(e);
        }

        return nullCompletedFuture();
    }

    @Override
    public LogStorageFactory factory(LogStorageBudgetView budgetView) {
        return new VolatileLogStorageFactory(budgetView, db, columnFamily, executorService);
    }
}
