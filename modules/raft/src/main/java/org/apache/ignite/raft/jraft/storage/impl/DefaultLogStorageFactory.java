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

package org.apache.ignite.raft.jraft.storage.impl;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.rocksdb.RocksDB.DEFAULT_COLUMN_FAMILY;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.apache.ignite.raft.jraft.storage.LogStorageFactory;
import org.apache.ignite.raft.jraft.util.ExecutorServiceHelper;
import org.apache.ignite.raft.jraft.util.Platform;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.Env;
import org.rocksdb.Priority;
import org.rocksdb.RocksDB;
import org.rocksdb.util.SizeUnit;

/** Implementation of the {@link LogStorageFactory} that creates {@link RocksDbSharedLogStorage}s. */
public class DefaultLogStorageFactory implements LogStorageFactory {
    /** Database path. */
    private final Path path;

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

    /**
     * Constructor.
     *
     * @param path Path to the storage.
     */
    public DefaultLogStorageFactory(Path path) {
        this.path = path;

        executorService = Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors() * 2,
                new NamedThreadFactory("raft-shared-log-storage-pool")
        );
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        try {
            Files.createDirectories(path);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create directory: " + this.path, e);
        }

        List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

        this.dbOptions = createDbOptions();

        ColumnFamilyOptions cfOption = createColumnFamilyOptions();


        List<ColumnFamilyDescriptor> columnFamilyDescriptors = List.of(
                // Column family to store configuration log entry.
                new ColumnFamilyDescriptor("Configuration".getBytes(UTF_8), cfOption),
                // Default column family to store user data log entry.
                new ColumnFamilyDescriptor(DEFAULT_COLUMN_FAMILY, cfOption)
        );

        try {
            this.db = RocksDB.open(this.dbOptions, this.path.toString(), columnFamilyDescriptors, columnFamilyHandles);

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
            throw new RuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws Exception {
        ExecutorServiceHelper.shutdownAndAwaitTermination(executorService);

        IgniteUtils.closeAll(confHandle, dataHandle, db, dbOptions);
    }

    /** {@inheritDoc} */
    @Override
    public LogStorage getLogStorage(String groupId, RaftOptions raftOptions) {
        return new RocksDbSharedLogStorage(db, confHandle, dataHandle, groupId, raftOptions, executorService);
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
