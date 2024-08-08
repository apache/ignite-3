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

package org.apache.ignite.internal.tx.storage.state.rocksdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.reverse;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntSupplier;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.rocksdb.flush.RocksDbFlusher;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;

/**
 * Shared RocksDB storage instance to be used in {@link TxStateRocksDbTableStorage}. Exists to make "createTable" operation faster, as well
 * as reducing the amount of resources that would otherwise be used by multiple RocksDB instances, if they existed on per-table basis.
 */
public class TxStateRocksDbSharedStorage implements ManuallyCloseable {
    static {
        RocksDB.loadLibrary();
    }

    /** Column family name for transaction states. */
    private static final String TX_STATE_CF = new String(RocksDB.DEFAULT_COLUMN_FAMILY, UTF_8);

    /** Rocks DB instance. */
    private volatile RocksDB db;

    /** RocksDb database options. */
    private volatile DBOptions dbOptions;

    /** Write options. */
    final WriteOptions writeOptions = new WriteOptions().setDisableWAL(true);

    /** Read options for regular reads. */
    final ReadOptions readOptions = new ReadOptions();

    /** Database path. */
    private final Path dbPath;

    /** RocksDB flusher instance. */
    private volatile RocksDbFlusher flusher;

    /** Prevents double stopping the storage. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Scheduled executor to be used by internal operations, such as {@link #awaitFlush(boolean)}. */
    private final ScheduledExecutorService scheduledExecutor;

    /** Thread pool to execute after-flush actions. */
    private final ExecutorService threadPool;

    /** Supplier for the value of delay for scheduled database flush. */
    private final IntSupplier flushDelaySupplier;

    /** Write-ahead log synchronizer. */
    private final LogSyncer logSyncer;

    /**
     * Constructor.
     *
     * @param dbPath Database path.
     * @param scheduledExecutor Scheduled executor for delayed flushes.
     * @param threadPool Thread pool for internal operations.
     * @param logSyncer Write-ahead log synchronizer.
     * @param flushDelaySupplier Flush delay supplier.
     *
     * @see RocksDbFlusher
     */
    public TxStateRocksDbSharedStorage(
            Path dbPath,
            ScheduledExecutorService scheduledExecutor,
            ExecutorService threadPool,
            LogSyncer logSyncer,
            IntSupplier flushDelaySupplier
    ) {
        this.dbPath = dbPath;
        this.scheduledExecutor = scheduledExecutor;
        this.threadPool = threadPool;
        this.flushDelaySupplier = flushDelaySupplier;
        this.logSyncer = logSyncer;
    }

    /**
     * Returns shared {@link RocksDB} instance.
     */
    RocksDB db() {
        return db;
    }

    /**
     * Returns a future to await flush.
     */
    CompletableFuture<Void> awaitFlush(boolean schedule) {
        return flusher.awaitFlush(schedule);
    }

    /**
     * Starts the storage.
     *
     * @throws IgniteInternalException If failed to create directory or start the RocksDB storage.
     */
    public void start() {
        try {
            Files.createDirectories(dbPath);

            flusher = new RocksDbFlusher(
                    busyLock,
                    scheduledExecutor,
                    threadPool,
                    flushDelaySupplier,
                    logSyncer,
                    () -> {} // No-op.
            );

            this.dbOptions = new DBOptions()
                    .setCreateIfMissing(true)
                    .setAtomicFlush(true)
                    .setListeners(List.of(flusher.listener()));

            List<ColumnFamilyDescriptor> cfDescriptors;

            try (Options opts = new Options()) {
                cfDescriptors = RocksDB.listColumnFamilies(opts, dbPath.toAbsolutePath().toString())
                        .stream()
                        .map(nameBytes -> new ColumnFamilyDescriptor(nameBytes, new ColumnFamilyOptions()))
                        .collect(toList());

                cfDescriptors = cfDescriptors.isEmpty()
                        ? List.of(new ColumnFamilyDescriptor(TX_STATE_CF.getBytes(UTF_8), new ColumnFamilyOptions()))
                        : cfDescriptors;
            }

            List<ColumnFamilyHandle> cfHandles = new ArrayList<>(cfDescriptors.size());

            this.db = RocksDB.open(dbOptions, dbPath.toString(), cfDescriptors, cfHandles);

            flusher.init(db, cfHandles);
        } catch (Exception e) {
            throw new IgniteInternalException("Could not create transaction state storage", e);
        }
    }

    @Override
    public void close() throws Exception {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        List<AutoCloseable> resources = new ArrayList<>();

        resources.add(flusher::stop);
        resources.add(readOptions);
        resources.add(writeOptions);
        resources.add(dbOptions);
        resources.add(db);

        reverse(resources);

        closeAll(resources);
    }
}
