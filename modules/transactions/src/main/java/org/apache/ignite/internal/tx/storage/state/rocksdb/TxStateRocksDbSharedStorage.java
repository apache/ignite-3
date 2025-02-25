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
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
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
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.rocksdb.flush.RocksDbFlusher;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;

/**
 * Shared RocksDB storage instance to be used in {@link TxStateRocksDbStorage}. Exists to make "createTable" operation faster, as well as
 * reducing the amount of resources that would otherwise be used by multiple RocksDB instances, if they existed on per-table basis.
 */
public class TxStateRocksDbSharedStorage implements IgniteComponent {
    static {
        RocksDB.loadLibrary();
    }

    /** Column family name for transaction states. */
    private static final byte[] TX_STATE_CF_NAME = RocksDB.DEFAULT_COLUMN_FAMILY;

    private static final byte[] TX_META_CF_NAME = "TX_META".getBytes(UTF_8);

    /** Transaction storage flush delay. */
    private static final int TX_STATE_STORAGE_FLUSH_DELAY = 100;
    private static final IntSupplier TX_STATE_STORAGE_FLUSH_DELAY_SUPPLIER = () -> TX_STATE_STORAGE_FLUSH_DELAY;

    /** Rocks DB instance. */
    private volatile RocksDB db;

    /** RocksDb database options. */
    private volatile DBOptions dbOptions;

    /** Write options. */
    final WriteOptions writeOptions = new WriteOptions().setDisableWAL(true);

    /** Database path. */
    private final Path dbPath;

    /** RocksDB flusher instance. */
    private volatile RocksDbFlusher flusher;

    /** Prevents double stopping the storage. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /**
     * Scheduled executor. Needed only for asynchronous start of scheduled operations without performing blocking, long or IO operations.
     */
    private final ScheduledExecutorService scheduledExecutor;

    /** Thread pool to execute after-flush actions. */
    private final ExecutorService threadPool;

    /** Supplier for the value of delay for scheduled database flush. */
    private final IntSupplier flushDelaySupplier;

    /** Write-ahead log synchronizer. */
    private final LogSyncer logSyncer;

    private volatile ColumnFamily txStateColumnFamily;

    private volatile ColumnFamily txStateMetaColumnFamily;

    /**
     * Constructor.
     *
     * @param dbPath Database path.
     * @param scheduledExecutor Scheduled executor. Needed only for asynchronous start of scheduled operations without performing
     *         blocking, long or IO operations.
     * @param threadPool Thread pool for internal operations.
     * @param logSyncer Write-ahead log synchronizer.
     * @see RocksDbFlusher
     */
    public TxStateRocksDbSharedStorage(
            Path dbPath,
            ScheduledExecutorService scheduledExecutor,
            ExecutorService threadPool,
            LogSyncer logSyncer
    ) {
        this(dbPath, scheduledExecutor, threadPool, logSyncer, TX_STATE_STORAGE_FLUSH_DELAY_SUPPLIER);
    }

    /**
     * Constructor.
     *
     * @param dbPath Database path.
     * @param scheduledExecutor Scheduled executor. Needed only for asynchronous start of scheduled operations without performing
     *         blocking, long or IO operations.
     * @param threadPool Thread pool for internal operations.
     * @param logSyncer Write-ahead log synchronizer.
     * @param flushDelaySupplier Flush delay supplier.
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

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        start();

        return nullCompletedFuture();
    }

    /**
     * Starts the storage.
     *
     * @throws IgniteInternalException If failed to create directory or start the RocksDB storage.
     */
    private void start() {
        try {
            Files.createDirectories(dbPath);

            flusher = new RocksDbFlusher(
                    "tx state storage",
                    busyLock,
                    scheduledExecutor,
                    threadPool,
                    flushDelaySupplier,
                    logSyncer,
                    () -> {} // No-op.
            );

            this.dbOptions = new DBOptions()
                    .setCreateIfMissing(true)
                    .setCreateMissingColumnFamilies(true)
                    .setAtomicFlush(true)
                    .setListeners(List.of(flusher.listener()))
                    // Don't flush on shutdown to speed up node shutdown as on recovery we'll apply commands from log.
                    .setAvoidFlushDuringShutdown(true);

            List<ColumnFamilyDescriptor> cfDescriptors = columnFamilyDescriptors();

            List<ColumnFamilyHandle> cfHandles = new ArrayList<>(cfDescriptors.size());

            this.db = RocksDB.open(dbOptions, dbPath.toString(), cfDescriptors, cfHandles);

            txStateColumnFamily = ColumnFamily.wrap(db, cfHandles.get(0));

            txStateMetaColumnFamily = ColumnFamily.wrap(db, cfHandles.get(1));

            flusher.init(db, cfHandles);
        } catch (Exception e) {
            throw new IgniteInternalException(Common.INTERNAL_ERR, "Could not create transaction state storage", e);
        }
    }

    private static List<ColumnFamilyDescriptor> columnFamilyDescriptors() {
        return List.of(
                new ColumnFamilyDescriptor(TX_STATE_CF_NAME),
                new ColumnFamilyDescriptor(TX_META_CF_NAME)
        );
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        try {
            close();
        } catch (Exception e) {
            return failedFuture(e);
        }

        return nullCompletedFuture();
    }

    private void close() throws Exception {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        RocksDbFlusher flusher = this.flusher;

        List<AutoCloseable> resources = new ArrayList<>();

        resources.add(flusher == null ? null : flusher::stop);
        resources.add(db);
        resources.add(dbOptions);
        resources.add(writeOptions);

        closeAll(resources);
    }

    public ColumnFamily txStateColumnFamily() {
        return txStateColumnFamily;
    }

    public ColumnFamily txStateMetaColumnFamily() {
        return txStateMetaColumnFamily;
    }
}
