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

import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.unmodifiableSet;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.ignite.internal.rocksdb.RocksUtils.incrementPrefix;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntSupplier;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.rocksdb.flush.RocksDbFlusher;
import org.apache.ignite.internal.tx.storage.state.TxStateStorageException;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.jetbrains.annotations.TestOnly;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

/**
 * Shared RocksDB storage instance to be used in {@link TxStateRocksDbStorage}. Exists to make createTable/createZone operations faster,
 * as well as reducing the amount of resources that would otherwise be used by multiple RocksDB instances, if they existed
 * on per-table/per-zone basis.
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

    static final ByteOrder BYTE_ORDER = BIG_ENDIAN;

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

    private final FailureProcessor failureProcessor;

    private final String nodeName;

    private volatile ColumnFamily txStateColumnFamily;

    private volatile ColumnFamily txStateMetaColumnFamily;

    /**
     * Constructor.
     *
     * @param nodeName Node name.
     * @param dbPath Database path.
     * @param scheduledExecutor Scheduled executor. Needed only for asynchronous start of scheduled operations without performing
     *         blocking, long or IO operations.
     * @param threadPool Thread pool for internal operations.
     * @param logSyncer Write-ahead log synchronizer.
     * @param failureProcessor Failure processor.
     * @see RocksDbFlusher
     */
    public TxStateRocksDbSharedStorage(
            String nodeName,
            Path dbPath,
            ScheduledExecutorService scheduledExecutor,
            ExecutorService threadPool,
            LogSyncer logSyncer,
            FailureProcessor failureProcessor
    ) {
        this(nodeName, dbPath, scheduledExecutor, threadPool, logSyncer, failureProcessor, TX_STATE_STORAGE_FLUSH_DELAY_SUPPLIER);
    }

    /**
     * Constructor.
     *
     * @param nodeName Node name.
     * @param dbPath Database path.
     * @param scheduledExecutor Scheduled executor. Needed only for asynchronous start of scheduled operations without performing
     *         blocking, long or IO operations.
     * @param threadPool Thread pool for internal operations.
     * @param logSyncer Write-ahead log synchronizer.
     * @param failureProcessor Failure processor.
     * @param flushDelaySupplier Flush delay supplier.
     * @see RocksDbFlusher
     */
    public TxStateRocksDbSharedStorage(
            String nodeName,
            Path dbPath,
            ScheduledExecutorService scheduledExecutor,
            ExecutorService threadPool,
            LogSyncer logSyncer,
            FailureProcessor failureProcessor,
            IntSupplier flushDelaySupplier
    ) {
        this.dbPath = dbPath;
        this.scheduledExecutor = scheduledExecutor;
        this.threadPool = threadPool;
        this.flushDelaySupplier = flushDelaySupplier;
        this.logSyncer = logSyncer;
        this.failureProcessor = failureProcessor;
        this.nodeName = nodeName;
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
     * @throws TxStateStorageException If failed to create directory or start the RocksDB storage.
     */
    private void start() {
        try {
            Files.createDirectories(dbPath);

            flusher = new RocksDbFlusher(
                    "tx state storage",
                    nodeName,
                    busyLock,
                    scheduledExecutor,
                    threadPool,
                    flushDelaySupplier,
                    logSyncer,
                    failureProcessor,
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
            throw new TxStateStorageException(INTERNAL_ERR, "Could not create transaction state storage", e);
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

    /**
     * Destroys tx state storage for table or zone by its ID.
     *
     * @param tableOrZoneId ID of the table or zone.
     */
    public void destroyStorage(int tableOrZoneId) {
        byte[] dataStart = ByteBuffer.allocate(TxStateRocksDbStorage.TABLE_OR_ZONE_PREFIX_SIZE_BYTES)
                .order(BYTE_ORDER)
                .putInt(tableOrZoneId)
                .array();
        byte[] dataEnd = incrementPrefix(dataStart);

        try (WriteBatch writeBatch = new WriteBatch()) {
            writeBatch.deleteRange(txStateColumnFamily.handle(), dataStart, dataEnd);

            TxStateMetaRocksDbPartitionStorage.clearForTableOrZone(writeBatch, txStateMetaColumnFamily().handle(), tableOrZoneId);

            db.write(writeOptions, writeBatch);
        } catch (Exception e) {
            throw new TxStateStorageException("Failed to destroy the transaction state storage [tableOrZoneId={}]", e, tableOrZoneId);
        }
    }

    /**
     * Returns IDs of tables/zones for which there are tx state partition storages on disk. Those were created and flushed to disk; either
     * destruction was not started for them, or it failed.
     *
     * <p>This method should only be called when the tx state storage is not accessed otherwise (so no storages in it can appear or
     * be destroyed in parallel with this call).
     */
    public Set<Integer> tableOrZoneIdsOnDisk() {
        Set<Integer> ids = new HashSet<>();

        byte[] lastAppliedGlobalPrefix = {TxStateMetaRocksDbPartitionStorage.LAST_APPLIED_PREFIX};

        try (
                var upperBound = new Slice(incrementPrefix(lastAppliedGlobalPrefix));
                var readOptions = new ReadOptions().setIterateUpperBound(upperBound);
                RocksIterator it = txStateMetaColumnFamily.newIterator(readOptions)
        ) {
            it.seek(lastAppliedGlobalPrefix);

            while (it.isValid()) {
                byte[] key = it.key();
                int tableOrZoneId = ByteUtils.bytesToInt(key, lastAppliedGlobalPrefix.length);
                ids.add(tableOrZoneId);

                it.next();
            }

            // Doing this to make an exception thrown if the iteration was stopped due to an error and not due to exhausting
            // the iteration space.
            it.status();
        } catch (RocksDBException e) {
            throw new TxStateStorageException(INTERNAL_ERR, "Cannot get table/zone IDs", e);
        }

        return unmodifiableSet(ids);
    }

    /**
     * Flushes the whole storage to disk.
     */
    @TestOnly
    public void flush() {
        try {
            awaitFlush(true).get(1, MINUTES);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new TxStateStorageException("Interrupted while waiting for a flush", e);
        } catch (ExecutionException e) {
            throw new TxStateStorageException("Flush failed", e);
        } catch (TimeoutException e) {
            throw new TxStateStorageException("Flush failed to finish in time", e);
        }
    }
}
