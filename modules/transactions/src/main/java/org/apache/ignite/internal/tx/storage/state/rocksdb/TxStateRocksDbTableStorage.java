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

import static java.util.Collections.reverse;
import static java.util.stream.Collectors.toList;
import static org.rocksdb.ReadTier.PERSISTED_TIER;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.IntSupplier;
import org.apache.ignite.internal.configuration.storage.StorageException;
import org.apache.ignite.internal.rocksdb.flush.RocksDbFlusher;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateTableStorage;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;

/**
 * RocksDb implementation of {@link TxStateTableStorage}.
 */
public class TxStateRocksDbTableStorage implements TxStateTableStorage {
    static {
        RocksDB.loadLibrary();
    }

    /** Column family name for transaction states. */
    private static final String TX_STATE_CF = new String(RocksDB.DEFAULT_COLUMN_FAMILY, StandardCharsets.UTF_8);

    /** Rocks DB instance. */
    private volatile RocksDB db;

    /** RocksDb database options. */
    private volatile DBOptions dbOptions;

    /** Write options. */
    private final WriteOptions writeOptions = new WriteOptions().setDisableWAL(true);

    /** Read options for regular reads. */
    private final ReadOptions readOptions = new ReadOptions();

    /** Read options for reading persisted data. */
    private final ReadOptions persistedTierReadOptions = new ReadOptions().setReadTier(PERSISTED_TIER);

    /** Database path. */
    private final Path dbPath;

    /** Partition storages. */
    private volatile AtomicReferenceArray<TxStateRocksDbStorage> storages;

    /** Table configuration. */
    private final TableConfiguration tableCfg;

    /** RocksDB flusher instance. */
    private volatile RocksDbFlusher flusher;

    /** Prevents double stopping the storage. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Busy lock to stop synchronously. */
    final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final ScheduledExecutorService scheduledExecutor;

    /** Thread pool to execute after-flush actions. */
    private final ExecutorService threadPool;

    /** Supplier for the value of delay for scheduled database flush. */
    private final IntSupplier flushDelaySupplier;

    /**
     * Constructor.
     *
     * @param tableCfg Table configuration.
     * @param dbPath Database path.
     * @param scheduledExecutor Scheduled executor.
     */
    public TxStateRocksDbTableStorage(
            TableConfiguration tableCfg,
            Path dbPath,
            ScheduledExecutorService scheduledExecutor,
            ExecutorService threadPool,
            IntSupplier flushDelaySupplier
    ) {
        this.tableCfg = tableCfg;
        this.dbPath = dbPath;
        this.scheduledExecutor = scheduledExecutor;
        this.threadPool = threadPool;
        this.flushDelaySupplier = flushDelaySupplier;
    }

    /**
     * Checks that a passed partition id is within the proper bounds.
     *
     * @param partitionId Partition id.
     */
    private void checkPartitionId(int partitionId) {
        if (partitionId < 0 || partitionId >= storages.length()) {
            throw new IllegalArgumentException(S.toString(
                "Unable to access partition with id outside of configured range",
                "table", tableCfg.name().value(), false,
                "partitionId", partitionId, false,
                "partitions", storages.length(), false
            ));
        }
    }

    @Override
    public TxStateStorage getOrCreateTxStateStorage(int partitionId) throws StorageException {
        checkPartitionId(partitionId);

        TxStateRocksDbStorage storage = storages.get(partitionId);

        if (storage == null) {
            storage = new TxStateRocksDbStorage(
                db,
                writeOptions,
                readOptions,
                persistedTierReadOptions,
                partitionId,
                this
            );
        }

        storages.set(partitionId, storage);

        return storage;
    }

    @Override
    public @Nullable TxStateStorage getTxStateStorage(int partitionId) {
        return storages.get(partitionId);
    }

    @Override
    public void destroyTxStateStorage(int partitionId) throws StorageException {
        checkPartitionId(partitionId);

        TxStateStorage storage = storages.getAndSet(partitionId, null);

        if (storage != null) {
            storage.destroy();

            try {
                storage.close();
            } catch (RuntimeException e) {
                throw new StorageException("Couldn't close the transaction state storage of partition "
                        + partitionId + ", table " + tableCfg.value().name(), e);
            }
        }
    }

    @Override
    public TableConfiguration configuration() {
        return tableCfg;
    }

    @Override
    public void start() throws StorageException {
        try {
            flusher = new RocksDbFlusher(
                busyLock,
                scheduledExecutor,
                threadPool,
                flushDelaySupplier,
                this::refreshPersistedIndexes
            );

            storages = new AtomicReferenceArray<>(tableCfg.value().partitions());

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
                        ? List.of(new ColumnFamilyDescriptor(TX_STATE_CF.getBytes(StandardCharsets.UTF_8), new ColumnFamilyOptions()))
                        : cfDescriptors;
            }

            List<ColumnFamilyHandle> cfHandles = new ArrayList<>(cfDescriptors.size());

            this.db = RocksDB.open(dbOptions, dbPath.toString(), cfDescriptors, cfHandles);

            flusher.init(db, cfHandles);
        } catch (Exception e) {
            throw new StorageException("Could not create transaction state storage for the table " + tableCfg.value().name(), e);
        }
    }

    @Override
    public void stop() throws StorageException {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        try {
            List<AutoCloseable> resources = new ArrayList<>();

            resources.add(persistedTierReadOptions);
            resources.add(readOptions);
            resources.add(writeOptions);
            resources.add(dbOptions);
            resources.add(db);

            for (int i = 0; i < storages.length(); i++) {
                TxStateStorage storage = storages.get(i);

                if (storage != null) {
                    resources.add(storage::close);
                }
            }

            reverse(resources);
            IgniteUtils.closeAll(resources);
        } catch (Exception e) {
            throw new StorageException("Failed to stop transaction state storage of the table " + tableCfg.value().name(), e);
        }
    }

    @Override
    public void destroy() throws StorageException {
        try (Options options = new Options()) {
            close();

            RocksDB.destroyDB(dbPath.toString(), options);

            IgniteUtils.deleteIfExists(dbPath);
        } catch (Exception e) {
            throw new StorageException("Failed to destroy the transaction state storage of the table " + tableCfg.value().name(), e);
        }
    }

    /**
     * Refresh persisted indexes in storages.
     */
    private void refreshPersistedIndexes() {
        if (!busyLock.enterBusy()) {
            return;
        }

        try {
            for (int i = 0; i < storages.length(); i++) {
                TxStateRocksDbStorage storage = storages.get(i);

                if (storage != null) {
                    storage.refreshPersistedIndex();
                }
            }
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public void close() {
        stop();
    }

    /**
     * Returns a future to await flush.
     */
    public CompletableFuture<Void> awaitFlush(boolean schedule) {
        return flusher.awaitFlush(schedule);
    }

    @Override
    public CompletableFuture<Void> startRebalance(int partitionId) throws StorageException {
        // TODO: IGNITE-18024 Implement
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> abortRebalance(int partitionId) throws StorageException {
        // TODO: IGNITE-18024 Implement
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> finishRebalance(int partitionId) throws StorageException {
        // TODO: IGNITE-18024 Implement
        throw new UnsupportedOperationException();
    }
}
