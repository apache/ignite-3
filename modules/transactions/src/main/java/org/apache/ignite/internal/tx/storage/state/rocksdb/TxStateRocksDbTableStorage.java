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
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.rocksdb.flush.RocksDbFlusher;
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

    /** Database path. */
    private final Path dbPath;

    /** Partition storages. */
    private final AtomicReferenceArray<TxStateRocksDbStorage> storages;

    /** RocksDB flusher instance. */
    private volatile RocksDbFlusher flusher;

    /** Prevents double stopping the storage. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final ScheduledExecutorService scheduledExecutor;

    /** Thread pool to execute after-flush actions. */
    private final ExecutorService threadPool;

    /** Supplier for the value of delay for scheduled database flush. */
    private final IntSupplier flushDelaySupplier;

    /** Table ID. */
    final int id;

    /**
     * Constructor.
     *
     * @param id Table ID.
     * @param partitions Count of partitions.
     * @param dbPath Database path.
     * @param scheduledExecutor Scheduled executor.
     */
    public TxStateRocksDbTableStorage(
            int id,
            int partitions,
            Path dbPath,
            ScheduledExecutorService scheduledExecutor,
            ExecutorService threadPool,
            IntSupplier flushDelaySupplier
    ) {
        this.id = id;
        this.dbPath = dbPath;
        this.scheduledExecutor = scheduledExecutor;
        this.threadPool = threadPool;
        this.flushDelaySupplier = flushDelaySupplier;

        this.storages = new AtomicReferenceArray<>(partitions);
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
                "tableId", id, false,
                "partitionId", partitionId, false,
                "partitions", storages.length(), false
            ));
        }
    }

    @Override
    public TxStateStorage getOrCreateTxStateStorage(int partitionId) {
        checkPartitionId(partitionId);

        TxStateRocksDbStorage storage = storages.get(partitionId);

        if (storage == null) {
            storage = new TxStateRocksDbStorage(
                db,
                writeOptions,
                readOptions,
                partitionId,
                this
            );

            storage.start();
        }

        storages.set(partitionId, storage);

        return storage;
    }

    @Override
    public @Nullable TxStateStorage getTxStateStorage(int partitionId) {
        return storages.get(partitionId);
    }

    @Override
    public void destroyTxStateStorage(int partitionId) {
        checkPartitionId(partitionId);

        TxStateStorage storage = storages.getAndSet(partitionId, null);

        if (storage != null) {
            storage.destroy();
        }
    }

    @Override
    public void start() {
        try {
            flusher = new RocksDbFlusher(
                    busyLock,
                    scheduledExecutor,
                    threadPool,
                    flushDelaySupplier,
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
                        ? List.of(new ColumnFamilyDescriptor(TX_STATE_CF.getBytes(StandardCharsets.UTF_8), new ColumnFamilyOptions()))
                        : cfDescriptors;
            }

            List<ColumnFamilyHandle> cfHandles = new ArrayList<>(cfDescriptors.size());

            this.db = RocksDB.open(dbOptions, dbPath.toString(), cfDescriptors, cfHandles);

            flusher.init(db, cfHandles);
        } catch (Exception e) {
            throw new IgniteInternalException("Could not create transaction state storage for the table: " + id, e);
        }
    }

    @Override
    public void stop() {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        try {
            List<AutoCloseable> resources = new ArrayList<>();

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
            throw new IgniteInternalException("Failed to stop transaction state storage of the table: " + id, e);
        }
    }

    @Override
    public void destroy() {
        try (Options options = new Options()) {
            close();

            RocksDB.destroyDB(dbPath.toString(), options);

            IgniteUtils.deleteIfExists(dbPath);
        } catch (Exception e) {
            throw new IgniteInternalException("Failed to destroy the transaction state storage of the table: " + id, e);
        }
    }

    @Override
    public void close() {
        stop();
    }

    /**
     * Returns a future to await flush.
     */
    CompletableFuture<Void> awaitFlush(boolean schedule) {
        return flusher.awaitFlush(schedule);
    }
}
