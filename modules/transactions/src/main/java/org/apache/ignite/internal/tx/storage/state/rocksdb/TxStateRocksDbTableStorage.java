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

package org.apache.ignite.internal.tx.storage.state.rocksdb;

import static java.util.Collections.reverse;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.util.ByteUtils.bytesToLong;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_STATE_STORAGE_ERR;
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
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.internal.configuration.storage.StorageException;
import org.apache.ignite.internal.rocksdb.flush.RocksDbFlusher;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateTableStorage;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.TransactionDB;
import org.rocksdb.TransactionDBOptions;
import org.rocksdb.WriteOptions;

/**
 * RocksDb implementation of {@link TxStateTableStorage}.
 */
public class TxStateRocksDbTableStorage implements TxStateTableStorage {
    static {
        RocksDB.loadLibrary();
    }

    /** Key for the applied index. */
    public static final byte[] APPLIED_INDEX_KEY = ArrayUtils.BYTE_EMPTY_ARRAY;

    /** Column family name for transaction states. */
    private static final String TXN_STATE_CF = new String(RocksDB.DEFAULT_COLUMN_FAMILY, StandardCharsets.UTF_8);

    /** Rocks DB instance. */
    private volatile TransactionDB db;

    /** RocksDb database options. */
    private volatile DBOptions dbOptions;

    /** Transactional RocksDb options. */
    private volatile TransactionDBOptions txDbOptions;

    /** Write options. */
    private final WriteOptions writeOptions = new WriteOptions().setDisableWAL(true);

    /** Read options for regular reads. */
    private final ReadOptions readOptions = new ReadOptions();

    /** Read options for reading persisted data. */
    private final ReadOptions persistedTierReadOptions = new ReadOptions().setReadTier(PERSISTED_TIER);

    /** Database path. */
    private final Path dbPath;

    /** Partition storages. */
    private volatile AtomicReferenceArray<TxStateStorage> storages;

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

    private final IntSupplier flushDelaySupplier;

    /** On-heap-cached last applied index value. */
    private volatile long lastAppliedIndex;

    /** The value of {@link #lastAppliedIndex} persisted to the device at this moment. */
    private volatile long persistedIndex;

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

    /** {@inheritDoc} */
    @Override public TxStateStorage getOrCreateTxnStateStorage(int partitionId) throws StorageException {
        checkPartitionId(partitionId);

        TxStateStorage storage = new TxStateRocksDbStorage(db, writeOptions, readOptions, busyLock, partitionId, this);

        storages.set(partitionId, storage);

        return storage;
    }

    /** {@inheritDoc} */
    @Override public @Nullable TxStateStorage getTxnStateStorage(int partitionId) {
        return storages.get(partitionId);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> destroyTxnStateStorage(int partitionId) throws StorageException {
        checkPartitionId(partitionId);

        TxStateStorage storage = storages.get(partitionId);

        if (storage != null) {
            try {
                storage.close();
            } catch (Exception e) {
                throw new StorageException("Couldn't close the transaction state storage of partition "
                        + partitionId + ", table " + tableCfg.value().name());
            }

            storage.destroy();
        }

        return completedFuture(null);
    }

    /** {@inheritDoc} */
    @Override public TableConfiguration configuration() {
        return tableCfg;
    }

    /** {@inheritDoc} */
    @Override public void start() throws StorageException {
        try {
            flusher = new RocksDbFlusher(
                busyLock,
                scheduledExecutor,
                threadPool,
                flushDelaySupplier,
                this::refreshPersistedIndex
            );

            storages = new AtomicReferenceArray<>(tableCfg.value().partitions());

            this.dbOptions = new DBOptions()
                    .setCreateIfMissing(true)
                    .setAtomicFlush(true)
                    .setListeners(List.of(flusher.listener()));

            this.txDbOptions = new TransactionDBOptions();

            List<ColumnFamilyDescriptor> cfDescriptors;

            try (Options opts = new Options()) {
                cfDescriptors = RocksDB.listColumnFamilies(opts, dbPath.toAbsolutePath().toString())
                        .stream()
                        .map(nameBytes -> new ColumnFamilyDescriptor(nameBytes, new ColumnFamilyOptions()))
                        .collect(toList());

                cfDescriptors = cfDescriptors.isEmpty()
                        ? List.of(new ColumnFamilyDescriptor(TXN_STATE_CF.getBytes(StandardCharsets.UTF_8), new ColumnFamilyOptions()))
                        : cfDescriptors;
            }

            List<ColumnFamilyHandle> cfHandles = new ArrayList<>(cfDescriptors.size());

            this.db = TransactionDB.open(dbOptions, txDbOptions, dbPath.toString(), cfDescriptors, cfHandles);

            flusher.init(db, cfHandles);

            lastAppliedIndex = readLastAppliedIndex(readOptions);

            persistedIndex = lastAppliedIndex;
        } catch (Exception e) {
            throw new StorageException("Could not create transaction state storage for the table " + tableCfg.value().name(), e);
        }
    }

    /** {@inheritDoc} */
    @Override public void stop() throws StorageException {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        try {
            List<AutoCloseable> resources = new ArrayList<>();

            resources.add(persistedTierReadOptions);
            resources.add(readOptions);
            resources.add(writeOptions);
            resources.add(dbOptions);
            resources.add(txDbOptions);
            resources.add(db);

            for (int i = 0; i < storages.length(); i++) {
                TxStateStorage storage = storages.get(0);

                if (storage != null) {
                    resources.add(storage);
                }
            }

            reverse(resources);
            IgniteUtils.closeAll(resources);
        } catch (Exception e) {
            throw new StorageException("Failed to stop transaction state storage of the table " + tableCfg.value().name(), e);
        }
    }

    /** {@inheritDoc} */
    @Override public void destroy() throws StorageException {
        try (Options options = new Options()) {
            close();

            RocksDB.destroyDB(dbPath.toString(), options);
        } catch (Exception e) {
            throw new StorageException("Failed to destroy the transaction state storage of the table " + tableCfg.value().name(), e);
        }
    }

    /** {@inheritDoc} */
    @Override public long lastAppliedIndex() {
        return lastAppliedIndex;
    }

    /** {@inheritDoc} */
    @Override public long persistedIndex() {
        return persistedIndex;
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        stop();
    }

    private void refreshPersistedIndex() {
        if (!busyLock.enterBusy()) {
            return;
        }

        try {
            persistedIndex = readLastAppliedIndex(persistedTierReadOptions);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Reads the value of {@link #lastAppliedIndex} from the storage.
     *
     * @param readOptions Read options to be used for reading.
     * @return The value of last applied index.
     */
    private long readLastAppliedIndex(ReadOptions readOptions) {
        byte[] appliedIndexBytes;

        try {
            appliedIndexBytes = db.get(readOptions, APPLIED_INDEX_KEY);
        } catch (RocksDBException e) {
            throw new IgniteInternalException(TX_STATE_STORAGE_ERR, "Failed to read applied index value from transaction state storage", e);
        }

        return appliedIndexBytes == null ? 0 : bytesToLong(appliedIndexBytes);
    }

    /**
     * Returns a future to await flush.
     *
     * @return Flush future.
     */
    public CompletableFuture<Void> awaitFlush(boolean schedule) {
        return flusher.awaitFlush(schedule);
    }
}
