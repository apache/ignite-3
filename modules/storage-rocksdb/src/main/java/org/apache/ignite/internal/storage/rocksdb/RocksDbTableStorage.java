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

package org.apache.ignite.internal.storage.rocksdb;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.storage.rocksdb.instance.SharedRocksDbInstance.DFLT_WRITE_OPTS;
import static org.apache.ignite.internal.storage.util.StorageUtils.createMissingMvPartitionErrorMessage;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionDependingOnStorageState;
import static org.apache.ignite.internal.storage.util.StorageUtils.transitionToClosedState;
import static org.apache.ignite.internal.storage.util.StorageUtils.transitionToDestroyedState;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.StorageRebalanceException;
import org.apache.ignite.internal.storage.engine.MvPartitionMeta;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptorSupplier;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.storage.rocksdb.index.AbstractRocksDbIndexStorage;
import org.apache.ignite.internal.storage.rocksdb.instance.SharedRocksDbInstance;
import org.apache.ignite.internal.storage.util.MvPartitionStorages;
import org.apache.ignite.internal.storage.util.StorageState;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

/**
 * Table storage implementation based on {@link RocksDB} instance.
 */
public class RocksDbTableStorage implements MvTableStorage {
    private static final IgniteLogger LOG = Loggers.forClass(RocksDbTableStorage.class);

    private final SharedRocksDbInstance rocksDb;

    /** Partition storages. */
    private final MvPartitionStorages<RocksDbMvPartitionStorage> mvPartitionStorages;

    private final RocksDbIndexes indexes;

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicReference<StorageState> state = new AtomicReference<>(StorageState.RUNNABLE);

    /** Table descriptor. */
    private final StorageTableDescriptor tableDescriptor;

    private final StorageIndexDescriptorSupplier indexDescriptorSupplier;

    /**
     * Constructor.
     *
     * @param rocksDb Shared RocksDB instance.
     * @param tableDescriptor Table descriptor.
     */
    RocksDbTableStorage(
            SharedRocksDbInstance rocksDb,
            StorageTableDescriptor tableDescriptor,
            StorageIndexDescriptorSupplier indexDescriptorSupplier
    ) {
        this.rocksDb = rocksDb;
        this.tableDescriptor = tableDescriptor;
        this.mvPartitionStorages = new MvPartitionStorages<>(tableDescriptor.getId(), tableDescriptor.getPartitions());
        this.indexes = new RocksDbIndexes(rocksDb, tableDescriptor.getId());
        this.indexDescriptorSupplier = indexDescriptorSupplier;
    }

    void start() {
        try {
            indexes.recoverIndexes(indexDescriptorSupplier);
        } catch (RocksDBException e) {
            throw new IgniteRocksDbException("Unable to recover indexes", e);
        }
    }

    /**
     * Returns a storage engine instance.
     */
    public RocksDbStorageEngine engine() {
        return rocksDb.engine;
    }

    /**
     * Returns a {@link RocksDB} instance.
     */
    public RocksDB db() {
        return rocksDb.db;
    }

    /**
     * Returns a column family handle for partitions column family.
     */
    ColumnFamilyHandle partitionCfHandle() {
        return rocksDb.partitionCf.handle();
    }

    /**
     * Returns a column family handle for meta column family.
     */
    ColumnFamilyHandle metaCfHandle() {
        return rocksDb.meta.columnFamily().handle();
    }

    /**
     * Returns a column family handle for GC queue.
     */
    ColumnFamilyHandle gcQueueHandle() {
        return rocksDb.gcQueueCf.handle();
    }

    /**
     * Returns a column family handle for data CF.
     */
    ColumnFamilyHandle dataCfHandle() {
        return rocksDb.dataCf.handle();
    }

    /**
     * Returns a future to wait next flush operation from the current point in time. Uses {@link RocksDB#getLatestSequenceNumber()} to
     * achieve this.
     *
     * @param schedule {@code true} if {@link RocksDB#flush(FlushOptions)} should be explicitly triggered in the near future.
     */
    public CompletableFuture<Void> awaitFlush(boolean schedule) {
        return busy(() -> rocksDb.flusher.awaitFlush(schedule));
    }

    private CompletableFuture<Void> stop(boolean destroy) {
        boolean transitionedToTerminalState = destroy
                ? transitionToDestroyedState(state)
                : transitionToClosedState(state, this::createStorageInfo);

        if (!transitionedToTerminalState) {
            return nullCompletedFuture();
        }

        busyLock.block();

        return mvPartitionStorages.getAllForCloseOrDestroy()
                .thenAccept(partitionStorages -> {
                    var resources = new ArrayList<AutoCloseable>();

                    if (destroy) {
                        resources.addAll(indexes.getResourcesForDestroy());

                        partitionStorages.forEach(mvPartitionStorage -> resources.add(mvPartitionStorage::transitionToDestroyedState));
                    } else {
                        resources.addAll(indexes.getResourcesForClose());

                        partitionStorages.forEach(mvPartitionStorage -> resources.add(mvPartitionStorage::close));
                    }

                    try {
                        closeAll(resources);
                    } catch (Exception e) {
                        throw new StorageException("Failed to stop RocksDB table storage: " + getTableId(), e);
                    }

                    if (destroy) {
                        rocksDb.destroyTable(getTableId());
                    }
                });
    }

    @Override
    public void close() throws StorageException {
        // 10 seconds is taken by analogy with shutdown of thread pool, in general this should be fairly fast.
        try {
            stop(false).get(10, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new StorageException("Failed to stop RocksDB table storage: " + getTableId(), e);
        }
    }

    @Override
    public CompletableFuture<Void> destroy() {
        return stop(true);
    }

    @Override
    public CompletableFuture<MvPartitionStorage> createMvPartition(int partitionId) throws StorageException {
        return busy(() -> mvPartitionStorages.create(partitionId, partId -> {
            RocksDbMvPartitionStorage partition = new RocksDbMvPartitionStorage(this, partitionId);

            if (partition.lastAppliedIndex() == 0L) {
                // Explicitly save meta-information about partition's creation.
                partition.runConsistently(locker -> {
                    partition.lastApplied(partition.lastAppliedIndex(), partition.lastAppliedTerm());

                    return null;
                });
            }

            return partition;
        }));
    }

    @Override
    public @Nullable RocksDbMvPartitionStorage getMvPartition(int partitionId) {
        return busy(() -> mvPartitionStorages.get(partitionId));
    }

    @Override
    public CompletableFuture<Void> destroyPartition(int partitionId) {
        if (!busyLock.enterBusy()) {
            return nullCompletedFuture();
        }

        try {
            return mvPartitionStorages.destroy(partitionId, mvPartitionStorage -> {
                try (WriteBatch writeBatch = new WriteBatch()) {
                    mvPartitionStorage.transitionToDestroyedState();

                    // Operation to delete partition data should be fast, since we will write only the range of keys for deletion, and the
                    // RocksDB itself will then destroy the data on flush.
                    mvPartitionStorage.destroyData(writeBatch);

                    indexes.destroyAllIndexesForPartition(partitionId, writeBatch);

                    rocksDb.db.write(DFLT_WRITE_OPTS, writeBatch);

                    return nullCompletedFuture();
                } catch (RocksDBException e) {
                    throw new IgniteRocksDbException(
                            String.format("Error when destroying storage: [%s]", mvPartitionStorages.createStorageInfo(partitionId)), e
                    );
                }
            });
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public void createSortedIndex(int partitionId, StorageSortedIndexDescriptor indexDescriptor) {
        busy(() -> {
            // TODO: IGNITE-24926 - invoke checkPartitionExists() instead to throw StorageException is partition is absent.
            if (partitionExists(partitionId)) {
                indexes.createSortedIndex(partitionId, indexDescriptor);
            }
        });
    }

    @Override
    public void createHashIndex(int partitionId, StorageHashIndexDescriptor indexDescriptor) {
        busy(() -> {
            // TODO: IGNITE-24926 - invoke checkPartitionExists() instead to throw StorageException is partition is absent.
            if (partitionExists(partitionId)) {
                indexes.createHashIndex(partitionId, indexDescriptor);
            }
        });
    }

    @Override
    public CompletableFuture<Void> destroyIndex(int indexId) {
        if (!busyLock.enterBusy()) {
            return nullCompletedFuture();
        }

        try {
            indexes.destroyIndex(indexId);

            return nullCompletedFuture();
        } catch (RocksDBException e) {
            throw new IgniteRocksDbException(String.format("Error when destroying index: %d", indexId), e);
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public boolean isVolatile() {
        return false;
    }

    @Override
    public CompletableFuture<Void> startRebalancePartition(int partitionId) {
        return busy(() -> mvPartitionStorages.startRebalance(partitionId, mvPartitionStorage -> {
            if (LOG.isInfoEnabled()) {
                LOG.info("Starting rebalance for partition [tableId={}, partitionId={}]", getTableId(), partitionId);
            }

            try (WriteBatch writeBatch = new WriteBatch()) {
                mvPartitionStorage.startRebalance(writeBatch);

                indexes.startRebalance(partitionId, writeBatch);

                rocksDb.db.write(DFLT_WRITE_OPTS, writeBatch);

                return nullCompletedFuture();
            } catch (RocksDBException e) {
                throw new StorageRebalanceException(
                        "Error when trying to start rebalancing storage: [{}]",
                        e,
                        mvPartitionStorage.createStorageInfo()
                );
            }
        }));
    }

    @Override
    public CompletableFuture<Void> abortRebalancePartition(int partitionId) {
        return busy(() -> mvPartitionStorages.abortRebalance(partitionId, mvPartitionStorage -> {
            try (WriteBatch writeBatch = new WriteBatch()) {
                mvPartitionStorage.abortRebalance(writeBatch);

                indexes.abortRebalance(partitionId, writeBatch);

                rocksDb.db.write(DFLT_WRITE_OPTS, writeBatch);

                return nullCompletedFuture();
            } catch (RocksDBException e) {
                throw new StorageRebalanceException("Error when trying to abort rebalancing storage: [{}]",
                        e,
                        mvPartitionStorage.createStorageInfo()
                );
            }
        }));
    }

    @Override
    public CompletableFuture<Void> finishRebalancePartition(int partitionId, MvPartitionMeta partitionMeta) {
        return busy(() -> mvPartitionStorages.finishRebalance(partitionId, mvPartitionStorage -> {
            try (WriteBatch writeBatch = new WriteBatch()) {
                mvPartitionStorage.finishRebalance(writeBatch, partitionMeta);

                indexes.finishRebalance(partitionId);

                rocksDb.db.write(DFLT_WRITE_OPTS, writeBatch);

                return nullCompletedFuture();
            } catch (RocksDBException e) {
                throw new StorageRebalanceException("Error when trying to finish rebalancing storage: [{}]",
                        e,
                        mvPartitionStorage.createStorageInfo()
                );
            }
        }));
    }

    @Override
    public CompletableFuture<Void> clearPartition(int partitionId) {
        return busy(() -> mvPartitionStorages.clear(partitionId, mvPartitionStorage -> {
            List<AbstractRocksDbIndexStorage> indexStorages = indexes.getAllStorages(partitionId).collect(toList());

            try (WriteBatch writeBatch = new WriteBatch()) {
                mvPartitionStorage.startCleanup(writeBatch);

                for (AbstractRocksDbIndexStorage storage : indexStorages) {
                    storage.startCleanup(writeBatch);
                }

                rocksDb.db.write(DFLT_WRITE_OPTS, writeBatch);

                return nullCompletedFuture();
            } catch (RocksDBException e) {
                throw new IgniteRocksDbException(
                        String.format("Error when trying to cleanup storage: [%s]", mvPartitionStorage.createStorageInfo()), e
                );
            } finally {
                mvPartitionStorage.finishCleanup();

                indexStorages.forEach(AbstractRocksDbIndexStorage::finishCleanup);
            }
        }));
    }

    /**
     * Returns the table ID.
     */
    int getTableId() {
        return tableDescriptor.getId();
    }

    private void checkPartitionExists(int partitionId) {
        if (!partitionExists(partitionId)) {
            throw new StorageException(createMissingMvPartitionErrorMessage(partitionId));
        }
    }

    private boolean partitionExists(int partitionId) {
        return mvPartitionStorages.get(partitionId) != null;
    }

    @Override
    public @Nullable IndexStorage getIndex(int partitionId, int indexId) {
        return busy(() -> {
            checkPartitionExists(partitionId);

            return indexes.getIndex(partitionId, indexId);
        });
    }

    @Override
    public StorageTableDescriptor getTableDescriptor() {
        return tableDescriptor;
    }

    private <V> V busy(Supplier<V> supplier) {
        if (!busyLock.enterBusy()) {
            throwExceptionDependingOnStorageState(state.get(), createStorageInfo());
        }

        try {
            return supplier.get();
        } finally {
            busyLock.leaveBusy();
        }
    }

    private void busy(Runnable action) {
        if (!busyLock.enterBusy()) {
            throwExceptionDependingOnStorageState(state.get(), createStorageInfo());
        }

        try {
            action.run();
        } finally {
            busyLock.leaveBusy();
        }
    }

    private String createStorageInfo() {
        return IgniteStringFormatter.format("tableId={}", getTableId());
    }
}
