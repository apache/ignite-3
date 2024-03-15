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
import static org.apache.ignite.internal.storage.rocksdb.RocksDbMetaStorage.PARTITION_CONF_PREFIX;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbMetaStorage.PARTITION_META_PREFIX;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbMetaStorage.createKey;
import static org.apache.ignite.internal.storage.rocksdb.instance.SharedRocksDbInstance.DFLT_WRITE_OPTS;
import static org.apache.ignite.internal.storage.rocksdb.instance.SharedRocksDbInstance.deleteByPrefix;
import static org.apache.ignite.internal.storage.util.StorageUtils.createMissingMvPartitionErrorMessage;
import static org.apache.ignite.internal.util.ArrayUtils.BYTE_EMPTY_ARRAY;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.StorageRebalanceException;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.storage.rocksdb.index.RocksDbHashIndexStorage;
import org.apache.ignite.internal.storage.rocksdb.index.RocksDbSortedIndexStorage;
import org.apache.ignite.internal.storage.rocksdb.instance.SharedRocksDbInstance;
import org.apache.ignite.internal.storage.util.MvPartitionStorages;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
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
    private final SharedRocksDbInstance rocksDb;

    /** Partition storages. */
    private final MvPartitionStorages<RocksDbMvPartitionStorage> mvPartitionStorages;

    /** Hash Index storages by Index IDs. */
    private final ConcurrentMap<Integer, HashIndex> hashIndices = new ConcurrentHashMap<>();

    /** Sorted Index storages by Index IDs. */
    private final ConcurrentMap<Integer, SortedIndex> sortedIndices = new ConcurrentHashMap<>();

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping of the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Table descriptor. */
    private final StorageTableDescriptor tableDescriptor;

    /**
     * Constructor.
     *
     * @param rocksDb Shared RocksDB instance.
     * @param tableDescriptor Table descriptor.
     */
    RocksDbTableStorage(
            SharedRocksDbInstance rocksDb,
            StorageTableDescriptor tableDescriptor
    ) {
        this.rocksDb = rocksDb;
        this.tableDescriptor = tableDescriptor;
        this.mvPartitionStorages = new MvPartitionStorages<>(tableDescriptor.getId(), tableDescriptor.getPartitions());
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
    public ColumnFamilyHandle partitionCfHandle() {
        return rocksDb.partitionCf.handle();
    }

    /**
     * Returns a column family handle for meta column family.
     */
    public ColumnFamilyHandle metaCfHandle() {
        return rocksDb.meta.columnFamily().handle();
    }

    /**
     * Returns a column family handle for GC queue.
     */
    public ColumnFamilyHandle gcQueueHandle() {
        return rocksDb.gcQueueCf.handle();
    }

    /**
     * Returns a future to wait next flush operation from the current point in time. Uses {@link RocksDB#getLatestSequenceNumber()} to
     * achieve this.
     *
     * @param schedule {@code true} if {@link RocksDB#flush(FlushOptions)} should be explicitly triggerred in the near future.
     */
    public CompletableFuture<Void> awaitFlush(boolean schedule) {
        return inBusyLock(busyLock, () -> rocksDb.flusher.awaitFlush(schedule));
    }

    private CompletableFuture<Void> stop(boolean destroy) {
        if (!stopGuard.compareAndSet(false, true)) {
            return nullCompletedFuture();
        }

        busyLock.block();

        return mvPartitionStorages.getAllForCloseOrDestroy()
                .thenAccept(partitionStorages -> {
                    var resources = new ArrayList<AutoCloseable>();

                    Stream.concat(hashIndices.values().stream(), sortedIndices.values().stream()).forEach(index -> resources.add(
                            destroy ? index::transitionToDestroyedState : index::close
                    ));

                    partitionStorages.forEach(mvPartitionStorage -> resources.add(
                            destroy ? mvPartitionStorage::transitionToDestroyedState : mvPartitionStorage::close
                    ));

                    try {
                        IgniteUtils.closeAll(resources);
                    } catch (Exception e) {
                        throw new StorageException("Failed to stop RocksDB table storage: " + getTableId(), e);
                    }

                    if (destroy) {
                        destroyTableData();
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

    private void destroyTableData() {
        try (WriteBatch writeBatch = new WriteBatch()) {
            int tableId = getTableId();

            byte[] tablePrefix = createKey(BYTE_EMPTY_ARRAY, tableId);

            deleteByPrefix(writeBatch, rocksDb.partitionCf, tablePrefix);
            deleteByPrefix(writeBatch, rocksDb.gcQueueCf, tablePrefix);

            for (HashIndex hashIndex : hashIndices.values()) {
                hashIndex.destroy(writeBatch);
            }

            for (SortedIndex sortedIndex : sortedIndices.values()) {
                sortedIndex.destroy(writeBatch);
            }

            deleteByPrefix(writeBatch, rocksDb.meta.columnFamily(), createKey(PARTITION_META_PREFIX, tableId));
            deleteByPrefix(writeBatch, rocksDb.meta.columnFamily(), createKey(PARTITION_CONF_PREFIX, tableId));

            rocksDb.db.write(DFLT_WRITE_OPTS, writeBatch);
        } catch (RocksDBException e) {
            throw new StorageException("Failed to destroy table data. [tableId={}]", e, getTableId());
        }
    }

    @Override
    public CompletableFuture<MvPartitionStorage> createMvPartition(int partitionId) throws StorageException {
        return inBusyLock(busyLock, () -> mvPartitionStorages.create(partitionId, partId -> {
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
        return inBusyLock(busyLock, () -> mvPartitionStorages.get(partitionId));
    }

    @Override
    public CompletableFuture<Void> destroyPartition(int partitionId) {
        return inBusyLock(busyLock, () -> mvPartitionStorages.destroy(partitionId, mvPartitionStorage -> {
            try (WriteBatch writeBatch = new WriteBatch()) {
                mvPartitionStorage.transitionToDestroyedState();

                // Operation to delete partition data should be fast, since we will write only the range of keys for deletion, and the
                // RocksDB itself will then destroy the data on flush.
                mvPartitionStorage.destroyData(writeBatch);

                for (HashIndex hashIndex : hashIndices.values()) {
                    hashIndex.destroy(partitionId, writeBatch);
                }

                for (SortedIndex sortedIndex : sortedIndices.values()) {
                    sortedIndex.destroy(partitionId, writeBatch);
                }

                rocksDb.db.write(DFLT_WRITE_OPTS, writeBatch);

                return nullCompletedFuture();
            } catch (RocksDBException e) {
                throw new StorageException("Error when destroying storage: [{}]", e, mvPartitionStorages.createStorageInfo(partitionId));
            }
        }));
    }

    @Override
    public SortedIndexStorage getOrCreateSortedIndex(int partitionId, StorageSortedIndexDescriptor indexDescriptor) {
        return inBusyLock(busyLock, () -> {
            SortedIndex storages = sortedIndices.computeIfAbsent(
                    indexDescriptor.id(),
                    id -> createSortedIndex(indexDescriptor)
            );

            RocksDbMvPartitionStorage partitionStorage = mvPartitionStorages.get(partitionId);

            if (partitionStorage == null) {
                throw new StorageException(createMissingMvPartitionErrorMessage(partitionId));
            }

            return storages.getOrCreateStorage(partitionStorage);
        });
    }

    private SortedIndex createSortedIndex(StorageSortedIndexDescriptor indexDescriptor) {
        return new SortedIndex(rocksDb, indexDescriptor, rocksDb.meta);
    }

    @Override
    public HashIndexStorage getOrCreateHashIndex(int partitionId, StorageHashIndexDescriptor indexDescriptor) {
        return inBusyLock(busyLock, () -> {
            HashIndex storages = hashIndices.computeIfAbsent(
                    indexDescriptor.id(),
                    id -> new HashIndex(rocksDb.hashIndexCf, indexDescriptor, rocksDb.meta)
            );

            RocksDbMvPartitionStorage partitionStorage = mvPartitionStorages.get(partitionId);

            if (partitionStorage == null) {
                throw new StorageException(createMissingMvPartitionErrorMessage(partitionId));
            }

            return storages.getOrCreateStorage(partitionStorage);
        });
    }

    @Override
    public CompletableFuture<Void> destroyIndex(int indexId) {
        return inBusyLock(busyLock, () -> {
            HashIndex hashIdx = hashIndices.remove(indexId);

            SortedIndex sortedIdx = sortedIndices.remove(indexId);

            if (hashIdx == null && sortedIdx == null) {
                return nullCompletedFuture();
            }

            try (WriteBatch writeBatch = new WriteBatch()) {
                if (hashIdx != null) {
                    hashIdx.destroy(writeBatch);
                }

                if (sortedIdx != null) {
                    sortedIdx.destroy(writeBatch);
                }

                rocksDb.db.write(DFLT_WRITE_OPTS, writeBatch);

                return nullCompletedFuture();
            } catch (RocksDBException e) {
                throw new StorageException("Error when destroying index: {}", e, indexId);
            }
        });
    }

    @Override
    public boolean isVolatile() {
        return false;
    }

    @Override
    public CompletableFuture<Void> startRebalancePartition(int partitionId) {
        return inBusyLock(busyLock, () -> mvPartitionStorages.startRebalance(partitionId, mvPartitionStorage -> {
            try (WriteBatch writeBatch = new WriteBatch()) {
                mvPartitionStorage.startRebalance(writeBatch);

                getHashIndexStorages(partitionId).forEach(index -> index.startRebalance(writeBatch));
                getSortedIndexStorages(partitionId).forEach(index -> index.startRebalance(writeBatch));

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
        return inBusyLock(busyLock, () -> mvPartitionStorages.abortRebalance(partitionId, mvPartitionStorage -> {
            try (WriteBatch writeBatch = new WriteBatch()) {
                mvPartitionStorage.abortRebalance(writeBatch);

                getHashIndexStorages(partitionId).forEach(index -> index.abortRebalance(writeBatch));
                getSortedIndexStorages(partitionId).forEach(index -> index.abortRebalance(writeBatch));

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
    public CompletableFuture<Void> finishRebalancePartition(
            int partitionId,
            long lastAppliedIndex,
            long lastAppliedTerm,
            byte[] groupConfig
    ) {
        return inBusyLock(busyLock, () -> mvPartitionStorages.finishRebalance(partitionId, mvPartitionStorage -> {
            try (WriteBatch writeBatch = new WriteBatch()) {
                mvPartitionStorage.finishRebalance(writeBatch, lastAppliedIndex, lastAppliedTerm, groupConfig);

                getHashIndexStorages(partitionId).forEach(RocksDbHashIndexStorage::finishRebalance);
                getSortedIndexStorages(partitionId).forEach(RocksDbSortedIndexStorage::finishRebalance);

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
        return inBusyLock(busyLock, () -> mvPartitionStorages.clear(partitionId, mvPartitionStorage -> {
            List<RocksDbHashIndexStorage> hashIndexStorages = getHashIndexStorages(partitionId);
            List<RocksDbSortedIndexStorage> sortedIndexStorages = getSortedIndexStorages(partitionId);

            try (WriteBatch writeBatch = new WriteBatch()) {
                mvPartitionStorage.startCleanup(writeBatch);

                for (RocksDbHashIndexStorage hashIndexStorage : hashIndexStorages) {
                    hashIndexStorage.startCleanup(writeBatch);
                }

                for (RocksDbSortedIndexStorage sortedIndexStorage : sortedIndexStorages) {
                    sortedIndexStorage.startCleanup(writeBatch);
                }

                rocksDb.db.write(DFLT_WRITE_OPTS, writeBatch);

                return nullCompletedFuture();
            } catch (RocksDBException e) {
                throw new StorageException("Error when trying to cleanup storage: [{}]", e, mvPartitionStorage.createStorageInfo());
            } finally {
                mvPartitionStorage.finishCleanup();

                hashIndexStorages.forEach(RocksDbHashIndexStorage::finishCleanup);
                sortedIndexStorages.forEach(RocksDbSortedIndexStorage::finishCleanup);
            }
        }));
    }

    /**
     * Returns the table ID.
     */
    int getTableId() {
        return tableDescriptor.getId();
    }

    private List<RocksDbHashIndexStorage> getHashIndexStorages(int partitionId) {
        return hashIndices.values().stream().map(indexes -> indexes.get(partitionId)).filter(Objects::nonNull).collect(toList());
    }

    private List<RocksDbSortedIndexStorage> getSortedIndexStorages(int partitionId) {
        return sortedIndices.values().stream().map(indexes -> indexes.get(partitionId)).filter(Objects::nonNull).collect(toList());
    }

    @Override
    public @Nullable IndexStorage getIndex(int partitionId, int indexId) {
        return inBusyLock(busyLock, () -> {
            if (mvPartitionStorages.get(partitionId) == null) {
                throw new StorageException(createMissingMvPartitionErrorMessage(partitionId));
            }

            HashIndex hashIndex = hashIndices.get(indexId);

            if (hashIndex != null) {
                return hashIndex.get(partitionId);
            }

            SortedIndex sortedIndex = sortedIndices.get(indexId);

            if (sortedIndex != null) {
                return sortedIndex.get(partitionId);
            }

            return (IndexStorage) null;
        });
    }

    @Override
    public StorageTableDescriptor getTableDescriptor() {
        return tableDescriptor;
    }
}
