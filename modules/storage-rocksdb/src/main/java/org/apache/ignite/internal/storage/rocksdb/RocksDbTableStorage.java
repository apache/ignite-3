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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbMetaStorage.PARTITION_CONF_PREFIX;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbMetaStorage.PARTITION_META_PREFIX;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbMetaStorage.createKey;
import static org.apache.ignite.internal.storage.rocksdb.instance.SharedRocksDbInstance.DFLT_WRITE_OPTS;
import static org.apache.ignite.internal.storage.util.StorageUtils.createMissingMvPartitionErrorMessage;
import static org.apache.ignite.internal.util.ArrayUtils.BYTE_EMPTY_ARRAY;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.apache.ignite.internal.storage.rocksdb.index.RocksDbBinaryTupleComparator;
import org.apache.ignite.internal.storage.rocksdb.index.RocksDbHashIndexStorage;
import org.apache.ignite.internal.storage.rocksdb.index.RocksDbSortedIndexStorage;
import org.apache.ignite.internal.storage.rocksdb.instance.SharedRocksDbInstance;
import org.apache.ignite.internal.storage.util.MvPartitionStorages;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
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
    private volatile MvPartitionStorages<RocksDbMvPartitionStorage> mvPartitionStorages;

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

    @Override
    public void start() throws StorageException {
        inBusyLock(busyLock, () -> {
            MvPartitionStorages<RocksDbMvPartitionStorage> mvPartitionStorages =
                    new MvPartitionStorages<>(tableDescriptor.getId(), tableDescriptor.getPartitions());

            this.mvPartitionStorages = mvPartitionStorages;
        });
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

    @Override
    public void stop() throws StorageException {
        stop(false);
    }

    private void stop(boolean destroy) {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        if (destroy) {
            destroyTableData();
        }

        List<AutoCloseable> resources = new ArrayList<>();

        resources.addAll(
                sortedIndices.values().stream()
                        .map(index -> (AutoCloseable) index::close)
                        .collect(toList())
        );

        try {
            mvPartitionStorages
                    .getAllForCloseOrDestroy()
                    // 10 seconds is taken by analogy with shutdown of thread pool, in general this should be fairly fast.
                    .get(10, TimeUnit.SECONDS)
                    .forEach(mvPartitionStorage -> resources.add(mvPartitionStorage::close));

            for (HashIndex index : hashIndices.values()) {
                resources.add(index::close);
            }

            for (SortedIndex index : sortedIndices.values()) {
                resources.add(index::close);
            }

            Collections.reverse(resources);

            IgniteUtils.closeAll(resources);
        } catch (Exception e) {
            throw new StorageException("Failed to stop RocksDB table storage: " + getTableId(), e);
        }
    }

    @Override
    public void close() throws StorageException {
        stop();
    }

    @Override
    public CompletableFuture<Void> destroy() {
        try {
            stop(true);

            return nullCompletedFuture();
        } catch (Throwable t) {
            return failedFuture(new StorageException("Failed to destroy RocksDB table storage: " + getTableId(), t));
        }
    }

    private void destroyTableData() {
        try (WriteBatch writeBatch = new WriteBatch()) {
            int tableId = getTableId();

            byte[] tablePrefix = createKey(BYTE_EMPTY_ARRAY, tableId);

            SharedRocksDbInstance.deleteByPrefix(writeBatch, rocksDb.partitionCf, tablePrefix);
            SharedRocksDbInstance.deleteByPrefix(writeBatch, rocksDb.gcQueueCf, tablePrefix);

            for (int indexId : hashIndices.keySet()) {
                SharedRocksDbInstance.deleteByPrefix(writeBatch, rocksDb.hashIndexCf, createKey(BYTE_EMPTY_ARRAY, indexId));
            }

            SharedRocksDbInstance.deleteByPrefix(writeBatch, rocksDb.meta.columnFamily(), createKey(PARTITION_META_PREFIX, tableId));
            SharedRocksDbInstance.deleteByPrefix(writeBatch, rocksDb.meta.columnFamily(), createKey(PARTITION_CONF_PREFIX, tableId));

            rocksDb.db.write(DFLT_WRITE_OPTS, writeBatch);
        } catch (RocksDBException e) {
            throw new StorageException("Failed to destroy table data. [tableId={}]", e, getTableId());
        }

        for (SortedIndex sortedIndex : sortedIndices.values()) {
            sortedIndex.destroy();
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
                mvPartitionStorage.close();

                // Operation to delete partition data should be fast, since we will write only the range of keys for deletion, and the
                // RocksDB itself will then destroy the data on flash.
                mvPartitionStorage.destroyData(writeBatch);

                for (HashIndex hashIndex : hashIndices.values()) {
                    hashIndex.destroy(partitionId, writeBatch);
                }

                for (SortedIndex sortedIndex : sortedIndices.values()) {
                    sortedIndex.destroy(partitionId, writeBatch);
                }

                rocksDb.db.write(DFLT_WRITE_OPTS, writeBatch);

                return awaitFlush(true);
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

            if (hashIdx != null) {
                hashIdx.destroy();
            }

            // Sorted Indexes have a separate Column Family per index, so we simply destroy it immediately after a flush completes
            // in order to avoid concurrent access to the CF.
            SortedIndex sortedIdx = sortedIndices.remove(indexId);

            if (sortedIdx != null) {
                sortedIdx.destroy();
            }

            if (hashIdx == null) {
                return nullCompletedFuture();
            } else {
                return awaitFlush(false);
            }
        });
    }

    @Override
    public boolean isVolatile() {
        return false;
    }

    /**
     * Creates a Column Family descriptor for a Sorted Index.
     */
    private static ColumnFamilyDescriptor sortedIndexCfDescriptor(String cfName, StorageSortedIndexDescriptor descriptor) {
        var comparator = new RocksDbBinaryTupleComparator(descriptor.columns());

        ColumnFamilyOptions options = new ColumnFamilyOptions().setComparator(comparator);

        return new ColumnFamilyDescriptor(cfName.getBytes(UTF_8), options);
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

                getHashIndexStorages(partitionId).forEach(index -> index.abortReblance(writeBatch));
                getSortedIndexStorages(partitionId).forEach(index -> index.abortReblance(writeBatch));

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
