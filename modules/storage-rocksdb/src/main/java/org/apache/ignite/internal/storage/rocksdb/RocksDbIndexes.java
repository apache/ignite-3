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
import static org.apache.ignite.internal.storage.rocksdb.RocksDbStorageUtils.INDEX_ID_SIZE;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbStorageUtils.KEY_BYTE_ORDER;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbStorageUtils.TABLE_ID_SIZE;
import static org.apache.ignite.internal.storage.rocksdb.instance.SharedRocksDbInstance.DFLT_WRITE_OPTS;
import static org.apache.ignite.internal.storage.rocksdb.instance.SharedRocksDbInstance.deleteByPrefix;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;
import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptorSupplier;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.storage.rocksdb.index.AbstractRocksDbIndexStorage;
import org.apache.ignite.internal.storage.rocksdb.instance.IndexColumnFamily;
import org.apache.ignite.internal.storage.rocksdb.instance.SharedRocksDbInstance;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

/** Manager for all RocksDB-based indexes. */
class RocksDbIndexes {
    /** Hash Index storages by Index IDs. */
    private final ConcurrentMap<Integer, HashIndex> hashIndices = new ConcurrentHashMap<>();

    /** Sorted Index storages by Index IDs. */
    private final ConcurrentMap<Integer, SortedIndex> sortedIndices = new ConcurrentHashMap<>();

    private final SharedRocksDbInstance rocksDb;

    private final int tableId;

    RocksDbIndexes(SharedRocksDbInstance rocksDb, int tableId) {
        this.rocksDb = rocksDb;
        this.tableId = tableId;
    }

    void recoverIndexes(StorageIndexDescriptorSupplier indexDescriptorSupplier) throws RocksDBException {
        try (WriteBatch writeBatch = new WriteBatch()) {
            for (int indexId : rocksDb.hashIndexIds(tableId)) {
                var descriptor = (StorageHashIndexDescriptor) indexDescriptorSupplier.get(indexId);

                if (descriptor == null) {
                    deleteByPrefix(writeBatch, rocksDb.hashIndexCf(), indexPrefix(tableId, indexId));
                } else {
                    hashIndices.put(indexId, new HashIndex(tableId, rocksDb.hashIndexCf(), descriptor, rocksDb.meta));
                }
            }

            var indexCfsToDestroy = new ArrayList<ColumnFamily>();

            for (IndexColumnFamily indexColumnFamily : rocksDb.sortedIndexes(tableId)) {
                int indexId = indexColumnFamily.indexId();

                ColumnFamily cf = indexColumnFamily.columnFamily();

                var descriptor = (StorageSortedIndexDescriptor) indexDescriptorSupplier.get(indexId);

                if (descriptor == null) {
                    rocksDb.removeSortedIndex(indexId, cf);

                    deleteByPrefix(writeBatch, cf, indexPrefix(tableId, indexId));

                    indexCfsToDestroy.add(cf);
                } else {
                    sortedIndices.put(indexId, SortedIndex.restoreExisting(tableId, cf, descriptor, rocksDb.meta));
                }
            }

            rocksDb.db.write(DFLT_WRITE_OPTS, writeBatch);

            if (!indexCfsToDestroy.isEmpty()) {
                rocksDb.scheduleIndexCfsDestroyIfNeeded(indexCfsToDestroy);
            }
        }
    }

    SortedIndexStorage getOrCreateSortedIndex(int partitionId, StorageSortedIndexDescriptor indexDescriptor) {
        SortedIndex sortedIndex = sortedIndices.computeIfAbsent(
                indexDescriptor.id(),
                id -> SortedIndex.createNew(rocksDb, tableId, indexDescriptor, rocksDb.meta)
        );

        return sortedIndex.getOrCreateStorage(partitionId);
    }

    HashIndexStorage getOrCreateHashIndex(int partitionId, StorageHashIndexDescriptor indexDescriptor) {
        HashIndex hashIndex = hashIndices.computeIfAbsent(
                indexDescriptor.id(),
                id -> new HashIndex(tableId, rocksDb.hashIndexCf(), indexDescriptor, rocksDb.meta)
        );

        return hashIndex.getOrCreateStorage(partitionId);
    }

    @Nullable IndexStorage getIndex(int partitionId, int indexId) {
        HashIndex hashIndex = hashIndices.get(indexId);

        if (hashIndex != null) {
            assert !sortedIndices.containsKey(indexId) : indexId;

            return hashIndex.getOrCreateStorage(partitionId);
        }

        SortedIndex sortedIndex = sortedIndices.get(indexId);

        if (sortedIndex != null) {
            return sortedIndex.getOrCreateStorage(partitionId);
        }

        return null;
    }

    void startRebalance(int partitionId, WriteBatch writeBatch) {
        getAllStorages(partitionId).forEach(indexStorage -> indexStorage.startRebalance(writeBatch));
    }

    void abortRebalance(int partitionId, WriteBatch writeBatch) {
        getAllStorages(partitionId).forEach(indexStorage -> indexStorage.abortRebalance(writeBatch));
    }

    void finishRebalance(int partitionId) {
        getAllStorages(partitionId).forEach(AbstractRocksDbIndexStorage::finishRebalance);
    }

    List<AutoCloseable> getResourcesForClose() {
        return allIndexes().map(index -> (AutoCloseable) index::close).collect(toList());
    }

    List<AutoCloseable> getResourcesForDestroy() {
        return allIndexes().map(index -> (AutoCloseable) index::transitionToDestroyedState).collect(toList());
    }

    private Stream<Index<?>> allIndexes() {
        return Stream.concat(hashIndices.values().stream(), sortedIndices.values().stream());
    }

    void destroyIndex(int indexId) throws RocksDBException {
        HashIndex hashIdx = hashIndices.remove(indexId);

        SortedIndex sortedIdx = sortedIndices.remove(indexId);

        if (hashIdx == null && sortedIdx == null) {
            return;
        }

        try (WriteBatch writeBatch = new WriteBatch()) {
            if (hashIdx != null) {
                hashIdx.destroy(writeBatch);
            }

            if (sortedIdx != null) {
                rocksDb.removeSortedIndex(indexId, sortedIdx.columnFamily());

                sortedIdx.destroy(writeBatch);
            }

            rocksDb.db.write(DFLT_WRITE_OPTS, writeBatch);
        }

        if (sortedIdx != null) {
            rocksDb.scheduleIndexCfsDestroyIfNeeded(List.of(sortedIdx.columnFamily()));
        }
    }

    void destroyAllIndexesForPartition(int partitionId, WriteBatch writeBatch) throws RocksDBException {
        for (HashIndex hashIndex : hashIndices.values()) {
            hashIndex.destroy(partitionId, writeBatch);
        }

        for (SortedIndex sortedIndex : sortedIndices.values()) {
            sortedIndex.destroy(partitionId, writeBatch);
        }
    }

    Stream<AbstractRocksDbIndexStorage> getAllStorages(int partitionId) {
        return Stream.concat(
                hashIndices.values().stream().map(index -> index.getStorage(partitionId)),
                sortedIndices.values().stream().map(index -> index.getStorage(partitionId))
        );
    }

    /**
     * Creates a byte array for the given table ID and index ID that can be used as a key prefix for the index storages.
     */
    static byte[] indexPrefix(int tableId, int indexId) {
        return ByteBuffer.allocate(TABLE_ID_SIZE + INDEX_ID_SIZE)
                .order(KEY_BYTE_ORDER)
                .putInt(tableId)
                .putInt(indexId)
                .array();
    }
}
