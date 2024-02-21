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

import static org.apache.ignite.internal.storage.rocksdb.ColumnFamilyUtils.sortedIndexCfName;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.storage.rocksdb.index.RocksDbSortedIndexStorage;
import org.apache.ignite.internal.storage.rocksdb.instance.SharedRocksDbInstance;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

/**
 * Class that represents a Sorted Index defined for all partitions of a Table.
 */
class SortedIndex implements ManuallyCloseable {
    private final StorageSortedIndexDescriptor descriptor;

    private final SharedRocksDbInstance rocksDb;

    private final ColumnFamily indexCf;

    private final ConcurrentMap<Integer, RocksDbSortedIndexStorage> storages = new ConcurrentHashMap<>();

    private final RocksDbMetaStorage indexMetaStorage;

    SortedIndex(
            SharedRocksDbInstance rocksDb,
            StorageSortedIndexDescriptor descriptor,
            RocksDbMetaStorage indexMetaStorage
    ) {
        this.rocksDb = rocksDb;
        this.descriptor = descriptor;
        this.indexCf = rocksDb.getSortedIndexCfOnIndexCreate(
                sortedIndexCfName(descriptor.columns()),
                descriptor.id()
        );
        this.indexMetaStorage = indexMetaStorage;
    }

    /**
     * Creates a new Sorted Index storage or returns an existing one.
     */
    SortedIndexStorage getOrCreateStorage(RocksDbMvPartitionStorage partitionStorage) {
        return storages.computeIfAbsent(
                partitionStorage.partitionId(),
                partId -> new RocksDbSortedIndexStorage(descriptor, indexCf, partitionStorage.helper(), indexMetaStorage)
        );
    }

    /**
     * Removes all data associated with the index.
     */
    void destroy(WriteBatch writeBatch) {
        close();

        rocksDb.dropCfOnIndexDestroy(indexCf.nameBytes(), descriptor.id());
    }

    /**
     * Deletes the data associated with the partition in the index, using passed write batch for the operation.
     * Index storage instance is closed after this method, if it ever existed.
     *
     * @throws RocksDBException If failed to delete data.
     */
    void destroy(int partitionId, WriteBatch writeBatch) throws RocksDBException {
        RocksDbSortedIndexStorage sortedIndex = storages.remove(partitionId);

        if (sortedIndex != null) {
            sortedIndex.close();

            sortedIndex.destroyData(writeBatch);
        }
    }

    @Override
    public void close() {
        try {
            IgniteUtils.closeAll(storages.values().stream().map(index -> index::close));
        } catch (Exception e) {
            throw new StorageException("Failed to close index storages: " + descriptor.id(), e);
        }
    }

    /**
     * Returns sorted index storage for partition.
     *
     * @param partitionId Partition ID.
     */
    @Nullable RocksDbSortedIndexStorage get(int partitionId) {
        return storages.get(partitionId);
    }
}
