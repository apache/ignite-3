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
import static org.apache.ignite.internal.storage.rocksdb.RocksDbStorageUtils.createKey;
import static org.apache.ignite.internal.storage.rocksdb.instance.SharedRocksDbInstance.deleteByPrefix;
import static org.apache.ignite.internal.util.ArrayUtils.BYTE_EMPTY_ARRAY;

import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.storage.rocksdb.index.RocksDbSortedIndexStorage;
import org.apache.ignite.internal.storage.rocksdb.instance.SharedRocksDbInstance;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

/**
 * Class that represents a Sorted Index defined for all partitions of a Table.
 */
class SortedIndex extends Index<RocksDbSortedIndexStorage> {
    private final StorageSortedIndexDescriptor descriptor;

    private final SharedRocksDbInstance rocksDb;

    private final ColumnFamily indexCf;

    private final RocksDbMetaStorage indexMetaStorage;

    private SortedIndex(
            SharedRocksDbInstance rocksDb,
            ColumnFamily indexCf,
            StorageSortedIndexDescriptor descriptor,
            RocksDbMetaStorage indexMetaStorage
    ) {
        super(descriptor.id());

        this.rocksDb = rocksDb;
        this.descriptor = descriptor;
        this.indexCf = indexCf;
        this.indexMetaStorage = indexMetaStorage;
    }

    static SortedIndex createNew(
            SharedRocksDbInstance rocksDb,
            StorageSortedIndexDescriptor descriptor,
            RocksDbMetaStorage indexMetaStorage
    ) {
        ColumnFamily indexCf = rocksDb.getOrCreateSortedIndexCf(sortedIndexCfName(descriptor.columns()), descriptor.id());

        return new SortedIndex(rocksDb, indexCf, descriptor, indexMetaStorage);
    }

    static SortedIndex restoreExisting(
            SharedRocksDbInstance rocksDb,
            ColumnFamily indexCf,
            StorageSortedIndexDescriptor descriptor,
            RocksDbMetaStorage indexMetaStorage
    ) {
        return new SortedIndex(rocksDb, indexCf, descriptor, indexMetaStorage);
    }

    /**
     * Returns sorted index storage for partition.
     *
     * @param partitionId Partition ID.
     */
    @Nullable RocksDbSortedIndexStorage getStorage(int partitionId) {
        return storageByPartitionId.get(partitionId);
    }

    /**
     * Creates a new Sorted Index storage or returns an existing one.
     */
    SortedIndexStorage getOrCreateStorage(RocksDbMvPartitionStorage partitionStorage) {
        return storageByPartitionId.computeIfAbsent(
                partitionStorage.partitionId(),
                partId -> new RocksDbSortedIndexStorage(descriptor, indexCf, partitionStorage.helper(), indexMetaStorage)
        );
    }

    /**
     * Removes all data associated with the index.
     */
    void destroy(WriteBatch writeBatch) throws RocksDBException {
        transitionToDestroyedState();

        deleteByPrefix(writeBatch, indexCf, createKey(BYTE_EMPTY_ARRAY, descriptor.id()));
    }

    /**
     * Signals the shared RocksDB instance that this index has been destroyed and all shared resources (like the Column Family) can
     * be de-allocated.
     */
    void destroySortedIndexCfIfNeeded() {
        rocksDb.destroySortedIndexCfIfNeeded(indexCf.nameBytes(), descriptor.id());
    }
}
