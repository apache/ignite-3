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

import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.storage.rocksdb.index.RocksDbSortedIndexStorage;
import org.apache.ignite.internal.storage.rocksdb.instance.SharedRocksDbInstance;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.WriteBatch;

/**
 * Class that represents a Sorted Index defined for all partitions of a Table.
 */
class SortedIndex extends Index<RocksDbSortedIndexStorage> {
    private final StorageSortedIndexDescriptor descriptor;

    private final SharedRocksDbInstance rocksDb;

    private final ColumnFamily indexCf;

    private final RocksDbMetaStorage indexMetaStorage;

    SortedIndex(
            SharedRocksDbInstance rocksDb,
            StorageSortedIndexDescriptor descriptor,
            RocksDbMetaStorage indexMetaStorage
    ) {
        super(descriptor.id());

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
        return storageByPartitionId.computeIfAbsent(
                partitionStorage.partitionId(),
                partId -> new RocksDbSortedIndexStorage(descriptor, indexCf, partitionStorage.helper(), indexMetaStorage)
        );
    }

    /**
     * Removes all data associated with the index.
     */
    void destroy(WriteBatch writeBatch) {
        transitionToDestroyedState();

        rocksDb.dropCfOnIndexDestroy(indexCf.nameBytes(), descriptor.id());
    }

    /**
     * Returns sorted index storage for partition.
     *
     * @param partitionId Partition ID.
     */
    @Nullable RocksDbSortedIndexStorage get(int partitionId) {
        return storageByPartitionId.get(partitionId);
    }
}
