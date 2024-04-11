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
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.storage.rocksdb.index.RocksDbSortedIndexStorage;
import org.apache.ignite.internal.storage.rocksdb.instance.SharedRocksDbInstance;

/**
 * Class that represents a Sorted Index defined for all partitions of a Table.
 */
class SortedIndex extends Index<RocksDbSortedIndexStorage> {
    private final StorageSortedIndexDescriptor descriptor;

    private final RocksDbMetaStorage indexMetaStorage;

    private SortedIndex(
            int tableId,
            ColumnFamily indexCf,
            StorageSortedIndexDescriptor descriptor,
            RocksDbMetaStorage indexMetaStorage
    ) {
        super(tableId, descriptor.id(), indexCf);

        this.descriptor = descriptor;
        this.indexMetaStorage = indexMetaStorage;
    }

    static SortedIndex createNew(
            SharedRocksDbInstance rocksDb,
            int tableId,
            StorageSortedIndexDescriptor descriptor,
            RocksDbMetaStorage indexMetaStorage
    ) {
        ColumnFamily indexCf = rocksDb.getOrCreateSortedIndexCf(sortedIndexCfName(descriptor.columns()), descriptor.id(), tableId);

        return new SortedIndex(tableId, indexCf, descriptor, indexMetaStorage);
    }

    static SortedIndex restoreExisting(
            int tableId,
            ColumnFamily indexCf,
            StorageSortedIndexDescriptor descriptor,
            RocksDbMetaStorage indexMetaStorage
    ) {
        return new SortedIndex(tableId, indexCf, descriptor, indexMetaStorage);
    }

    @Override
    RocksDbSortedIndexStorage createStorage(int partitionId) {
        return new RocksDbSortedIndexStorage(descriptor, tableId(), partitionId, columnFamily(), indexMetaStorage);
    }
}
