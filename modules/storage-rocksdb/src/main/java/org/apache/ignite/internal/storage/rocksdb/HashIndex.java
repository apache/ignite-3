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

import static org.apache.ignite.internal.storage.rocksdb.RocksDbStorageUtils.createKey;
import static org.apache.ignite.internal.storage.rocksdb.instance.SharedRocksDbInstance.deleteByPrefix;
import static org.apache.ignite.internal.util.ArrayUtils.BYTE_EMPTY_ARRAY;

import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.rocksdb.index.RocksDbHashIndexStorage;
import org.apache.ignite.internal.storage.rocksdb.instance.SharedRocksDbInstance;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

/**
 * Class that represents a Hash Index defined for all partitions of a Table.
 */
class HashIndex extends Index<RocksDbHashIndexStorage> {
    private final ColumnFamily indexCf;

    private final StorageHashIndexDescriptor descriptor;

    private final RocksDbMetaStorage indexMetaStorage;

    HashIndex(SharedRocksDbInstance rocksDb, StorageHashIndexDescriptor descriptor, RocksDbMetaStorage indexMetaStorage) {
        super(descriptor.id());

        this.indexCf = rocksDb.hashIndexCf();
        this.descriptor = descriptor;
        this.indexMetaStorage = indexMetaStorage;
    }

    /**
     * Returns hash index storage for partition.
     *
     * @param partitionId Partition ID.
     */
    @Nullable RocksDbHashIndexStorage getStorage(int partitionId) {
        return storageByPartitionId.get(partitionId);
    }

    /**
     * Creates a new Hash Index storage or returns an existing one.
     */
    HashIndexStorage getOrCreateStorage(RocksDbMvPartitionStorage partitionStorage) {
        return storageByPartitionId.computeIfAbsent(
                partitionStorage.partitionId(),
                partId -> new RocksDbHashIndexStorage(descriptor, indexCf, partitionStorage.helper(), indexMetaStorage)
        );
    }

    /**
     * Removes all data associated with the index.
     */
    void destroy(WriteBatch writeBatch) throws RocksDBException {
        transitionToDestroyedState();

        // Every index storage uses an "index ID" + "partition ID" prefix. We can remove everything by just using the index ID prefix.
        deleteByPrefix(writeBatch, indexCf, createKey(BYTE_EMPTY_ARRAY, descriptor.id()));
    }
}
