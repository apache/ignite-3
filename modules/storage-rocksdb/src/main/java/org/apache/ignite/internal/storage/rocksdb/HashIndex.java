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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.storage.index.HashIndexDescriptor;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.rocksdb.index.RocksDbHashIndexStorage;

/**
 * Class that represents a Hash Index defined for all partitions of a Table.
 */
class HashIndex {
    private final ColumnFamily indexCf;

    private final HashIndexDescriptor descriptor;

    private final ConcurrentMap<Integer, HashIndexStorage> storages = new ConcurrentHashMap<>();

    HashIndex(ColumnFamily indexCf, HashIndexDescriptor descriptor) {
        this.indexCf = indexCf;
        this.descriptor = descriptor;
    }

    /**
     * Creates a new Hash Index storage or returns an existing one.
     */
    HashIndexStorage getOrCreateStorage(RocksDbMvPartitionStorage partitionStorage) {
        return storages.computeIfAbsent(
                partitionStorage.partitionId(),
                partId -> new RocksDbHashIndexStorage(descriptor, indexCf, partitionStorage)
        );
    }

    /**
     * Destroys the storages and the data in it.
     */
    void destroy() {
        storages.forEach((partitionId, storage) -> storage.destroy());

        storages.clear();
    }

    /**
     * Destroys the storage and the data in it for partition.
     *
     * @param partitionId Partition ID.
     */
    void destroy(int partitionId) {
        HashIndexStorage removed = storages.remove(partitionId);

        if (removed != null) {
            removed.destroy();
        }
    }
}
