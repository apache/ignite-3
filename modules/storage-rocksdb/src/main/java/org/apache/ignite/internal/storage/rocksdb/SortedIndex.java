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
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.rocksdb.index.RocksDbSortedIndexStorage;
import org.rocksdb.RocksDBException;

/**
 * Class that represents a Sorted Index defined for all partitions of a Table.
 */
class SortedIndex implements ManuallyCloseable {
    private final SortedIndexDescriptor descriptor;

    private final ColumnFamily indexCf;

    private final ConcurrentMap<Integer, SortedIndexStorage> storages = new ConcurrentHashMap<>();

    SortedIndex(ColumnFamily indexCf, SortedIndexDescriptor descriptor) {
        this.descriptor = descriptor;
        this.indexCf = indexCf;
    }

    /**
     * Creates a new Sorted Index storage or returns an existing one.
     */
    SortedIndexStorage getOrCreateStorage(RocksDbMvPartitionStorage partitionStorage) {
        return storages.computeIfAbsent(
                partitionStorage.partitionId(),
                partId -> new RocksDbSortedIndexStorage(descriptor, indexCf, partitionStorage)
        );
    }

    /**
     * Returns the Column Family associated with this index.
     */
    ColumnFamily indexCf() {
        return indexCf;
    }

    /**
     * Removes all data associated with the index.
     */
    void destroy() {
        try {
            indexCf.destroy();
        } catch (RocksDBException e) {
            throw new StorageException("Unable to destroy index " + descriptor.id(), e);
        }
    }

    @Override
    public void close() {
        indexCf.handle().close();
    }
}
