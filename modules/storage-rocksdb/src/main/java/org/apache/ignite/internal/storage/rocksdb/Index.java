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

import static org.apache.ignite.internal.storage.rocksdb.RocksDbIndexes.indexPrefix;
import static org.apache.ignite.internal.storage.rocksdb.instance.SharedRocksDbInstance.deleteByPrefix;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.rocksdb.index.AbstractRocksDbIndexStorage;
import org.apache.ignite.internal.storage.util.StorageState;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

/**
 * Represents an index for all its partitions.
 */
abstract class Index<S extends AbstractRocksDbIndexStorage> {
    private final int tableId;

    private final int indexId;

    private final ColumnFamily columnFamily;

    private final ConcurrentMap<Integer, S> storageByPartitionId = new ConcurrentHashMap<>();

    Index(int tableId, int indexId, ColumnFamily cf) {
        this.tableId = tableId;
        this.indexId = indexId;
        this.columnFamily = cf;
    }

    int tableId() {
        return tableId;
    }

    int indexId() {
        return indexId;
    }

    ColumnFamily columnFamily() {
        return columnFamily;
    }

    /**
     * Returns an index storage for a partition.
     *
     * @param partitionId Partition ID.
     */
    @Nullable S getStorage(int partitionId) {
        return storageByPartitionId.get(partitionId);
    }

    /**
     * Creates a new Index storage or returns an existing one.
     */
    S getOrCreateStorage(int partitionId) {
        return storageByPartitionId.computeIfAbsent(partitionId, this::createStorage);
    }

    abstract S createStorage(int partitionId);

    /**
     * Closes all index storages.
     */
    final void close() {
        try {
            IgniteUtils.closeAll(storageByPartitionId.values().stream().map(index -> index::close));
        } catch (Exception e) {
            throw new StorageException("Failed to close index storages: " + indexId, e);
        }
    }

    /**
     * Transitions all index storages to the {@link StorageState#DESTROYED}.
     */
    void transitionToDestroyedState() {
        try {
            IgniteUtils.closeAll(storageByPartitionId.values().stream().map(index -> index::transitionToDestroyedState));
        } catch (Exception e) {
            throw new StorageException("Failed to transition index storages to the DESTROYED state: " + indexId, e);
        }
    }

    /**
     * Deletes the data associated with the partition in the index, using passed write batch for the operation.
     * Index storage instance is closed after this method, if it ever existed.
     *
     * @throws RocksDBException If failed to delete data.
     */
    void destroy(int partitionId, WriteBatch writeBatch) throws RocksDBException {
        S storage = storageByPartitionId.remove(partitionId);

        if (storage != null) {
            storage.transitionToDestroyedState();

            storage.destroyData(writeBatch);
        }
    }

    /**
     * Removes all data associated with the index.
     */
    void destroy(WriteBatch writeBatch) throws RocksDBException {
        transitionToDestroyedState();

        deleteByPrefix(writeBatch, columnFamily, indexPrefix(tableId, indexId));
    }
}
