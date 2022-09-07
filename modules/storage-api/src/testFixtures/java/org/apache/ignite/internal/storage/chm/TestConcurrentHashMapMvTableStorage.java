/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.storage.chm;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.index.HashIndexDescriptor;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.index.impl.TestHashIndexStorage;
import org.apache.ignite.internal.storage.index.impl.TestSortedIndexStorage;
import org.jetbrains.annotations.Nullable;

/**
 * Test table storage implementation.
 */
public class TestConcurrentHashMapMvTableStorage implements MvTableStorage {
    private final TableConfiguration tableConfig;

    private final Map<Integer, MvPartitionStorage> partitions = new ConcurrentHashMap<>();

    private final Map<UUID, SortedIndices> sortedIndicesById = new ConcurrentHashMap<>();

    private final Map<UUID, HashIndices> hashIndicesById = new ConcurrentHashMap<>();

    /**
     * Class for storing Sorted Indices for a particular partition.
     */
    private static class SortedIndices {
        private final SortedIndexDescriptor descriptor;

        final Map<Integer, SortedIndexStorage> storageByPartitionId = new ConcurrentHashMap<>();

        SortedIndices(SortedIndexDescriptor descriptor) {
            this.descriptor = descriptor;
        }

        SortedIndexStorage getOrCreateStorage(Integer partitionId) {
            return storageByPartitionId.computeIfAbsent(partitionId, id -> new TestSortedIndexStorage(descriptor));
        }
    }

    /**
     * Class for storing Hash Indices for a particular partition.
     */
    private static class HashIndices {
        private final HashIndexDescriptor descriptor;

        final Map<Integer, HashIndexStorage> storageByPartitionId = new ConcurrentHashMap<>();

        HashIndices(HashIndexDescriptor descriptor) {
            this.descriptor = descriptor;
        }

        HashIndexStorage getOrCreateStorage(Integer partitionId) {
            return storageByPartitionId.computeIfAbsent(partitionId, id -> new TestHashIndexStorage(descriptor));
        }
    }

    public TestConcurrentHashMapMvTableStorage(TableConfiguration tableCfg) {
        this.tableConfig = tableCfg;
    }

    @Override
    public MvPartitionStorage getOrCreateMvPartition(int partitionId) throws StorageException {
        return partitions.computeIfAbsent(partitionId, TestConcurrentHashMapMvPartitionStorage::new);
    }

    @Override
    @Nullable
    public MvPartitionStorage getMvPartition(int partitionId) {
        return partitions.get(partitionId);
    }

    @Override
    public CompletableFuture<Void> destroyPartition(int partitionId) throws StorageException {
        Integer boxedPartitionId = partitionId;

        partitions.remove(boxedPartitionId);

        sortedIndicesById.values().forEach(indices -> indices.storageByPartitionId.remove(boxedPartitionId));
        hashIndicesById.values().forEach(indices -> indices.storageByPartitionId.remove(boxedPartitionId));

        return CompletableFuture.completedFuture(null);
    }

    @Override
    public SortedIndexStorage getOrCreateSortedIndex(int partitionId, UUID indexId) {
        if (!partitions.containsKey(partitionId)) {
            throw new StorageException("Partition ID " + partitionId + " does not exist");
        }

        SortedIndices sortedIndices = sortedIndicesById.computeIfAbsent(
                indexId,
                id -> new SortedIndices(new SortedIndexDescriptor(id, tableConfig.value()))
        );

        return sortedIndices.getOrCreateStorage(partitionId);
    }

    @Override
    public HashIndexStorage getOrCreateHashIndex(int partitionId, UUID indexId) {
        if (!partitions.containsKey(partitionId)) {
            throw new StorageException("Partition ID " + partitionId + " does not exist");
        }

        HashIndices sortedIndices = hashIndicesById.computeIfAbsent(
                indexId,
                id -> new HashIndices(new HashIndexDescriptor(id, tableConfig.value()))
        );

        return sortedIndices.getOrCreateStorage(partitionId);
    }

    @Override
    public CompletableFuture<Void> destroyIndex(UUID indexId) {
        sortedIndicesById.remove(indexId);

        HashIndices hashIndex = hashIndicesById.remove(indexId);

        if (hashIndex != null) {
            hashIndex.storageByPartitionId.values().forEach(HashIndexStorage::destroy);
        }

        return CompletableFuture.completedFuture(null);
    }

    @Override
    public boolean isVolatile() {
        return true;
    }

    @Override
    public TableConfiguration configuration() {
        return tableConfig;
    }

    @Override
    public void start() throws StorageException {
    }

    @Override
    public void stop() throws StorageException {
    }

    @Override
    public void destroy() throws StorageException {
    }
}
