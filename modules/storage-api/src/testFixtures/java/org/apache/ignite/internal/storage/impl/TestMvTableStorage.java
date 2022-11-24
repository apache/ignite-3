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

package org.apache.ignite.internal.storage.impl;

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.index.HashIndexDescriptor;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.index.impl.TestHashIndexStorage;
import org.apache.ignite.internal.storage.index.impl.TestSortedIndexStorage;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * Test table storage implementation.
 */
public class TestMvTableStorage implements MvTableStorage {
    private final Map<Integer, MvPartitionStorage> partitions = new ConcurrentHashMap<>();

    private final Map<Integer, MvPartitionStorage> backupPartitions = new ConcurrentHashMap<>();

    private final Map<UUID, SortedIndices> sortedIndicesById = new ConcurrentHashMap<>();

    private final Map<UUID, HashIndices> hashIndicesById = new ConcurrentHashMap<>();

    private final Map<Integer, CompletableFuture<Void>> partitionIdDestroyFuture = new ConcurrentHashMap<>();

    private final TableConfiguration tableCfg;

    private final TablesConfiguration tablesCfg;

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

    /** Costructor. */
    public TestMvTableStorage(TableConfiguration tableCfg, TablesConfiguration tablesCfg) {
        this.tableCfg = tableCfg;
        this.tablesCfg = tablesCfg;
    }

    @Override
    public MvPartitionStorage getOrCreateMvPartition(int partitionId) throws StorageException {
        return partitions.computeIfAbsent(partitionId, TestMvPartitionStorage::new);
    }

    @Override
    @Nullable
    public MvPartitionStorage getMvPartition(int partitionId) {
        return partitions.get(partitionId);
    }

    @Override
    public void destroyPartition(int partitionId) throws StorageException {
        checkPartitionId(partitionId);

        MvPartitionStorage removedMvPartitionStorage = partitions.remove(partitionId);

        if (removedMvPartitionStorage != null) {
            ((TestMvPartitionStorage) removedMvPartitionStorage).destroy();

            for (HashIndices hashIndices : hashIndicesById.values()) {
                HashIndexStorage removedHashIndexStorage = hashIndices.storageByPartitionId.remove(partitionId);

                if (removedHashIndexStorage != null) {
                    removedHashIndexStorage.destroy();
                }
            }

            for (SortedIndices sortedIndices : sortedIndicesById.values()) {
                SortedIndexStorage removedSortedIndexStorage = sortedIndices.storageByPartitionId.remove(partitionId);

                if (removedSortedIndexStorage != null) {
                    ((TestSortedIndexStorage) removedSortedIndexStorage).destroy();
                }
            }
        }
    }

    @Override
    public SortedIndexStorage getOrCreateSortedIndex(int partitionId, UUID indexId) {
        if (!partitions.containsKey(partitionId)) {
            throw new StorageException("Partition ID " + partitionId + " does not exist");
        }

        SortedIndices sortedIndices = sortedIndicesById.computeIfAbsent(
                indexId,
                id -> new SortedIndices(new SortedIndexDescriptor(id, tablesCfg.value()))
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
                id -> new HashIndices(new HashIndexDescriptor(id, tablesCfg.value()))
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

        return completedFuture(null);
    }

    @Override
    public boolean isVolatile() {
        return true;
    }

    @Override
    public TableConfiguration configuration() {
        return tableCfg;
    }

    @Override
    public TablesConfiguration tablesConfiguration() {
        return tablesCfg;
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

    @Override
    public CompletableFuture<Void> startRebalanceMvPartition(int partitionId) {
        MvPartitionStorage oldPartitionStorage = partitions.get(partitionId);

        assert oldPartitionStorage != null : "Partition does not exist: " + partitionId;

        if (backupPartitions.putIfAbsent(partitionId, oldPartitionStorage) == null) {
            partitions.put(partitionId, new TestMvPartitionStorage(partitionId));
        }

        return completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> abortRebalanceMvPartition(int partitionId) {
        MvPartitionStorage oldPartitionStorage = backupPartitions.remove(partitionId);

        if (oldPartitionStorage != null) {
            partitions.put(partitionId, oldPartitionStorage);
        }

        return completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> finishRebalanceMvPartition(int partitionId) {
        backupPartitions.remove(partitionId);

        return completedFuture(null);
    }

    private void checkPartitionId(int partitionId) {
        Integer partitions = tableCfg.partitions().value();

        if (partitionId < 0 || partitionId >= partitions) {
            throw new IllegalArgumentException(S.toString(
                    "Unable to access partition with id outside of configured range",
                    "table", tableCfg.value().name(), false,
                    "partitionId", partitionId, false,
                    "partitions", partitions, false
            ));
        }
    }
}
