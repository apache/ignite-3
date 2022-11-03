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
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.index.HashIndexDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.impl.TestHashIndexStorage;
import org.apache.ignite.internal.storage.index.impl.TestSortedIndexStorage;
import org.jetbrains.annotations.Nullable;

/**
 * Test table storage implementation.
 */
public class TestMvTableStorage implements MvTableStorage {
    private final Map<Integer, Storages> storagesByPartitionId = new ConcurrentHashMap<>();

    private final Map<Integer, Storages> backupStoragesByPartitionId = new ConcurrentHashMap<>();

    private final TableConfiguration tableCfg;

    private final TablesConfiguration tablesCfg;

    /**
     * Storages container for the partition.
     */
    private static class Storages {
        public Storages(TestMvPartitionStorage mvPartitionStorage) {
            this.mvPartitionStorage = mvPartitionStorage;
        }

        private final TestMvPartitionStorage mvPartitionStorage;

        private final Map<UUID, TestHashIndexStorage> hashIndexes = new ConcurrentHashMap<>();

        private final Map<UUID, TestSortedIndexStorage> sortedIndexes = new ConcurrentHashMap<>();
    }

    /** Costructor. */
    public TestMvTableStorage(TableConfiguration tableCfg, TablesConfiguration tablesCfg) {
        this.tableCfg = tableCfg;
        this.tablesCfg = tablesCfg;
    }

    @Override
    public TestMvPartitionStorage getOrCreateMvPartition(int partitionId) throws StorageException {
        return storagesByPartitionId
                .computeIfAbsent(partitionId, partId -> new Storages(new TestMvPartitionStorage(partId)))
                .mvPartitionStorage;
    }

    @Override
    @Nullable
    public TestMvPartitionStorage getMvPartition(int partitionId) {
        Storages storages = storagesByPartitionId.get(partitionId);

        return storages == null ? null : storages.mvPartitionStorage;
    }

    @Override
    public void destroyPartition(int partitionId) throws StorageException {
        storagesByPartitionId.remove(partitionId);
    }

    @Override
    public TestSortedIndexStorage getOrCreateSortedIndex(int partitionId, UUID indexId) throws StorageException {
        Storages storages = storagesByPartitionId.get(partitionId);

        checkPartitionStoragesExists(storages, partitionId);

        return storages.sortedIndexes
                .computeIfAbsent(indexId, uuid -> new TestSortedIndexStorage(new SortedIndexDescriptor(uuid, tablesCfg.value())));
    }

    @Override
    public TestHashIndexStorage getOrCreateHashIndex(int partitionId, UUID indexId) throws StorageException {
        Storages storages = storagesByPartitionId.get(partitionId);

        checkPartitionStoragesExists(storages, partitionId);

        return storages.hashIndexes
                .computeIfAbsent(indexId, uuid -> new TestHashIndexStorage(new HashIndexDescriptor(uuid, tablesCfg.value())));
    }

    @Override
    public CompletableFuture<Void> destroyIndex(int partitionId, UUID indexId) throws StorageException {
        Storages storages = storagesByPartitionId.get(partitionId);

        checkPartitionStoragesExists(storages, partitionId);

        TestHashIndexStorage hashIndex = storages.hashIndexes.remove(indexId);

        if (hashIndex != null) {
            hashIndex.destroy();
        }

        storages.sortedIndexes.remove(indexId);

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
    public CompletableFuture<Void> startRebalanceMvPartition(int partitionId) throws StorageException {
        Storages oldStorages = storagesByPartitionId.get(partitionId);

        checkPartitionStoragesExists(oldStorages, partitionId);

        if (backupStoragesByPartitionId.putIfAbsent(partitionId, oldStorages) == null) {
            Storages newStorages = new Storages(new TestMvPartitionStorage(partitionId));

            oldStorages.hashIndexes.keySet().forEach(indexId -> newStorages.hashIndexes.put(
                    indexId,
                    new TestHashIndexStorage(new HashIndexDescriptor(indexId, tablesCfg.value()))
            ));

            oldStorages.sortedIndexes.keySet().forEach(indexId -> newStorages.sortedIndexes.put(
                    indexId,
                    new TestSortedIndexStorage(new SortedIndexDescriptor(indexId, tablesCfg.value()))
            ));

            storagesByPartitionId.put(partitionId, newStorages);
        }

        return completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> abortRebalanceMvPartition(int partitionId) {
        Storages oldStorages = backupStoragesByPartitionId.remove(partitionId);

        if (oldStorages != null) {
            storagesByPartitionId.put(partitionId, oldStorages);
        }

        return completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> finishRebalanceMvPartition(int partitionId) {
        backupStoragesByPartitionId.remove(partitionId);

        return completedFuture(null);
    }

    private void checkPartitionStoragesExists(@Nullable Storages storages, int partitionId) throws StorageException {
        if (storages == null) {
            throw new StorageException("Partition ID " + partitionId + " does not exist");
        }
    }
}
