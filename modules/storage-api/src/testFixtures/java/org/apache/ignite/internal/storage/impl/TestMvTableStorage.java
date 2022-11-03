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

import static java.util.Collections.unmodifiableMap;
import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.MvPartitionStorageDecorator;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.index.HashIndexDescriptor;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.index.HashIndexStorageDecorator;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.index.SortedIndexStorageDecorator;
import org.apache.ignite.internal.storage.index.impl.TestHashIndexStorage;
import org.apache.ignite.internal.storage.index.impl.TestSortedIndexStorage;
import org.jetbrains.annotations.Nullable;

/**
 * Test table storage implementation.
 */
public class TestMvTableStorage implements MvTableStorage {
    private final Map<Integer, Storages> storagesByPartitionId = new ConcurrentHashMap<>();

    private final Map<Integer, BackupStorages> backupStoragesByPartitionId = new ConcurrentHashMap<>();

    private final TableConfiguration tableCfg;

    private final TablesConfiguration tablesCfg;

    /**
     * Container for delegates of partition storages.
     */
    private static class Storages {
        private final MvPartitionStorageDecorator mvPartitionStorage;

        private final Map<UUID, HashIndexStorageDecorator> hashIndexStorageById = new ConcurrentHashMap<>();

        private final Map<UUID, SortedIndexStorageDecorator> sortedIndexStorageById = new ConcurrentHashMap<>();

        private Storages(MvPartitionStorage mvPartitionStorage) {
            this.mvPartitionStorage = new MvPartitionStorageDecorator(mvPartitionStorage);
        }

        HashIndexStorageDecorator getOrCreateHashIndexStorage(UUID indexId, Supplier<HashIndexStorage> createIndexStorage) {
            return hashIndexStorageById.computeIfAbsent(indexId, uuid -> new HashIndexStorageDecorator(createIndexStorage.get()));
        }

        SortedIndexStorageDecorator getOrCreateSortedIndexStorage(UUID indexId, Supplier<SortedIndexStorage> createIndexStorage) {
            return sortedIndexStorageById.computeIfAbsent(indexId, uuid -> new SortedIndexStorageDecorator(createIndexStorage.get()));
        }
    }

    /**
     * Container for backup of partition storages.
     */
    private static class BackupStorages {
        private final MvPartitionStorage mvPartitionStorage;

        private final Map<UUID, HashIndexStorage> hashIndexStorageById;

        private final Map<UUID, SortedIndexStorage> sortedIndexStorageById;

        private BackupStorages(
                MvPartitionStorage mvPartitionStorage,
                Map<UUID, HashIndexStorage> hashIndexStorageById,
                Map<UUID, SortedIndexStorage> sortedIndexStorageById
        ) {
            this.mvPartitionStorage = mvPartitionStorage;
            this.hashIndexStorageById = hashIndexStorageById;
            this.sortedIndexStorageById = sortedIndexStorageById;
        }
    }

    /** Costructor. */
    public TestMvTableStorage(TableConfiguration tableCfg, TablesConfiguration tablesCfg) {
        this.tableCfg = tableCfg;
        this.tablesCfg = tablesCfg;
    }

    @Override
    public MvPartitionStorage getOrCreateMvPartition(int partitionId) throws StorageException {
        return storagesByPartitionId
                .computeIfAbsent(partitionId, partId -> new Storages(new TestMvPartitionStorage(partId)))
                .mvPartitionStorage;
    }

    @Override
    @Nullable
    public MvPartitionStorage getMvPartition(int partitionId) {
        Storages storages = storagesByPartitionId.get(partitionId);

        return storages == null ? null : storages.mvPartitionStorage;
    }

    @Override
    public void destroyPartition(int partitionId) throws StorageException {
        storagesByPartitionId.remove(partitionId);
        backupStoragesByPartitionId.remove(partitionId);
    }

    @Override
    public SortedIndexStorage getOrCreateSortedIndex(int partitionId, UUID indexId) throws StorageException {
        Storages storages = storagesByPartitionId.get(partitionId);

        checkPartitionStoragesExists(storages, partitionId);

        return storages.getOrCreateSortedIndexStorage(
                indexId,
                () -> new TestSortedIndexStorage(new SortedIndexDescriptor(indexId, tablesCfg.value()))
        );
    }

    @Override
    public HashIndexStorage getOrCreateHashIndex(int partitionId, UUID indexId) throws StorageException {
        Storages storages = storagesByPartitionId.get(partitionId);

        checkPartitionStoragesExists(storages, partitionId);

        return storages.getOrCreateHashIndexStorage(
                indexId,
                () -> new TestHashIndexStorage(new HashIndexDescriptor(indexId, tablesCfg.value()))
        );
    }

    @Override
    public CompletableFuture<Void> destroyIndex(UUID indexId) throws StorageException {
        for (Storages storages : storagesByPartitionId.values()) {
            storages.sortedIndexStorageById.remove(indexId);

            HashIndexStorage hashIndexStorage = storages.hashIndexStorageById.remove(indexId);

            if (hashIndexStorage != null) {
                hashIndexStorage.destroy();
            }
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
    public CompletableFuture<Void> startRebalance(int partitionId) throws StorageException {
        Storages storages = storagesByPartitionId.get(partitionId);

        checkPartitionStoragesExists(storages, partitionId);

        backupStoragesByPartitionId.computeIfAbsent(partitionId, partId -> {
            MvPartitionStorage oldMvPartitionStorage = storages.mvPartitionStorage.replaceDelegate(
                    new TestMvPartitionStorage(partitionId)
            );

            Map<UUID, HashIndexStorage> oldHashIndexStorages = new HashMap<>();

            for (Entry<UUID, HashIndexStorageDecorator> entry : storages.hashIndexStorageById.entrySet()) {
                UUID indexId = entry.getKey();

                oldHashIndexStorages.put(
                        indexId,
                        entry.getValue().replaceDelegate(new TestHashIndexStorage(new HashIndexDescriptor(indexId, tablesCfg.value())))
                );
            }

            Map<UUID, SortedIndexStorage> oldSortedIndexStorages = new HashMap<>();

            for (Entry<UUID, SortedIndexStorageDecorator> entry : storages.sortedIndexStorageById.entrySet()) {
                UUID indexId = entry.getKey();

                oldSortedIndexStorages.put(
                        indexId,
                        entry.getValue().replaceDelegate(new TestSortedIndexStorage(new SortedIndexDescriptor(indexId, tablesCfg.value())))
                );
            }

            return new BackupStorages(
                    oldMvPartitionStorage,
                    unmodifiableMap(oldHashIndexStorages),
                    unmodifiableMap(oldSortedIndexStorages)
            );
        });

        return completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> abortRebalance(int partitionId) {
        BackupStorages backupStorages = backupStoragesByPartitionId.remove(partitionId);

        if (backupStorages != null) {
            Storages storages = storagesByPartitionId.get(partitionId);

            checkPartitionStoragesExists(storages, partitionId);

            storages.mvPartitionStorage.replaceDelegate(backupStorages.mvPartitionStorage);

            for (Entry<UUID, HashIndexStorageDecorator> entry : storages.hashIndexStorageById.entrySet()) {
                entry.getValue().replaceDelegate(backupStorages.hashIndexStorageById.get(entry.getKey()));
            }

            for (Entry<UUID, SortedIndexStorageDecorator> entry : storages.sortedIndexStorageById.entrySet()) {
                entry.getValue().replaceDelegate(backupStorages.sortedIndexStorageById.get(entry.getKey()));
            }
        }

        return completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> finishRebalance(int partitionId) {
        backupStoragesByPartitionId.remove(partitionId);

        return completedFuture(null);
    }

    private void checkPartitionStoragesExists(@Nullable Storages storages, int partitionId) throws StorageException {
        if (storages == null) {
            throw new StorageException("Partition ID " + partitionId + " does not exist");
        }
    }
}
