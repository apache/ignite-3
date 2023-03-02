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

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.storage.util.StorageUtils.createMissingMvPartitionErrorMessage;
import static org.mockito.Mockito.spy;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
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
import org.apache.ignite.internal.storage.util.MvPartitionStorages;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.jetbrains.annotations.Nullable;

/**
 * Test table storage implementation.
 */
public class TestMvTableStorage implements MvTableStorage {
    private final MvPartitionStorages<TestMvPartitionStorage> mvPartitionStorages;

    private final Map<UUID, SortedIndices> sortedIndicesById = new ConcurrentHashMap<>();

    private final Map<UUID, HashIndices> hashIndicesById = new ConcurrentHashMap<>();

    private final TableConfiguration tableCfg;

    private final TablesConfiguration tablesCfg;

    /**
     * Class for storing Sorted Indices for a particular partition.
     */
    private static class SortedIndices {
        private final SortedIndexDescriptor descriptor;

        final Map<Integer, TestSortedIndexStorage> storageByPartitionId = new ConcurrentHashMap<>();

        SortedIndices(SortedIndexDescriptor descriptor) {
            this.descriptor = descriptor;
        }

        TestSortedIndexStorage getOrCreateStorage(Integer partitionId) {
            return storageByPartitionId.computeIfAbsent(partitionId, id -> new TestSortedIndexStorage(descriptor));
        }
    }

    /**
     * Class for storing Hash Indices for a particular partition.
     */
    private static class HashIndices {
        private final HashIndexDescriptor descriptor;

        final Map<Integer, TestHashIndexStorage> storageByPartitionId = new ConcurrentHashMap<>();

        HashIndices(HashIndexDescriptor descriptor) {
            this.descriptor = descriptor;
        }

        TestHashIndexStorage getOrCreateStorage(Integer partitionId) {
            return storageByPartitionId.computeIfAbsent(partitionId, id -> new TestHashIndexStorage(descriptor));
        }
    }

    /** Constructor. */
    public TestMvTableStorage(TableConfiguration tableCfg, TablesConfiguration tablesCfg) {
        this.tableCfg = tableCfg;
        this.tablesCfg = tablesCfg;

        mvPartitionStorages = new MvPartitionStorages<>(tableCfg.value());
    }

    @Override
    public CompletableFuture<MvPartitionStorage> createMvPartition(int partitionId) {
        return mvPartitionStorages.create(partitionId, partId -> spy(new TestMvPartitionStorage(partId)));
    }

    @Override
    public @Nullable MvPartitionStorage getMvPartition(int partitionId) {
        return mvPartitionStorages.get(partitionId);
    }

    @Override
    public CompletableFuture<Void> destroyPartition(int partitionId) {
        return mvPartitionStorages.destroy(partitionId, this::destroyPartition);
    }

    private CompletableFuture<Void> destroyPartition(TestMvPartitionStorage mvPartitionStorage) {
        mvPartitionStorage.destroy();

        for (HashIndices hashIndices : hashIndicesById.values()) {
            HashIndexStorage removedHashIndexStorage = hashIndices.storageByPartitionId.remove(mvPartitionStorage.partitionId);

            if (removedHashIndexStorage != null) {
                removedHashIndexStorage.destroy();
            }
        }

        for (SortedIndices sortedIndices : sortedIndicesById.values()) {
            SortedIndexStorage removedSortedIndexStorage = sortedIndices.storageByPartitionId.remove(mvPartitionStorage.partitionId);

            if (removedSortedIndexStorage != null) {
                ((TestSortedIndexStorage) removedSortedIndexStorage).destroy();
            }
        }

        return completedFuture(null);
    }

    @Override
    public SortedIndexStorage getOrCreateSortedIndex(int partitionId, UUID indexId) {
        TestMvPartitionStorage mvPartitionStorage = mvPartitionStorages.get(partitionId);

        if (mvPartitionStorage == null) {
            throw new StorageException(createMissingMvPartitionErrorMessage(partitionId));
        }

        SortedIndices sortedIndices = sortedIndicesById.computeIfAbsent(
                indexId,
                id -> new SortedIndices(new SortedIndexDescriptor(id, tablesCfg.value()))
        );

        return sortedIndices.getOrCreateStorage(partitionId);
    }

    @Override
    public HashIndexStorage getOrCreateHashIndex(int partitionId, UUID indexId) {
        TestMvPartitionStorage mvPartitionStorage = mvPartitionStorages.get(partitionId);

        if (mvPartitionStorage == null) {
            throw new StorageException(createMissingMvPartitionErrorMessage(partitionId));
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
    public void close() throws StorageException {
        stop();
    }

    @Override
    public CompletableFuture<Void> destroy() {
        stop();

        return mvPartitionStorages.getAllForCloseOrDestroy()
                .thenCompose(mvStorages -> allOf(mvStorages.stream().map(this::destroyPartition).toArray(CompletableFuture[]::new)));
    }

    @Override
    public CompletableFuture<Void> startRebalancePartition(int partitionId) {
        return mvPartitionStorages.startRebalace(partitionId, mvPartitionStorage -> {
            mvPartitionStorage.startRebalance();

            testHashIndexStorageStream(partitionId).forEach(TestHashIndexStorage::startRebalance);

            testSortedIndexStorageStream(partitionId).forEach(TestSortedIndexStorage::startRebalance);

            return completedFuture(null);
        });
    }

    @Override
    public CompletableFuture<Void> abortRebalancePartition(int partitionId) {
        return mvPartitionStorages.abortRebalance(partitionId, mvPartitionStorage -> {
            mvPartitionStorage.abortRebalance();

            testHashIndexStorageStream(partitionId).forEach(TestHashIndexStorage::abortRebalance);

            testSortedIndexStorageStream(partitionId).forEach(TestSortedIndexStorage::abortRebalance);

            return completedFuture(null);
        });
    }

    @Override
    public CompletableFuture<Void> finishRebalancePartition(
            int partitionId,
            long lastAppliedIndex,
            long lastAppliedTerm,
            byte[] groupConfig
    ) {
        return mvPartitionStorages.finishRebalance(partitionId, mvPartitionStorage -> {
            mvPartitionStorage.finishRebalance(lastAppliedIndex, lastAppliedTerm, groupConfig);

            testHashIndexStorageStream(partitionId).forEach(TestHashIndexStorage::finishRebalance);

            testSortedIndexStorageStream(partitionId).forEach(TestSortedIndexStorage::finishRebalance);

            return completedFuture(null);
        });
    }

    @Override
    public CompletableFuture<Void> clearPartition(int partitionId) {
        return mvPartitionStorages.clear(partitionId, mvPartitionStorage -> {
            mvPartitionStorage.clear();

            testHashIndexStorageStream(partitionId).forEach(TestHashIndexStorage::clear);
            testSortedIndexStorageStream(partitionId).forEach(TestSortedIndexStorage::clear);

            return completedFuture(null);
        });
    }

    private Stream<TestHashIndexStorage> testHashIndexStorageStream(Integer partitionId) {
        return hashIndicesById.values().stream()
                .map(hashIndices -> hashIndices.storageByPartitionId.get(partitionId))
                .filter(Objects::nonNull);
    }

    private Stream<TestSortedIndexStorage> testSortedIndexStorageStream(Integer partitionId) {
        return sortedIndicesById.values().stream()
                .map(hashIndices -> hashIndices.storageByPartitionId.get(partitionId))
                .filter(Objects::nonNull);
    }

    private String createStorageInfo(int partitionId) {
        return IgniteStringFormatter.format("table={}, partitionId={}", tableCfg.name().value(), partitionId);
    }
}
