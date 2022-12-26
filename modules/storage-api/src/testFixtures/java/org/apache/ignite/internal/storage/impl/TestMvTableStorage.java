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
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.StorageRebalanceException;
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
    private final Map<Integer, TestMvPartitionStorage> partitions = new ConcurrentHashMap<>();

    private final Map<UUID, SortedIndices> sortedIndicesById = new ConcurrentHashMap<>();

    private final Map<UUID, HashIndices> hashIndicesById = new ConcurrentHashMap<>();

    private final Map<Integer, CompletableFuture<Void>> destroyFutureByPartitionId = new ConcurrentHashMap<>();

    private final Map<Integer, CompletableFuture<Void>> rebalanceFutureByPartitionId = new ConcurrentHashMap<>();

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
    public CompletableFuture<Void> destroyPartition(int partitionId) {
        checkPartitionId(partitionId);

        if (rebalanceFutureByPartitionId.containsKey(partitionId)) {
            throw new StorageException("Partition in the process of rebalancing: " + partitionId);
        }

        CompletableFuture<Void> destroyPartitionFuture = new CompletableFuture<>();

        CompletableFuture<Void> previousDestroyPartitionFuture = destroyFutureByPartitionId.putIfAbsent(
                partitionId,
                destroyPartitionFuture
        );

        if (previousDestroyPartitionFuture != null) {
            return previousDestroyPartitionFuture;
        }

        MvPartitionStorage removedMvPartitionStorage = partitions.remove(partitionId);

        if (removedMvPartitionStorage != null) {
            try {
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

                destroyFutureByPartitionId.remove(partitionId).complete(null);
            } catch (Throwable throwable) {
                destroyFutureByPartitionId.remove(partitionId).completeExceptionally(throwable);
            }
        } else {
            destroyFutureByPartitionId.remove(partitionId).complete(null);
        }

        return destroyPartitionFuture;
    }

    @Override
    public SortedIndexStorage getOrCreateSortedIndex(int partitionId, UUID indexId) {
        if (!partitions.containsKey(partitionId)) {
            throw new StorageException(createPartitionDoesNotExistsErrorMessage(partitionId));
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
            throw new StorageException(createPartitionDoesNotExistsErrorMessage(partitionId));
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
        return completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> startRebalancePartition(int partitionId) {
        checkPartitionId(partitionId);

        TestMvPartitionStorage partitionStorage = partitions.get(partitionId);

        if (partitionStorage == null) {
            throw new StorageRebalanceException(createPartitionDoesNotExistsErrorMessage(partitionId));
        }

        if (destroyFutureByPartitionId.containsKey(partitionId)) {
            throw new StorageRebalanceException("Partition in the process of destruction: " + partitionId);
        }

        CompletableFuture<Void> rebalanceFuture = new CompletableFuture<>();

        if (rebalanceFutureByPartitionId.putIfAbsent(partitionId, rebalanceFuture) != null) {
            throw new StorageRebalanceException("Rebalance for the partition is already in progress: " + partitionId);
        }

        try {
            partitionStorage.startRebalance();

            testHashIndexStorageStream(partitionId).forEach(TestHashIndexStorage::startRebalance);

            testSortedIndexStorageStream(partitionId).forEach(TestSortedIndexStorage::startRebalance);

            rebalanceFuture.complete(null);
        } catch (Throwable t) {
            rebalanceFuture.completeExceptionally(t);
        }

        return rebalanceFuture;
    }

    @Override
    public CompletableFuture<Void> abortRebalancePartition(int partitionId) {
        checkPartitionId(partitionId);

        CompletableFuture<Void> rebalanceFuture = rebalanceFutureByPartitionId.remove(partitionId);

        if (rebalanceFuture == null) {
            return completedFuture(null);
        }

        TestMvPartitionStorage partitionStorage = partitions.get(partitionId);

        if (partitionStorage == null) {
            throw new StorageRebalanceException(createPartitionDoesNotExistsErrorMessage(partitionId));
        }

        return rebalanceFuture
                .thenAccept(unused -> {
                    partitionStorage.abortRebalance();

                    testHashIndexStorageStream(partitionId).forEach(TestHashIndexStorage::abortRebalance);

                    testSortedIndexStorageStream(partitionId).forEach(TestSortedIndexStorage::abortRebalance);
                });
    }

    @Override
    public CompletableFuture<Void> finishRebalancePartition(int partitionId, long lastAppliedIndex, long lastAppliedTerm) {
        checkPartitionId(partitionId);

        CompletableFuture<Void> rebalanceFuture = rebalanceFutureByPartitionId.remove(partitionId);

        if (rebalanceFuture == null) {
            throw new StorageRebalanceException("Rebalance for the partition did not start: " + partitionId);
        }

        TestMvPartitionStorage partitionStorage = partitions.get(partitionId);

        if (partitionStorage == null) {
            throw new StorageRebalanceException(createPartitionDoesNotExistsErrorMessage(partitionId));
        }

        return rebalanceFuture
                .thenAccept(unused -> {
                    partitionStorage.finishRebalance(lastAppliedIndex, lastAppliedTerm);

                    testHashIndexStorageStream(partitionId).forEach(TestHashIndexStorage::finishRebalance);

                    testSortedIndexStorageStream(partitionId).forEach(TestSortedIndexStorage::finishRebalance);
                });
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

    private static String createPartitionDoesNotExistsErrorMessage(int partitionId) {
        return "Partition ID " + partitionId + " does not exist";
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
}
