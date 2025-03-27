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
import static org.apache.ignite.internal.storage.util.StorageUtils.createMissingMvPartitionErrorMessage;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.closeAllManually;
import static org.mockito.Mockito.spy;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.MvPartitionMeta;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.impl.AbstractTestIndexStorage;
import org.apache.ignite.internal.storage.index.impl.TestHashIndexStorage;
import org.apache.ignite.internal.storage.index.impl.TestSortedIndexStorage;
import org.apache.ignite.internal.storage.lease.LeaseInfo;
import org.apache.ignite.internal.storage.util.MvPartitionStorages;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Test table storage implementation.
 */
public class TestMvTableStorage implements MvTableStorage {
    private final MvPartitionStorages<TestMvPartitionStorage> mvPartitionStorages;

    private final Map<Integer, SortedIndices> sortedIndicesById = new ConcurrentHashMap<>();

    private final Map<Integer, HashIndices> hashIndicesById = new ConcurrentHashMap<>();

    private final StorageTableDescriptor tableDescriptor;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Class for storing Sorted Indices for a particular partition.
     */
    private static class SortedIndices {
        private final StorageSortedIndexDescriptor descriptor;

        final Map<Integer, TestSortedIndexStorage> storageByPartitionId = new ConcurrentHashMap<>();

        SortedIndices(StorageSortedIndexDescriptor descriptor) {
            this.descriptor = descriptor;
        }

        TestSortedIndexStorage getOrCreateStorage(Integer partitionId) {
            return storageByPartitionId.computeIfAbsent(partitionId, id -> spy(new TestSortedIndexStorage(id, descriptor)));
        }
    }

    /**
     * Class for storing Hash Indices for a particular partition.
     */
    private static class HashIndices {
        private final StorageHashIndexDescriptor descriptor;

        final Map<Integer, TestHashIndexStorage> storageByPartitionId = new ConcurrentHashMap<>();

        HashIndices(StorageHashIndexDescriptor descriptor) {
            this.descriptor = descriptor;
        }

        TestHashIndexStorage getOrCreateStorage(Integer partitionId) {
            return storageByPartitionId.computeIfAbsent(partitionId, id -> spy(new TestHashIndexStorage(id, descriptor)));
        }
    }

    /**
     * Constructor.
     *
     * @param tableId Table ID.
     * @param partitions Count of partitions.
     */
    public TestMvTableStorage(int tableId, int partitions) {
        this(new StorageTableDescriptor(tableId, partitions, "none"));
    }

    /**
     * Constructor.
     *
     * @param tableDescriptor Table descriptor.
     */
    public TestMvTableStorage(StorageTableDescriptor tableDescriptor) {
        this.tableDescriptor = tableDescriptor;

        mvPartitionStorages = new MvPartitionStorages<>(tableDescriptor.getId(), tableDescriptor.getPartitions());
    }

    private <T> T inBusyLock(Supplier<T> fn) {
        return IgniteUtils.inBusyLock(busyLock, fn);
    }

    @Override
    public CompletableFuture<MvPartitionStorage> createMvPartition(int partitionId) {
        return inBusyLock(() -> mvPartitionStorages.create(partitionId, partId -> spy(new TestMvPartitionStorage(partId))));
    }

    @Override
    public @Nullable MvPartitionStorage getMvPartition(int partitionId) {
        return inBusyLock(() -> mvPartitionStorages.get(partitionId));
    }

    @Override
    public CompletableFuture<Void> destroyPartition(int partitionId) {
        if (!busyLock.enterBusy()) {
            return nullCompletedFuture();
        }

        try {
            return mvPartitionStorages.destroy(partitionId, this::destroyPartition);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private CompletableFuture<Void> destroyPartition(TestMvPartitionStorage mvPartitionStorage) {
        mvPartitionStorage.destroy();

        for (HashIndices hashIndices : hashIndicesById.values()) {
            TestHashIndexStorage removedHashIndexStorage = hashIndices.storageByPartitionId.remove(mvPartitionStorage.partitionId);

            if (removedHashIndexStorage != null) {
                removedHashIndexStorage.destroy();
            }
        }

        for (SortedIndices sortedIndices : sortedIndicesById.values()) {
            TestSortedIndexStorage removedSortedIndexStorage = sortedIndices.storageByPartitionId.remove(mvPartitionStorage.partitionId);

            if (removedSortedIndexStorage != null) {
                removedSortedIndexStorage.destroy();
            }
        }

        return nullCompletedFuture();
    }

    @Override
    public void createSortedIndex(int partitionId, StorageSortedIndexDescriptor indexDescriptor) {
        inBusyLock(() -> getOrCreateSortedIndexBusy(partitionId, indexDescriptor));
    }

    private TestSortedIndexStorage getOrCreateSortedIndexBusy(int partitionId, StorageSortedIndexDescriptor indexDescriptor) {
        TestMvPartitionStorage mvPartitionStorage = mvPartitionStorages.get(partitionId);

        if (mvPartitionStorage == null) {
            throw new StorageException(createMissingMvPartitionErrorMessage(partitionId));
        }

        SortedIndices sortedIndices = sortedIndicesById.computeIfAbsent(
                indexDescriptor.id(),
                id -> new SortedIndices(indexDescriptor)
        );

        return sortedIndices.getOrCreateStorage(partitionId);
    }

    @Override
    public void createHashIndex(int partitionId, StorageHashIndexDescriptor indexDescriptor) {
        inBusyLock(() -> getOrCreateHashIndexBusy(partitionId, indexDescriptor));
    }

    private TestHashIndexStorage getOrCreateHashIndexBusy(int partitionId, StorageHashIndexDescriptor indexDescriptor) {
        TestMvPartitionStorage mvPartitionStorage = mvPartitionStorages.get(partitionId);

        if (mvPartitionStorage == null) {
            throw new StorageException(createMissingMvPartitionErrorMessage(partitionId));
        }

        HashIndices sortedIndices = hashIndicesById.computeIfAbsent(
                indexDescriptor.id(),
                id -> new HashIndices(indexDescriptor)
        );

        return sortedIndices.getOrCreateStorage(partitionId);
    }

    @Override
    public CompletableFuture<Void> destroyIndex(int indexId) {
        if (!busyLock.enterBusy()) {
            return nullCompletedFuture();
        }

        try {
            return destroyIndexBusy(indexId);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private CompletableFuture<Void> destroyIndexBusy(int indexId) {
        HashIndices hashIndices = hashIndicesById.remove(indexId);

        if (hashIndices != null) {
            hashIndices.storageByPartitionId.values().forEach(TestHashIndexStorage::destroy);
        }

        SortedIndices sortedIndices = sortedIndicesById.remove(indexId);

        if (sortedIndices != null) {
            sortedIndices.storageByPartitionId.values().forEach(TestSortedIndexStorage::destroy);
        }

        return nullCompletedFuture();
    }

    @Override
    public boolean isVolatile() {
        return true;
    }

    @Override
    public void close() throws StorageException {
        if (!blockBusyLockIfNotBlocked()) {
            return;
        }

        try {
            closeAllManually(mvPartitionStorages.getAllForCloseOrDestroy().get(10, TimeUnit.SECONDS));
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    private boolean blockBusyLockIfNotBlocked() {
        if (!closed.compareAndSet(false, true)) {
            return false;
        }

        busyLock.block();

        return true;
    }

    @Override
    public CompletableFuture<Void> destroy() {
        if (!blockBusyLockIfNotBlocked()) {
            return nullCompletedFuture();
        }

        return mvPartitionStorages.getAllForCloseOrDestroy()
                .thenCompose(mvStorages -> allOf(mvStorages.stream().map(this::destroyPartition).toArray(CompletableFuture[]::new)));
    }

    @Override
    public CompletableFuture<Void> startRebalancePartition(int partitionId) {
        return inBusyLock(() -> startRebalancePartitionBusy(partitionId));
    }

    private CompletableFuture<Void> startRebalancePartitionBusy(int partitionId) {
        return mvPartitionStorages.startRebalance(partitionId, mvPartitionStorage -> {
            mvPartitionStorage.startRebalance();

            testHashIndexStorageStream(partitionId).forEach(TestHashIndexStorage::startRebalance);

            testSortedIndexStorageStream(partitionId).forEach(TestSortedIndexStorage::startRebalance);

            return nullCompletedFuture();
        });
    }

    @Override
    public CompletableFuture<Void> abortRebalancePartition(int partitionId) {
        return inBusyLock(() -> abortRebalancePartitionBusy(partitionId));
    }

    private CompletableFuture<Void> abortRebalancePartitionBusy(int partitionId) {
        return mvPartitionStorages.abortRebalance(partitionId, mvPartitionStorage -> {
            mvPartitionStorage.abortRebalance();

            testHashIndexStorageStream(partitionId).forEach(TestHashIndexStorage::abortRebalance);

            testSortedIndexStorageStream(partitionId).forEach(TestSortedIndexStorage::abortRebalance);

            return nullCompletedFuture();
        });
    }

    @Override
    public CompletableFuture<Void> finishRebalancePartition(int partitionId, MvPartitionMeta partitionMeta) {
        return inBusyLock(() -> finishRebalancePartitionBusy(partitionId, partitionMeta));
    }

    private CompletableFuture<Void> finishRebalancePartitionBusy(int partitionId, MvPartitionMeta partitionMeta) {
        return mvPartitionStorages.finishRebalance(partitionId, mvPartitionStorage -> {
            mvPartitionStorage.finishRebalance(partitionMeta);

            LeaseInfo leaseInfo = partitionMeta.leaseInfo();

            if (leaseInfo != null) {
                mvPartitionStorage.updateLease(leaseInfo);
            }

            testHashIndexStorageStream(partitionId).forEach(TestHashIndexStorage::finishRebalance);

            testSortedIndexStorageStream(partitionId).forEach(TestSortedIndexStorage::finishRebalance);

            return nullCompletedFuture();
        });
    }

    @Override
    public CompletableFuture<Void> clearPartition(int partitionId) {
        return inBusyLock(() -> clearPartitionBusy(partitionId));
    }

    private CompletableFuture<Void> clearPartitionBusy(int partitionId) {
        return mvPartitionStorages.clear(partitionId, mvPartitionStorage -> {
            mvPartitionStorage.clear();

            testHashIndexStorageStream(partitionId).forEach(TestHashIndexStorage::clear);
            testSortedIndexStorageStream(partitionId).forEach(TestSortedIndexStorage::clear);

            return nullCompletedFuture();
        });
    }

    @Override
    public @Nullable IndexStorage getIndex(int partitionId, int indexId) {
        return inBusyLock(() -> getIndexBusy(partitionId, indexId));
    }

    private @Nullable AbstractTestIndexStorage getIndexBusy(int partitionId, int indexId) {
        if (mvPartitionStorages.get(partitionId) == null) {
            throw new StorageException(createMissingMvPartitionErrorMessage(partitionId));
        }

        HashIndices hashIndices = hashIndicesById.get(indexId);

        if (hashIndices != null) {
            return hashIndices.storageByPartitionId.get(partitionId);
        }

        SortedIndices sortedIndices = sortedIndicesById.get(indexId);

        if (sortedIndices != null) {
            return sortedIndices.storageByPartitionId.get(partitionId);
        }

        return null;
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

    @Override
    public StorageTableDescriptor getTableDescriptor() {
        return tableDescriptor;
    }
}
