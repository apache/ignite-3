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

package org.apache.ignite.internal.storage.pagememory;

import static java.util.concurrent.CompletableFuture.allOf;
import static org.apache.ignite.internal.storage.MvPartitionStorage.REBALANCE_IN_PROGRESS;
import static org.apache.ignite.internal.storage.util.StorageUtils.createMissingMvPartitionErrorMessage;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.ignite.internal.pagememory.DataRegion;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.freelist.FreeList;
import org.apache.ignite.internal.pagememory.reuse.ReuseList;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptorSupplier;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.storage.pagememory.mv.AbstractPageMemoryMvPartitionStorage;
import org.apache.ignite.internal.storage.util.MvPartitionStorages;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract table storage implementation based on {@link PageMemory}.
 */
public abstract class AbstractPageMemoryTableStorage implements MvTableStorage {
    private final MvPartitionStorages<AbstractPageMemoryMvPartitionStorage> mvPartitionStorages;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping of the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Table descriptor. */
    private final StorageTableDescriptor tableDescriptor;

    /** Index descriptor supplier. */
    private final StorageIndexDescriptorSupplier indexDescriptorSupplier;

    /**
     * Constructor.
     *
     * @param tableDescriptor Table descriptor.
     * @param indexDescriptorSupplier Index descriptor supplier.
     */
    AbstractPageMemoryTableStorage(
            StorageTableDescriptor tableDescriptor,
            StorageIndexDescriptorSupplier indexDescriptorSupplier
    ) {
        this.tableDescriptor = tableDescriptor;
        this.indexDescriptorSupplier = indexDescriptorSupplier;
        this.mvPartitionStorages = new MvPartitionStorages<>(tableDescriptor.getId(), tableDescriptor.getPartitions());
    }

    /**
     * Returns index descriptor supplier.
     */
    public StorageIndexDescriptorSupplier getIndexDescriptorSupplier() {
        return indexDescriptorSupplier;
    }

    /**
     * Returns a data region instance for the table.
     */
    public abstract DataRegion<?> dataRegion();

    @Override
    public CompletableFuture<Void> destroy() {
        if (!stopGuard.compareAndSet(false, true)) {
            return nullCompletedFuture();
        }

        busyLock.block();

        return mvPartitionStorages.getAllForCloseOrDestroy()
                .thenCompose(storages -> allOf(storages.stream().map(this::destroyMvPartitionStorage).toArray(CompletableFuture[]::new)))
                .whenComplete((unused, throwable) -> {
                    if (throwable == null) {
                        finishDestruction();
                    }
                });
    }

    /**
     * Executes actions needed to finish destruction after the table storage has been stopped and all partitions destroyed.
     */
    protected abstract void finishDestruction();

    /**
     * Returns a new instance of {@link AbstractPageMemoryMvPartitionStorage}.
     *
     * @param partitionId Partition ID.
     * @throws StorageException If there is an error while creating the mv partition storage.
     */
    public abstract AbstractPageMemoryMvPartitionStorage createMvPartitionStorage(int partitionId) throws StorageException;

    /**
     * Destroys the partition multi-version storage and all its indexes.
     *
     * @param mvPartitionStorage Multi-versioned partition storage.
     * @return Future that will complete when the partition multi-version storage and its indexes are destroyed.
     */
    abstract CompletableFuture<Void> destroyMvPartitionStorage(AbstractPageMemoryMvPartitionStorage mvPartitionStorage);

    @Override
    public CompletableFuture<MvPartitionStorage> createMvPartition(int partitionId) {
        return busy(() -> mvPartitionStorages.create(partitionId, partId -> {
            AbstractPageMemoryMvPartitionStorage partition = createMvPartitionStorage(partitionId);

            partition.start();

            return partition;
        }));
    }

    @Override
    public @Nullable MvPartitionStorage getMvPartition(int partitionId) {
        return busy(() -> mvPartitionStorages.get(partitionId));
    }

    @Override
    public CompletableFuture<Void> destroyPartition(int partitionId) {
        return busy(() -> mvPartitionStorages.destroy(partitionId, this::destroyMvPartitionStorage));
    }

    @Override
    public SortedIndexStorage getOrCreateSortedIndex(int partitionId, StorageSortedIndexDescriptor indexDescriptor) {
        return busy(() -> {
            AbstractPageMemoryMvPartitionStorage partitionStorage = mvPartitionStorages.get(partitionId);

            if (partitionStorage == null) {
                throw new StorageException(createMissingMvPartitionErrorMessage(partitionId));
            }

            return partitionStorage.getOrCreateSortedIndex(indexDescriptor);
        });
    }

    @Override
    public HashIndexStorage getOrCreateHashIndex(int partitionId, StorageHashIndexDescriptor indexDescriptor) {
        return busy(() -> {
            AbstractPageMemoryMvPartitionStorage partitionStorage = mvPartitionStorages.get(partitionId);

            if (partitionStorage == null) {
                throw new StorageException(createMissingMvPartitionErrorMessage(partitionId));
            }

            return partitionStorage.getOrCreateHashIndex(indexDescriptor);
        });
    }

    @Override
    public CompletableFuture<Void> destroyIndex(int indexId) {
        if (!busyLock.enterBusy()) {
            return nullCompletedFuture();
        }

        try {
            List<AbstractPageMemoryMvPartitionStorage> storages = mvPartitionStorages.getAll();

            var destroyFutures = new CompletableFuture[storages.size()];

            for (int i = 0; i < storages.size(); i++) {
                AbstractPageMemoryMvPartitionStorage storage = storages.get(i);

                destroyFutures[i] = storage.runConsistently(locker -> storage.destroyIndex(indexId));
            }

            return allOf(destroyFutures);
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public void close() throws StorageException {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        try {
            CompletableFuture<List<AbstractPageMemoryMvPartitionStorage>> allForCloseOrDestroy
                    = mvPartitionStorages.getAllForCloseOrDestroy();

            // 10 seconds is taken by analogy with shutdown of thread pool, in general this should be fairly fast.
            IgniteUtils.closeAllManually(allForCloseOrDestroy.get(10, TimeUnit.SECONDS).stream());
        } catch (Exception e) {
            throw new StorageException("Failed to stop PageMemory table storage: " + getTableId(), e);
        }
    }

    private <V> V busy(Supplier<V> supplier) {
        return inBusyLock(busyLock, supplier);
    }

    @Override
    public CompletableFuture<Void> startRebalancePartition(int partitionId) {
        return inBusyLock(busyLock, () -> mvPartitionStorages.startRebalance(partitionId, mvPartitionStorage -> {
            mvPartitionStorage.startRebalance();

            return clearStorageAndUpdateDataStructures(mvPartitionStorage)
                    .thenAccept(unused ->
                            mvPartitionStorage.runConsistently(locker -> {
                                mvPartitionStorage.lastAppliedOnRebalance(REBALANCE_IN_PROGRESS, REBALANCE_IN_PROGRESS);

                                return null;
                            })
                    );
        }));
    }

    @Override
    public CompletableFuture<Void> abortRebalancePartition(int partitionId) {
        return inBusyLock(busyLock, () -> mvPartitionStorages.abortRebalance(partitionId, mvPartitionStorage ->
                clearStorageAndUpdateDataStructures(mvPartitionStorage)
                        .thenAccept(unused -> {
                            mvPartitionStorage.runConsistently(locker -> {
                                mvPartitionStorage.lastAppliedOnRebalance(0, 0);

                                return null;
                            });

                            mvPartitionStorage.completeRebalance();
                        })
        ));
    }

    @Override
    public CompletableFuture<Void> finishRebalancePartition(
            int partitionId,
            long lastAppliedIndex,
            long lastAppliedTerm,
            byte[] groupConfig
    ) {
        return inBusyLock(busyLock, () -> mvPartitionStorages.finishRebalance(partitionId, mvPartitionStorage -> {
            mvPartitionStorage.runConsistently(locker -> {
                mvPartitionStorage.lastAppliedOnRebalance(lastAppliedIndex, lastAppliedTerm);

                mvPartitionStorage.committedGroupConfigurationOnRebalance(groupConfig);

                return null;
            });

            mvPartitionStorage.completeRebalance();

            return nullCompletedFuture();
        }));
    }

    @Override
    public CompletableFuture<Void> clearPartition(int partitionId) {
        return inBusyLock(busyLock, () -> mvPartitionStorages.clear(partitionId, mvPartitionStorage -> {
            try {
                mvPartitionStorage.startCleanup();

                return clearStorageAndUpdateDataStructures(mvPartitionStorage)
                        .whenComplete((unused, throwable) -> mvPartitionStorage.finishCleanup());
            } catch (StorageException e) {
                mvPartitionStorage.finishCleanup();

                throw e;
            } catch (Throwable t) {
                mvPartitionStorage.finishCleanup();

                throw new StorageException("Failed to cleanup storage: [{}]", t, mvPartitionStorage.createStorageInfo());
            }
        }));
    }

    /**
     * Clears the partition multi-version storage and all its indexes, updates their internal data structures such as {@link BplusTree},
     * {@link FreeList} and {@link ReuseList}.
     *
     * @return Future of the operation.
     */
    abstract CompletableFuture<Void> clearStorageAndUpdateDataStructures(AbstractPageMemoryMvPartitionStorage mvPartitionStorage);

    /**
     * Returns the table ID.
     */
    public int getTableId() {
        return tableDescriptor.getId();
    }

    @Override
    public @Nullable IndexStorage getIndex(int partitionId, int indexId) {
        return busy(() -> {
            AbstractPageMemoryMvPartitionStorage partitionStorage = mvPartitionStorages.get(partitionId);

            if (partitionStorage == null) {
                throw new StorageException(createMissingMvPartitionErrorMessage(partitionId));
            }

            return partitionStorage.getIndex(indexId);
        });
    }

    @Override
    public StorageTableDescriptor getTableDescriptor() {
        return tableDescriptor;
    }
}
