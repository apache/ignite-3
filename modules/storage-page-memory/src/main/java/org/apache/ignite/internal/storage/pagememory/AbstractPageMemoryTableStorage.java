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
import static java.util.function.Function.identity;
import static org.apache.ignite.internal.storage.MvPartitionStorage.REBALANCE_IN_PROGRESS;
import static org.apache.ignite.internal.storage.util.StorageUtils.createMissingMvPartitionErrorMessage;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionDependingOnStorageState;
import static org.apache.ignite.internal.storage.util.StorageUtils.transitionToClosedState;
import static org.apache.ignite.internal.storage.util.StorageUtils.transitionToDestroyedState;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.pagememory.DataRegion;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.freelist.FreeList;
import org.apache.ignite.internal.pagememory.reuse.ReuseList;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.util.GradualTaskExecutor.GradualTaskCancellationException;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageDestroyedException;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.MvPartitionMeta;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptorSupplier;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.storage.lease.LeaseInfo;
import org.apache.ignite.internal.storage.pagememory.mv.AbstractPageMemoryMvPartitionStorage;
import org.apache.ignite.internal.storage.util.MvPartitionStorages;
import org.apache.ignite.internal.storage.util.StorageState;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract table storage implementation based on {@link PageMemory}.
 */
public abstract class AbstractPageMemoryTableStorage<T extends AbstractPageMemoryMvPartitionStorage> implements MvTableStorage {
    private static final IgniteLogger LOG = Loggers.forClass(AbstractPageMemoryTableStorage.class);

    final MvPartitionStorages<T> mvPartitionStorages;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicReference<StorageState> state = new AtomicReference<>(StorageState.RUNNABLE);

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

    /** Returns storage engine for the table. */
    public abstract AbstractPageMemoryStorageEngine engine();

    @Override
    public CompletableFuture<Void> destroy() {
        if (!transitionToDestroyedState(state)) {
            return nullCompletedFuture();
        }

        busyLock.block();

        beforeCloseOrDestroy();

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
    public abstract T createMvPartitionStorage(int partitionId) throws StorageException;

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
            T partition = createMvPartitionStorage(partitionId);

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
        if (!busyLock.enterBusy()) {
            return nullCompletedFuture();
        }

        try {
            return mvPartitionStorages.destroy(partitionId, this::destroyMvPartitionStorage);
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public void createSortedIndex(int partitionId, StorageSortedIndexDescriptor indexDescriptor) {
        busy(() -> {
            AbstractPageMemoryMvPartitionStorage partitionStorage = mvPartitionStorages.get(partitionId);

            // TODO: IGNITE-24926 - throw StorageException if partitionStorage is absent.
            if (partitionStorage != null) {
                partitionStorage.createSortedIndex(indexDescriptor);
            }
        });
    }

    @Override
    public void createHashIndex(int partitionId, StorageHashIndexDescriptor indexDescriptor) {
        busy(() -> {
            AbstractPageMemoryMvPartitionStorage partitionStorage = mvPartitionStorages.get(partitionId);

            // TODO: IGNITE-24926 - throw StorageException if partitionStorage is absent.
            if (partitionStorage != null) {
                partitionStorage.createHashIndex(indexDescriptor);
            }
        });
    }

    @Override
    public CompletableFuture<Void> destroyIndex(int indexId) {
        if (!busyLock.enterBusy()) {
            return nullCompletedFuture();
        }

        try {
            List<T> storages = mvPartitionStorages.getAll();

            var destroyFutures = new CompletableFuture[storages.size()];

            for (int i = 0; i < storages.size(); i++) {
                AbstractPageMemoryMvPartitionStorage storage = storages.get(i);

                try {
                    destroyFutures[i] = storage.runConsistently(locker -> storage.destroyIndex(indexId))
                            .handle((res, ex) -> {
                                if (ex != null && isIgnorableIndexDestructionException(ex)) {
                                    return CompletableFutures.<Void>nullCompletedFuture();
                                }
                                return CompletableFutures.completedOrFailedFuture(res, ex);
                            })
                            .thenCompose(identity());
                } catch (StorageDestroyedException e) {
                    destroyFutures[i] = nullCompletedFuture();
                }
            }

            return allOf(destroyFutures);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private static boolean isIgnorableIndexDestructionException(Throwable ex) {
        Throwable cause = ExceptionUtils.unwrapCause(ex);

        // Ignore StorageDestroyedException as all we want is to destroy the storage.
        return cause instanceof StorageDestroyedException
                // Also ignore GradualTaskCancellationException as it means that either the partition has been destroyed
                // or that the node is being stopped (and we cannot proceed; destruction will be reattempted after restart).
                || cause instanceof GradualTaskCancellationException;
    }

    @Override
    public void close() throws StorageException {
        if (!transitionToClosedState(state, this::createStorageInfo)) {
            return;
        }

        busyLock.block();

        beforeCloseOrDestroy();

        try {
            CompletableFuture<List<T>> allForCloseOrDestroy = mvPartitionStorages.getAllForCloseOrDestroy();

            // 10 seconds is taken by analogy with shutdown of thread pool, in general this should be fairly fast.
            IgniteUtils.closeAllManually(allForCloseOrDestroy.get(10, TimeUnit.SECONDS).stream());
        } catch (Exception e) {
            throw new StorageException("Failed to stop PageMemory table storage: " + getTableId(), e);
        }
    }

    private String createStorageInfo() {
        return IgniteStringFormatter.format("tableId={}", getTableId());
    }

    private <V> V busy(Supplier<V> supplier) {
        if (!busyLock.enterBusy()) {
            throwExceptionDependingOnStorageState(state.get(), createStorageInfo());
        }

        try {
            return supplier.get();
        } finally {
            busyLock.leaveBusy();
        }
    }

    private void busy(Runnable action) {
        if (!busyLock.enterBusy()) {
            throwExceptionDependingOnStorageState(state.get(), createStorageInfo());
        }

        try {
            action.run();
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public CompletableFuture<Void> startRebalancePartition(int partitionId) {
        return busy(() -> mvPartitionStorages.startRebalance(partitionId, mvPartitionStorage -> {
            LOG.info("Starting rebalance for partition [tableId={}, partitionId={}]", getTableId(), partitionId);

            mvPartitionStorage.startRebalance();

            return clearStorageAndUpdateDataStructures(
                    mvPartitionStorage,
                    () -> mvPartitionStorage.runConsistently(locker -> {
                        mvPartitionStorage.lastAppliedOnRebalance(REBALANCE_IN_PROGRESS, REBALANCE_IN_PROGRESS);

                        return null;
                    })
            );
        }));
    }

    @Override
    public CompletableFuture<Void> abortRebalancePartition(int partitionId) {
        return busy(() -> mvPartitionStorages.abortRebalance(partitionId, mvPartitionStorage -> {
            mvPartitionStorage.startAbortRebalance();

            return clearStorageAndUpdateDataStructures(
                    mvPartitionStorage,
                    () -> {
                        mvPartitionStorage.runConsistently(locker -> {
                            mvPartitionStorage.lastAppliedOnRebalance(0, 0);

                            return null;
                        });

                        mvPartitionStorage.completeRebalance();
                    }
            );
        }));
    }

    @Override
    public CompletableFuture<Void> finishRebalancePartition(int partitionId, MvPartitionMeta partitionMeta) {
        return busy(() -> mvPartitionStorages.finishRebalance(partitionId, mvPartitionStorage -> {
            mvPartitionStorage.runConsistently(locker -> {
                mvPartitionStorage.lastAppliedOnRebalance(partitionMeta.lastAppliedIndex(), partitionMeta.lastAppliedTerm());
                mvPartitionStorage.committedGroupConfigurationOnRebalance(partitionMeta.groupConfig());

                LeaseInfo leaseInfo = partitionMeta.leaseInfo();

                if (leaseInfo != null) {
                    mvPartitionStorage.updateLeaseOnRebalance(leaseInfo);
                }

                return null;
            });

            mvPartitionStorage.completeRebalance();

            return nullCompletedFuture();
        }));
    }

    @Override
    public CompletableFuture<Void> clearPartition(int partitionId) {
        return busy(() -> mvPartitionStorages.clear(partitionId, mvPartitionStorage -> {
            try {
                mvPartitionStorage.startCleanup();

                return clearStorageAndUpdateDataStructures(mvPartitionStorage, () -> {})
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
     * @param mvPartitionStorage Storage to be cleared.
     * @param afterUpdateStructuresCallback Callback to be invoked after updating internal structures.
     * @return Future of the operation.
     */
    abstract CompletableFuture<Void> clearStorageAndUpdateDataStructures(
            AbstractPageMemoryMvPartitionStorage mvPartitionStorage,
            Runnable afterUpdateStructuresCallback
    );

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

    protected void beforeCloseOrDestroy() {
        // No-op.
    }
}
