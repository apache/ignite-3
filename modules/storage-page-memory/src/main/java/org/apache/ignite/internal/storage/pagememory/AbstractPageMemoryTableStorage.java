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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.storage.MvPartitionStorage.REBALANCE_IN_PROGRESS;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Function;
import org.apache.ignite.internal.pagememory.DataRegion;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.freelist.FreeList;
import org.apache.ignite.internal.pagememory.reuse.ReuseList;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.schema.configuration.TableView;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.StorageRebalanceException;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.pagememory.mv.AbstractPageMemoryMvPartitionStorage;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract table storage implementation based on {@link PageMemory}.
 */
public abstract class AbstractPageMemoryTableStorage implements MvTableStorage {
    protected final TableConfiguration tableConfig;

    protected final TablesConfiguration tablesConfig;

    protected volatile AtomicReferenceArray<AbstractPageMemoryMvPartitionStorage> mvPartitions;

    protected final ConcurrentMap<Integer, CompletableFuture<Void>> destroyFutureByPartitionId = new ConcurrentHashMap<>();

    protected final ConcurrentMap<Integer, CompletableFuture<Void>> rebalanceFutureByPartitionId = new ConcurrentHashMap<>();

    /** Busy lock to stop synchronously. */
    protected final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** To avoid double closure. */
    @SuppressWarnings("unused")
    protected final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Constructor.
     *
     * @param tableConfig Table configuration.
     * @param tablesConfig Tables configuration.
     */
    protected AbstractPageMemoryTableStorage(TableConfiguration tableConfig, TablesConfiguration tablesConfig) {
        this.tableConfig = tableConfig;
        this.tablesConfig = tablesConfig;
    }

    @Override
    public TableConfiguration configuration() {
        return tableConfig;
    }

    @Override
    public TablesConfiguration tablesConfiguration() {
        return tablesConfig;
    }

    /**
     * Returns a data region instance for the table.
     */
    public abstract DataRegion<?> dataRegion();

    @Override
    public void start() throws StorageException {
        inBusyLock(busyLock, () -> {
            TableView tableView = tableConfig.value();

            mvPartitions = new AtomicReferenceArray<>(tableView.partitions());
        });
    }

    @Override
    public void stop() throws StorageException {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        List<AutoCloseable> closeables = new ArrayList<>();

        for (int i = 0; i < mvPartitions.length(); i++) {
            AbstractPageMemoryMvPartitionStorage partition = mvPartitions.getAndUpdate(i, p -> null);

            if (partition != null) {
                closeables.add(partition::close);
            }
        }

        try {
            IgniteUtils.closeAll(closeables);
        } catch (Exception e) {
            throw new StorageException("Failed to stop PageMemory table storage: " + getTableName(), e);
        }
    }

    @Override
    public CompletableFuture<Void> destroy() {
        if (!closed.compareAndSet(false, true)) {
            return completedFuture(null);
        }

        busyLock.block();

        List<CompletableFuture<Void>> destroyFutures = new ArrayList<>();

        for (int i = 0; i < mvPartitions.length(); i++) {
            CompletableFuture<Void> destroyPartitionFuture = destroyFutureByPartitionId.get(i);

            if (destroyPartitionFuture != null) {
                destroyFutures.add(destroyPartitionFuture);
            } else {
                AbstractPageMemoryMvPartitionStorage partition = mvPartitions.getAndUpdate(i, p -> null);

                if (partition != null) {
                    destroyFutures.add(destroyMvPartitionStorage(partition));
                }
            }
        }

        if (destroyFutures.isEmpty()) {
            finishDestruction();

            return completedFuture(null);
        } else {
            return CompletableFuture.allOf(destroyFutures.toArray(CompletableFuture[]::new))
                    .whenComplete((unused, throwable) -> {
                        if (throwable == null) {
                            finishDestruction();
                        }
                    });
        }
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
    public AbstractPageMemoryMvPartitionStorage getOrCreateMvPartition(int partitionId) throws StorageException {
        return inBusyLock(busyLock, () -> {
            AbstractPageMemoryMvPartitionStorage partition = getMvPartitionStorageWithoutBusyLock(partitionId);

            if (partition != null) {
                return partition;
            }

            partition = createMvPartitionStorage(partitionId);

            partition.start();

            mvPartitions.set(partitionId, partition);

            return partition;
        });
    }

    @Override
    public @Nullable AbstractPageMemoryMvPartitionStorage getMvPartition(int partitionId) {
        return inBusyLock(busyLock, () -> getMvPartitionStorageWithoutBusyLock(partitionId));
    }

    @Override
    public CompletableFuture<Void> destroyPartition(int partitionId) {
        return inBusyLock(busyLock, () -> {
            checkPartitionId(partitionId);

            assert !rebalanceFutureByPartitionId.containsKey(partitionId)
                    : IgniteStringFormatter.format("table={}, partitionId={}", getTableName(), partitionId);

            CompletableFuture<Void> destroyPartitionFuture = new CompletableFuture<>();

            CompletableFuture<Void> previousDestroyPartitionFuture = destroyFutureByPartitionId.putIfAbsent(
                    partitionId,
                    destroyPartitionFuture
            );

            if (previousDestroyPartitionFuture != null) {
                return previousDestroyPartitionFuture;
            }

            MvPartitionStorage partition = mvPartitions.getAndSet(partitionId, null);

            if (partition != null) {
                destroyMvPartitionStorage((AbstractPageMemoryMvPartitionStorage) partition).whenComplete((unused, throwable) -> {
                    destroyFutureByPartitionId.remove(partitionId);

                    if (throwable != null) {
                        destroyPartitionFuture.completeExceptionally(throwable);
                    } else {
                        destroyPartitionFuture.complete(null);
                    }
                });
            } else {
                destroyFutureByPartitionId.remove(partitionId).complete(null);
            }

            return destroyPartitionFuture;
        });
    }

    @Override
    public SortedIndexStorage getOrCreateSortedIndex(int partitionId, UUID indexId) {
        return inBusyLock(busyLock, () ->
                getMvPartitionStorageWithoutBusyLock(partitionId, StorageException::new).getOrCreateSortedIndex(indexId)
        );
    }

    @Override
    public HashIndexStorage getOrCreateHashIndex(int partitionId, UUID indexId) {
        return inBusyLock(busyLock, () ->
                getMvPartitionStorageWithoutBusyLock(partitionId, StorageException::new).getOrCreateHashIndex(indexId)
        );
    }

    @Override
    public CompletableFuture<Void> destroyIndex(UUID indexId) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void close() throws StorageException {
        stop();
    }

    @Override
    public CompletableFuture<Void> startRebalancePartition(int partitionId) {
        return inBusyLock(busyLock, () -> {
            AbstractPageMemoryMvPartitionStorage mvPartitionStorage = getMvPartitionStorageWithoutBusyLock(
                    partitionId,
                    StorageRebalanceException::new
            );

            assert !destroyFutureByPartitionId.containsKey(partitionId) : mvPartitionStorage.createStorageInfo();

            mvPartitionStorage.startRebalance();

            CompletableFuture<Void> rebalanceFuture = clearStorageAndUpdateDataStructures(mvPartitionStorage)
                    .thenAccept(unused ->
                            mvPartitionStorage.runConsistently(() -> {
                                mvPartitionStorage.lastAppliedOnRebalance(REBALANCE_IN_PROGRESS, REBALANCE_IN_PROGRESS);

                                return null;
                            })
                    );

            CompletableFuture<Void> previousRebalanceFuture = rebalanceFutureByPartitionId.putIfAbsent(partitionId, rebalanceFuture);

            assert previousRebalanceFuture == null : mvPartitionStorage.createStorageInfo();

            return rebalanceFuture;
        });
    }

    @Override
    public CompletableFuture<Void> abortRebalancePartition(int partitionId) {
        return inBusyLock(busyLock, () -> {
            AbstractPageMemoryMvPartitionStorage mvPartitionStorage = getMvPartitionStorageWithoutBusyLock(
                    partitionId,
                    StorageRebalanceException::new
            );

            CompletableFuture<Void> rebalanceFuture = rebalanceFutureByPartitionId.remove(partitionId);

            if (rebalanceFuture == null) {
                return completedFuture(null);
            }

            return rebalanceFuture
                    .thenCompose(unused -> clearStorageAndUpdateDataStructures(mvPartitionStorage))
                    .thenAccept(unused -> {
                        mvPartitionStorage.runConsistently(() -> {
                            mvPartitionStorage.lastAppliedOnRebalance(0, 0);

                            return null;
                        });

                        mvPartitionStorage.completeRebalance();
                    });
        });
    }

    @Override
    public CompletableFuture<Void> finishRebalancePartition(int partitionId, long lastAppliedIndex, long lastAppliedTerm) {
        return inBusyLock(busyLock, () -> {
            AbstractPageMemoryMvPartitionStorage mvPartitionStorage = getMvPartitionStorageWithoutBusyLock(
                    partitionId,
                    StorageRebalanceException::new
            );

            CompletableFuture<Void> rebalanceFuture = rebalanceFutureByPartitionId.remove(partitionId);

            if (rebalanceFuture == null) {
                throw new StorageRebalanceException("Rebalance for partition did not start: " + mvPartitionStorage.createStorageInfo());
            }

            return rebalanceFuture.thenAccept(unused -> {
                mvPartitionStorage.runConsistently(() -> {
                    mvPartitionStorage.lastAppliedOnRebalance(lastAppliedIndex, lastAppliedTerm);

                    return null;
                });

                mvPartitionStorage.completeRebalance();
            });
        });
    }

    /**
     * Clears the partition multi-version storage and all its indexes, updates their internal data structures such as {@link BplusTree},
     * {@link FreeList} and {@link ReuseList}.
     *
     * @return Future of the operation.
     */
    abstract CompletableFuture<Void> clearStorageAndUpdateDataStructures(AbstractPageMemoryMvPartitionStorage mvPartitionStorage);

    /**
     * Checks that the partition ID is within the scope of the configuration.
     *
     * @param partitionId Partition ID.
     */
    private void checkPartitionId(int partitionId) {
        int partitions = mvPartitions.length();

        if (partitionId < 0 || partitionId >= partitions) {
            throw new IllegalArgumentException(IgniteStringFormatter.format(
                    "Unable to access partition with id outside of configured range: [table={}, partitionId={}, partitions={}]",
                    getTableName(),
                    partitionId,
                    partitions
            ));
        }
    }

    /**
     * Returns multi-versioned partition storage without using {@link #busyLock}.
     *
     * @param partitionId Partition ID.
     * @return {@code Null} if there is no storage.
     */
    @Nullable
    AbstractPageMemoryMvPartitionStorage getMvPartitionStorageWithoutBusyLock(int partitionId) {
        checkPartitionId(partitionId);

        return mvPartitions.get(partitionId);
    }

    /**
     * Returns multi-versioned partition storage, if it doesn't exist it will throw an exception from the
     * {@code missingStorageExceptionFunction}, without using {@link #busyLock}.
     *
     * @param partitionId Partition ID.
     * @param missingStorageExceptionFunction Function to create an exception if the store is missing.
     */
    AbstractPageMemoryMvPartitionStorage getMvPartitionStorageWithoutBusyLock(
            int partitionId,
            Function<String, ? extends StorageException> missingStorageExceptionFunction
    ) {
        AbstractPageMemoryMvPartitionStorage mvPartitionStorage = getMvPartitionStorageWithoutBusyLock(partitionId);

        if (mvPartitionStorage == null) {
            throw missingStorageExceptionFunction.apply(IgniteStringFormatter.format("Partition ID {} does not exist", partitionId));
        }

        return mvPartitionStorage;
    }

    /**
     * Returns table name.
     */
    public String getTableName() {
        return tableConfig.name().value();
    }
}
