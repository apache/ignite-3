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
import static org.apache.ignite.internal.storage.util.StorageUtils.createMissingMvPartitionErrorMessage;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Supplier;
import org.apache.ignite.internal.pagememory.DataRegion;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.freelist.FreeList;
import org.apache.ignite.internal.pagememory.reuse.ReuseList;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.schema.configuration.TableView;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.storage.RaftGroupConfiguration;
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
    protected final TableConfiguration tableCfg;

    protected final TablesConfiguration tablesCfg;

    protected volatile AtomicReferenceArray<AbstractPageMemoryMvPartitionStorage> mvPartitions;

    protected final ConcurrentMap<Integer, CompletableFuture<Void>> destroyFutureByPartitionId = new ConcurrentHashMap<>();

    protected final ConcurrentMap<Integer, CompletableFuture<Void>> rebalanceFutureByPartitionId = new ConcurrentHashMap<>();

    /** Busy lock. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping of the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /**
     * Constructor.
     *
     * @param tableCfg Table configuration.
     * @param tablesCfg Tables configuration.
     */
    protected AbstractPageMemoryTableStorage(TableConfiguration tableCfg, TablesConfiguration tablesCfg) {
        this.tableCfg = tableCfg;
        this.tablesCfg = tablesCfg;
    }

    @Override
    public TableConfiguration configuration() {
        return tableCfg;
    }

    @Override
    public TablesConfiguration tablesConfiguration() {
        return tablesCfg;
    }

    /**
     * Returns a data region instance for the table.
     */
    public abstract DataRegion<?> dataRegion();

    @Override
    public void start() throws StorageException {
        busy(() -> {
            TableView tableView = tableCfg.value();

            mvPartitions = new AtomicReferenceArray<>(tableView.partitions());

            return null;
        });
    }

    @Override
    public void stop() throws StorageException {
        if (!stopGuard.compareAndSet(false, true)) {
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
        if (!stopGuard.compareAndSet(false, true)) {
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
        return busy(() -> {
            AbstractPageMemoryMvPartitionStorage partition = getMvPartitionBusy(partitionId);

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
        return busy(() -> getMvPartitionBusy(partitionId));
    }

    private @Nullable AbstractPageMemoryMvPartitionStorage getMvPartitionBusy(int partitionId) {
        checkPartitionId(partitionId);

        return mvPartitions.get(partitionId);
    }

    @Override
    public CompletableFuture<Void> destroyPartition(int partitionId) {
        return busy(() -> {
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

            AbstractPageMemoryMvPartitionStorage partition = mvPartitions.getAndSet(partitionId, null);

            if (partition != null) {
                destroyMvPartitionStorage(partition).whenComplete((unused, throwable) -> {
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
        return busy(() -> {
            AbstractPageMemoryMvPartitionStorage partitionStorage = getMvPartitionBusy(partitionId);

            if (partitionStorage == null) {
                throw new StorageException(createMissingMvPartitionErrorMessage(partitionId));
            }

            return partitionStorage.getOrCreateSortedIndex(indexId);
        });
    }

    @Override
    public HashIndexStorage getOrCreateHashIndex(int partitionId, UUID indexId) {
        return busy(() -> {
            AbstractPageMemoryMvPartitionStorage partitionStorage = getMvPartitionBusy(partitionId);

            if (partitionStorage == null) {
                throw new StorageException(createMissingMvPartitionErrorMessage(partitionId));
            }

            return partitionStorage.getOrCreateHashIndex(indexId);
        });
    }

    @Override
    public CompletableFuture<Void> destroyIndex(UUID indexId) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void close() throws StorageException {
        stop();
    }

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

    private <V> V busy(Supplier<V> supplier) {
        return inBusyLock(busyLock, supplier);
    }

    @Override
    public CompletableFuture<Void> startRebalancePartition(int partitionId) {
        return inBusyLock(busyLock, () -> {
            AbstractPageMemoryMvPartitionStorage mvPartitionStorage = getMvPartitionBusy(partitionId);

            if (mvPartitionStorage == null) {
                throw new StorageRebalanceException(createMissingMvPartitionErrorMessage(partitionId));
            }

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
            AbstractPageMemoryMvPartitionStorage mvPartitionStorage = getMvPartitionBusy(partitionId);

            if (mvPartitionStorage == null) {
                throw new StorageRebalanceException(createMissingMvPartitionErrorMessage(partitionId));
            }

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
    public CompletableFuture<Void> finishRebalancePartition(
            int partitionId,
            long lastAppliedIndex,
            long lastAppliedTerm,
            RaftGroupConfiguration raftGroupConfig
    ) {
        return inBusyLock(busyLock, () -> {
            AbstractPageMemoryMvPartitionStorage mvPartitionStorage = getMvPartitionBusy(partitionId);

            if (mvPartitionStorage == null) {
                throw new StorageRebalanceException(createMissingMvPartitionErrorMessage(partitionId));
            }

            CompletableFuture<Void> rebalanceFuture = rebalanceFutureByPartitionId.remove(partitionId);

            if (rebalanceFuture == null) {
                throw new StorageRebalanceException("Rebalance for partition did not start: " + mvPartitionStorage.createStorageInfo());
            }

            return rebalanceFuture.thenAccept(unused -> {
                mvPartitionStorage.runConsistently(() -> {
                    mvPartitionStorage.lastAppliedOnRebalance(lastAppliedIndex, lastAppliedTerm);

                    mvPartitionStorage.committedGroupConfigurationOnRebalance(raftGroupConfig);

                    return null;
                });

                mvPartitionStorage.completeRebalance();
            });
        });
    }

    @Override
    public CompletableFuture<Void> clearPartition(int partitionId) {
        return inBusyLock(busyLock, () -> {
            AbstractPageMemoryMvPartitionStorage mvPartitionStorage = getMvPartitionBusy(partitionId);

            if (mvPartitionStorage == null) {
                throw new StorageException(createMissingMvPartitionErrorMessage(partitionId));
            }

            // TODO: IGNITE-18603 реализовать

            return completedFuture(null);
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
     * Returns table name.
     */
    public String getTableName() {
        return tableCfg.name().value();
    }
}
