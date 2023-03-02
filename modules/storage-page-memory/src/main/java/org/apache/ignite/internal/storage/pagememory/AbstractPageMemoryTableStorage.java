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
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.storage.MvPartitionStorage.REBALANCE_IN_PROGRESS;
import static org.apache.ignite.internal.storage.util.StorageUtils.createMissingMvPartitionErrorMessage;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.ignite.internal.pagememory.DataRegion;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.freelist.FreeList;
import org.apache.ignite.internal.pagememory.reuse.ReuseList;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.pagememory.mv.AbstractPageMemoryMvPartitionStorage;
import org.apache.ignite.internal.storage.util.MvPartitionStorages;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract table storage implementation based on {@link PageMemory}.
 */
public abstract class AbstractPageMemoryTableStorage implements MvTableStorage {
    protected final TableConfiguration tableCfg;

    protected final TablesConfiguration tablesCfg;

    protected volatile MvPartitionStorages<AbstractPageMemoryMvPartitionStorage> mvPartitionStorages;

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
            mvPartitionStorages = new MvPartitionStorages(tableCfg.value());

            return null;
        });
    }

    @Override
    public void stop() throws StorageException {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        try {
            IgniteUtils.closeAllManually(mvPartitionStorages.getAllForCloseOrDestroy().get(10, TimeUnit.SECONDS).stream());
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
    public SortedIndexStorage getOrCreateSortedIndex(int partitionId, UUID indexId) {
        return busy(() -> {
            AbstractPageMemoryMvPartitionStorage partitionStorage = mvPartitionStorages.get(partitionId);

            if (partitionStorage == null) {
                throw new StorageException(createMissingMvPartitionErrorMessage(partitionId));
            }

            return partitionStorage.getOrCreateSortedIndex(indexId);
        });
    }

    @Override
    public HashIndexStorage getOrCreateHashIndex(int partitionId, UUID indexId) {
        return busy(() -> {
            AbstractPageMemoryMvPartitionStorage partitionStorage = mvPartitionStorages.get(partitionId);

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

    private <V> V busy(Supplier<V> supplier) {
        return inBusyLock(busyLock, supplier);
    }

    @Override
    public CompletableFuture<Void> startRebalancePartition(int partitionId) {
        return inBusyLock(busyLock, () -> mvPartitionStorages.startRebalace(partitionId, mvPartitionStorage -> {
            mvPartitionStorage.startRebalance();

            return clearStorageAndUpdateDataStructures(mvPartitionStorage)
                    .thenAccept(unused ->
                            mvPartitionStorage.runConsistently(() -> {
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
                            mvPartitionStorage.runConsistently(() -> {
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
            mvPartitionStorage.runConsistently(() -> {
                mvPartitionStorage.lastAppliedOnRebalance(lastAppliedIndex, lastAppliedTerm);

                mvPartitionStorage.committedGroupConfigurationOnRebalance(groupConfig);

                return null;
            });

            mvPartitionStorage.completeRebalance();

            return completedFuture(null);
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
     * Returns table name.
     */
    public String getTableName() {
        return tableCfg.name().value();
    }
}
