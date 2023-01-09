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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.apache.ignite.internal.pagememory.DataRegion;
import org.apache.ignite.internal.pagememory.PageMemory;
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
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract table storage implementation based on {@link PageMemory}.
 */
public abstract class AbstractPageMemoryTableStorage implements MvTableStorage {
    protected final TableConfiguration tableConfig;

    protected final TablesConfiguration tablesConfig;

    protected volatile boolean started;

    protected volatile AtomicReferenceArray<AbstractPageMemoryMvPartitionStorage> mvPartitions;

    protected final ConcurrentMap<Integer, CompletableFuture<Void>> destroyFutureByPartitionId = new ConcurrentHashMap<>();

    protected final ConcurrentMap<Integer, CompletableFuture<Void>> rebalanceFutureByPartitionId = new ConcurrentHashMap<>();

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
        TableView tableView = tableConfig.value();

        mvPartitions = new AtomicReferenceArray<>(tableView.partitions());

        started = true;
    }

    @Override
    public void stop() throws StorageException {
        started = false;

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
            throw new StorageException("Failed to stop PageMemory table storage.", e);
        }
    }

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
        AbstractPageMemoryMvPartitionStorage partition = getMvPartition(partitionId);

        if (partition != null) {
            return partition;
        }

        partition = createMvPartitionStorage(partitionId);

        partition.start();

        mvPartitions.set(partitionId, partition);

        return partition;
    }

    @Override
    public @Nullable AbstractPageMemoryMvPartitionStorage getMvPartition(int partitionId) {
        checkPartitionId(partitionId);

        return mvPartitions.get(partitionId);
    }

    @Override
    public CompletableFuture<Void> destroyPartition(int partitionId) {
        checkPartitionId(partitionId);

        assert !rebalanceFutureByPartitionId.containsKey(partitionId) : partitionId;

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
    }

    @Override
    public SortedIndexStorage getOrCreateSortedIndex(int partitionId, UUID indexId) {
        return getMvPartitionStorageWithChecks(partitionId).getOrCreateSortedIndex(indexId);
    }

    @Override
    public HashIndexStorage getOrCreateHashIndex(int partitionId, UUID indexId) {
        return getMvPartitionStorageWithChecks(partitionId).getOrCreateHashIndex(indexId);
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
     * @throws IllegalArgumentException If the partition ID is out of config range.
     */
    protected void checkPartitionId(int partitionId) {
        assert started : "Storage has not started yet: " + getTableName();

        if (partitionId < 0 || partitionId >= mvPartitions.length()) {
            throw new IllegalArgumentException(IgniteStringFormatter.format(
                    "Unable to access partition with id outside of configured range: [table={}, partitionId={}, partitions={}]",
                    getTableName(),
                    partitionId,
                    mvPartitions.length()
            ));
        }
    }

    @Override
    public CompletableFuture<Void> startRebalancePartition(int partitionId) {
        AbstractPageMemoryMvPartitionStorage mvPartition = getMvPartitionStorageForRebalance(partitionId);

        if (destroyFutureByPartitionId.containsKey(partitionId)) {
            throw new StorageRebalanceException(IgniteStringFormatter.format(
                    "Partition in the process of destruction: [table={}, partitionId={}]",
                    getTableName(),
                    partitionId
            ));
        }

        if (mvPartition.closed()) {
            throw new StorageRebalanceException(IgniteStringFormatter.format(
                    "Partition closed: [table={}, partitionId={}]",
                    getTableName(),
                    partitionId
            ));
        }

        CompletableFuture<Void> rebalanceFuture = new CompletableFuture<>();

        if (rebalanceFutureByPartitionId.putIfAbsent(partitionId, rebalanceFuture) != null) {
            throw new StorageRebalanceException(IgniteStringFormatter.format(
                    "Rebalance for the partition is already in progress: [table={}, partitionId={}]",
                    getTableName(),
                    partitionId
            ));
        }

        try {
            // TODO: IGNITE-18029 реализуй

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

        AbstractPageMemoryMvPartitionStorage mvPartition = getMvPartitionStorageForRebalance(partitionId);

        return rebalanceFuture
                .thenAccept(unused -> {
                    // TODO: IGNITE-18029 реализуй
                });
    }

    @Override
    public CompletableFuture<Void> finishRebalancePartition(int partitionId, long lastAppliedIndex, long lastAppliedTerm) {
        checkPartitionId(partitionId);

        CompletableFuture<Void> rebalanceFuture = rebalanceFutureByPartitionId.remove(partitionId);

        if (rebalanceFuture == null) {
            throw new StorageRebalanceException(IgniteStringFormatter.format(
                    "Rebalance for the partition did not start: [table={}, partitionId={}]",
                    getTableName(),
                    partitionId
            ));
        }

        AbstractPageMemoryMvPartitionStorage mvPartition = getMvPartitionStorageForRebalance(partitionId);

        return rebalanceFuture
                .thenAccept(unused -> {
                    // TODO: IGNITE-18029 реализуй
                });
    }

    private AbstractPageMemoryMvPartitionStorage getMvPartitionStorageForRebalance(int partitionId) {
        AbstractPageMemoryMvPartitionStorage mvPartition = getMvPartition(partitionId);

        if (mvPartition == null) {
            throw new StorageRebalanceException(createPartitionDoesNotExistsErrorMessage(partitionId));
        }

        return mvPartition;
    }

    /**
     * Returns the partition of the table, if partition does not exist then throws an {@link StorageException}.
     *
     * @param partitionId Partition ID.
     * @throws IllegalArgumentException If the partition ID is out of config range.
     * @throws StorageException If the partition is missing.
     */
    protected AbstractPageMemoryMvPartitionStorage getMvPartitionStorageWithChecks(int partitionId) {
        AbstractPageMemoryMvPartitionStorage mvPartition = getMvPartition(partitionId);

        if (mvPartition == null) {
            throw new StorageException(createPartitionDoesNotExistsErrorMessage(partitionId));
        }

        return mvPartition;
    }

    /**
     * Returns table name.
     */
    protected String getTableName() {
        return tableConfig.name().value();
    }

    /**
     * Creates an error message for a table partition that does not exist.
     *
     * @param partitionId Partition ID.
     */
    protected static String createPartitionDoesNotExistsErrorMessage(int partitionId) {
        return "Partition ID " + partitionId + " does not exist";
    }
}
