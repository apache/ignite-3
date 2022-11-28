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
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.pagememory.mv.AbstractPageMemoryMvPartitionStorage;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract table storage implementation based on {@link PageMemory}.
 */
public abstract class AbstractPageMemoryTableStorage implements MvTableStorage {
    protected final TableConfiguration tableCfg;

    protected TablesConfiguration tablesConfiguration;

    protected volatile boolean started;

    protected volatile AtomicReferenceArray<AbstractPageMemoryMvPartitionStorage> mvPartitions;

    protected final ConcurrentMap<Integer, CompletableFuture<Void>> partitionIdDestroyFutureMap = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param tableCfg Table configuration.
     */
    protected AbstractPageMemoryTableStorage(TableConfiguration tableCfg, TablesConfiguration tablesCfg) {
        this.tableCfg = tableCfg;
        tablesConfiguration = tablesCfg;
    }

    @Override
    public TableConfiguration configuration() {
        return tableCfg;
    }

    @Override
    public TablesConfiguration tablesConfiguration() {
        return tablesConfiguration;
    }

    /**
     * Returns a data region instance for the table.
     */
    public abstract DataRegion<?> dataRegion();

    @Override
    public void start() throws StorageException {
        TableView tableView = tableCfg.value();

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
        assert started : "Storage has not started yet";

        checkPartitionId(partitionId);

        return mvPartitions.get(partitionId);
    }

    @Override
    public CompletableFuture<Void> destroyPartition(int partitionId) {
        assert started : "Storage has not started yet";

        checkPartitionId(partitionId);

        CompletableFuture<Void> destroyPartitionFuture = new CompletableFuture<>();

        CompletableFuture<Void> previousDestroyPartitionFuture = partitionIdDestroyFutureMap.putIfAbsent(
                partitionId,
                destroyPartitionFuture
        );

        if (previousDestroyPartitionFuture != null) {
            return previousDestroyPartitionFuture;
        }

        MvPartitionStorage partition = mvPartitions.getAndSet(partitionId, null);

        if (partition != null) {
            destroyMvPartitionStorage((AbstractPageMemoryMvPartitionStorage) partition).whenComplete((unused, throwable) -> {
                partitionIdDestroyFutureMap.remove(partitionId);

                if (throwable != null) {
                    destroyPartitionFuture.completeExceptionally(throwable);
                } else {
                    destroyPartitionFuture.complete(null);
                }
            });
        } else {
            partitionIdDestroyFutureMap.remove(partitionId).complete(null);
        }

        return destroyPartitionFuture;
    }

    @Override
    public SortedIndexStorage getOrCreateSortedIndex(int partitionId, UUID indexId) {
        AbstractPageMemoryMvPartitionStorage partitionStorage = getMvPartition(partitionId);

        if (partitionStorage == null) {
            throw new StorageException(String.format("Partition ID %d does not exist", partitionId));
        }

        return partitionStorage.getOrCreateSortedIndex(indexId);
    }

    @Override
    public HashIndexStorage getOrCreateHashIndex(int partitionId, UUID indexId) {
        AbstractPageMemoryMvPartitionStorage partitionStorage = getMvPartition(partitionId);

        if (partitionStorage == null) {
            throw new StorageException(String.format("Partition ID %d does not exist", partitionId));
        }

        return partitionStorage.getOrCreateHashIndex(indexId);
    }

    @Override
    public CompletableFuture<Void> destroyIndex(UUID indexId) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    /**
     * Checks that the partition ID is within the scope of the configuration.
     *
     * @param partitionId Partition ID.
     */
    private void checkPartitionId(int partitionId) {
        int partitions = mvPartitions.length();

        if (partitionId < 0 || partitionId >= partitions) {
            throw new IllegalArgumentException(S.toString(
                    "Unable to access partition with id outside of configured range",
                    "table", tableCfg.value().name(), false,
                    "partitionId", partitionId, false,
                    "partitions", partitions, false
            ));
        }
    }
}
