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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.apache.ignite.internal.pagememory.DataRegion;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.schema.configuration.TableView;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
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
    private static final VarHandle STARTED;

    static {
        try {
            STARTED = MethodHandles.lookup().findVarHandle(AbstractPageMemoryTableStorage.class, "started", boolean.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    protected final TableConfiguration tableConfig;

    protected TablesConfiguration tablesConfig;

    protected volatile boolean started;

    private volatile AtomicReferenceArray<AbstractPageMemoryMvPartitionStorage> mvPartitions;

    /**
     * Constructor.
     *
     * @param tableConfig Table configuration.
     */
    protected AbstractPageMemoryTableStorage(TableConfiguration tableConfig, TablesConfiguration tablesCfg) {
        this.tableConfig = tableConfig;
        this.tablesConfig = tablesCfg;
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
        close(false);
    }

    /**
     * Returns a new instance of {@link AbstractPageMemoryMvPartitionStorage}.
     *
     * @param partitionId Partition id.
     * @throws StorageException If there is an error while creating the mv partition storage.
     */
    public abstract AbstractPageMemoryMvPartitionStorage createMvPartitionStorage(int partitionId) throws StorageException;

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
    public void destroyPartition(int partitionId) throws StorageException {
        assert started : "Storage has not started yet";

        checkPartitionId(partitionId);

        AbstractPageMemoryMvPartitionStorage partition = mvPartitions.getAndSet(partitionId, null);

        if (partition != null) {
            partition.destroy();
        }
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
     * Closes all {@link #mvPartitions}.
     *
     * @param destroy Destroy partitions.
     * @throws StorageException If failed.
     */
    protected void close(boolean destroy) throws StorageException {
        if (!STARTED.compareAndSet(this, true, false)) {
            return;
        }

        List<AutoCloseable> closeables = new ArrayList<>();

        for (int i = 0; i < mvPartitions.length(); i++) {
            AbstractPageMemoryMvPartitionStorage partition = mvPartitions.getAndUpdate(i, p -> null);

            if (partition != null) {
                closeables.add(destroy ? partition::destroy : partition::close);
            }
        }

        try {
            IgniteUtils.closeAll(closeables);
        } catch (Exception e) {
            throw new StorageException("Failed to stop PageMemory table storage.", e);
        }
    }

    /**
     * Checks that a passed partition id is within the proper bounds.
     *
     * @param partitionId Partition id.
     */
    protected void checkPartitionId(int partitionId) {
        int partitions = mvPartitions.length();

        if (partitionId < 0 || partitionId >= partitions) {
            throw new IllegalArgumentException(S.toString(
                    "Unable to access partition with id outside of configured range",
                    "table", tableConfig.value().name(), false,
                    "partitionId", partitionId, false,
                    "partitions", partitions, false
            ));
        }
    }
}
