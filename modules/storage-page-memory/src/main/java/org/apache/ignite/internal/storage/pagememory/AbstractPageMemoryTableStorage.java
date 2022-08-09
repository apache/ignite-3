/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.PartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.engine.TableStorage;
import org.apache.ignite.internal.storage.pagememory.mv.AbstractPageMemoryMvPartitionStorage;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract table storage implementation based on {@link PageMemory}.
 */
// TODO: IGNITE-16642 Support indexes.
public abstract class AbstractPageMemoryTableStorage implements TableStorage, MvTableStorage {
    protected final TableConfiguration tableCfg;

    /** List of objects to be closed on the {@link #stop}. */
    protected final List<AutoCloseable> autoCloseables = new CopyOnWriteArrayList<>();

    protected volatile boolean started;

    protected volatile AtomicReferenceArray<PartitionStorage> partitions;

    protected volatile AtomicReferenceArray<MvPartitionStorage> mvPartitions;

    /**
     * Constructor.
     *
     * @param tableCfg Table configuration.
     */
    protected AbstractPageMemoryTableStorage(TableConfiguration tableCfg) {
        this.tableCfg = tableCfg;
    }

    /** {@inheritDoc} */
    @Override
    public TableConfiguration configuration() {
        return tableCfg;
    }

    /** {@inheritDoc} */
    @Override
    public void start() throws StorageException {
        TableView tableView = tableCfg.value();

        partitions = new AtomicReferenceArray<>(tableView.partitions());

        mvPartitions = new AtomicReferenceArray<>(tableView.partitions());

        started = true;
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws StorageException {
        close(false);
    }

    /** {@inheritDoc} */
    @Override
    public PartitionStorage getOrCreatePartition(int partId) throws StorageException {
        PartitionStorage partition = getPartition(partId);

        if (partition != null) {
            return partition;
        }

        partition = createPartitionStorage(partId);

        partitions.set(partId, partition);

        return partition;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable PartitionStorage getPartition(int partId) {
        assert started : "Storage has not started yet";

        if (partId < 0 || partId >= partitions.length()) {
            throw new IllegalArgumentException(S.toString(
                    "Unable to access partition with id outside of configured range",
                    "table", tableCfg.name().value(), false,
                    "partitionId", partId, false,
                    "partitions", partitions.length(), false
            ));
        }

        return partitions.get(partId);
    }

    /** {@inheritDoc} */
    @Override
    public void dropPartition(int partId) throws StorageException {
        assert started : "Storage has not started yet";

        PartitionStorage partition = getPartition(partId);

        if (partition != null) {
            partitions.set(partId, null);

            partition.destroy();
        }
    }

    /**
     * Returns a new instance of {@link AbstractPageMemoryPartitionStorage}.
     *
     * @param partitionId Partition id.
     * @throws StorageException If there is an error while creating the partition storage.
     */
    protected abstract AbstractPageMemoryPartitionStorage createPartitionStorage(int partitionId) throws StorageException;

    /**
     * Returns a new instance of {@link AbstractPageMemoryMvPartitionStorage}.
     *
     * @param partitionId Partition id.
     * @throws StorageException If there is an error while creating the mv partition storage.
     */
    public abstract AbstractPageMemoryMvPartitionStorage createMvPartitionStorage(int partitionId) throws StorageException;

    /** {@inheritDoc} */
    @Override
    public MvPartitionStorage getOrCreateMvPartition(int partitionId) throws StorageException {
        MvPartitionStorage partition = getMvPartition(partitionId);

        if (partition != null) {
            return partition;
        }

        partition = createMvPartitionStorage(partitionId);

        mvPartitions.set(partitionId, partition);

        return partition;
    }

    /** {@inheritDoc} */
    @Override
    public MvPartitionStorage getMvPartition(int partitionId) {
        assert started : "Storage has not started yet";

        if (partitionId < 0 || partitionId >= mvPartitions.length()) {
            throw new IllegalArgumentException(S.toString(
                    "Unable to access partition with id outside of configured range",
                    "table", tableCfg.name().value(), false,
                    "partitionId", partitionId, false,
                    "partitions", mvPartitions.length(), false
            ));
        }

        return mvPartitions.get(partitionId);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<?> destroyPartition(int partitionId) throws StorageException {
        assert started : "Storage has not started yet";

        MvPartitionStorage partition = getMvPartition(partitionId);

        if (partition != null) {
            mvPartitions.set(partitionId, null);

            // TODO: IGNITE-17197 Actually destroy the partition.
            //partition.destroy();
        }

        // TODO: IGNITE-17197 Convert this to true async code.
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Closes all {@link #partitions} and {@link #autoCloseables}.
     *
     * @param destroy Destroy partitions.
     * @throws StorageException If failed.
     */
    protected void close(boolean destroy) throws StorageException {
        started = false;

        List<AutoCloseable> autoCloseables = new ArrayList<>(this.autoCloseables);

        for (int i = 0; i < partitions.length(); i++) {
            PartitionStorage partition = partitions.getAndUpdate(i, p -> null);

            if (partition != null) {
                autoCloseables.add(destroy ? partition::destroy : partition);
            }
        }

        Collections.reverse(autoCloseables);

        try {
            IgniteUtils.closeAll(autoCloseables);
        } catch (Exception e) {
            throw new StorageException("Failed to stop PageMemory table storage.", e);
        }

        this.autoCloseables.clear();
        partitions = null;
    }
}
