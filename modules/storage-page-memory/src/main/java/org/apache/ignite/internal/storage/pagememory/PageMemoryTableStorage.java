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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.storage.PartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.TableStorage;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.pagememory.mv.PageMemoryMvPartitionStorage;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Abstract table storage implementation based on {@link PageMemory}.
 */
// TODO: IGNITE-16641 Add support for persistent case.
// TODO: IGNITE-16642 Support indexes.
public abstract class PageMemoryTableStorage implements TableStorage {
    protected final AbstractPageMemoryDataRegion dataRegion;

    protected final TableConfiguration tableCfg;

    /** List of objects to be closed on the {@link #stop}. */
    protected final List<AutoCloseable> autoCloseables = new CopyOnWriteArrayList<>();

    protected volatile boolean started;

    protected volatile AtomicReferenceArray<PartitionStorage> partitions;

    /**
     * Constructor.
     *
     * @param tableCfg – Table configuration.
     * @param dataRegion – Data region for the table.
     */
    public PageMemoryTableStorage(TableConfiguration tableCfg, AbstractPageMemoryDataRegion dataRegion) {
        this.dataRegion = dataRegion;
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

        started = true;
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws StorageException {
        started = false;

        List<AutoCloseable> autoCloseables = new ArrayList<>(this.autoCloseables);

        for (int i = 0; i < partitions.length(); i++) {
            PartitionStorage partition = partitions.getAndUpdate(i, p -> null);

            if (partition != null) {
                autoCloseables.add(partition);
            }

            autoCloseables.add(partition);
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

    /** {@inheritDoc} */
    @Override
    public void destroy() throws StorageException {
        stop();
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

    /** {@inheritDoc} */
    @Override
    public SortedIndexStorage getOrCreateSortedIndex(String indexName) {
        throw new UnsupportedOperationException("Indexes are not supported yet.");
    }

    /** {@inheritDoc} */
    @Override
    public void dropIndex(String indexName) {
        throw new UnsupportedOperationException("Indexes are not supported yet.");
    }

    /**
     * Returns a new instance of {@link PageMemoryPartitionStorage}.
     *
     * @param partId Partition id.
     * @throws StorageException If there is an error while creating the partition storage.
     */
    protected abstract PageMemoryPartitionStorage createPartitionStorage(int partId) throws StorageException;

    /**
     * This API is not yet ready. But we need to test mv storages anyways.
     */
    @TestOnly
    public PageMemoryMvPartitionStorage createMvPartitionStorage(int partitionId) {
        return new PageMemoryMvPartitionStorage(partitionId,
                tableCfg.value(),
                dataRegion,
                ((VolatilePageMemoryDataRegion) dataRegion).versionChainFreeList(),
                ((VolatilePageMemoryDataRegion) dataRegion).rowVersionFreeList()
        );
    }
}
