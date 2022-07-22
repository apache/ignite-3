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

import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_AUX;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.internal.pagememory.util.PageLockListenerNoOp;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.pagememory.mv.PageMemoryMvPartitionStorage;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * Implementation of {@link AbstractPageMemoryTableStorage} for in-memory case.
 */
class VolatilePageMemoryTableStorage extends AbstractPageMemoryTableStorage {
    private VolatilePageMemoryDataRegion dataRegion;

    /**
     * Constructor.
     *
     * @param tableCfg – Table configuration.
     * @param dataRegion – Data region for the table.
     */
    public VolatilePageMemoryTableStorage(TableConfiguration tableCfg, VolatilePageMemoryDataRegion dataRegion) {
        super(tableCfg);

        this.dataRegion = dataRegion;
    }

    /** {@inheritDoc} */
    @Override
    protected VolatilePageMemoryPartitionStorage createPartitionStorage(int partId) throws StorageException {
        TableFreeList tableFreeList = dataRegion.tableFreeList();

        TableTree tableTree = createTableTree(tableCfg.value(), partId, tableFreeList);

        return new VolatilePageMemoryPartitionStorage(
                partId,
                tableFreeList,
                tableTree
        );
    }

    @Override
    public boolean isVolatile() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public void destroy() throws StorageException {
        stop();
    }

    /** {@inheritDoc} */
    @Override
    public PageMemoryMvPartitionStorage createMvPartitionStorage(int partitionId) {
        return new PageMemoryMvPartitionStorage(
                partitionId,
                tableCfg.value(),
                dataRegion,
                dataRegion.versionChainFreeList(),
                dataRegion.rowVersionFreeList()
        );
    }

    /**
     * Returns new {@link TableTree} instance for partition.
     *
     * @param tableView Table configuration.
     * @param partId Partition ID.
     * @param freeList Table free list.
     * @throws StorageException If failed.
     */
    TableTree createTableTree(
            TableView tableView,
            int partId,
            TableFreeList freeList
    ) throws StorageException {
        int grpId = tableView.intId();

        try {
            return new TableTree(
                    grpId,
                    tableView.name(),
                    partId,
                    dataRegion.pageMemory(),
                    PageLockListenerNoOp.INSTANCE,
                    new AtomicLong(),
                    dataRegion.pageMemory().allocatePage(grpId, partId, FLAG_AUX),
                    freeList,
                    true
            );
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException(
                    String.format("Error creating TableTree [tableName=%s, partitionId=%s]", tableView.name(), partId),
                    e
            );
        }
    }
}
