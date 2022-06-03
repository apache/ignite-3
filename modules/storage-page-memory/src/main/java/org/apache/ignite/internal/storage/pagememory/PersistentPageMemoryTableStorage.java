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
import static org.apache.ignite.internal.storage.StorageUtils.groupId;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.evict.PageEvictionTrackerNoOp;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.pagememory.persistence.PageMemoryImpl;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointTimeoutLock;
import org.apache.ignite.internal.pagememory.util.PageLockListenerNoOp;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.pagememory.io.TablePartitionMetaIo;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * Implementation of {@link AbstractPageMemoryTableStorage} for persistent case.
 */
class PersistentPageMemoryTableStorage extends AbstractPageMemoryTableStorage {
    /**
     * Constructor.
     *
     * @param tableCfg Table configuration.
     * @param dataRegion Data region for the table.
     */
    public PersistentPageMemoryTableStorage(
            TableConfiguration tableCfg,
            PersistentPageMemoryDataRegion dataRegion
    ) {
        super(tableCfg, dataRegion);
    }

    /** {@inheritDoc} */
    @Override
    public void start() throws StorageException {
        super.start();

        TableView tableView = tableCfg.value();

        try {
            ((PersistentPageMemoryDataRegion) dataRegion)
                    .filePageStoreManager()
                    .initialize(tableView.name(), groupId(tableView), tableView.partitions());
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error initializing file page stores for table: " + tableView.name(), e);
        }
    }

    /** {@inheritDoc} */
    @Override
    protected VolatilePageMemoryPartitionStorage createPartitionStorage(int partId) throws StorageException {
        TableView tableView = tableCfg.value();

        ensurePartitionFilePageStore(tableView, partId);

        TableTreePartitionMetas tableTreePartitionMetas = getOrCreateTableTreePartitionMetas(tableView, partId);

        CheckpointTimeoutLock checkpointTimeoutLock = ((PersistentPageMemoryDataRegion) dataRegion)
                .checkpointManager()
                .checkpointTimeoutLock();

        checkpointTimeoutLock.checkpointReadLock();

        TableFreeList tableFreeList;

        try {
            tableFreeList = createTableFreeList(tableView, partId, tableTreePartitionMetas);

            autoCloseables.add(tableFreeList::close);
        } finally {
            checkpointTimeoutLock.checkpointReadUnlock();
        }

        TableTree tableTree = createTableTree(tableView, partId, tableFreeList, tableTreePartitionMetas);

        return new PersistentPageMemoryPartitionStorage(partId, tableFreeList, tableTree, checkpointTimeoutLock);
    }

    /**
     * Initializes the partition file page store if it hasn't already.
     *
     * @param tableView Table configuration.
     * @param partId Partition ID.
     * @throws StorageException If failed.
     */
    void ensurePartitionFilePageStore(TableView tableView, int partId) throws StorageException {
        try {
            ((PersistentPageMemoryDataRegion) dataRegion)
                    .filePageStoreManager()
                    .getStore(groupId(tableView), partId)
                    .ensure();
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException(
                    String.format("Error initializing file page store [tableName=%s, partitionId=%s]", tableView.name(), partId),
                    e
            );
        }
    }

    /**
     * Returns the read and created new partition {@link TableTreePartitionMetas}.
     *
     * @param tableView Table configuration.
     * @param partId Partition id.
     * @throws StorageException If failed.
     */
    TableTreePartitionMetas getOrCreateTableTreePartitionMetas(
            TableView tableView,
            int partId
    ) throws StorageException {
        PageMemoryImpl pageMemoryImpl = (PageMemoryImpl) dataRegion.pageMemory();

        int grpId = groupId(tableView);

        long partMetaPageId = pageMemoryImpl.partitionMetaPageId(grpId, partId);

        AtomicBoolean partMetaPageAllocated = new AtomicBoolean();

        try {
            long partMetaPage = pageMemoryImpl.acquirePage(grpId, partMetaPageId, partMetaPageAllocated);

            try {
                long partMetaPageAddr = pageMemoryImpl.writeLock(grpId, partMetaPageId, partMetaPage);

                try {
                    long treeRootPageId;
                    long reuseListRootPageId;

                    if (partMetaPageAllocated.get()) {
                        TablePartitionMetaIo partMetaIo = TablePartitionMetaIo.VERSIONS.latest();

                        partMetaIo.initNewPage(partMetaPageAddr, partMetaPageId, pageMemoryImpl.realPageSize(grpId));

                        treeRootPageId = pageMemoryImpl.allocatePage(grpId, partId, FLAG_AUX);
                        reuseListRootPageId = pageMemoryImpl.allocatePage(grpId, partId, FLAG_AUX);

                        partMetaIo.setTreeRootPageId(partMetaPageAddr, treeRootPageId);
                        partMetaIo.setReuseListRootPageId(partMetaPageAddr, reuseListRootPageId);
                    } else {
                        TablePartitionMetaIo partMetaIo = pageMemoryImpl.ioRegistry().resolve(partMetaPageAddr);

                        treeRootPageId = partMetaIo.getTreeRootPageId(partMetaPageAddr);
                        reuseListRootPageId = partMetaIo.getReuseListRootPageId(partMetaPageAddr);
                    }

                    return new TableTreePartitionMetas(
                            new FullPageId(treeRootPageId, grpId),
                            new FullPageId(reuseListRootPageId, grpId),
                            partMetaPageAllocated.get()
                    );
                } finally {
                    pageMemoryImpl.writeUnlock(grpId, partMetaPageId, partMetaPage, partMetaPageAllocated.get());
                }
            } finally {
                pageMemoryImpl.releasePage(grpId, partMetaPageId, partMetaPage);
            }
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException(
                    String.format("Error getting or creating partition metadata [tableName=%s, partitionId=%s]", tableView.name(), partId),
                    e
            );
        }
    }

    /**
     * Returns new {@link TableFreeList} instance for partition.
     *
     * @param tableView Table configuration.
     * @param partId Partition ID.
     * @param tableTreePartitionMetas Table partition metadata.
     * @throws StorageException If failed.
     */
    TableFreeList createTableFreeList(
            TableView tableView,
            int partId,
            TableTreePartitionMetas tableTreePartitionMetas
    ) throws StorageException {
        try {
            return new TableFreeList(
                    groupId(tableView),
                    dataRegion.pageMemory(),
                    PageLockListenerNoOp.INSTANCE,
                    tableTreePartitionMetas.reuseListRoot.pageId(),
                    tableTreePartitionMetas.allocated,
                    null,
                    PageEvictionTrackerNoOp.INSTANCE,
                    IoStatisticsHolderNoOp.INSTANCE
            );
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException(
                    String.format("Error creating TableFreeList [tableName=%s, partitionId=%s]", tableView.name(), partId),
                    e
            );
        }
    }

    /**
     * Returns new {@link TableTree} instance for partition.
     *
     * @param tableView Table configuration.
     * @param partId Partition ID.
     * @param freeList Table free list.
     * @param tableTreePartitionMetas Table partition metadata.
     * @throws StorageException If failed.
     */
    TableTree createTableTree(
            TableView tableView,
            int partId,
            TableFreeList freeList,
            TableTreePartitionMetas tableTreePartitionMetas
    ) throws StorageException {
        int grpId = groupId(tableView);

        try {
            return new TableTree(
                    grpId,
                    tableView.name(),
                    dataRegion.pageMemory(),
                    PageLockListenerNoOp.INSTANCE,
                    new AtomicLong(),
                    tableTreePartitionMetas.treeRoot.pageId(),
                    freeList,
                    partId,
                    tableTreePartitionMetas.allocated
            );
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException(
                    String.format("Error creating TableTree [tableName=%s, partitionId=%s]", tableView.name(), partId),
                    e
            );
        }
    }
}
