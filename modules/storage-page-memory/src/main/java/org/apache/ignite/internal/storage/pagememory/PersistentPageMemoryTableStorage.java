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

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.evict.PageEvictionTrackerNoOp;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointTimeoutLock;
import org.apache.ignite.internal.pagememory.persistence.io.PartitionMetaIo;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStore;
import org.apache.ignite.internal.pagememory.util.PageLockListenerNoOp;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.pagememory.mv.PageMemoryMvPartitionStorage;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * Implementation of {@link AbstractPageMemoryTableStorage} for persistent case.
 */
class PersistentPageMemoryTableStorage extends AbstractPageMemoryTableStorage {
    private final PersistentPageMemoryDataRegion dataRegion;

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
        super(tableCfg);

        this.dataRegion = dataRegion;
    }

    @Override
    public boolean isVolatile() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public void start() throws StorageException {
        super.start();

        TableView tableView = tableCfg.value();

        try {
            // TODO: IGNITE-16665 Directory name needs to be corrected to support table renaming
            dataRegion.filePageStoreManager().initialize(tableView.name(), groupId(tableView), tableView.partitions());
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error initializing file page stores for table: " + tableView.name(), e);
        }
    }

    /** {@inheritDoc} */
    @Override
    protected PersistentPageMemoryPartitionStorage createPartitionStorage(int partId) throws StorageException {
        TableView tableView = tableCfg.value();

        FilePageStore partitionFilePageStore = ensurePartitionFilePageStore(tableView, partId);

        CheckpointTimeoutLock checkpointTimeoutLock = dataRegion.checkpointManager().checkpointTimeoutLock();

        checkpointTimeoutLock.checkpointReadLock();

        try {
            PartitionMeta partitionMeta = getOrCreatePartitionMeta(tableView, partId, partitionFilePageStore);

            TableFreeList tableFreeList = createTableFreeList(tableView, partId, partitionMeta);

            autoCloseables.add(tableFreeList::close);

            TableTree tableTree = createTableTree(tableView, partId, tableFreeList, partitionMeta);

            return new PersistentPageMemoryPartitionStorage(partId, tableFreeList, tableTree, checkpointTimeoutLock);
        } finally {
            checkpointTimeoutLock.checkpointReadUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void destroy() throws StorageException {
        close(true);
    }

    /** {@inheritDoc} */
    @Override
    public PageMemoryMvPartitionStorage createMvPartitionStorage(int partitionId) {
        throw new UnsupportedOperationException("Not supported yet");
    }

    /**
     * Initializes the partition file page store if it hasn't already.
     *
     * @param tableView Table configuration.
     * @param partId Partition ID.
     * @return Partition file page store.
     * @throws StorageException If failed.
     */
    FilePageStore ensurePartitionFilePageStore(TableView tableView, int partId) throws StorageException {
        try {
            FilePageStore filePageStore = dataRegion.filePageStoreManager().getStore(groupId(tableView), partId);

            filePageStore.ensure();

            return filePageStore;
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException(
                    String.format("Error initializing file page store [tableName=%s, partitionId=%s]", tableView.name(), partId),
                    e
            );
        }
    }

    /**
     * Creates new or reads existing partition meta.
     *
     * @param tableView Table configuration.
     * @param partId Partition ID.
     * @param filePageStore Partition file page store.
     * @return Partition meta.
     * @throws StorageException If failed.
     */
    PartitionMeta getOrCreatePartitionMeta(
            TableView tableView,
            int partId,
            FilePageStore filePageStore
    ) throws StorageException {
        PersistentPageMemory pageMemoryImpl = dataRegion.pageMemory();

        int grpId = groupId(tableView);

        boolean allocatePartMetaPage = filePageStore.pages() == 0;

        try {
            long partMetaPageId = allocatePartMetaPage
                    ? pageMemoryImpl.allocatePage(grpId, partId, FLAG_AUX)
                    : pageMemoryImpl.partitionMetaPageId(grpId, partId);

            long partMetaPage = pageMemoryImpl.acquirePage(grpId, partMetaPageId);

            try {
                long partMetaPageAddr = pageMemoryImpl.writeLock(grpId, partMetaPageId, partMetaPage);

                try {
                    long treeRootPageId;
                    long reuseListRootPageId;

                    if (allocatePartMetaPage) {
                        PartitionMetaIo partMetaIo = PartitionMetaIo.VERSIONS.latest();

                        partMetaIo.initNewPage(partMetaPageAddr, partMetaPageId, pageMemoryImpl.realPageSize(grpId));

                        treeRootPageId = pageMemoryImpl.allocatePage(grpId, partId, FLAG_AUX);
                        reuseListRootPageId = pageMemoryImpl.allocatePage(grpId, partId, FLAG_AUX);

                        partMetaIo.setTreeRootPageId(partMetaPageAddr, treeRootPageId);
                        partMetaIo.setReuseListRootPageId(partMetaPageAddr, reuseListRootPageId);
                    } else {
                        PartitionMetaIo partMetaIo = pageMemoryImpl.ioRegistry().resolve(partMetaPageAddr);

                        treeRootPageId = partMetaIo.getTreeRootPageId(partMetaPageAddr);
                        reuseListRootPageId = partMetaIo.getReuseListRootPageId(partMetaPageAddr);
                    }

                    return new PartitionMeta(
                            new FullPageId(treeRootPageId, grpId),
                            new FullPageId(reuseListRootPageId, grpId),
                            allocatePartMetaPage
                    );
                } finally {
                    pageMemoryImpl.writeUnlock(grpId, partMetaPageId, partMetaPage, allocatePartMetaPage);
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
     * @param partitionMeta Table partition metadata.
     * @throws StorageException If failed.
     */
    TableFreeList createTableFreeList(
            TableView tableView,
            int partId,
            PartitionMeta partitionMeta
    ) throws StorageException {
        try {
            return new TableFreeList(
                    groupId(tableView),
                    partId,
                    dataRegion.pageMemory(),
                    PageLockListenerNoOp.INSTANCE,
                    partitionMeta.reuseListRoot.pageId(),
                    partitionMeta.allocated,
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
     * @param partitionMeta Table partition metadata.
     * @throws StorageException If failed.
     */
    TableTree createTableTree(
            TableView tableView,
            int partId,
            TableFreeList freeList,
            PartitionMeta partitionMeta
    ) throws StorageException {
        int grpId = groupId(tableView);

        try {
            return new TableTree(
                    grpId,
                    tableView.name(),
                    partId, dataRegion.pageMemory(),
                    PageLockListenerNoOp.INSTANCE,
                    new AtomicLong(),
                    partitionMeta.treeRoot.pageId(),
                    freeList,
                    partitionMeta.allocated
            );
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException(
                    String.format("Error creating TableTree [tableName=%s, partitionId=%s]", tableView.name(), partId),
                    e
            );
        }
    }
}
