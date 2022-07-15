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

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.internal.pagememory.evict.PageEvictionTrackerNoOp;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.PartitionMeta;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointManager;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointTimeoutLock;
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

        FilePageStore filePageStore = ensurePartitionFilePageStore(tableView, partId);

        CheckpointManager checkpointManager = dataRegion.checkpointManager();

        CheckpointTimeoutLock checkpointTimeoutLock = checkpointManager.checkpointTimeoutLock();

        checkpointTimeoutLock.checkpointReadLock();

        try {
            PersistentPageMemory persistentPageMemory = dataRegion.pageMemory();

            int grpId = groupId(tableView);

            CheckpointProgress currentProgress = checkpointManager.currentProgress();

            UUID checkpointId = currentProgress == null ? null : currentProgress.id();

            PartitionMeta meta = dataRegion.partitionMetaManager().readOrCreateMeta(
                    checkpointId,
                    new GroupPartitionId(grpId, partId),
                    dataRegion.pageMemory(),
                    filePageStore
            );

            dataRegion.partitionMetaManager().addMeta(new GroupPartitionId(grpId, partId), meta);

            filePageStore.pages(meta.pageCount());

            filePageStore.setPageAllocationListener(pageIdx -> {
                assert checkpointTimeoutLock.checkpointLockIsHeldByThread();

                CheckpointProgress curr = checkpointManager.currentProgress();

                meta.incrementPageCount(curr == null ? null : curr.id());
            });

            boolean initNewTree = false;

            if (meta.treeRootPageId() == 0) {
                meta.treeRootPageId(checkpointId, persistentPageMemory.allocatePage(grpId, partId, FLAG_AUX));

                initNewTree = true;
            }

            boolean initNewReuseList = false;

            if (meta.reuseListRootPageId() == 0) {
                meta.reuseListRootPageId(checkpointId, persistentPageMemory.allocatePage(grpId, partId, FLAG_AUX));

                initNewReuseList = true;
            }

            TableFreeList tableFreeList = createTableFreeList(tableView, partId, meta, initNewReuseList);

            autoCloseables.add(tableFreeList::close);

            TableTree tableTree = createTableTree(tableView, partId, tableFreeList, meta, initNewTree);

            return new PersistentPageMemoryPartitionStorage(partId, tableFreeList, tableTree, checkpointTimeoutLock);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException(
                    String.format("Error getting or creating partition metadata [tableName=%s, partitionId=%s]", tableView.name(), partId),
                    e
            );
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
     * Returns new {@link TableFreeList} instance for partition.
     *
     * @param tableView Table configuration.
     * @param partId Partition ID.
     * @param partitionMeta Partition metadata.
     * @param initNew {@code True} if new metadata should be initialized.
     * @throws StorageException If failed.
     */
    TableFreeList createTableFreeList(
            TableView tableView,
            int partId,
            PartitionMeta partitionMeta,
            boolean initNew
    ) throws StorageException {
        try {
            return new TableFreeList(
                    groupId(tableView),
                    partId,
                    dataRegion.pageMemory(),
                    PageLockListenerNoOp.INSTANCE,
                    partitionMeta.reuseListRootPageId(),
                    initNew,
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
     * @param partitionMeta Partition metadata.
     * @param initNewTree {@code True} if new tree should be created.
     * @throws StorageException If failed.
     */
    TableTree createTableTree(
            TableView tableView,
            int partId,
            TableFreeList freeList,
            PartitionMeta partitionMeta,
            boolean initNewTree
    ) throws StorageException {
        int grpId = groupId(tableView);

        try {
            return new TableTree(
                    grpId,
                    tableView.name(),
                    partId, dataRegion.pageMemory(),
                    PageLockListenerNoOp.INSTANCE,
                    new AtomicLong(),
                    partitionMeta.treeRootPageId(),
                    freeList,
                    initNewTree
            );
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException(
                    String.format("Error creating TableTree [tableName=%s, partitionId=%s]", tableView.name(), partId),
                    e
            );
        }
    }
}
