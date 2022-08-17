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
import org.apache.ignite.internal.pagememory.reuse.ReuseList;
import org.apache.ignite.internal.pagememory.util.PageLockListenerNoOp;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.pagememory.mv.PersistentPageMemoryMvPartitionStorage;
import org.apache.ignite.internal.storage.pagememory.mv.RowVersionFreeList;
import org.apache.ignite.internal.storage.pagememory.mv.VersionChain;
import org.apache.ignite.internal.storage.pagememory.mv.VersionChainTree;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * Implementation of {@link AbstractPageMemoryTableStorage} for persistent case.
 */
public class PersistentPageMemoryTableStorage extends AbstractPageMemoryTableStorage {
    /** Storage engine instance. */
    private final PersistentPageMemoryStorageEngine engine;

    /** Data region instance. */
    private final PersistentPageMemoryDataRegion dataRegion;

    /**
     * Constructor.
     *
     * @param engine Storage engine instance.
     * @param tableCfg Table configuration.
     * @param dataRegion Data region for the table.
     */
    public PersistentPageMemoryTableStorage(
            PersistentPageMemoryStorageEngine engine,
            TableConfiguration tableCfg,
            PersistentPageMemoryDataRegion dataRegion
    ) {
        super(tableCfg);

        this.engine = engine;
        this.dataRegion = dataRegion;
    }

    /**
     * Returns a storage engine instance.
     */
    public PersistentPageMemoryStorageEngine engine() {
        return engine;
    }

    /** {@inheritDoc} */
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
            dataRegion.filePageStoreManager().initialize(tableView.name(), tableView.tableId(), tableView.partitions());

            int deltaFileCount = dataRegion.filePageStoreManager().getStores(tableView.tableId()).stream()
                    .mapToInt(FilePageStore::deltaFileCount)
                    .sum();

            dataRegion.checkpointManager().addDeltaFileCountForCompaction(deltaFileCount);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error initializing file page stores for table: " + tableView.name(), e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void destroy() throws StorageException {
        close(true);
    }

    /** {@inheritDoc} */
    @Override
    public PersistentPageMemoryMvPartitionStorage createMvPartitionStorage(int partitionId) {
        TableView tableView = tableCfg.value();

        FilePageStore filePageStore = ensurePartitionFilePageStore(tableView, partitionId);

        CheckpointManager checkpointManager = dataRegion.checkpointManager();

        CheckpointTimeoutLock checkpointTimeoutLock = checkpointManager.checkpointTimeoutLock();

        checkpointTimeoutLock.checkpointReadLock();

        try {
            PersistentPageMemory persistentPageMemory = dataRegion.pageMemory();

            int grpId = tableView.tableId();

            CheckpointProgress lastCheckpointProgress = checkpointManager.lastCheckpointProgress();

            UUID checkpointId = lastCheckpointProgress == null ? null : lastCheckpointProgress.id();

            PartitionMeta meta = dataRegion.partitionMetaManager().readOrCreateMeta(
                    checkpointId,
                    new GroupPartitionId(grpId, partitionId),
                    filePageStore
            );

            dataRegion.partitionMetaManager().addMeta(new GroupPartitionId(grpId, partitionId), meta);

            filePageStore.pages(meta.pageCount());

            filePageStore.setPageAllocationListener(pageIdx -> {
                assert checkpointTimeoutLock.checkpointLockIsHeldByThread();

                CheckpointProgress last = checkpointManager.lastCheckpointProgress();

                meta.incrementPageCount(last == null ? null : last.id());
            });

            boolean initNewVersionChainTree = false;

            if (meta.versionChainTreeRootPageId() == 0) {
                meta.versionChainTreeRootPageId(checkpointId, persistentPageMemory.allocatePage(grpId, partitionId, FLAG_AUX));

                initNewVersionChainTree = true;
            }

            boolean initRowVersionFreeList = false;

            if (meta.rowVersionFreeListRootPageId() == 0) {
                meta.rowVersionFreeListRootPageId(checkpointId, persistentPageMemory.allocatePage(grpId, partitionId, FLAG_AUX));

                initRowVersionFreeList = true;
            }

            RowVersionFreeList rowVersionFreeList = createRowVersionFreeList(
                    tableView,
                    partitionId,
                    meta.rowVersionFreeListRootPageId(),
                    initRowVersionFreeList
            );

            autoCloseables.add(rowVersionFreeList::close);

            VersionChainTree versionChainTree = createVersionChainTree(
                    tableView,
                    partitionId,
                    rowVersionFreeList,
                    meta.versionChainTreeRootPageId(),
                    initNewVersionChainTree
            );

            return new PersistentPageMemoryMvPartitionStorage(
                    this,
                    partitionId,
                    tableView,
                    dataRegion,
                    checkpointManager,
                    meta,
                    rowVersionFreeList,
                    versionChainTree
            );
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException(
                    String.format("Error getting or creating partition [tableName=%s, partitionId=%s]", tableView.name(), partitionId),
                    e
            );
        } finally {
            checkpointTimeoutLock.checkpointReadUnlock();
        }
    }

    /**
     * Initializes the partition file page store if it hasn't already.
     *
     * @param tableView Table configuration.
     * @param partId Partition ID.
     * @return Partition file page store.
     * @throws StorageException If failed.
     */
    private FilePageStore ensurePartitionFilePageStore(TableView tableView, int partId) throws StorageException {
        try {
            FilePageStore filePageStore = dataRegion.filePageStoreManager().getStore(tableView.tableId(), partId);

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
     * Returns new {@link RowVersionFreeList} instance for partition.
     *
     * @param tableView Table configuration.
     * @param partId Partition ID.
     * @param rootPageId Root page ID.
     * @param initNew {@code True} if new metadata should be initialized.
     * @throws StorageException If failed.
     */
    private RowVersionFreeList createRowVersionFreeList(
            TableView tableView,
            int partId,
            long rootPageId,
            boolean initNew
    ) throws StorageException {
        try {
            return new RowVersionFreeList(
                    tableView.tableId(),
                    partId,
                    dataRegion.pageMemory(),
                    null,
                    PageLockListenerNoOp.INSTANCE,
                    rootPageId,
                    initNew,
                    dataRegion.pageListCacheLimit(),
                    PageEvictionTrackerNoOp.INSTANCE,
                    IoStatisticsHolderNoOp.INSTANCE
            );
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException(
                    String.format("Error creating RowVersionFreeList [tableName=%s, partitionId=%s]", tableView.name(), partId),
                    e
            );
        }
    }

    /**
     * Returns new {@link TableTree} instance for partition.
     *
     * @param tableView Table configuration.
     * @param partId Partition ID.
     * @param freeList {@link VersionChain} free list.
     * @param rootPageId Root page ID.
     * @param initNewTree {@code True} if new tree should be created.
     * @throws StorageException If failed.
     */
    private VersionChainTree createVersionChainTree(
            TableView tableView,
            int partId,
            ReuseList freeList,
            long rootPageId,
            boolean initNewTree
    ) throws StorageException {
        int grpId = tableView.tableId();

        try {
            return new VersionChainTree(
                    grpId,
                    tableView.name(),
                    partId,
                    dataRegion.pageMemory(),
                    PageLockListenerNoOp.INSTANCE,
                    new AtomicLong(),
                    rootPageId,
                    freeList,
                    initNewTree
            );
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException(
                    String.format("Error creating VersionChainTree [tableName=%s, partitionId=%s]", tableView.name(), partId),
                    e
            );
        }
    }
}
