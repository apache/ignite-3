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

import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_AUX;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.schema.configuration.TableView;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumnsFreeList;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMetaTree;
import org.apache.ignite.internal.storage.pagememory.mv.AbstractPageMemoryMvPartitionStorage;
import org.apache.ignite.internal.storage.pagememory.mv.PersistentPageMemoryMvPartitionStorage;
import org.apache.ignite.internal.storage.pagememory.mv.RowVersionFreeList;
import org.apache.ignite.internal.storage.pagememory.mv.VersionChainTree;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.jetbrains.annotations.Nullable;

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
     * @param tableCfg Table configuration.
     * @param engine Storage engine instance.
     * @param dataRegion Data region for the table.
     */
    public PersistentPageMemoryTableStorage(
            TableConfiguration tableCfg,
            TablesConfiguration tablesCfg,
            PersistentPageMemoryStorageEngine engine,
            PersistentPageMemoryDataRegion dataRegion
    ) {
        super(tableCfg, tablesCfg);

        this.engine = engine;
        this.dataRegion = dataRegion;
    }

    /**
     * Returns a storage engine instance.
     */
    public PersistentPageMemoryStorageEngine engine() {
        return engine;
    }

    @Override
    public PersistentPageMemoryDataRegion dataRegion() {
        return dataRegion;
    }

    @Override
    public boolean isVolatile() {
        return false;
    }

    @Override
    protected void finishDestruction() {
        dataRegion.pageMemory().onGroupDestroyed(tableCfg.tableId().value());
    }

    @Override
    public PersistentPageMemoryMvPartitionStorage createMvPartitionStorage(int partitionId) {
        CompletableFuture<Void> partitionDestroyFuture = partitionIdDestroyFutureMap.get(partitionId);

        if (partitionDestroyFuture != null) {
            try {
                // Time is chosen randomly (long enough) so as not to call #join().
                partitionDestroyFuture.get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new StorageException("Error waiting for the destruction of the previous version of the partition: " + partitionId, e);
            }
        }

        TableView tableView = tableCfg.value();

        GroupPartitionId groupPartitionId = new GroupPartitionId(tableView.tableId(), partitionId);

        FilePageStore filePageStore = ensurePartitionFilePageStore(tableView, groupPartitionId);

        CheckpointManager checkpointManager = dataRegion.checkpointManager();

        CheckpointTimeoutLock checkpointTimeoutLock = checkpointManager.checkpointTimeoutLock();

        checkpointTimeoutLock.checkpointReadLock();

        try {
            PersistentPageMemory pageMemory = dataRegion.pageMemory();

            PartitionMeta meta = dataRegion.partitionMetaManager().readOrCreateMeta(lastCheckpointId(), groupPartitionId, filePageStore);

            dataRegion.partitionMetaManager().addMeta(groupPartitionId, meta);

            filePageStore.pages(meta.pageCount());

            filePageStore.setPageAllocationListener(pageIdx -> {
                assert checkpointTimeoutLock.checkpointLockIsHeldByThread();

                meta.incrementPageCount(lastCheckpointId());
            });

            RowVersionFreeList rowVersionFreeList = createRowVersionFreeList(tableView, partitionId, pageMemory, meta);

            IndexColumnsFreeList indexColumnsFreeList
                    = createIndexColumnsFreeList(tableView, partitionId, rowVersionFreeList, pageMemory, meta);

            VersionChainTree versionChainTree = createVersionChainTree(tableView, partitionId, rowVersionFreeList, pageMemory, meta);

            IndexMetaTree indexMetaTree = createIndexMetaTree(tableView, partitionId, rowVersionFreeList, pageMemory, meta);

            return new PersistentPageMemoryMvPartitionStorage(
                    this,
                    partitionId,
                    meta,
                    rowVersionFreeList,
                    indexColumnsFreeList,
                    versionChainTree,
                    indexMetaTree,
                    tablesCfg
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
     * @param groupPartitionId Pair of group ID with partition ID.
     * @return Partition file page store.
     * @throws StorageException If failed.
     */
    private FilePageStore ensurePartitionFilePageStore(TableView tableView, GroupPartitionId groupPartitionId) throws StorageException {
        try {
            dataRegion.filePageStoreManager().initialize(tableView.name(), groupPartitionId);

            FilePageStore filePageStore = dataRegion.filePageStoreManager().getStore(groupPartitionId);

            assert !filePageStore.isMarkedToDestroy() : IgniteStringFormatter.format(
                    "Should not be marked for deletion: [tableName={}, tableId={}, partitionId={}]",
                    tableView.name(),
                    tableView.tableId(),
                    groupPartitionId.getPartitionId()
            );

            filePageStore.ensure();

            if (filePageStore.deltaFileCount() > 0) {
                dataRegion.checkpointManager().triggerCompaction();
            }

            return filePageStore;
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException(
                    String.format("Error initializing file page store [tableName=%s, partitionId=%s]",
                            tableView.name(),
                            groupPartitionId.getPartitionId()
                    ),
                    e
            );
        }
    }

    /**
     * Returns id of the last started checkpoint, or {@code null} if no checkpoints were started yet.
     */
    public @Nullable UUID lastCheckpointId() {
        CheckpointProgress lastCeckpointProgress = dataRegion.checkpointManager().lastCheckpointProgress();

        return lastCeckpointProgress == null ? null : lastCeckpointProgress.id();
    }

    /**
     * Returns new {@link RowVersionFreeList} instance for partition.
     *
     * @param tableView Table configuration.
     * @param partId Partition ID.
     * @param pageMemory Persistent page memory instance.
     * @param meta Partition metadata.
     * @throws StorageException If failed.
     */
    private RowVersionFreeList createRowVersionFreeList(
            TableView tableView,
            int partId,
            PersistentPageMemory pageMemory,
            PartitionMeta meta
    ) throws StorageException {
        try {
            boolean initNew = false;

            if (meta.rowVersionFreeListRootPageId() == 0) {
                long rootPageId = pageMemory.allocatePage(tableView.tableId(), partId, FLAG_AUX);

                meta.rowVersionFreeListRootPageId(lastCheckpointId(), rootPageId);

                initNew = true;
            }

            return new RowVersionFreeList(
                    tableView.tableId(),
                    partId,
                    dataRegion.pageMemory(),
                    null,
                    PageLockListenerNoOp.INSTANCE,
                    meta.rowVersionFreeListRootPageId(),
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
     * Returns new {@link IndexColumnsFreeList} instance for partition.
     *
     * @param tableView Table configuration.
     * @param partitionId Partition ID.
     * @param reuseList Reuse list.
     * @param pageMemory Persistent page memory instance.
     * @param meta Partition metadata.
     * @throws StorageException If failed.
     */
    private IndexColumnsFreeList createIndexColumnsFreeList(
            TableView tableView,
            int partitionId,
            ReuseList reuseList,
            PersistentPageMemory pageMemory,
            PartitionMeta meta
    ) {
        try {
            boolean initNew = false;

            if (meta.indexColumnsFreeListRootPageId() == 0L) {
                long rootPageId = pageMemory.allocatePage(tableView.tableId(), partitionId, FLAG_AUX);

                meta.indexColumnsFreeListRootPageId(lastCheckpointId(), rootPageId);

                initNew = true;
            }

            return new IndexColumnsFreeList(
                    tableView.tableId(),
                    partitionId,
                    pageMemory,
                    reuseList,
                    PageLockListenerNoOp.INSTANCE,
                    meta.indexColumnsFreeListRootPageId(),
                    initNew,
                    new AtomicLong(),
                    PageEvictionTrackerNoOp.INSTANCE,
                    IoStatisticsHolderNoOp.INSTANCE
            );
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException(
                    String.format("Error creating IndexColumnsFreeList [tableName=%s, partitionId=%s]", tableView.name(), partitionId),
                    e
            );
        }
    }

    /**
     * Returns new {@link VersionChainTree} instance for partition.
     *
     * @param tableView Table configuration.
     * @param partId Partition ID.
     * @param reuseList Reuse list.
     * @param pageMemory Persistent page memory instance.
     * @param meta Partition metadata.
     * @throws StorageException If failed.
     */
    private VersionChainTree createVersionChainTree(
            TableView tableView,
            int partId,
            ReuseList reuseList,
            PersistentPageMemory pageMemory,
            PartitionMeta meta
    ) throws StorageException {
        try {
            boolean initNew = false;

            if (meta.versionChainTreeRootPageId() == 0) {
                long rootPageId = pageMemory.allocatePage(tableView.tableId(), partId, FLAG_AUX);

                meta.versionChainTreeRootPageId(lastCheckpointId(), rootPageId);

                initNew = true;
            }

            return new VersionChainTree(
                    tableView.tableId(),
                    tableView.name(),
                    partId,
                    dataRegion.pageMemory(),
                    PageLockListenerNoOp.INSTANCE,
                    new AtomicLong(),
                    meta.versionChainTreeRootPageId(),
                    reuseList,
                    initNew
            );
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException(
                    String.format("Error creating VersionChainTree [tableName=%s, partitionId=%s]", tableView.name(), partId),
                    e
            );
        }
    }

    /**
     * Returns new {@link IndexMetaTree} instance for partition.
     *
     * @param tableView Table configuration.
     * @param partitionId Partition ID.
     * @param reuseList Reuse list.
     * @param pageMemory Persistent page memory instance.
     * @param meta Partition metadata.
     * @throws StorageException If failed.
     */
    private IndexMetaTree createIndexMetaTree(
            TableView tableView,
            int partitionId,
            ReuseList reuseList,
            PersistentPageMemory pageMemory,
            PartitionMeta meta
    ) {
        try {
            boolean initNew = false;

            if (meta.indexTreeMetaPageId() == 0) {
                long rootPageId = pageMemory.allocatePage(tableView.tableId(), partitionId, FLAG_AUX);

                meta.indexTreeMetaPageId(lastCheckpointId(), rootPageId);

                initNew = true;
            }

            return new IndexMetaTree(
                    tableView.tableId(),
                    tableView.name(),
                    partitionId,
                    dataRegion.pageMemory(),
                    PageLockListenerNoOp.INSTANCE,
                    new AtomicLong(),
                    meta.indexTreeMetaPageId(),
                    reuseList,
                    initNew
            );
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException(
                    String.format("Error creating IndexMetaTree [tableName=%s, partitionId=%s]", tableView.name(), partitionId),
                    e
            );
        }
    }

    @Override
    public CompletableFuture<Void> startRebalancePartition(int partitionId) {
        // TODO: IGNITE-18029 Implement
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> abortRebalancePartition(int partitionId) {
        // TODO: IGNITE-18029 Implement
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> finishRebalancePartition(int partitionId, long lastAppliedIndex, long lastAppliedTerm) {
        // TODO: IGNITE-18029 Implement
        throw new UnsupportedOperationException();
    }

    @Override
    CompletableFuture<Void> destroyMvPartitionStorage(AbstractPageMemoryMvPartitionStorage mvPartitionStorage) {
        int partitionId = mvPartitionStorage.partitionId();

        // It is enough for us to close the partition storage and its indexes (do not destroy). Prepare the data region, checkpointer, and
        // compactor to remove the partition, and then simply delete the partition file and its delta files.

        mvPartitionStorage.close();

        int tableId = tableCfg.tableId().value();

        GroupPartitionId groupPartitionId = new GroupPartitionId(tableId, partitionId);

        dataRegion.filePageStoreManager().getStore(groupPartitionId).markToDestroy();

        dataRegion.pageMemory().invalidate(tableId, partitionId);

        return dataRegion.checkpointManager().onPartitionDestruction(groupPartitionId)
                .thenAccept(unused -> dataRegion.partitionMetaManager().removeMeta(groupPartitionId))
                .thenCompose(unused -> dataRegion.filePageStoreManager().destroyPartition(groupPartitionId));
    }
}
