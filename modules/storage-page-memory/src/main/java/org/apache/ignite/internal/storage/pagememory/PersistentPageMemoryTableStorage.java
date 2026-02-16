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
import static org.apache.ignite.internal.util.GridUnsafe.allocateBuffer;
import static org.apache.ignite.internal.util.GridUnsafe.freeBuffer;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.freelist.FreeListImpl;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointTimeoutLock;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStore;
import org.apache.ignite.internal.pagememory.reuse.ReuseList;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptorSupplier;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMetaTree;
import org.apache.ignite.internal.storage.pagememory.mv.AbstractPageMemoryMvPartitionStorage;
import org.apache.ignite.internal.storage.pagememory.mv.PersistentPageMemoryMvPartitionStorage;
import org.apache.ignite.internal.storage.pagememory.mv.StorageConsistencyMetrics;
import org.apache.ignite.internal.storage.pagememory.mv.VersionChainTree;
import org.apache.ignite.internal.storage.pagememory.mv.gc.GcQueue;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link AbstractPageMemoryTableStorage} for persistent case.
 */
public class PersistentPageMemoryTableStorage extends AbstractPageMemoryTableStorage<PersistentPageMemoryMvPartitionStorage> {
    /** Storage engine instance. */
    private final PersistentPageMemoryStorageEngine engine;

    /** Data region instance. */
    private final PersistentPageMemoryDataRegion dataRegion;

    private final ExecutorService destructionExecutor;

    private final FailureProcessor failureProcessor;

    private final StorageConsistencyMetrics consistencyMetrics;

    /**
     * Constructor.
     *
     * @param tableDescriptor Table descriptor.
     * @param indexDescriptorSupplier Index descriptor supplier.
     * @param engine Storage engine instance.
     * @param dataRegion Data region for the table.
     * @param destructionExecutor Executor service for destruction tasks.
     * @param failureProcessor Failure processor.
     * @param consistencyMetrics Storage consistency metrics.
     */
    public PersistentPageMemoryTableStorage(
            StorageTableDescriptor tableDescriptor,
            StorageIndexDescriptorSupplier indexDescriptorSupplier,
            PersistentPageMemoryStorageEngine engine,
            PersistentPageMemoryDataRegion dataRegion,
            ExecutorService destructionExecutor,
            FailureProcessor failureProcessor,
            StorageConsistencyMetrics consistencyMetrics
    ) {
        super(tableDescriptor, indexDescriptorSupplier);

        this.engine = engine;
        this.dataRegion = dataRegion;
        this.destructionExecutor = destructionExecutor;
        this.failureProcessor = failureProcessor;
        this.consistencyMetrics = consistencyMetrics;
    }

    @Override
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
        dataRegion.pageMemory().onGroupDestroyed(getTableId());

        dataRegion.checkpointManager().partitionDestructionLockManager().removeLockForGroup(getTableId());

        try {
            dataRegion.filePageStoreManager().destroyGroupIfExists(getTableId());
        } catch (IOException e) {
            throw new IgniteInternalException(INTERNAL_ERR, "Could not destroy table directory", e);
        }
    }

    @Override
    public PersistentPageMemoryMvPartitionStorage createMvPartitionStorage(int partitionId) {
        GroupPartitionId groupPartitionId = createGroupPartitionId(partitionId);

        StoragePartitionMeta meta = getOrCreatePartitionMetaOnCreatePartition(groupPartitionId);

        return inCheckpointLock(() -> {
            PersistentPageMemory pageMemory = dataRegion.pageMemory();

            FreeListImpl freeList = createFreeList(partitionId, pageMemory, meta);

            VersionChainTree versionChainTree = createVersionChainTree(partitionId, freeList, pageMemory, meta);

            IndexMetaTree indexMetaTree = createIndexMetaTree(partitionId, freeList, pageMemory, meta);

            GcQueue gcQueue = createGcQueue(partitionId, freeList, pageMemory, meta);

            return new PersistentPageMemoryMvPartitionStorage(
                    this,
                    partitionId,
                    meta,
                    freeList,
                    versionChainTree,
                    indexMetaTree,
                    gcQueue,
                    destructionExecutor,
                    failureProcessor,
                    consistencyMetrics
            );
        });
    }

    /**
     * Returns id of the last started checkpoint, or {@code null} if no checkpoints were started yet.
     */
    public @Nullable UUID lastCheckpointId() {
        CheckpointProgress lastCeckpointProgress = dataRegion.checkpointManager().lastCheckpointProgress();

        return lastCeckpointProgress == null ? null : lastCeckpointProgress.id();
    }

    /**
     * Returns new {@link FreeListImpl} instance for partition.
     *
     * @param partId Partition ID.
     * @param pageMemory Persistent page memory instance.
     * @param meta Partition metadata.
     * @throws StorageException If failed.
     */
    private FreeListImpl createFreeList(
            int partId,
            PersistentPageMemory pageMemory,
            StoragePartitionMeta meta
    ) throws StorageException {
        try {
            boolean initNew = false;

            if (meta.freeListRootPageId() == 0) {
                long rootPageId = pageMemory.allocatePageNoReuse(getTableId(), partId, FLAG_AUX);

                meta.freeListRootPageId(lastCheckpointId(), rootPageId);

                initNew = true;
            }

            return new FreeListImpl(
                    "PersistentFreeList",
                    getTableId(),
                    partId,
                    dataRegion.pageMemory(),
                    meta.freeListRootPageId(),
                    initNew,
                    dataRegion.pageListCacheLimit()
            );
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error creating free list: [tableId={}, partitionId={}]", e, getTableId(), partId);
        }
    }

    /**
     * Returns new {@link VersionChainTree} instance for partition.
     *
     * @param partId Partition ID.
     * @param reuseList Reuse list.
     * @param pageMemory Persistent page memory instance.
     * @param meta Partition metadata.
     * @throws StorageException If failed.
     */
    private VersionChainTree createVersionChainTree(
            int partId,
            ReuseList reuseList,
            PersistentPageMemory pageMemory,
            StoragePartitionMeta meta
    ) throws StorageException {
        try {
            boolean initNew = false;

            if (meta.versionChainTreeRootPageId() == 0) {
                long rootPageId = pageMemory.allocatePage(reuseList, getTableId(), partId, FLAG_AUX);

                meta.versionChainTreeRootPageId(lastCheckpointId(), rootPageId);

                initNew = true;
            }

            return new VersionChainTree(
                    getTableId(),
                    Integer.toString(getTableId()),
                    partId,
                    dataRegion.pageMemory(),
                    engine.generateGlobalRemoveId(),
                    meta.versionChainTreeRootPageId(),
                    reuseList,
                    initNew
            );
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error creating VersionChainTree: [tableId={}, partitionId={}]", e, getTableId(), partId);
        }
    }

    /**
     * Returns new {@link IndexMetaTree} instance for partition.
     *
     * @param partitionId Partition ID.
     * @param reuseList Reuse list.
     * @param pageMemory Persistent page memory instance.
     * @param meta Partition metadata.
     * @throws StorageException If failed.
     */
    private IndexMetaTree createIndexMetaTree(
            int partitionId,
            ReuseList reuseList,
            PersistentPageMemory pageMemory,
            StoragePartitionMeta meta
    ) {
        try {
            boolean initNew = false;

            if (meta.indexTreeMetaPageId() == 0) {
                long rootPageId = pageMemory.allocatePage(reuseList, getTableId(), partitionId, FLAG_AUX);

                meta.indexTreeMetaPageId(lastCheckpointId(), rootPageId);

                initNew = true;
            }

            return new IndexMetaTree(
                    getTableId(),
                    Integer.toString(getTableId()),
                    partitionId,
                    dataRegion.pageMemory(),
                    engine.generateGlobalRemoveId(),
                    meta.indexTreeMetaPageId(),
                    reuseList,
                    initNew
            );
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error creating IndexMetaTree: [tableId={}, partitionId={}]", e, getTableId(), partitionId);
        }
    }

    /**
     * Returns new {@link GcQueue} instance for partition.
     *
     * @param partitionId Partition ID.
     * @param reuseList Reuse list.
     * @param pageMemory Persistent page memory instance.
     * @param meta Partition metadata.
     * @throws StorageException If failed.
     */
    private GcQueue createGcQueue(
            int partitionId,
            ReuseList reuseList,
            PersistentPageMemory pageMemory,
            StoragePartitionMeta meta
    ) {
        try {
            boolean initNew = false;

            if (meta.gcQueueMetaPageId() == 0) {
                long rootPageId = pageMemory.allocatePage(reuseList, getTableId(), partitionId, FLAG_AUX);

                meta.gcQueueMetaPageId(lastCheckpointId(), rootPageId);

                initNew = true;
            }

            return new GcQueue(
                    getTableId(),
                    Integer.toString(getTableId()),
                    partitionId,
                    dataRegion.pageMemory(),
                    engine.generateGlobalRemoveId(),
                    meta.gcQueueMetaPageId(),
                    reuseList,
                    initNew
            );
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error creating GarbageCollectionTree: [tableId={}, partitionId={}]", e, getTableId(), partitionId);
        }
    }

    @Override
    CompletableFuture<Void> destroyMvPartitionStorage(AbstractPageMemoryMvPartitionStorage mvPartitionStorage) {
        // It is enough for us to close the partition storage and its indexes (do not destroy). Prepare the data region, checkpointer, and
        // compactor to remove the partition, and then simply delete the partition file and its delta files.
        boolean transitioned = mvPartitionStorage.transitionToDestroyedState();
        if (transitioned) {
            mvPartitionStorage.closeResources();
        }

        return destroyPartitionPhysically(createGroupPartitionId(mvPartitionStorage.partitionId()));
    }

    @Override
    CompletableFuture<Void> clearStorageAndUpdateDataStructures(
            AbstractPageMemoryMvPartitionStorage mvPartitionStorage,
            Runnable afterUpdateStructuresCallback
    ) {
        GroupPartitionId groupPartitionId = createGroupPartitionId(mvPartitionStorage.partitionId());

        return destroyPartitionPhysically(groupPartitionId).thenAccept(unused -> {
            PersistentPageMemory pageMemory = dataRegion.pageMemory();

            int partitionId = groupPartitionId.getPartitionId();

            StoragePartitionMeta meta = getOrCreatePartitionMetaOnCreatePartition(groupPartitionId);

            inCheckpointLock(() -> {
                FreeListImpl freeList = createFreeList(partitionId, pageMemory, meta);

                VersionChainTree versionChainTree = createVersionChainTree(partitionId, freeList, pageMemory, meta);

                IndexMetaTree indexMetaTree = createIndexMetaTree(partitionId, freeList, pageMemory, meta);

                GcQueue gcQueue = createGcQueue(partitionId, freeList, pageMemory, meta);

                ((PersistentPageMemoryMvPartitionStorage) mvPartitionStorage).updateDataStructures(
                        meta,
                        freeList,
                        versionChainTree,
                        indexMetaTree,
                        gcQueue
                );

                afterUpdateStructuresCallback.run();

                return null;
            });
        });
    }

    private CompletableFuture<Void> destroyPartitionPhysically(GroupPartitionId groupPartitionId) {
        dataRegion.filePageStoreManager().getStore(groupPartitionId).markToDestroy();

        Lock partitionDestructionLock = dataRegion.checkpointManager().partitionDestructionLockManager().destructionLock(groupPartitionId)
                .writeLock();

        partitionDestructionLock.lock();

        try {
            dataRegion.pageMemory().invalidate(groupPartitionId.getGroupId(), groupPartitionId.getPartitionId());

            dataRegion.partitionMetaManager().removeMeta(groupPartitionId);

            return dataRegion.filePageStoreManager().destroyPartition(groupPartitionId);
        } finally {
            partitionDestructionLock.unlock();
        }
    }

    private GroupPartitionId createGroupPartitionId(int partitionId) {
        return new GroupPartitionId(getTableId(), partitionId);
    }

    private <V> V inCheckpointLock(Supplier<V> supplier) {
        CheckpointTimeoutLock checkpointTimeoutLock = dataRegion.checkpointManager().checkpointTimeoutLock();

        checkpointTimeoutLock.checkpointReadLock();

        try {
            return supplier.get();
        } finally {
            checkpointTimeoutLock.checkpointReadUnlock();
        }
    }

    /**
     * Creates or receives partition meta from a file.
     *
     * <p>Safe to use without a checkpointReadLock as we read the meta directly without using {@link PageMemory}.</p>
     *
     * @param groupPartitionId Partition of the group.
     */
    private StoragePartitionMeta getOrCreatePartitionMetaOnCreatePartition(GroupPartitionId groupPartitionId) {
        ByteBuffer buffer = allocateBuffer(dataRegion.pageMemory().pageSize());

        try {
            FilePageStore filePageStore = dataRegion.filePageStoreManager().getStore(groupPartitionId);

            // TODO: IGNITE-20983 This shouldn't happen, we should read the page store and its meta again
            if (filePageStore != null) {
                StoragePartitionMeta partitionMeta = (StoragePartitionMeta) dataRegion.partitionMetaManager().getMeta(groupPartitionId);

                assert partitionMeta != null : groupPartitionId;

                return partitionMeta;
            }

            filePageStore = readOrCreateAndInitFilePageStore(groupPartitionId, buffer);

            StoragePartitionMeta partitionMeta = readOrCreatePartitionMeta(groupPartitionId, filePageStore, buffer.rewind());

            filePageStore.pages(partitionMeta.pageCount());

            filePageStore.setPageAllocationListener(pageIdx -> {
                assert dataRegion.checkpointManager().checkpointTimeoutLock().checkpointLockIsHeldByThread();

                partitionMeta.incrementPageCount(lastCheckpointId());
            });

            dataRegion.filePageStoreManager().addStore(groupPartitionId, filePageStore);
            dataRegion.partitionMetaManager().addMeta(groupPartitionId, partitionMeta);

            if (filePageStore.deltaFileCount() > 0) {
                dataRegion.checkpointManager().triggerCompaction();
            }

            return partitionMeta;
        } finally {
            freeBuffer(buffer);
        }
    }

    private FilePageStore readOrCreateAndInitFilePageStore(
            GroupPartitionId groupPartitionId,
            ByteBuffer buffer
    ) throws StorageException {
        try {
            FilePageStore filePageStore = dataRegion.filePageStoreManager().readOrCreateStore(groupPartitionId, buffer);

            assert !filePageStore.isMarkedToDestroy() : IgniteStringFormatter.format(
                    "Should not be marked for deletion: [tableId={}, partitionId={}]",
                    groupPartitionId.getGroupId(),
                    groupPartitionId.getPartitionId()
            );

            filePageStore.ensure();

            return filePageStore;
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException(
                    "Error read and initializing file page store: [tableId={}, partitionId={}]",
                    e,
                    groupPartitionId.getGroupId(), groupPartitionId.getPartitionId()
            );
        }
    }

    private StoragePartitionMeta readOrCreatePartitionMeta(
            GroupPartitionId groupPartitionId,
            FilePageStore filePageStore,
            ByteBuffer buffer
    ) throws StorageException {
        try {
            int partGen = dataRegion.pageMemory().partGeneration(groupPartitionId.getGroupId(), groupPartitionId.getPartitionId());

            return (StoragePartitionMeta) dataRegion.partitionMetaManager()
                    .readOrCreateMeta(lastCheckpointId(), groupPartitionId, filePageStore, buffer, partGen);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException(
                    "Error reading or creating partition meta information: [tableId={}, partitionId={}]",
                    e,
                    getTableId(), groupPartitionId.getPartitionId()
            );
        }
    }

    @Override
    protected void beforeCloseOrDestroy() {
        dataRegion.removeTableStorage(this);
    }
}
