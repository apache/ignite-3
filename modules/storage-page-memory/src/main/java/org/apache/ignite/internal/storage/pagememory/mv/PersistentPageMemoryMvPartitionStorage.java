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

package org.apache.ignite.internal.storage.pagememory.mv;

import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionIfStorageNotInCleanupOrRebalancedState;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionIfStorageNotInProgressOfRebalance;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionIfStorageNotInRunnableOrRebalanceState;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.DataRegion;
import org.apache.ignite.internal.pagememory.freelist.FreeListImpl;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointListener;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointManager;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointTimeoutLock;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.util.GradualTaskExecutor;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryTableStorage;
import org.apache.ignite.internal.storage.pagememory.StoragePartitionMeta;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryStorageEngineView;
import org.apache.ignite.internal.storage.pagememory.index.hash.PageMemoryHashIndexStorage;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMetaTree;
import org.apache.ignite.internal.storage.pagememory.index.sorted.PageMemorySortedIndexStorage;
import org.apache.ignite.internal.storage.pagememory.mv.gc.GcQueue;
import org.apache.ignite.internal.storage.util.LocalLocker;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link MvPartitionStorage} based on a {@link BplusTree} for persistent case.
 */
public class PersistentPageMemoryMvPartitionStorage extends AbstractPageMemoryMvPartitionStorage {
    /** Checkpoint manager instance. */
    private final CheckpointManager checkpointManager;

    /** Checkpoint lock instance. */
    private final CheckpointTimeoutLock checkpointTimeoutLock;

    /** Partition meta instance. */
    private volatile StoragePartitionMeta meta;

    /** Checkpoint listener. */
    private final CheckpointListener checkpointListener;

    private volatile BlobStorage blobStorage;

    /** Lock that protects group config read/write. */
    private final ReadWriteLock replicationProtocolGroupConfigReadWriteLock = new ReentrantReadWriteLock();

    /**
     * Constructor.
     *
     * @param tableStorage Table storage.
     * @param partitionId Partition id.
     * @param meta Partition meta.
     * @param freeList Free list.
     * @param versionChainTree Table tree for {@link VersionChain}.
     * @param indexMetaTree Tree that contains SQL indexes' metadata.
     * @param gcQueue Garbage collection queue.
     */
    public PersistentPageMemoryMvPartitionStorage(
            PersistentPageMemoryTableStorage tableStorage,
            int partitionId,
            StoragePartitionMeta meta,
            FreeListImpl freeList,
            VersionChainTree versionChainTree,
            IndexMetaTree indexMetaTree,
            GcQueue gcQueue,
            ExecutorService destructionExecutor
    ) {
        super(
                partitionId,
                tableStorage,
                new RenewablePartitionStorageState(
                        tableStorage,
                        partitionId,
                        versionChainTree,
                        freeList,
                        indexMetaTree,
                        gcQueue
                ),
                destructionExecutor
        );

        checkpointManager = tableStorage.engine().checkpointManager();
        checkpointTimeoutLock = checkpointManager.checkpointTimeoutLock();

        DataRegion<PersistentPageMemory> dataRegion = tableStorage.dataRegion();

        this.meta = meta;

        checkpointManager.addCheckpointListener(checkpointListener = new CheckpointListener() {
            @Override
            public void beforeCheckpointBegin(CheckpointProgress progress, @Nullable Executor exec) throws IgniteInternalCheckedException {
                // It may take some time, it's not scary because we keep a read lock here.
                syncMetadataOnCheckpoint(exec);
            }

            @Override
            public void onMarkCheckpointBegin(CheckpointProgress progress, @Nullable Executor exec) throws IgniteInternalCheckedException {
                // Should be fast, because here we only need to save the delta, reduce write lock holding time.
                syncMetadataOnCheckpoint(exec);
            }
        }, dataRegion);

        blobStorage = new BlobStorage(
                freeList,
                dataRegion.pageMemory(),
                tableStorage.getTableId(),
                partitionId,
                IoStatisticsHolderNoOp.INSTANCE
        );
    }

    @Override
    protected GradualTaskExecutor createGradualTaskExecutor(ExecutorService threadPool) {
        return new ConsistentGradualTaskExecutor(this, threadPool);
    }

    @Override
    public <V> V runConsistently(WriteClosure<V> closure) throws StorageException {
        LocalLocker locker = THREAD_LOCAL_LOCKER.get();

        if (locker != null) {
            return closure.execute(locker);
        } else {
            return busy(() -> {
                throwExceptionIfStorageNotInRunnableOrRebalanceState(state.get(), this::createStorageInfo);

                LocalLocker locker0 = new LocalLocker(lockByRowId);

                checkpointTimeoutLock.checkpointReadLock();

                THREAD_LOCAL_LOCKER.set(locker0);

                try {
                    return closure.execute(locker0);
                } finally {
                    THREAD_LOCAL_LOCKER.set(null);

                    // Can't throw any exception, it's safe to do it without try/finally.
                    locker0.unlockAll();

                    checkpointTimeoutLock.checkpointReadUnlock();
                }
            });
        }
    }

    @Override
    public CompletableFuture<Void> flush(boolean trigger) {
        return busy(() -> {
            throwExceptionIfStorageNotInRunnableOrRebalanceState(state.get(), this::createStorageInfo);

            CheckpointProgress lastCheckpoint = checkpointManager.lastCheckpointProgress();

            CheckpointProgress scheduledCheckpoint;

            if (!trigger) {
                // Scheduling a checkpoint with an "infinite" delay (24+ days to prevent overflow)
                // so the checkpoint will definitely not be triggered.
                scheduledCheckpoint = checkpointManager.scheduleCheckpoint(
                        Integer.MAX_VALUE,
                        "subscribe to next checkpoint"
                );
            } else if (lastCheckpoint != null && meta.metaSnapshot(lastCheckpoint.id()).lastAppliedIndex() == meta.lastAppliedIndex()) {
                scheduledCheckpoint = lastCheckpoint;
            } else {
                var persistentTableStorage = (PersistentPageMemoryTableStorage) tableStorage;

                PersistentPageMemoryStorageEngineView engineCfg = persistentTableStorage.engine().configuration().value();

                int checkpointDelayMillis = engineCfg.checkpoint().checkpointDelayMillis();
                scheduledCheckpoint = checkpointManager.scheduleCheckpoint(checkpointDelayMillis, "Triggered by replicator");
            }

            return scheduledCheckpoint.futureFor(CheckpointState.FINISHED);
        });
    }

    @Override
    public long lastAppliedIndex() {
        return busy(() -> {
            throwExceptionIfStorageNotInRunnableOrRebalanceState(state.get(), this::createStorageInfo);

            return meta.lastAppliedIndex();
        });
    }

    @Override
    public long lastAppliedTerm() {
        return busy(() -> {
            throwExceptionIfStorageNotInRunnableOrRebalanceState(state.get(), this::createStorageInfo);

            return meta.lastAppliedTerm();
        });
    }

    @Override
    public void lastApplied(long lastAppliedIndex, long lastAppliedTerm) throws StorageException {
        busy(() -> {
            throwExceptionIfStorageNotInRunnableState();

            lastAppliedBusy(lastAppliedIndex, lastAppliedTerm);

            return null;
        });
    }

    private void lastAppliedBusy(long lastAppliedIndex, long lastAppliedTerm) throws StorageException {
        updateMeta((lastCheckpointId, meta) -> meta.lastApplied(lastCheckpointId, lastAppliedIndex, lastAppliedTerm));
    }

    /**
     * Closure interface for {@link #update(UUID, StoragePartitionMeta)}.
     */
    @FunctionalInterface
    private interface MetaUpdateClosure {
        void update(UUID lastCheckpointId, StoragePartitionMeta meta);
    }

    /**
     * Updates partition meta. Hides all the necessary boilerplate in a single place.
     */
    private void updateMeta(MetaUpdateClosure closure) {
        assert checkpointTimeoutLock.checkpointLockIsHeldByThread();

        CheckpointProgress lastCheckpoint = checkpointManager.lastCheckpointProgress();

        UUID lastCheckpointId = lastCheckpoint == null ? null : lastCheckpoint.id();

        closure.update(lastCheckpointId, meta);

        checkpointManager.markPartitionAsDirty(tableStorage.dataRegion(), tableStorage.getTableId(), partitionId);
    }

    @Override
    public byte @Nullable [] committedGroupConfiguration() {
        return busy(() -> {
            throwExceptionIfStorageNotInRunnableOrRebalanceState(state.get(), this::createStorageInfo);

            try {
                replicationProtocolGroupConfigReadWriteLock.readLock().lock();

                try {
                    long configFirstPageId = meta.lastReplicationProtocolGroupConfigFirstPageId();

                    if (configFirstPageId == BlobStorage.NO_PAGE_ID) {
                        return null;
                    }

                    return blobStorage.readBlob(meta.lastReplicationProtocolGroupConfigFirstPageId());
                } finally {
                    replicationProtocolGroupConfigReadWriteLock.readLock().unlock();
                }
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException(
                        "Failed to read group config: [tableId={}, partitionId={}]",
                        e,
                        tableStorage.getTableId(), partitionId
                );
            }
        });
    }

    @Override
    public void committedGroupConfiguration(byte[] config) {
        busy(() -> {
            throwExceptionIfStorageNotInRunnableState();

            committedGroupConfigurationBusy(config);

            return null;
        });
    }

    @Override
    public void updateLease(long leaseStartTime) {
        busy(() -> {
            throwExceptionIfStorageNotInRunnableState();

            updateMeta((lastCheckpointId, meta) -> meta.updateLease(lastCheckpointId, leaseStartTime));

            return null;
        });
    }

    @Override
    public long leaseStartTime() {
        return busy(() -> {
            throwExceptionIfStorageNotInRunnableState();

            return meta.leaseStartTime();
        });
    }

    private void committedGroupConfigurationBusy(byte[] groupConfigBytes) {
        updateMeta((lastCheckpointId, meta) -> {
            replicationProtocolGroupConfigReadWriteLock.writeLock().lock();

            try {
                if (meta.lastReplicationProtocolGroupConfigFirstPageId() == BlobStorage.NO_PAGE_ID) {
                    long configPageId = blobStorage.addBlob(groupConfigBytes);

                    meta.lastReplicationProtocolGroupConfigFirstPageId(lastCheckpointId, configPageId);
                } else {
                    blobStorage.updateBlob(meta.lastReplicationProtocolGroupConfigFirstPageId(), groupConfigBytes);
                }
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException(
                        "Cannot save committed group configuration: [tableId={}, partitionId={}]",
                        e,
                        tableStorage.getTableId(), partitionId
                );
            } finally {
                replicationProtocolGroupConfigReadWriteLock.writeLock().unlock();
            }
        });
    }

    @Override
    public PageMemoryHashIndexStorage getOrCreateHashIndex(StorageHashIndexDescriptor indexDescriptor) {
        return runConsistently(locker -> super.getOrCreateHashIndex(indexDescriptor));
    }

    @Override
    public PageMemorySortedIndexStorage getOrCreateSortedIndex(StorageSortedIndexDescriptor indexDescriptor) {
        return runConsistently(locker -> super.getOrCreateSortedIndex(indexDescriptor));
    }

    @Override
    protected List<AutoCloseable> getResourcesToClose() {
        List<AutoCloseable> resourcesToClose = super.getResourcesToClose();

        resourcesToClose.add(() -> checkpointManager.removeCheckpointListener(checkpointListener));

        RenewablePartitionStorageState localState = renewableState;

        resourcesToClose.add(localState.freeList()::close);
        resourcesToClose.add(blobStorage::close);

        return resourcesToClose;
    }

    /**
     * Syncs and saves meta-information on checkpoint.
     *
     * @param executor Executor for asynchronous data synchronization, {@code null} if absent.
     */
    private void syncMetadataOnCheckpoint(@Nullable Executor executor) {
        RenewablePartitionStorageState localState = renewableState;

        if (executor == null) {
            busySafe(() -> {
                saveFreeListMetadataBusy(localState);
            });
        } else {
            executor.execute(() -> busySafe(() -> saveFreeListMetadataBusy(localState)));
        }
    }

    @Override
    public void lastAppliedOnRebalance(long lastAppliedIndex, long lastAppliedTerm) throws StorageException {
        throwExceptionIfStorageNotInProgressOfRebalance(state.get(), this::createStorageInfo);

        lastAppliedBusy(lastAppliedIndex, lastAppliedTerm);
    }

    /**
     * Updates the internal data structures of the storage and its indexes on rebalance or cleanup.
     *
     * @param meta Partition meta.
     * @param freeList Free list.
     * @param versionChainTree Table tree for {@link VersionChain}.
     * @param indexMetaTree Tree that contains SQL indexes' metadata.
     * @param gcQueue Garbage collection queue.
     * @throws StorageException If failed.
     */
    public void updateDataStructures(
            StoragePartitionMeta meta,
            FreeListImpl freeList,
            VersionChainTree versionChainTree,
            IndexMetaTree indexMetaTree,
            GcQueue gcQueue
    ) {
        throwExceptionIfStorageNotInCleanupOrRebalancedState(state.get(), this::createStorageInfo);

        this.meta = meta;

        this.blobStorage = new BlobStorage(
                freeList,
                tableStorage.dataRegion().pageMemory(),
                tableStorage.getTableId(),
                partitionId,
                IoStatisticsHolderNoOp.INSTANCE
        );

        updateRenewableState(
                versionChainTree,
                freeList,
                indexMetaTree,
                gcQueue
        );
    }

    @Override
    List<AutoCloseable> getResourcesToCloseOnCleanup() {
        RenewablePartitionStorageState localState = renewableState;

        return List.of(
                localState.freeList()::close,
                localState.versionChainTree()::close,
                localState.indexMetaTree()::close,
                localState.gcQueue()::close,
                blobStorage::close
        );
    }

    @Override
    public void committedGroupConfigurationOnRebalance(byte[] config) {
        throwExceptionIfStorageNotInProgressOfRebalance(state.get(), this::createStorageInfo);

        committedGroupConfigurationBusy(config);
    }

    private void saveFreeListMetadataBusy(RenewablePartitionStorageState localState) {
        try {
            localState.freeList().saveMetadata();
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Failed to save free list metadata: [{}]", e, createStorageInfo());
        }
    }

    @Override
    public long estimatedSize() {
        return meta.estimatedSize();
    }

    @Override
    public void incrementEstimatedSize() {
        updateMeta((lastCheckpointId, meta) -> meta.incrementEstimatedSize(lastCheckpointId));
    }

    @Override
    public void decrementEstimatedSize() {
        updateMeta((lastCheckpointId, meta) -> meta.decrementEstimatedSize(lastCheckpointId));
    }
}
