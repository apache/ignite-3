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

import static org.apache.ignite.internal.pagememory.util.PageIdUtils.NULL_LINK;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionIfStorageNotInCleanupOrRebalancedState;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionIfStorageNotInProgressOfRebalance;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionIfStorageNotInRunnableOrRebalanceState;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwStorageExceptionIfItCause;
import static org.apache.ignite.internal.util.ByteUtils.stringToBytes;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.DataRegion;
import org.apache.ignite.internal.pagememory.freelist.FreeListImpl;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointListener;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointManager;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointTimeoutLock;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.util.GradualTaskExecutor;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.storage.lease.LeaseInfo;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryTableStorage;
import org.apache.ignite.internal.storage.pagememory.StoragePartitionMeta;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryStorageEngineView;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMetaTree;
import org.apache.ignite.internal.storage.pagememory.mv.gc.GcQueue;
import org.apache.ignite.internal.storage.pagememory.mv.gc.GcRowVersion;
import org.apache.ignite.internal.storage.util.LocalLocker;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.Cursor;
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
     * Cached lease info in order not to touch blobStorage each time.
     */
    private volatile @Nullable LeaseInfo leaseInfo;

    private long wiHeadLink = NULL_LINK;

    private final ReentrantLock wiHeadLock = new ReentrantLock();

    /**
     * Lock for updating lease info in the storage.
     */
    private final Object leaseInfoLock = new Object();

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
     * @param failureProcessor Failure processor.
     */
    public PersistentPageMemoryMvPartitionStorage(
            PersistentPageMemoryTableStorage tableStorage,
            int partitionId,
            StoragePartitionMeta meta,
            FreeListImpl freeList,
            VersionChainTree versionChainTree,
            IndexMetaTree indexMetaTree,
            GcQueue gcQueue,
            ExecutorService destructionExecutor,
            FailureProcessor failureProcessor
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
                destructionExecutor,
                failureProcessor
        );

        checkpointManager = tableStorage.engine().checkpointManager();
        checkpointTimeoutLock = checkpointManager.checkpointTimeoutLock();

        DataRegion<PersistentPageMemory> dataRegion = tableStorage.dataRegion();

        setNewMeta(meta);

        checkpointManager.addCheckpointListener(checkpointListener = new CheckpointListener() {
            @Override
            public void beforeCheckpointBegin(CheckpointProgress progress, @Nullable Executor exec) {
                // It may take some time, it's not scary because we keep a read lock here.
                syncMetadataOnCheckpoint(exec);
            }

            @Override
            public void onMarkCheckpointBegin(CheckpointProgress progress, @Nullable Executor exec) {
                // Should be fast, because here we only need to save the delta, reduce write lock holding time.
                syncMetadataOnCheckpoint(exec);
            }
        }, dataRegion);

        blobStorage = new BlobStorage(
                freeList,
                dataRegion.pageMemory(),
                tableStorage.getTableId(),
                partitionId
        );

        leaseInfo = leaseInfoFromMeta();
    }

    /**
     * Updates the {@link #meta} fields and all other values associated with it.
     * @param meta New instance for partition's meta.
     */
    private void setNewMeta(StoragePartitionMeta meta) {
        this.meta = meta;
        this.wiHeadLink = meta.wiHeadLink();
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

                LocalLocker locker0 = new PersistentPageMemoryLocker();

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
        void update(@Nullable UUID lastCheckpointId, StoragePartitionMeta meta);
    }

    /**
     * Updates partition meta. Hides all the necessary boilderplate in a single place.
     */
    private void updateMeta(MetaUpdateClosure closure) {
        assert checkpointTimeoutLock.checkpointLockIsHeldByThread();

        CheckpointProgress lastCheckpoint = checkpointManager.lastCheckpointProgress();

        UUID lastCheckpointId = lastCheckpoint == null ? null : lastCheckpoint.id();

        closure.update(lastCheckpointId, meta);

        checkpointManager.markPartitionAsDirty(
                tableStorage.dataRegion(),
                tableStorage.getTableId(),
                partitionId,
                meta.partitionGeneration()
        );
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
    public void updateLease(LeaseInfo leaseInfo) {
        busy(() -> {
            throwExceptionIfStorageNotInRunnableState();

            updateLeaseBusy(leaseInfo);

            return null;
        });
    }

    private void updateLeaseBusy(LeaseInfo leaseInfo) {
        updateMeta((lastCheckpointId, meta) -> {
            synchronized (leaseInfoLock) {
                if (leaseInfo.leaseStartTime() <= meta.leaseStartTime()) {
                    return;
                }

                try {
                    byte[] primaryReplicaNodeNameBytes = stringToBytes(leaseInfo.primaryReplicaNodeName());

                    if (meta.primaryReplicaNodeNameFirstPageId() == BlobStorage.NO_PAGE_ID) {
                        long primaryReplicaNodeNameFirstPageId = blobStorage.addBlob(primaryReplicaNodeNameBytes);

                        meta.primaryReplicaNodeNameFirstPageId(lastCheckpointId, primaryReplicaNodeNameFirstPageId);
                    } else {
                        blobStorage.updateBlob(meta.primaryReplicaNodeNameFirstPageId(), primaryReplicaNodeNameBytes);
                    }

                    meta.primaryReplicaNodeId(lastCheckpointId, leaseInfo.primaryReplicaNodeId());
                    meta.updateLease(lastCheckpointId, leaseInfo.leaseStartTime());
                } catch (IgniteInternalCheckedException e) {
                    throw new StorageException(
                            "Cannot save lease meta: [tableId={}, partitionId={}]",
                            e,
                            tableStorage.getTableId(), partitionId
                    );
                }

                this.leaseInfo = leaseInfo;
            }
        });
    }

    /**
     * Retrieves the link to the head of the write intent list for the partition and locks the head.
     *
     * <p>If the list is empty, it returns a @{NULL_LINK}.
     */
    long lockWriteIntentListHead() {
        return busy(() -> {
            throwExceptionIfStorageNotInRunnableOrRebalanceState(state.get(), this::createStorageInfo);

            wiHeadLock.lock();

            return wiHeadLink;
        });
    }

    /**
     * Update a head link in partition metadata and unlocks the head.
     *
     * @param wiHeadLink Link to the first write intents list element, or {@code NULL_LINK} if the list is empty.
     */
    void updateWriteIntentListHeadAndUnlock(long wiHeadLink) {
        try {
            if (wiHeadLink == this.wiHeadLink) {
                return;
            }

            busy(() -> {
                throwExceptionIfStorageNotInRunnableOrRebalanceState(state.get(), this::createStorageInfo);

                this.wiHeadLink = wiHeadLink;

                updateWiHeadBusy(wiHeadLink);
            });
        } finally {
            wiHeadLock.unlock();
        }
    }

    private void updateWiHeadBusy(long link) {
        updateMeta((lastCheckpointId, meta) -> {
            meta.updateWiHead(lastCheckpointId, link);
        });
    }

    boolean writeIntentHeadIsLockedByCurrentThread() {
        return wiHeadLock.isHeldByCurrentThread();
    }

    @Override
    public @Nullable LeaseInfo leaseInfo() {
        return busy(() -> {
            throwExceptionIfStorageNotInRunnableState();

            return leaseInfo;
        });
    }

    private @Nullable LeaseInfo leaseInfoFromMeta() {
        long primaryReplicaNodeNameFirstPageId = meta.primaryReplicaNodeNameFirstPageId();

        // It's possible to face BlobStorage.NO_PAGE_ID if a lease information has not yet been recorded in storage,
        // for example, if the lease itself has not yet been elected.
        if (primaryReplicaNodeNameFirstPageId == BlobStorage.NO_PAGE_ID) {
            return null;
        }

        String primaryReplicaNodeName;
        try {
            primaryReplicaNodeName = ByteUtils.stringFromBytes(blobStorage.readBlob(primaryReplicaNodeNameFirstPageId));
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException(
                    "Failed to read primary replica node name: [tableId={}, partitionId={}]",
                    e,
                    tableStorage.getTableId(), partitionId
            );
        }

        return new LeaseInfo(meta.leaseStartTime(), meta.primaryReplicaNodeId(), primaryReplicaNodeName);
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
    public void createHashIndex(StorageHashIndexDescriptor indexDescriptor) {
        runConsistently(locker -> {
            super.createHashIndex(indexDescriptor);
            return null;
        });
    }

    @Override
    public void createSortedIndex(StorageSortedIndexDescriptor indexDescriptor) {
        runConsistently(locker -> {
            super.createSortedIndex(indexDescriptor);
            return null;
        });
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

        setNewMeta(meta);

        this.blobStorage = new BlobStorage(
                freeList,
                tableStorage.dataRegion().pageMemory(),
                tableStorage.getTableId(),
                partitionId
        );

        updateRenewableState(
                versionChainTree,
                freeList,
                indexMetaTree,
                gcQueue
        );

        checkpointManager.addCheckpointListener(checkpointListener, tableStorage.dataRegion());
    }

    @Override
    List<AutoCloseable> getResourcesToCloseOnCleanup() {
        RenewablePartitionStorageState localState = renewableState;

        return List.of(
                () -> checkpointManager.removeCheckpointListener(checkpointListener),
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

    @Override
    public void updateLeaseOnRebalance(LeaseInfo leaseInfo) {
        throwExceptionIfStorageNotInProgressOfRebalance(state.get(), this::createStorageInfo);

        updateLeaseBusy(leaseInfo);
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

    /**
     * Returns a total number of allocated pages for the storage, including pages that are not in the memory currently.
     */
    public int pageCount() {
        return meta.pageCount();
    }

    @Override
    public void incrementEstimatedSize() {
        updateMeta((lastCheckpointId, meta) -> meta.incrementEstimatedSize(lastCheckpointId));
    }

    @Override
    public void decrementEstimatedSize() {
        updateMeta((lastCheckpointId, meta) -> meta.decrementEstimatedSize(lastCheckpointId));
    }

    public int emptyDataPageCountInFreeList() {
        return renewableState.freeList().emptyDataPages();
    }

    @Override
    public Cursor<RowId> scanWriteIntents() {
        return busy(() -> new WriteIntentsCursor(lockWriteIntentListHead()));
    }

    @Override
    AddWriteLinkingWiInvokeClosure newAddWriteInvokeClosure(
            RowId rowId,
            @Nullable BinaryRow row,
            UUID txId,
            int commitZoneId,
            int commitPartitionId
    ) {
        return new AddWriteLinkingWiInvokeClosure(rowId, row, txId, commitZoneId, commitPartitionId, this);
    }

    WriteIntentLinks readWriteIntentLinks(long rowVersionLink) {
        var read = new ReadWriteIntentLinks(partitionId);

        try {
            rowVersionDataPageReader.traverse(rowVersionLink, read, null);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Write intent links lookup failed: [link={}, {}]", e, rowVersionLink, createStorageInfo());
        }

        return read.result();
    }

    /**
     * This optimization reduces the IO operations performed when executing {@link #vacuum} inside {@link #runConsistently}, which is
     * currently executed in a loop. This will allow the checkpoint to acquire a write lock more quickly.
     */
    @Override
    protected void preloadingForGcIfNeededBusy(GcRowVersion gcRowVersion) {
        RowId rowId = gcRowVersion.getRowId();
        HybridTimestamp timestamp = gcRowVersion.getTimestamp();

        var preloadingForGc = new PreloadingForGcInvokeClosure(rowId, timestamp, gcRowVersion.getLink(), this);

        lockByRowId.lock(rowId);

        try {
            renewableState.versionChainTree().invoke(new VersionChainKey(rowId), null, preloadingForGc);
        } catch (IgniteInternalCheckedException e) {
            throwStorageExceptionIfItCause(e);

            throw new StorageException(
                    "Error preloading row versions for garbage collection: [rowId={}, rowTimestamp={}, {}]",
                    e,
                    rowId, timestamp, createStorageInfo()
            );
        } finally {
            lockByRowId.unlockAll(rowId);
        }
    }

    private class PersistentPageMemoryLocker extends LocalLocker {
        private PersistentPageMemoryLocker() {
            super(lockByRowId);
        }

        @Override
        public boolean shouldRelease() {
            return checkpointTimeoutLock.shouldReleaseReadLock();
        }
    }

    private class WriteIntentsCursor implements Cursor<RowId> {
        private final long headLink;
        private long nextLink;

        private WriteIntentsCursor(long headLink) {
            this.headLink = headLink;
            nextLink = headLink;
        }

        @Override
        public boolean hasNext() {
            return busy(() -> nextLink != NULL_LINK);
        }

        @SuppressWarnings("IteratorNextCanNotThrowNoSuchElementException")
        @Override
        public RowId next() {
            return busy(() -> {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                RowVersion rowVersion = readRowVersion(nextLink, DONT_LOAD_VALUE);

                assert rowVersion instanceof WiLinkableRowVersion;
                WiLinkableRowVersion linkableRowVersion = (WiLinkableRowVersion) rowVersion;

                nextLink = linkableRowVersion.nextWriteIntentLink();

                return linkableRowVersion.requiredRowId();
            });
        }

        @Override
        public void close() {
            updateWriteIntentListHeadAndUnlock(headLink);
        }
    }
}
