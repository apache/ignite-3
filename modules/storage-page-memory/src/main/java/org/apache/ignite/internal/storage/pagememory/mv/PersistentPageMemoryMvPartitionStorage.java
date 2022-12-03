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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.pagememory.DataRegion;
import org.apache.ignite.internal.pagememory.datapage.DataPageReader;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.pagememory.persistence.PartitionMeta;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointListener;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointManager;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointTimeoutLock;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RaftGroupConfiguration;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryTableStorage;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryStorageEngineView;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumns;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumnsFreeList;
import org.apache.ignite.internal.storage.pagememory.index.hash.PageMemoryHashIndexStorage;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMetaTree;
import org.apache.ignite.internal.storage.pagememory.index.sorted.PageMemorySortedIndexStorage;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteInternalException;
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
    private final PartitionMeta meta;

    /** Value of currently persisted last applied index. */
    private volatile long persistedIndex;

    /** Checkpoint listener. */
    private final CheckpointListener checkpointListener;

    /** FreeList for blobs. */
    private final BlobFreeList blobFreeList;

    /** Used to read data from pagememory pages. */
    private final DataPageReader pageReader;

    /** Lock that protects group config read/write. */
    private final ReadWriteLock groupConfigReadWriteLock = new ReentrantReadWriteLock();

    /**
     * Constructor.
     *
     * @param tableStorage Table storage.
     * @param partitionId Partition id.
     * @param meta Partition meta.
     * @param rowVersionFreeList Free list for {@link RowVersion}.
     * @param indexFreeList Free list fot {@link IndexColumns}.
     * @param versionChainTree Table tree for {@link VersionChain}.
     * @param indexMetaTree Tree that contains SQL indexes' metadata.
     * @param blobFreeList Free list for {@link Blob}.
     */
    public PersistentPageMemoryMvPartitionStorage(
            PersistentPageMemoryTableStorage tableStorage,
            int partitionId,
            PartitionMeta meta,
            RowVersionFreeList rowVersionFreeList,
            IndexColumnsFreeList indexFreeList,
            VersionChainTree versionChainTree,
            IndexMetaTree indexMetaTree,
            BlobFreeList blobFreeList,
            TablesConfiguration tablesCfg
    ) {
        super(partitionId, tableStorage, rowVersionFreeList, indexFreeList, versionChainTree, indexMetaTree, tablesCfg);

        checkpointManager = tableStorage.engine().checkpointManager();
        checkpointTimeoutLock = checkpointManager.checkpointTimeoutLock();

        DataRegion<PersistentPageMemory> dataRegion = tableStorage.dataRegion();

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

            @Override
            public void afterCheckpointEnd(CheckpointProgress progress) {
                persistedIndex = meta.metaSnapshot(progress.id()).lastAppliedIndex();
            }
        }, dataRegion);

        this.meta = meta;

        this.blobFreeList = blobFreeList;

        pageReader = new DataPageReader(
                dataRegion.pageMemory(),
                tableStorage.configuration().value().tableId(),
                IoStatisticsHolderNoOp.INSTANCE
        );
    }

    @Override
    public <V> V runConsistently(WriteClosure<V> closure) throws StorageException {
        if (!closeBusyLock.enterBusy()) {
            throwStorageClosedException();
        }

        try {
            checkpointTimeoutLock.checkpointReadLock();

            try {
                return closure.execute();
            } finally {
                checkpointTimeoutLock.checkpointReadUnlock();
            }
        } finally {
            closeBusyLock.leaveBusy();
        }
    }

    @Override
    public CompletableFuture<Void> flush() {
        if (!closeBusyLock.enterBusy()) {
            throwStorageClosedException();
        }

        try {
            CheckpointProgress lastCheckpoint = checkpointManager.lastCheckpointProgress();

            CheckpointProgress scheduledCheckpoint;

            if (lastCheckpoint != null && meta.metaSnapshot(lastCheckpoint.id()).lastAppliedIndex() == meta.lastAppliedIndex()) {
                scheduledCheckpoint = lastCheckpoint;
            } else {
                var persistentTableStorage = (PersistentPageMemoryTableStorage) tableStorage;

                PersistentPageMemoryStorageEngineView engineCfg = persistentTableStorage.engine().configuration().value();

                int checkpointDelayMillis = engineCfg.checkpoint().checkpointDelayMillis();
                scheduledCheckpoint = checkpointManager.scheduleCheckpoint(checkpointDelayMillis, "Triggered by replicator");
            }

            return scheduledCheckpoint.futureFor(CheckpointState.FINISHED).thenApply(res -> null);
        } finally {
            closeBusyLock.leaveBusy();
        }
    }

    @Override
    public long lastAppliedIndex() {
        if (!closeBusyLock.enterBusy()) {
            throwStorageClosedException();
        }

        try {
            return meta.lastAppliedIndex();
        } finally {
            closeBusyLock.leaveBusy();
        }
    }

    @Override
    public long lastAppliedTerm() {
        if (!closeBusyLock.enterBusy()) {
            throwStorageClosedException();
        }

        try {
            return meta.lastAppliedTerm();
        } finally {
            closeBusyLock.leaveBusy();
        }
    }

    @Override
    public void lastApplied(long lastAppliedIndex, long lastAppliedTerm) throws StorageException {
        assert checkpointTimeoutLock.checkpointLockIsHeldByThread();

        CheckpointProgress lastCheckpoint = checkpointManager.lastCheckpointProgress();

        UUID lastCheckpointId = lastCheckpoint == null ? null : lastCheckpoint.id();

        meta.lastApplied(lastCheckpointId, lastAppliedIndex, lastAppliedTerm);
    }

    @Override
    public long persistedIndex() {
        if (!closeBusyLock.enterBusy()) {
            throwStorageClosedException();
        }

        try {
            return persistedIndex;
        } finally {
            closeBusyLock.leaveBusy();
        }
    }

    @Override
    @Nullable
    public RaftGroupConfiguration committedGroupConfiguration() {
        if (!closeBusyLock.enterBusy()) {
            throwStorageClosedException();
        }

        try {
            groupConfigReadWriteLock.readLock().lock();

            try {
                long configLink = meta.lastGroupConfigLink();

                if (configLink == 0) {
                    return null;
                }

                ReadBlob readBlob = new ReadBlob();

                pageReader.traverse(configLink, readBlob, null);

                return groupConfigFromBytes(readBlob.result());
            } finally {
                groupConfigReadWriteLock.readLock().unlock();
            }
        } catch (IgniteInternalCheckedException e) {
            throw new IgniteInternalException("Failed to read group config", e);
        } finally {
            closeBusyLock.leaveBusy();
        }
    }

    @Override
    public void committedGroupConfiguration(RaftGroupConfiguration config) {
        assert checkpointTimeoutLock.checkpointLockIsHeldByThread();

        CheckpointProgress lastCheckpoint = checkpointManager.lastCheckpointProgress();

        UUID lastCheckpointId = lastCheckpoint == null ? null : lastCheckpoint.id();

        Blob blob = new Blob(partitionId, groupConfigToBytes(config));

        groupConfigReadWriteLock.writeLock().lock();

        try {
            if (meta.lastGroupConfigLink() != 0) {
                try {
                    blobFreeList.removeBlobByLink(meta.lastGroupConfigLink());
                } catch (IgniteInternalCheckedException e) {
                    throw new IgniteInternalException("Failed to remove old group config", e);
                }
            }

            try {
                blobFreeList.insertBlob(blob);
            } catch (IgniteInternalCheckedException e) {
                throw new IgniteInternalException("Failed to save group config", e);
            }

            meta.lastGroupConfigLink(lastCheckpointId, blob.link());
        } finally {
            groupConfigReadWriteLock.writeLock().unlock();
        }
    }

    @Nullable
    private static RaftGroupConfiguration groupConfigFromBytes(byte @Nullable [] bytes) {
        if (bytes == null) {
            return null;
        }

        return ByteUtils.fromBytes(bytes);
    }

    private static byte[] groupConfigToBytes(RaftGroupConfiguration config) {
        return ByteUtils.toBytes(config);
    }

    @Override
    public PageMemoryHashIndexStorage getOrCreateHashIndex(UUID indexId) {
        return runConsistently(() -> super.getOrCreateHashIndex(indexId));
    }

    @Override
    public PageMemorySortedIndexStorage getOrCreateSortedIndex(UUID indexId) {
        return runConsistently(() -> super.getOrCreateSortedIndex(indexId));
    }

    @Override
    public void close() {
        if (!STARTED.compareAndSet(this, true, false)) {
            return;
        }

        closeBusyLock.block();

        checkpointManager.removeCheckpointListener(checkpointListener);

        rowVersionFreeList.close();
        indexFreeList.close();

        versionChainTree.close();
        indexMetaTree.close();

        blobFreeList.close();

        for (PageMemoryHashIndexStorage hashIndexStorage : hashIndexes.values()) {
            hashIndexStorage.close();
        }

        for (PageMemorySortedIndexStorage sortedIndexStorage : sortedIndexes.values()) {
            sortedIndexStorage.close();
        }

        hashIndexes.clear();
        sortedIndexes.clear();
    }

    /**
     * Syncs and saves meta-information on checkpoint.
     *
     * @param executor Executor for asynchronous data synchronization.
     * @throws IgniteInternalCheckedException If failed.
     */
    private void syncMetadataOnCheckpoint(@Nullable Executor executor) throws IgniteInternalCheckedException {
        if (executor == null) {
            rowVersionFreeList.saveMetadata();

            indexFreeList.saveMetadata();

            blobFreeList.saveMetadata();
        } else {
            executor.execute(() -> {
                try {
                    rowVersionFreeList.saveMetadata();
                } catch (IgniteInternalCheckedException e) {
                    throw new IgniteInternalException("Failed to save RowVersionFreeList metadata", e);
                }
            });

            executor.execute(() -> {
                try {
                    indexFreeList.saveMetadata();
                } catch (IgniteInternalCheckedException e) {
                    throw new IgniteInternalException("Failed to save IndexColumnsFreeList metadata", e);
                }
            });

            executor.execute(() -> {
                try {
                    blobFreeList.saveMetadata();
                } catch (IgniteInternalCheckedException e) {
                    throw new IgniteInternalException("Failed to save BlobFreeList metadata", e);
                }
            });
        }
    }
}
