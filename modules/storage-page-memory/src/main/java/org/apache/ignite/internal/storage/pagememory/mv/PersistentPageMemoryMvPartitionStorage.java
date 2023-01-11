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
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.pagememory.persistence.PartitionMeta;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointListener;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointManager;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointTimeoutLock;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
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

    private final BlobStorage blobStorage;

    /** Lock that protects group config read/write. */
    private final ReadWriteLock replicationProtocolGroupConfigReadWriteLock = new ReentrantReadWriteLock();

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
     */
    public PersistentPageMemoryMvPartitionStorage(
            PersistentPageMemoryTableStorage tableStorage,
            int partitionId,
            PartitionMeta meta,
            RowVersionFreeList rowVersionFreeList,
            IndexColumnsFreeList indexFreeList,
            VersionChainTree versionChainTree,
            IndexMetaTree indexMetaTree
    ) {
        super(partitionId, tableStorage, rowVersionFreeList, indexFreeList, versionChainTree, indexMetaTree);

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

        blobStorage = new BlobStorage(
                rowVersionFreeList,
                dataRegion.pageMemory(),
                tableStorage.configuration().value().tableId(),
                partitionId,
                IoStatisticsHolderNoOp.INSTANCE
        );
    }

    @Override
    public <V> V runConsistently(WriteClosure<V> closure) throws StorageException {
        return busy(() -> {
            checkpointTimeoutLock.checkpointReadLock();

            try {
                return closure.execute();
            } finally {
                checkpointTimeoutLock.checkpointReadUnlock();
            }
        });
    }

    @Override
    public CompletableFuture<Void> flush() {
        return busy(() -> {
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
        });
    }

    @Override
    public long lastAppliedIndex() {
        return busy(meta::lastAppliedIndex);
    }

    @Override
    public long lastAppliedTerm() {
        return busy(meta::lastAppliedTerm);
    }

    @Override
    public void lastApplied(long lastAppliedIndex, long lastAppliedTerm) throws StorageException {
        assert checkpointTimeoutLock.checkpointLockIsHeldByThread();

        checkIsStorageInProcessOfRebalance();

        CheckpointProgress lastCheckpoint = checkpointManager.lastCheckpointProgress();

        UUID lastCheckpointId = lastCheckpoint == null ? null : lastCheckpoint.id();

        meta.lastApplied(lastCheckpointId, lastAppliedIndex, lastAppliedTerm);
    }

    @Override
    public long persistedIndex() {
        return busy(() -> persistedIndex);
    }

    @Override
    @Nullable
    public RaftGroupConfiguration committedGroupConfiguration() {
        return busy(() -> {
            checkIsStorageInProcessOfRebalance();

            try {
                replicationProtocolGroupConfigReadWriteLock.readLock().lock();

                try {
                    long configFirstPageId = meta.lastReplicationProtocolGroupConfigFirstPageId();

                    if (configFirstPageId == BlobStorage.NO_PAGE_ID) {
                        return null;
                    }

                    byte[] bytes = blobStorage.readBlob(meta.lastReplicationProtocolGroupConfigFirstPageId());

                    return replicationProtocolGroupConfigFromBytes(bytes);
                } finally {
                    replicationProtocolGroupConfigReadWriteLock.readLock().unlock();
                }
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException("Failed to read group config, groupId=" + groupId + ", partitionId=" + partitionId, e);
            }
        });
    }

    @Override
    public void committedGroupConfiguration(RaftGroupConfiguration config) {
        assert checkpointTimeoutLock.checkpointLockIsHeldByThread();

        checkIsStorageInProcessOfRebalance();

        CheckpointProgress lastCheckpoint = checkpointManager.lastCheckpointProgress();
        UUID lastCheckpointId = lastCheckpoint == null ? null : lastCheckpoint.id();

        byte[] groupConfigBytes = replicationProtocolGroupConfigToBytes(config);

        replicationProtocolGroupConfigReadWriteLock.writeLock().lock();

        try {
            if (meta.lastReplicationProtocolGroupConfigFirstPageId() == BlobStorage.NO_PAGE_ID) {
                long configPageId = blobStorage.addBlob(groupConfigBytes);

                meta.lastReplicationProtocolGroupConfigFirstPageId(lastCheckpointId, configPageId);
            } else {
                blobStorage.updateBlob(meta.lastReplicationProtocolGroupConfigFirstPageId(), groupConfigBytes);
            }
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Cannot save committed group configuration, groupId=" + groupId + ", partitionId=" + groupId, e);
        } finally {
            replicationProtocolGroupConfigReadWriteLock.writeLock().unlock();
        }
    }

    @Nullable
    private static RaftGroupConfiguration replicationProtocolGroupConfigFromBytes(byte @Nullable [] bytes) {
        if (bytes == null) {
            return null;
        }

        return ByteUtils.fromBytes(bytes);
    }

    private static byte[] replicationProtocolGroupConfigToBytes(RaftGroupConfiguration config) {
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
    void closeAdditionalResources() {
        checkpointManager.removeCheckpointListener(checkpointListener);

        rowVersionFreeList.close();
        indexFreeList.close();
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
        }
    }
}
