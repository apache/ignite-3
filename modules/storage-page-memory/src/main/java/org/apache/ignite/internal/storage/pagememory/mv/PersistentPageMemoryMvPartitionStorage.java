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

package org.apache.ignite.internal.storage.pagememory.mv;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.internal.pagememory.DataRegion;
import org.apache.ignite.internal.pagememory.persistence.PartitionMeta;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointListener;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointManager;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointTimeoutLock;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryTableStorage;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryStorageEngineView;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link MvPartitionStorage} based on a {@link BplusTree} for persistent case.
 */
public class PersistentPageMemoryMvPartitionStorage extends AbstractPageMemoryMvPartitionStorage {
    /** Table storage instance. */
    private final PersistentPageMemoryTableStorage tableStorage;

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

    /**
     * Constructor.
     *
     * @param tableStorage Table storage.
     * @param partitionId Partition id.
     * @param tableView Table configuration.
     * @param dataRegion Data region.
     * @param checkpointManager Checkpoint manager.
     * @param meta Partition meta.
     * @param rowVersionFreeList Free list for {@link RowVersion}.
     * @param versionChainTree Table tree for {@link VersionChain}.
     */
    public PersistentPageMemoryMvPartitionStorage(
            PersistentPageMemoryTableStorage tableStorage,
            int partitionId,
            TableView tableView,
            DataRegion<PersistentPageMemory> dataRegion,
            CheckpointManager checkpointManager,
            PartitionMeta meta,
            RowVersionFreeList rowVersionFreeList,
            VersionChainTree versionChainTree
    ) {
        super(partitionId, tableView, dataRegion.pageMemory(), rowVersionFreeList, versionChainTree);

        this.tableStorage = tableStorage;

        this.checkpointManager = checkpointManager;
        checkpointTimeoutLock = checkpointManager.checkpointTimeoutLock();

        checkpointManager.addCheckpointListener(checkpointListener = new CheckpointListener() {
            /** {@inheritDoc} */
            @Override
            public void beforeCheckpointBegin(CheckpointProgress progress, @Nullable Executor exec) throws IgniteInternalCheckedException {
                // It may take some time, it's not scary because we keep a read lock here.
                syncMetadataOnCheckpoint(exec);
            }

            /** {@inheritDoc} */
            @Override
            public void onMarkCheckpointBegin(CheckpointProgress progress, @Nullable Executor exec) throws IgniteInternalCheckedException {
                // Should be fast, because here we only need to save the delta, reduce write lock holding time.
                syncMetadataOnCheckpoint(exec);
            }

            /** {@inheritDoc} */
            @Override
            public void afterCheckpointEnd(CheckpointProgress progress) {
                persistedIndex = meta.metaSnapshot(progress.id()).lastAppliedIndex();
            }
        }, dataRegion);

        this.meta = meta;
    }

    /** {@inheritDoc} */
    @Override
    public <V> V runConsistently(WriteClosure<V> closure) throws StorageException {
        checkpointTimeoutLock.checkpointReadLock();

        try {
            return closure.execute();
        } finally {
            checkpointTimeoutLock.checkpointReadUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> flush() {
        CheckpointProgress lastCheckpoint = checkpointManager.lastCheckpointProgress();

        CheckpointProgress scheduledCheckpoint;

        if (lastCheckpoint != null && meta.metaSnapshot(lastCheckpoint.id()).lastAppliedIndex() == meta.lastAppliedIndex()) {
            scheduledCheckpoint = lastCheckpoint;
        } else {
            PersistentPageMemoryStorageEngineView engineCfg = tableStorage.engine().configuration().value();

            int checkpointDelayMillis = engineCfg.checkpoint().checkpointDelayMillis();
            scheduledCheckpoint = checkpointManager.scheduleCheckpoint(checkpointDelayMillis, "Triggered by replicator");
        }

        return scheduledCheckpoint.futureFor(CheckpointState.FINISHED).thenApply(res -> null);
    }

    /** {@inheritDoc} */
    @Override
    public long lastAppliedIndex() {
        return meta.lastAppliedIndex();
    }

    /** {@inheritDoc} */
    @Override
    public void lastAppliedIndex(long lastAppliedIndex) throws StorageException {
        assert checkpointTimeoutLock.checkpointLockIsHeldByThread();

        CheckpointProgress lastCheckpoint = checkpointManager.lastCheckpointProgress();

        UUID lastCheckpointId = lastCheckpoint == null ? null : lastCheckpoint.id();

        meta.lastAppliedIndex(lastCheckpointId, lastAppliedIndex);
    }

    /** {@inheritDoc} */
    @Override
    public long persistedIndex() {
        return persistedIndex;
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        checkpointManager.removeCheckpointListener(checkpointListener);
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
        } else {
            executor.execute(() -> {
                try {
                    rowVersionFreeList.saveMetadata();
                } catch (IgniteInternalCheckedException e) {
                    throw new IgniteInternalException("Failed to save RowVersionFreeList metadata", e);
                }
            });
        }
    }
}
