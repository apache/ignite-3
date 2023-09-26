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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionIfStorageNotInCleanupOrRebalancedState;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionIfStorageNotInProgressOfRebalance;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionIfStorageNotInRunnableOrRebalanceState;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.util.GradualTaskExecutor;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.pagememory.VolatilePageMemoryTableStorage;
import org.apache.ignite.internal.storage.pagememory.index.hash.PageMemoryHashIndexStorage;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMetaTree;
import org.apache.ignite.internal.storage.pagememory.index.sorted.PageMemorySortedIndexStorage;
import org.apache.ignite.internal.storage.pagememory.mv.gc.GcQueue;
import org.apache.ignite.internal.storage.util.LocalLocker;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link MvPartitionStorage} based on a {@link BplusTree} for in-memory case.
 */
public class VolatilePageMemoryMvPartitionStorage extends AbstractPageMemoryMvPartitionStorage {
    private static final IgniteLogger LOG = Loggers.forClass(VolatilePageMemoryMvPartitionStorage.class);

    private static final Predicate<HybridTimestamp> NEVER_LOAD_VALUE = ts -> false;

    private final GradualTaskExecutor destructionExecutor;

    /** Last applied index value. */
    private volatile long lastAppliedIndex;

    /** Last applied term value. */
    private volatile long lastAppliedTerm;

    /** Last group configuration. */
    private volatile byte @Nullable [] groupConfig;

    /**
     * Constructor.
     *
     * @param tableStorage Table storage instance.
     * @param partitionId Partition id.
     * @param versionChainTree Table tree for {@link VersionChain}.
     * @param indexMetaTree Tree that contains SQL indexes' metadata.
     * @param destructionExecutor Executor used to destruct partitions.
     * @param gcQueue Garbage collection queue.
     */
    public VolatilePageMemoryMvPartitionStorage(
            VolatilePageMemoryTableStorage tableStorage,
            int partitionId,
            VersionChainTree versionChainTree,
            IndexMetaTree indexMetaTree,
            GcQueue gcQueue,
            GradualTaskExecutor destructionExecutor
    ) {
        super(
                partitionId,
                tableStorage,
                tableStorage.dataRegion().rowVersionFreeList(),
                tableStorage.dataRegion().indexColumnsFreeList(),
                versionChainTree,
                indexMetaTree,
                gcQueue
        );

        this.destructionExecutor = destructionExecutor;
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

                THREAD_LOCAL_LOCKER.set(locker0);

                try {
                    return closure.execute(locker0);
                } finally {
                    THREAD_LOCAL_LOCKER.set(null);

                    locker0.unlockAll();
                }
            });
        }
    }

    @Override
    public CompletableFuture<Void> flush() {
        return busy(() -> {
            throwExceptionIfStorageNotInRunnableOrRebalanceState(state.get(), this::createStorageInfo);

            return completedFuture(null);
        });
    }

    @Override
    public long lastAppliedIndex() {
        return busy(() -> {
            throwExceptionIfStorageNotInRunnableOrRebalanceState(state.get(), this::createStorageInfo);

            return lastAppliedIndex;
        });
    }

    @Override
    public long lastAppliedTerm() {
        return busy(() -> {
            throwExceptionIfStorageNotInRunnableOrRebalanceState(state.get(), this::createStorageInfo);

            return lastAppliedTerm;
        });
    }

    @Override
    public void lastApplied(long lastAppliedIndex, long lastAppliedTerm) throws StorageException {
        busy(() -> {
            throwExceptionIfStorageNotInRunnableState();

            this.lastAppliedIndex = lastAppliedIndex;
            this.lastAppliedTerm = lastAppliedTerm;

            return null;
        });
    }

    @Override
    public byte @Nullable [] committedGroupConfiguration() {
        return busy(() -> {
            throwExceptionIfStorageNotInRunnableOrRebalanceState(state.get(), this::createStorageInfo);

            byte[] currentConfig = groupConfig;
            return currentConfig == null ? null : Arrays.copyOf(currentConfig, currentConfig.length);
        });
    }

    @Override
    public void committedGroupConfiguration(byte[] config) {
        busy(() -> {
            throwExceptionIfStorageNotInRunnableState();

            groupConfig = Arrays.copyOf(config, config.length);

            return null;
        });
    }

    @Override
    public void lastAppliedOnRebalance(long lastAppliedIndex, long lastAppliedTerm) {
        throwExceptionIfStorageNotInProgressOfRebalance(state.get(), this::createStorageInfo);

        this.lastAppliedIndex = lastAppliedIndex;
        this.lastAppliedTerm = lastAppliedTerm;
    }

    @Override
    protected List<AutoCloseable> getResourcesToClose(boolean goingToDestroy) {
        List<AutoCloseable> resourcesToClose = super.getResourcesToClose(goingToDestroy);

        if (!goingToDestroy) {
            // If we are going to destroy after closure, we should retain indices because the destruction logic
            // will need to destroy them as well. It will clean the maps after it starts the destruction.

            resourcesToClose.add(hashIndexes::clear);
            resourcesToClose.add(sortedIndexes::clear);
        }

        return resourcesToClose;
    }

    /**
     * Cleans data backing this partition. Indices are destroyed, but index desscriptors are
     * not removed from this partition so that they can be refilled with data later.
     */
    public void cleanStructuresData() {
        destroyStructures(false);
    }

    /**
     * Destroys internal structures (including indices) backing this partition.
     */
    public void destroyStructures() {
        destroyStructures(true);
    }

    /**
     * Destroys internal structures (including indices) backing this partition.
     *
     * @param removeIndexDescriptors Whether indices should be completely removed, not just their contents destroyed.
     */
    private void destroyStructures(boolean removeIndexDescriptors) {
        startMvDataDestruction();
        startIndexMetaTreeDestruction();
        startGarbageCollectionTreeDestruction();

        hashIndexes.values().forEach(indexStorage -> indexStorage.startDestructionOn(destructionExecutor));
        sortedIndexes.values().forEach(indexStorage -> indexStorage.startDestructionOn(destructionExecutor));

        lastAppliedIndex = 0;
        lastAppliedTerm = 0;
        groupConfig = null;

        if (removeIndexDescriptors) {
            hashIndexes.clear();
            sortedIndexes.clear();
        }
    }

    private void startMvDataDestruction() {
        try {
            destructionExecutor.execute(
                    versionChainTree.startGradualDestruction(chainKey -> destroyVersionChain((VersionChain) chainKey), false)
            ).whenComplete((res, e) -> {
                if (e != null) {
                    LOG.error(
                            "Version chains destruction failed: [tableId={}, partitionId={}]",
                            e,
                            tableStorage.getTableId(), partitionId
                    );
                }
            });
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException(
                    "Cannot destroy MV partition: [tableId={}, partitionId={}]",
                    e,
                    tableStorage.getTableId(), partitionId
            );
        }
    }

    private void destroyVersionChain(VersionChain chainKey) {
        try {
            deleteRowVersionsFromFreeList(chainKey);
        } catch (IgniteInternalCheckedException e) {
            throw new IgniteInternalException(e);
        }
    }

    private void deleteRowVersionsFromFreeList(VersionChain chain) throws IgniteInternalCheckedException {
        long rowVersionLink = chain.headLink();

        while (rowVersionLink != PageIdUtils.NULL_LINK) {
            RowVersion rowVersion = readRowVersion(rowVersionLink, NEVER_LOAD_VALUE);

            rowVersionFreeList.removeDataRowByLink(rowVersion.link());

            rowVersionLink = rowVersion.nextLink();
        }
    }

    private void startIndexMetaTreeDestruction() {
        try {
            destructionExecutor.execute(
                    indexMetaTree.startGradualDestruction(null, false)
            ).whenComplete((res, e) -> {
                if (e != null) {
                    LOG.error(
                            "Index meta tree destruction failed: [tableId={}, partitionId={}",
                            e,
                            tableStorage.getTableId(), partitionId
                    );
                }
            });
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException(
                    "Cannot destroy index meta tree: [tableId={}, partitionId={}]",
                    e,
                    tableStorage.getTableId(), partitionId
            );
        }
    }

    private void startGarbageCollectionTreeDestruction() {
        try {
            destructionExecutor.execute(
                    gcQueue.startGradualDestruction(null, false)
            ).whenComplete((res, e) -> {
                if (e != null) {
                    LOG.error(
                            "Garbage collection tree destruction failed: [tableId={}, partitionId={}]",
                            e,
                            tableStorage.getTableId(), partitionId
                    );
                }
            });
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException(
                    "Cannot destroy garbage collection tree: [tableId={}, partitionId={}]",
                    e,
                    tableStorage.getTableId(), partitionId
            );
        }
    }

    @Override
    List<AutoCloseable> getResourcesToCloseOnCleanup() {
        return List.of(versionChainTree::close, indexMetaTree::close, gcQueue::close);
    }

    /**
     * Updates the internal data structures of the storage and its indexes on rebalance or cleanup.
     *
     * @param versionChainTree Table tree for {@link VersionChain}.
     * @param indexMetaTree Tree that contains SQL indexes' metadata.
     * @param gcQueue Garbage collection queue.
     * @throws StorageException If failed.
     */
    public void updateDataStructures(
            VersionChainTree versionChainTree,
            IndexMetaTree indexMetaTree,
            GcQueue gcQueue
    ) {
        throwExceptionIfStorageNotInCleanupOrRebalancedState(state.get(), this::createStorageInfo);

        this.versionChainTree = versionChainTree;
        this.indexMetaTree = indexMetaTree;
        this.gcQueue = gcQueue;

        for (PageMemoryHashIndexStorage indexStorage : hashIndexes.values()) {
            indexStorage.updateDataStructures(
                    indexFreeList,
                    createHashIndexTree(indexStorage.indexDescriptor(), createIndexMetaForNewIndex(indexStorage.indexDescriptor().id()))
            );
        }

        for (PageMemorySortedIndexStorage indexStorage : sortedIndexes.values()) {
            indexStorage.updateDataStructures(
                    indexFreeList,
                    createSortedIndexTree(indexStorage.indexDescriptor(), createIndexMetaForNewIndex(indexStorage.indexDescriptor().id()))
            );
        }
    }

    @Override
    public void committedGroupConfigurationOnRebalance(byte[] config) throws StorageException {
        throwExceptionIfStorageNotInProgressOfRebalance(state.get(), this::createStorageInfo);

        this.groupConfig = config;
    }
}
