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
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionIfStorageInProgressOfRebalance;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionIfStorageNotInProgressOfRebalance;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.util.GradualTaskExecutor;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RaftGroupConfiguration;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.StorageRebalanceException;
import org.apache.ignite.internal.storage.pagememory.VolatilePageMemoryTableStorage;
import org.apache.ignite.internal.storage.pagememory.index.hash.PageMemoryHashIndexStorage;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMeta;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMetaTree;
import org.apache.ignite.internal.storage.pagememory.index.sorted.PageMemorySortedIndexStorage;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link MvPartitionStorage} based on a {@link BplusTree} for in-memory case.
 */
public class VolatilePageMemoryMvPartitionStorage extends AbstractPageMemoryMvPartitionStorage {
    private static final Predicate<HybridTimestamp> NEVER_LOAD_VALUE = ts -> false;

    private final GradualTaskExecutor destructionExecutor;

    /** Last applied index value. */
    private volatile long lastAppliedIndex;

    /** Last applied term value. */
    private volatile long lastAppliedTerm;

    /** Last group configuration. */
    @Nullable
    private volatile RaftGroupConfiguration groupConfig;

    /**
     * Constructor.
     *
     * @param tableStorage Table storage instance.
     * @param partitionId Partition id.
     * @param versionChainTree Table tree for {@link VersionChain}.
     * @param indexMetaTree Tree that contains SQL indexes' metadata.
     * @param destructionExecutor Executor used to destruct partitions.
     */
    public VolatilePageMemoryMvPartitionStorage(
            VolatilePageMemoryTableStorage tableStorage,
            int partitionId,
            VersionChainTree versionChainTree,
            IndexMetaTree indexMetaTree,
            GradualTaskExecutor destructionExecutor
    ) {
        super(
                partitionId,
                tableStorage,
                tableStorage.dataRegion().rowVersionFreeList(),
                tableStorage.dataRegion().indexColumnsFreeList(),
                versionChainTree,
                indexMetaTree
        );

        this.destructionExecutor = destructionExecutor;
    }

    @Override
    public <V> V runConsistently(WriteClosure<V> closure) throws StorageException {
        return busy(closure::execute);
    }

    @Override
    public CompletableFuture<Void> flush() {
        return busy(() -> completedFuture(null));
    }

    @Override
    public long lastAppliedIndex() {
        return busy(() -> lastAppliedIndex);
    }

    @Override
    public long lastAppliedTerm() {
        return busy(() -> lastAppliedTerm);
    }

    @Override
    public void lastApplied(long lastAppliedIndex, long lastAppliedTerm) throws StorageException {
        busy(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            this.lastAppliedIndex = lastAppliedIndex;
            this.lastAppliedTerm = lastAppliedTerm;

            return null;
        });
    }

    @Override
    public long persistedIndex() {
        return busy(() -> lastAppliedIndex);
    }

    @Override
    public @Nullable RaftGroupConfiguration committedGroupConfiguration() {
        return busy(() -> groupConfig);
    }

    @Override
    public void committedGroupConfiguration(RaftGroupConfiguration config) {
        busy(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            groupConfig = config;

            return null;
        });
    }

    @Override
    public void lastAppliedOnRebalance(long lastAppliedIndex, long lastAppliedTerm) throws StorageException {
        throwExceptionIfStorageNotInProgressOfRebalance(state.get(), this::createStorageInfo);

        this.lastAppliedIndex = lastAppliedIndex;
        this.lastAppliedTerm = lastAppliedTerm;
    }

    /**
     * Destroys internal structures backing this partition.
     */
    public void destroyStructures() {
        startMvDataDestruction();
        startIndexMetaTreeDestruction();

        hashIndexes.values().forEach(indexStorage -> indexStorage.startDestructionOn(destructionExecutor));
        sortedIndexes.values().forEach(indexStorage -> indexStorage.startDestructionOn(destructionExecutor));
    }

    private void startMvDataDestruction() {
        try {
            destructionExecutor.execute(
                    versionChainTree.startGradualDestruction(chainKey -> destroyVersionChain((VersionChain) chainKey), false)
            );
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Cannot destroy MV partition in group=" + groupId + ", partition=" + partitionId, e);
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
            );
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Cannot destroy index meta tree in group=" + groupId + ", partition=" + partitionId, e);
        }
    }

    @Override
    List<AutoCloseable> getResourcesToCloseOnRebalance() {
        return List.of(versionChainTree::close, indexMetaTree::close);
    }

    /**
     * Updates the internal data structures of the storage and its indexes on rebalance.
     *
     * @param versionChainTree Table tree for {@link VersionChain}.
     * @param indexMetaTree Tree that contains SQL indexes' metadata.
     * @throws StorageRebalanceException If the storage is not in the process of rebalancing.
     */
    public void updateDataStructuresOnRebalance(
            VersionChainTree versionChainTree,
            IndexMetaTree indexMetaTree
    ) {
        throwExceptionIfStorageNotInProgressOfRebalance(state.get(), this::createStorageInfo);

        this.versionChainTree = versionChainTree;
        this.indexMetaTree = indexMetaTree;

        for (PageMemoryHashIndexStorage indexStorage : hashIndexes.values()) {
            indexStorage.updateDataStructuresOnRebalance(
                    indexFreeList,
                    createHashIndexTree(indexStorage.indexDescriptor(), new IndexMeta(indexStorage.indexDescriptor().id(), 0L))
            );
        }

        for (PageMemorySortedIndexStorage indexStorage : sortedIndexes.values()) {
            indexStorage.updateDataStructuresOnRebalance(
                    indexFreeList,
                    createSortedIndexTree(indexStorage.indexDescriptor(), new IndexMeta(indexStorage.indexDescriptor().id(), 0L))
            );
        }
    }
}
