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

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionIfStorageNotInCleanupOrRebalancedState;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionIfStorageNotInProgressOfRebalance;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionIfStorageNotInRunnableOrRebalanceState;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.util.GradualTask;
import org.apache.ignite.internal.pagememory.util.GradualTaskExecutor;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.pagememory.VolatilePageMemoryStorageEngine;
import org.apache.ignite.internal.storage.pagememory.VolatilePageMemoryTableStorage;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMetaTree;
import org.apache.ignite.internal.storage.pagememory.mv.gc.GcQueue;
import org.apache.ignite.internal.storage.util.LocalLocker;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link MvPartitionStorage} based on a {@link BplusTree} for in-memory case.
 */
public class VolatilePageMemoryMvPartitionStorage extends AbstractPageMemoryMvPartitionStorage {
    private static final IgniteLogger LOG = Loggers.forClass(VolatilePageMemoryMvPartitionStorage.class);

    private static final Predicate<HybridTimestamp> NEVER_LOAD_VALUE = ts -> false;

    private static final AtomicLongFieldUpdater<VolatilePageMemoryMvPartitionStorage> ESTIMATED_SIZE_UPDATER =
            AtomicLongFieldUpdater.newUpdater(VolatilePageMemoryMvPartitionStorage.class, "estimatedSize");

    /** Last applied index value. */
    private volatile long lastAppliedIndex;

    /** Last applied term value. */
    private volatile long lastAppliedTerm;

    /** Lease start time. */
    private volatile long leaseStartTime;

    /** Last group configuration. */
    private volatile byte @Nullable [] groupConfig;

    private volatile long estimatedSize;

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
            ExecutorService destructionExecutor
    ) {
        super(
                partitionId,
                tableStorage,
                new RenewablePartitionStorageState(
                        tableStorage,
                        partitionId,
                        versionChainTree,
                        tableStorage.dataRegion().freeList(),
                        indexMetaTree,
                        gcQueue
                ),
                destructionExecutor
        );
    }

    @Override
    protected GradualTaskExecutor createGradualTaskExecutor(ExecutorService threadPool) {
        return new GradualTaskExecutor(threadPool);
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
                    THREAD_LOCAL_LOCKER.remove();

                    locker0.unlockAll();
                }
            });
        }
    }

    @Override
    public CompletableFuture<Void> flush(boolean trigger) {
        return busy(() -> {
            throwExceptionIfStorageNotInRunnableOrRebalanceState(state.get(), this::createStorageInfo);

            return nullCompletedFuture();
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
    public void updateLease(long leaseStartTime) {
        busy(() -> {
            throwExceptionIfStorageNotInRunnableState();

            if (leaseStartTime <= this.leaseStartTime) {
                return null;
            }

            this.leaseStartTime = leaseStartTime;

            return null;
        });
    }

    @Override
    public long leaseStartTime() {
        return busy(() -> {
            throwExceptionIfStorageNotInRunnableState();

            return leaseStartTime;
        });
    }

    @Override
    public void lastAppliedOnRebalance(long lastAppliedIndex, long lastAppliedTerm) {
        throwExceptionIfStorageNotInProgressOfRebalance(state.get(), this::createStorageInfo);

        this.lastAppliedIndex = lastAppliedIndex;
        this.lastAppliedTerm = lastAppliedTerm;
    }

    /**
     * Destroys internal structures (including indices) backing this partition.
     */
    public CompletableFuture<Void> destroyStructures() {
        RenewablePartitionStorageState localState = renewableState;

        CompletableFuture<Void> destroyFuture = CompletableFuture.allOf(
                startMvDataDestruction(localState),
                startIndexMetaTreeDestruction(localState),
                startGarbageCollectionTreeDestruction(localState),
                indexes.destroyStructures()
        );

        lastAppliedIndex = 0;
        lastAppliedTerm = 0;
        groupConfig = null;
        leaseStartTime = HybridTimestamp.MIN_VALUE.longValue();

        return destroyFuture;
    }

    private CompletableFuture<Void> startMvDataDestruction(RenewablePartitionStorageState renewableState) {
        return destroyTree(renewableState.versionChainTree(), chainKey -> destroyVersionChain((VersionChain) chainKey, renewableState))
                .whenComplete((res, e) -> {
                    if (e != null) {
                        LOG.error(
                                "Version chains destruction failed: [tableId={}, partitionId={}]",
                                e,
                                tableStorage.getTableId(), partitionId
                        );
                    }
                });
    }

    private void destroyVersionChain(VersionChain chainKey, RenewablePartitionStorageState renewableState) {
        try {
            deleteRowVersionsFromFreeList(chainKey, renewableState);
        } catch (IgniteInternalCheckedException e) {
            throw new IgniteInternalException(e);
        }
    }

    private void deleteRowVersionsFromFreeList(VersionChain chain, RenewablePartitionStorageState renewableState)
            throws IgniteInternalCheckedException {
        long rowVersionLink = chain.headLink();

        while (rowVersionLink != PageIdUtils.NULL_LINK) {
            RowVersion rowVersion = readRowVersion(rowVersionLink, NEVER_LOAD_VALUE);

            renewableState.freeList().removeDataRowByLink(rowVersion.link());

            rowVersionLink = rowVersion.nextLink();
        }
    }

    private CompletableFuture<Void> startIndexMetaTreeDestruction(RenewablePartitionStorageState renewableState) {
        return destroyTree(renewableState.indexMetaTree(), null)
                .whenComplete((res, e) -> {
                    if (e != null) {
                        LOG.error(
                                "Index meta tree destruction failed: [tableId={}, partitionId={}]",
                                e,
                                tableStorage.getTableId(), partitionId
                        );
                    }
                });
    }

    private CompletableFuture<Void> startGarbageCollectionTreeDestruction(RenewablePartitionStorageState renewableState) {
        return destroyTree(renewableState.gcQueue(), null)
                .whenComplete((res, e) -> {
                    if (e != null) {
                        LOG.error(
                                "Garbage collection tree destruction failed: [tableId={}, partitionId={}]",
                                e,
                                tableStorage.getTableId(), partitionId
                        );
                    }
                });
    }

    private <T> CompletableFuture<Void> destroyTree(BplusTree<T, ?> target, @Nullable Consumer<T> consumer) {
        try {
            GradualTask task = target.startGradualDestruction(consumer, false, VolatilePageMemoryStorageEngine.MAX_DESTRUCTION_WORK_UNITS);

            return destructionExecutor.execute(task);
        } catch (IgniteInternalCheckedException e) {
            return failedFuture(e);
        }
    }

    @Override
    List<AutoCloseable> getResourcesToCloseOnCleanup() {
        RenewablePartitionStorageState localState = renewableState;

        return List.of(localState.versionChainTree()::close, localState.indexMetaTree()::close, localState.gcQueue()::close);
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

        RenewablePartitionStorageState prevState = this.renewableState;

        updateRenewableState(
                versionChainTree,
                prevState.freeList(),
                indexMetaTree,
                gcQueue
        );

        estimatedSize = 0;
    }

    @Override
    public void committedGroupConfigurationOnRebalance(byte[] config) throws StorageException {
        throwExceptionIfStorageNotInProgressOfRebalance(state.get(), this::createStorageInfo);

        this.groupConfig = config;
    }

    @Override
    public long estimatedSize() {
        return estimatedSize;
    }

    @Override
    public void incrementEstimatedSize() {
        ESTIMATED_SIZE_UPDATER.incrementAndGet(this);
    }

    @Override
    public void decrementEstimatedSize() {
        ESTIMATED_SIZE_UPDATER.decrementAndGet(this);
    }
}
