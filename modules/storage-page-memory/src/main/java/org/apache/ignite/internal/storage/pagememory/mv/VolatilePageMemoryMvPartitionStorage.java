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

import static java.util.Collections.emptyIterator;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionIfStorageNotInCleanupOrRebalancedState;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionIfStorageNotInProgressOfRebalance;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionIfStorageNotInRunnableOrRebalanceState;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.util.GradualTask;
import org.apache.ignite.internal.pagememory.util.GradualTaskExecutor;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.lease.LeaseInfo;
import org.apache.ignite.internal.storage.pagememory.VolatilePageMemoryStorageEngine;
import org.apache.ignite.internal.storage.pagememory.VolatilePageMemoryTableStorage;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMetaTree;
import org.apache.ignite.internal.storage.pagememory.mv.gc.GcQueue;
import org.apache.ignite.internal.storage.util.LocalLocker;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link MvPartitionStorage} based on a {@link BplusTree} for in-memory case.
 */
public class VolatilePageMemoryMvPartitionStorage extends AbstractPageMemoryMvPartitionStorage {
    private static final Predicate<HybridTimestamp> NEVER_LOAD_VALUE = ts -> false;

    private static final AtomicLongFieldUpdater<VolatilePageMemoryMvPartitionStorage> ESTIMATED_SIZE_UPDATER =
            AtomicLongFieldUpdater.newUpdater(VolatilePageMemoryMvPartitionStorage.class, "estimatedSize");

    /** Last applied index value. */
    private volatile long lastAppliedIndex;

    /** Last applied term value. */
    private volatile long lastAppliedTerm;

    @Nullable
    private volatile LeaseInfo leaseInfo;

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
     * @param failureProcessor Failure processor.
     */
    public VolatilePageMemoryMvPartitionStorage(
            VolatilePageMemoryTableStorage tableStorage,
            int partitionId,
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
                        tableStorage.dataRegion().freeList(),
                        indexMetaTree,
                        gcQueue
                ),
                destructionExecutor,
                failureProcessor
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
    public void updateLease(LeaseInfo leaseInfo) {
        busy(() -> {
            throwExceptionIfStorageNotInRunnableState();

            LeaseInfo thisLeaseInfo = this.leaseInfo;

            if (thisLeaseInfo != null && leaseInfo.leaseStartTime() <= thisLeaseInfo.leaseStartTime()) {
                return null;
            }

            this.leaseInfo = leaseInfo;

            return null;
        });
    }

    @Override
    public @Nullable LeaseInfo leaseInfo() {
        return busy(() -> {
            throwExceptionIfStorageNotInRunnableState();

            return leaseInfo;
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
        leaseInfo = null;

        return destroyFuture;
    }

    private CompletableFuture<Void> startMvDataDestruction(RenewablePartitionStorageState renewableState) {
        return destroyTree(renewableState.versionChainTree(), chainKey -> destroyVersionChain((VersionChain) chainKey, renewableState))
                .whenComplete((res, e) -> {
                    if (e != null) {
                        String errorMessage = String.format(
                                "Version chains destruction failed: [tableId=%s, partitionId=%s]",
                                tableStorage.getTableId(),
                                partitionId
                        );
                        failureProcessor.process(new FailureContext(e, errorMessage));
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
                        String errorMessage = String.format(
                                "Index meta tree destruction failed: [tableId=%s, partitionId=%s]",
                                tableStorage.getTableId(),
                                partitionId
                        );
                        failureProcessor.process(new FailureContext(e, errorMessage));
                    }
                });
    }

    private CompletableFuture<Void> startGarbageCollectionTreeDestruction(RenewablePartitionStorageState renewableState) {
        return destroyTree(renewableState.gcQueue(), null)
                .whenComplete((res, e) -> {
                    if (e != null) {
                        String errorMessage = String.format(
                                "Garbage collection tree destruction failed: [tableId=%s, partitionId=%s]",
                                tableStorage.getTableId(),
                                partitionId
                        );
                        failureProcessor.process(new FailureContext(e, errorMessage));
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
    public void updateLeaseOnRebalance(LeaseInfo leaseInfo) {
        throwExceptionIfStorageNotInProgressOfRebalance(state.get(), this::createStorageInfo);

        this.leaseInfo = leaseInfo;
    }

    @Override
    public long estimatedSize() {
        return estimatedSize;
    }

    @Override
    public Cursor<RowId> scanWriteIntents() {
        return Cursor.fromBareIterator(emptyIterator());
    }

    @Override
    public void incrementEstimatedSize() {
        ESTIMATED_SIZE_UPDATER.incrementAndGet(this);
    }

    @Override
    public void decrementEstimatedSize() {
        ESTIMATED_SIZE_UPDATER.decrementAndGet(this);
    }

    @Override
    AddWriteInvokeClosure newAddWriteInvokeClosure(
            RowId rowId,
            @Nullable BinaryRow row,
            UUID txId,
            int commitZoneId,
            int commitPartitionId
    ) {
        return new AddWriteInvokeClosure(rowId, row, txId, commitZoneId, commitPartitionId, this);
    }
}
