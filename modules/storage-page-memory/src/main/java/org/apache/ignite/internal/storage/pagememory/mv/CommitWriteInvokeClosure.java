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
import static org.apache.ignite.internal.storage.pagememory.mv.AbstractPageMemoryMvPartitionStorage.DONT_LOAD_VALUE;

import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.freelist.FreeList;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.tree.IgniteTree.InvokeClosure;
import org.apache.ignite.internal.pagememory.tree.IgniteTree.OperationType;
import org.apache.ignite.internal.pagememory.util.PageHandler;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;
import org.apache.ignite.internal.storage.CommitResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.pagememory.mv.gc.GcQueue;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link InvokeClosure} for {@link AbstractPageMemoryMvPartitionStorage#commitWrite}.
 *
 * <p>See {@link AbstractPageMemoryMvPartitionStorage} about synchronization.
 *
 * <p>Operation may throw {@link StorageException} which will cause form {@link BplusTree#invoke(Object, Object, InvokeClosure)}.
 */
class CommitWriteInvokeClosure implements InvokeClosure<VersionChain> {
    private final RowId rowId;

    private final HybridTimestamp timestamp;

    private final UUID txId;

    private final AbstractPageMemoryMvPartitionStorage storage;

    private final FreeList freeList;

    private final GcQueue gcQueue;

    private OperationType operationType;

    private @Nullable VersionChain newRow;

    private long linkToWriteIntentToCommit = NULL_LINK;

    private @Nullable RowVersion toRemove;

    private CommitResult commitResult;

    /**
     * Row version that will be added to the garbage collection queue when the {@link #afterCompletion() closure completes}.
     *
     * <p>Row version must be committed. It will be a {@link PageIdUtils#NULL_LINK} if the current and the previous row versions are
     * tombstones or have only one row version in the version chain.
     */
    private long rowLinkForAddToGcQueue = NULL_LINK;

    @Nullable
    private RowVersion currentRowVersion;

    @Nullable
    private RowVersion prevRowVersion;

    CommitWriteInvokeClosure(RowId rowId, HybridTimestamp timestamp, UUID txId, AbstractPageMemoryMvPartitionStorage storage) {
        this.rowId = rowId;
        this.timestamp = timestamp;
        this.txId = txId;
        this.storage = storage;

        RenewablePartitionStorageState localState = storage.renewableState;

        this.freeList = localState.freeList();
        this.gcQueue = localState.gcQueue();
    }

    @Override
    public void call(@Nullable VersionChain oldRow) throws IgniteInternalCheckedException {
        if (oldRow == null || oldRow.transactionId() == null) {
            // Row doesn't exist or the chain doesn't contain an uncommitted write intent.
            operationType = OperationType.NOOP;

            commitResult = CommitResult.noWriteIntent();

            return;
        } else if (!txId.equals(oldRow.transactionId())) {
            operationType = OperationType.NOOP;

            commitResult = CommitResult.txMismatch(oldRow.transactionId());

            return;
        }

        operationType = OperationType.PUT;

        commitResult = CommitResult.success();

        currentRowVersion = storage.readRowVersion(oldRow.headLink(), DONT_LOAD_VALUE);

        assert currentRowVersion != null : commitWriteInfo() + ", headLink=" + oldRow.headLink();

        prevRowVersion = oldRow.hasNextLink() ? storage.readRowVersion(oldRow.nextLink(), DONT_LOAD_VALUE) : null;

        if (prevRowVersion == null && currentRowVersion.isTombstone()) {
            // If there is only one version, and it is a tombstone, then remove the chain.
            operationType = OperationType.REMOVE;

            return;
        }

        boolean isPreviousRowTombstone = prevRowVersion != null && prevRowVersion.isTombstone();

        // If the previous and current version are tombstones, then delete the current version.
        if (isPreviousRowTombstone && currentRowVersion.isTombstone()) {
            toRemove = currentRowVersion;

            newRow = VersionChain.createCommitted(rowId, prevRowVersion.link(), prevRowVersion.nextLink());
        } else {
            linkToWriteIntentToCommit = currentRowVersion.link();

            newRow = VersionChain.createCommitted(rowId, currentRowVersion.link(), currentRowVersion.nextLink());

            if (currentRowVersion.hasNextLink()) {
                rowLinkForAddToGcQueue = currentRowVersion.link();
            }
        }
    }

    @Override
    public @Nullable VersionChain newRow() {
        assert (operationType == OperationType.PUT) == (newRow != null) :
                commitWriteInfo() + ", newRow=" + newRow + ", op=" + operationType;

        return newRow;
    }

    @Override
    public OperationType operationType() {
        assert operationType != null : commitWriteInfo();

        return operationType;
    }

    @Override
    public void onUpdate() {
        assert operationType == OperationType.PUT || linkToWriteIntentToCommit == NULL_LINK :
                commitWriteInfo() + ", link=" + linkToWriteIntentToCommit + ", op=" + operationType;

        if (linkToWriteIntentToCommit != NULL_LINK) {
            assert currentRowVersion != null;
            assert !currentRowVersion.isCommitted() : commitWriteInfo() + ", currentRowVersion=" + currentRowVersion;

            currentRowVersion.operations().removeFromWriteIntentsList(storage, this::commitWriteInfo);

            PageHandler<HybridTimestamp, Object> updateHandler = currentRowVersion.operations().converterToCommittedVersion();
            try {
                freeList.updateDataRow(linkToWriteIntentToCommit, updateHandler, timestamp);
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException(
                        "Error while updating committed row version: [link={}, {}]",
                        e,
                        linkToWriteIntentToCommit,
                        commitWriteInfo()
                );
            }
        }
    }

    /**
     * Method to call after {@link BplusTree#invoke(Object, Object, InvokeClosure)} has completed.
     */
    void afterCompletion() {
        assert operationType == OperationType.PUT || toRemove == null :
                commitWriteInfo() + ", toRemove=" + toRemove + ", op=" + operationType;

        if (operationType == OperationType.NOOP) {
            return;
        }

        assert currentRowVersion != null : commitWriteInfo();

        if (toRemove != null) {
            storage.removeRowVersion(toRemove);
        }

        if (rowLinkForAddToGcQueue != NULL_LINK) {
            gcQueue.add(rowId, timestamp, rowLinkForAddToGcQueue);
        }

        if (operationType == OperationType.PUT) {
            if (prevRowVersion == null || prevRowVersion.isTombstone()) {
                if (!currentRowVersion.isTombstone()) {
                    storage.incrementEstimatedSize();
                }
            } else {
                if (currentRowVersion.isTombstone()) {
                    storage.decrementEstimatedSize();
                }
            }
        }
    }

    CommitResult commitResult() {
        return commitResult;
    }

    private String commitWriteInfo() {
        return storage.commitWriteInfo(rowId, timestamp, txId);
    }
}
