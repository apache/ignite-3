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

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.tree.IgniteTree.InvokeClosure;
import org.apache.ignite.internal.pagememory.tree.IgniteTree.OperationType;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.pagememory.mv.gc.GcQueue;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link InvokeClosure} for {@link AbstractPageMemoryMvPartitionStorage#commitWrite(RowId, HybridTimestamp)}.
 *
 * <p>See {@link AbstractPageMemoryMvPartitionStorage} about synchronization.
 *
 * <p>Operation may throw {@link StorageException} which will cause form {@link BplusTree#invoke(Object, Object, InvokeClosure)}.
 */
class CommitWriteInvokeClosure implements InvokeClosure<VersionChain> {
    private final RowId rowId;

    private final HybridTimestamp timestamp;

    private final AbstractPageMemoryMvPartitionStorage storage;

    private final RowVersionFreeList rowVersionFreeList;

    private final GcQueue gcQueue;

    private OperationType operationType;

    private @Nullable VersionChain newRow;

    private long updateTimestampLink = NULL_LINK;

    private @Nullable RowVersion toRemove;

    /**
     * Row version that will be added to the garbage collection queue when the {@link #afterCompletion() closure completes}.
     *
     * <p>Row version must be committed. It will be a {@link PageIdUtils#NULL_LINK} if the current and the previous row versions are
     * tombstones or have only one row version in the version chain.
     */
    private long rowLinkForAddToGcQueue = NULL_LINK;

    CommitWriteInvokeClosure(RowId rowId, HybridTimestamp timestamp, AbstractPageMemoryMvPartitionStorage storage) {
        this.rowId = rowId;
        this.timestamp = timestamp;
        this.storage = storage;

        RenewablePartitionStorageState localState = storage.renewableState;

        this.rowVersionFreeList = localState.rowVersionFreeList();
        this.gcQueue = localState.gcQueue();
    }

    @Override
    public void call(@Nullable VersionChain oldRow) throws IgniteInternalCheckedException {
        if (oldRow == null || oldRow.transactionId() == null) {
            // Row doesn't exist or the chain doesn't contain an uncommitted write intent.
            operationType = OperationType.NOOP;

            return;
        }

        operationType = OperationType.PUT;

        RowVersion current = storage.readRowVersion(oldRow.headLink(), DONT_LOAD_VALUE);
        RowVersion next = oldRow.hasNextLink() ? storage.readRowVersion(oldRow.nextLink(), DONT_LOAD_VALUE) : null;

        if (next == null && current.isTombstone()) {
            // If there is only one version, and it is a tombstone, then remove the chain.
            operationType = OperationType.REMOVE;

            return;
        }

        // If the previous and current version are tombstones, then delete the current version.
        if (next != null && current.isTombstone() && next.isTombstone()) {
            toRemove = current;

            newRow = VersionChain.createCommitted(oldRow.rowId(), next.link(), next.nextLink());
        } else {
            updateTimestampLink = oldRow.headLink();

            newRow = VersionChain.createCommitted(oldRow.rowId(), oldRow.headLink(), oldRow.nextLink());

            if (oldRow.hasNextLink()) {
                rowLinkForAddToGcQueue = oldRow.headLink();
            }
        }
    }

    @Override
    public @Nullable VersionChain newRow() {
        assert operationType == OperationType.PUT ? newRow != null : newRow == null : "newRow=" + newRow + ", op=" + operationType;

        return newRow;
    }

    @Override
    public OperationType operationType() {
        assert operationType != null;

        return operationType;
    }

    @Override
    public void onUpdate() {
        assert operationType == OperationType.PUT || updateTimestampLink == NULL_LINK :
                "link=" + updateTimestampLink + ", op=" + operationType;

        if (updateTimestampLink != NULL_LINK) {
            try {
                rowVersionFreeList.updateTimestamp(updateTimestampLink, timestamp);
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException(
                        "Error while update timestamp: [link={}, timestamp={}, {}]",
                        e,
                        updateTimestampLink, timestamp, storage.createStorageInfo());
            }
        }
    }

    /**
     * Method to call after {@link BplusTree#invoke(Object, Object, InvokeClosure)} has completed.
     */
    void afterCompletion() {
        assert operationType == OperationType.PUT || toRemove == null : "toRemove=" + toRemove + ", op=" + operationType;

        if (toRemove != null) {
            storage.removeRowVersion(toRemove);
        }

        if (rowLinkForAddToGcQueue != NULL_LINK) {
            gcQueue.add(rowId, timestamp, rowLinkForAddToGcQueue);
        }
    }
}
