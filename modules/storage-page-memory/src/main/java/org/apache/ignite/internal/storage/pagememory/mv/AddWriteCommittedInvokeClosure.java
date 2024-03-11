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
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.pagememory.mv.gc.GcQueue;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link InvokeClosure} for
 * {@link AbstractPageMemoryMvPartitionStorage#addWriteCommitted(RowId, BinaryRow, HybridTimestamp)}.
 *
 * <p>See {@link AbstractPageMemoryMvPartitionStorage} about synchronization.
 *
 * <p>Operation may throw {@link StorageException} which will cause form {@link BplusTree#invoke(Object, Object, InvokeClosure)}.
 */
class AddWriteCommittedInvokeClosure implements InvokeClosure<VersionChain> {
    private final RowId rowId;

    private final @Nullable BinaryRow row;

    private final HybridTimestamp commitTimestamp;

    private final AbstractPageMemoryMvPartitionStorage storage;

    private final GcQueue gcQueue;

    private OperationType operationType;

    private @Nullable VersionChain newRow;

    /**
     * Row version that will be added to the garbage collection queue when the {@link #afterCompletion() closure completes}.
     *
     * <p>Row version must be committed. It will be a {@link PageIdUtils#NULL_LINK} if the current and the previous row versions are
     * tombstones or have only one row version in the version chain.
     */
    private long rowLinkForAddToGcQueue = NULL_LINK;

    AddWriteCommittedInvokeClosure(
            RowId rowId,
            @Nullable BinaryRow row,
            HybridTimestamp commitTimestamp,
            AbstractPageMemoryMvPartitionStorage storage
    ) {
        this.rowId = rowId;
        this.row = row;
        this.commitTimestamp = commitTimestamp;
        this.storage = storage;
        this.gcQueue = storage.renewableState.gcQueue();
    }

    @Override
    public void call(@Nullable VersionChain oldRow) throws IgniteInternalCheckedException {
        if (oldRow != null && oldRow.isUncommitted()) {
            // This means that there is a bug in our code as the caller must make sure that no write intent exists below this write.
            throw new StorageException("Write intent exists: [rowId={}, {}]", oldRow.rowId(), storage.createStorageInfo());
        }

        if (row == null && oldRow == null) {
            // If there is only one version, and it is a tombstone, then don't save the chain.
            operationType = OperationType.NOOP;

            return;
        }

        if (oldRow == null) {
            operationType = OperationType.PUT;

            RowVersion newVersion = insertCommittedRowVersion(row, commitTimestamp, NULL_LINK);

            newRow = VersionChain.createCommitted(rowId, newVersion.link(), newVersion.nextLink());
        } else {
            RowVersion current = storage.readRowVersion(oldRow.headLink(), DONT_LOAD_VALUE);

            // If the current and new version are tombstones, then there is no need to add a new version.
            if (current.isTombstone() && row == null) {
                operationType = OperationType.NOOP;
            } else {
                operationType = OperationType.PUT;

                RowVersion newVersion = insertCommittedRowVersion(row, commitTimestamp, oldRow.headLink());

                newRow = VersionChain.createCommitted(rowId, newVersion.link(), newVersion.nextLink());

                rowLinkForAddToGcQueue = newVersion.link();
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

    private RowVersion insertCommittedRowVersion(@Nullable BinaryRow row, HybridTimestamp commitTimestamp, long nextPartitionlessLink) {
        RowVersion rowVersion = new RowVersion(storage.partitionId, commitTimestamp, nextPartitionlessLink, row);

        storage.insertRowVersion(rowVersion);

        return rowVersion;
    }

    /**
     * Method to call after {@link BplusTree#invoke(Object, Object, InvokeClosure)} has completed.
     */
    void afterCompletion() {
        if (rowLinkForAddToGcQueue != NULL_LINK) {
            gcQueue.add(rowId, commitTimestamp, rowLinkForAddToGcQueue);
        }
    }
}
