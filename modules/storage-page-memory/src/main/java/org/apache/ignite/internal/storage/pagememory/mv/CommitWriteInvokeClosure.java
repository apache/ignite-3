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

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.tree.IgniteTree.InvokeClosure;
import org.apache.ignite.internal.pagememory.tree.IgniteTree.OperationType;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link InvokeClosure} for {@link AbstractPageMemoryMvPartitionStorage#commitWrite(RowId, HybridTimestamp)}.
 *
 * <p>Synchronization between reading and updating the version chain occurs due to the locks (read and write) of the page of the tree on
 * which the version chain is located.
 *
 * <p>Synchronization between update operations for the version chain must be external (by {@link RowId row ID}).
 *
 * <p>Operation may throw {@link StorageException} which will cause form {@link BplusTree#invoke(Object, Object, InvokeClosure)}.
 */
class CommitWriteInvokeClosure implements InvokeClosure<VersionChain> {
    private final HybridTimestamp timestamp;

    private final AbstractPageMemoryMvPartitionStorage storage;

    private OperationType operationType;

    private @Nullable VersionChain newRow;

    private @Nullable Long updateTimestampLink;

    CommitWriteInvokeClosure(HybridTimestamp timestamp, AbstractPageMemoryMvPartitionStorage storage) {
        this.timestamp = timestamp;
        this.storage = storage;
    }

    @Override
    public void call(@Nullable VersionChain oldRow) throws IgniteInternalCheckedException {
        if (oldRow == null || oldRow.transactionId() == null) {
            // Row doesn't exist or the chain doesn't contain an uncommitted write intent.
            operationType = OperationType.NOOP;

            return;
        }

        updateTimestampLink = oldRow.headLink();

        operationType = OperationType.PUT;

        newRow = VersionChain.createCommitted(oldRow.rowId(), oldRow.headLink(), oldRow.nextLink());
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
        assert operationType == OperationType.PUT ? updateTimestampLink != null : updateTimestampLink == null :
                "link=" + updateTimestampLink + ", op=" + operationType;

        if (updateTimestampLink != null) {
            try {
                storage.rowVersionFreeList.updateTimestamp(updateTimestampLink, timestamp);
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException(
                        "Error while update timestamp: [link={}, timestamp={}, {}]",
                        e,
                        updateTimestampLink, timestamp, storage.createStorageInfo());
            }
        }
    }
}
