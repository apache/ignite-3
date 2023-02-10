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
import static org.apache.ignite.internal.storage.pagememory.mv.AbstractPageMemoryMvPartitionStorage.ALWAYS_LOAD_VALUE;

import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.tree.IgniteTree.InvokeClosure;
import org.apache.ignite.internal.pagememory.tree.IgniteTree.OperationType;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link InvokeClosure} for {@link AbstractPageMemoryMvPartitionStorage#abortWrite(RowId)}.
 *
 * <p>Synchronization between reading and updating the version chain occurs due to the locks (read and write) of the page of the tree on
 * which the version chain is located.
 *
 * <p>Synchronization between update operations for the version chain must be external (by {@link RowId row ID}).
 *
 * <p>Operation may throw {@link StorageException} which will cause form {@link BplusTree#invoke(Object, Object, InvokeClosure)}.
 */
class AbortWriteInvokeClosure implements InvokeClosure<VersionChain> {
    private final RowId rowId;

    private final AbstractPageMemoryMvPartitionStorage storage;

    private OperationType operationType;

    private @Nullable VersionChain newRow;

    private @Nullable RowVersion toRemove;

    private @Nullable BinaryRow previousUncommittedRowVersion;

    AbortWriteInvokeClosure(RowId rowId, AbstractPageMemoryMvPartitionStorage storage) {
        this.rowId = rowId;
        this.storage = storage;
    }

    @Override
    public void call(@Nullable VersionChain oldRow) throws IgniteInternalCheckedException {
        if (oldRow == null || oldRow.transactionId() == null) {
            // Row doesn't exist or the chain doesn't contain an uncommitted write intent.
            operationType = OperationType.NOOP;

            return;
        }

        RowVersion latestVersion = storage.readRowVersion(oldRow.headLink(), ALWAYS_LOAD_VALUE);

        assert latestVersion.isUncommitted();

        toRemove = latestVersion;

        if (latestVersion.hasNextLink()) {
            // Next can be safely replaced with any value (like 0), because this field is only used when there
            // is some uncommitted value, but when we add an uncommitted value, we 'fix' such placeholder value
            // (like 0) by replacing it with a valid value.
            newRow = VersionChain.createCommitted(rowId, latestVersion.nextLink(), NULL_LINK);

            operationType = OperationType.PUT;
        } else {
            // It was the only version, let's remove the chain as well.
            operationType = OperationType.REMOVE;
        }

        previousUncommittedRowVersion = storage.rowVersionToBinaryRow(latestVersion);
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
        assert operationType == OperationType.NOOP ? toRemove == null : toRemove != null : "toRemove=" + toRemove + ", op=" + operationType;

        if (toRemove != null) {
            storage.removeRowVersion(toRemove);
        }
    }

    /**
     * Returns the result for {@link MvPartitionStorage#abortWrite(RowId)}.
     */
    @Nullable BinaryRow getPreviousUncommittedRowVersion() {
        return previousUncommittedRowVersion;
    }
}
