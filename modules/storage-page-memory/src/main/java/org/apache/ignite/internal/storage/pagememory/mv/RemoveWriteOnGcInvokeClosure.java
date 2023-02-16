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

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.tree.IgniteTree.InvokeClosure;
import org.apache.ignite.internal.pagememory.tree.IgniteTree.OperationType;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link InvokeClosure} for deleting a row version in version chain on garbage collection in
 * {@link AbstractPageMemoryMvPartitionStorage#pollForVacuum(HybridTimestamp)}.
 *
 * <p>See {@link AbstractPageMemoryMvPartitionStorage} about synchronization.
 *
 * <p>Operation may throw {@link StorageException} which will cause form {@link BplusTree#invoke(Object, Object, InvokeClosure)}.
 */
public class RemoveWriteOnGcInvokeClosure implements InvokeClosure<VersionChain> {
    private final HybridTimestamp timestamp;

    private final AbstractPageMemoryMvPartitionStorage storage;

    private OperationType operationType;

    private @Nullable VersionChain newRow;

    private RowVersion rowVersion;

    private RowVersion nextRowVersion;

    RemoveWriteOnGcInvokeClosure(HybridTimestamp timestamp, AbstractPageMemoryMvPartitionStorage storage) {
        this.timestamp = timestamp;
        this.storage = storage;
    }

    @Override
    public void call(@Nullable VersionChain oldRow) throws IgniteInternalCheckedException {
        assert oldRow != null : storage.createStorageInfo();
        assert oldRow.nextLink() != NULL_LINK : oldRow;

        rowVersion = findRowVersionLinkWithChecks(oldRow);
        nextRowVersion = storage.readRowVersion(rowVersion.nextLink(), ALWAYS_LOAD_VALUE, true);

        // If the head is a tombstone, then we must destroy both the found version and the chain so that there is no memory leak.
        if (oldRow.headLink() == rowVersion.link() && rowVersion.isTombstone()) {
            operationType = OperationType.REMOVE;

            return;
        }

        operationType = OperationType.PUT;

        if (oldRow.headLink() == rowVersion.link()) {
            newRow = oldRow.setNextLink(nextRowVersion.nextLink());
        } else {
            newRow = oldRow;
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
        if (operationType != OperationType.REMOVE) {
            try {
                storage.rowVersionFreeList.updateNextLink(rowVersion.link(), nextRowVersion.nextLink());
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException(
                        "Error updating the next link: [rowId={}, timestamp={}, row={}, nextRow={}, {}]",
                        e,
                        newRow.rowId(), timestamp, rowVersion, nextRowVersion, storage.createStorageInfo()
                );
            }
        }
    }

    private RowVersion findRowVersionLinkWithChecks(VersionChain versionChain) {
        RowVersion rowVersion = storage.foundRowVersion(versionChain, timestamp, false);

        if (rowVersion == null) {
            throw new StorageException(
                    "Could not find row version in the version chain: [rowId={}, timestamp={}, {}]",
                    versionChain.rowId(), timestamp, storage.createStorageInfo()
            );
        }

        if (!rowVersion.hasNextLink()) {
            throw new StorageException(
                    "Missing next row version: [rowId={}, timestamp={}, {}]",
                    versionChain.rowId(), timestamp, storage.createStorageInfo()
            );
        }

        return rowVersion;
    }

    /**
     * Method to call after {@link BplusTree#invoke(Object, Object, InvokeClosure)} has completed.
     */
    void afterCompletion() {
        storage.removeRowVersion(nextRowVersion);

        if (operationType == OperationType.REMOVE) {
            storage.removeRowVersion(rowVersion);
        }
    }

    /**
     * Returns the removed row version from the version chain.
     */
    RowVersion getRemovedRowVersion() {
        assert nextRowVersion != null;

        return nextRowVersion;
    }
}
