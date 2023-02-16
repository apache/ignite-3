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
import static org.apache.ignite.internal.storage.pagememory.mv.FindRowVersion.RowVersionFilter.equalsByNextLink;
import static org.apache.ignite.internal.storage.pagememory.mv.FindRowVersion.RowVersionFilter.equalsByTimestamp;

import java.util.List;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.tree.IgniteTree.InvokeClosure;
import org.apache.ignite.internal.pagememory.tree.IgniteTree.OperationType;
import org.apache.ignite.internal.storage.RowId;
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
    private final RowId rowId;

    private final HybridTimestamp timestamp;

    private final AbstractPageMemoryMvPartitionStorage storage;

    private OperationType operationType;

    private @Nullable VersionChain newRow;

    private List<RowVersion> toRemove;

    private RowVersion result;

    private @Nullable RowVersion toUpdate;

    RemoveWriteOnGcInvokeClosure(RowId rowId, HybridTimestamp timestamp, AbstractPageMemoryMvPartitionStorage storage) {
        this.rowId = rowId;
        this.timestamp = timestamp;
        this.storage = storage;
    }

    @Override
    public void call(@Nullable VersionChain oldRow) throws IgniteInternalCheckedException {
        assert oldRow != null : storage.createStorageInfo();
        assert oldRow.nextLink() != NULL_LINK : oldRow;

        RowVersion rowVersion = findRowVersionLinkWithChecks(oldRow);
        RowVersion nextRowVersion = storage.readRowVersion(rowVersion.nextLink(), ALWAYS_LOAD_VALUE, true);

        result = nextRowVersion;

        // If the found version is a tombstone, then we must remove it as well.
        if (rowVersion.isTombstone()) {
            toRemove = List.of(nextRowVersion, rowVersion);

            // If the found version is the head of the chain, then delete the entire chain.
            if (oldRow.headLink() == rowVersion.link()) {
                operationType = OperationType.REMOVE;
            } else if (oldRow.nextLink() == rowVersion.link()) {
                operationType = OperationType.PUT;

                toUpdate = storage.readRowVersion(oldRow.headLink(), ALWAYS_LOAD_VALUE, false);

                newRow = oldRow.setNextLink(nextRowVersion.nextLink());
            } else {
                operationType = OperationType.PUT;

                newRow = oldRow;

                // Let's find the parent row version and update the link for it.
                toUpdate = storage.foundRowVersion(oldRow, equalsByNextLink(rowVersion.link()), false);
            }
        } else {
            operationType = OperationType.PUT;

            toRemove = List.of(nextRowVersion);

            toUpdate = rowVersion;

            if (oldRow.headLink() == rowVersion.link()) {
                newRow = oldRow.setNextLink(nextRowVersion.nextLink());
            } else {
                newRow = oldRow;
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
        if (toUpdate != null) {
            try {
                storage.rowVersionFreeList.updateNextLink(toUpdate.link(), result.nextLink());
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException(
                        "Error updating the next link: [rowId={}, timestamp={}, rowLink={}, nextLink={}, {}]",
                        e,
                        newRow.rowId(), timestamp, toUpdate.link(), result.nextLink(), storage.createStorageInfo()
                );
            }
        }
    }

    private RowVersion findRowVersionLinkWithChecks(VersionChain versionChain) {
        RowVersion rowVersion = storage.foundRowVersion(versionChain, equalsByTimestamp(timestamp), false);

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
        toRemove.forEach(storage::removeRowVersion);

        if (toUpdate != null && !result.hasNextLink()) {
            storage.removeFromGc(rowId, toUpdate.timestamp());
        }
    }

    /**
     * Returns the version that was removed in the version chain on garbage collection.
     */
    RowVersion getResult() {
        assert result != null;

        return result;
    }
}
