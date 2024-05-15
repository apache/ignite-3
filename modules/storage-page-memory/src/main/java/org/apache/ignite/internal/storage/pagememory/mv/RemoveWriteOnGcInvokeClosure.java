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
import static org.apache.ignite.internal.pagememory.util.PartitionlessLinks.writePartitionless;
import static org.apache.ignite.internal.storage.pagememory.mv.AbstractPageMemoryMvPartitionStorage.ALWAYS_LOAD_VALUE;
import static org.apache.ignite.internal.storage.pagememory.mv.AbstractPageMemoryMvPartitionStorage.DONT_LOAD_VALUE;
import static org.apache.ignite.internal.storage.pagememory.mv.FindRowVersion.RowVersionFilter.equalsByNextLink;
import static org.apache.ignite.internal.util.GridUnsafe.pageSize;

import java.util.List;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.freelist.FreeList;
import org.apache.ignite.internal.pagememory.io.DataPageIo;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolder;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.tree.IgniteTree.InvokeClosure;
import org.apache.ignite.internal.pagememory.tree.IgniteTree.OperationType;
import org.apache.ignite.internal.pagememory.util.PageHandler;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.gc.GcEntry;
import org.apache.ignite.internal.storage.pagememory.mv.gc.GcQueue;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link InvokeClosure} for deleting a row version in version chain on garbage collection in
 * {@link AbstractPageMemoryMvPartitionStorage#vacuum(GcEntry)}.
 *
 * <p>See {@link AbstractPageMemoryMvPartitionStorage} about synchronization.
 *
 * <p>Operation may throw {@link StorageException} which will cause form {@link BplusTree#invoke(Object, Object, InvokeClosure)}.
 */
public class RemoveWriteOnGcInvokeClosure implements InvokeClosure<VersionChain> {
    private final RowId rowId;

    private final HybridTimestamp timestamp;

    private final long link;

    private final AbstractPageMemoryMvPartitionStorage storage;

    private final FreeList freeList;

    private final GcQueue gcQueue;

    private OperationType operationType;

    private @Nullable VersionChain newRow;

    private List<RowVersion> toRemove;

    private RowVersion result;

    private @Nullable RowVersion toUpdate;

    private @Nullable RowVersion toDropFromQueue;

    private final UpdateNextLinkHandler updateNextLinkHandler;

    RemoveWriteOnGcInvokeClosure(
            RowId rowId,
            HybridTimestamp timestamp,
            long link,
            UpdateNextLinkHandler updateNextLinkHandler,
            AbstractPageMemoryMvPartitionStorage storage
    ) {
        this.rowId = rowId;
        this.timestamp = timestamp;
        this.link = link;
        this.storage = storage;

        RenewablePartitionStorageState localState = storage.renewableState;

        this.freeList = localState.freeList();
        this.gcQueue = localState.gcQueue();

        this.updateNextLinkHandler = updateNextLinkHandler;
    }

    static class UpdateNextLinkHandler implements PageHandler<Long, Object> {

        @Override
        public Object run(
                int groupId,
                long pageId,
                long page,
                long pageAddr,
                PageIo io,
                Long nextLink,
                int itemId,
                IoStatisticsHolder statHolder
        ) throws IgniteInternalCheckedException {
            DataPageIo dataIo = (DataPageIo) io;

            int payloadOffset = dataIo.getPayloadOffset(pageAddr, itemId, pageSize(), 0);

            writePartitionless(pageAddr + payloadOffset + RowVersion.NEXT_LINK_OFFSET, nextLink);

            return true;
        }
    }

    @Override
    public void call(@Nullable VersionChain oldRow) throws IgniteInternalCheckedException {
        assert oldRow != null : "rowId=" + rowId + ", storage=" + storage.createStorageInfo();
        assert oldRow.hasNextLink() : oldRow;

        RowVersion rowVersion = readRowVersionWithChecks(oldRow);
        RowVersion nextRowVersion = storage.readRowVersion(rowVersion.nextLink(), ALWAYS_LOAD_VALUE);

        result = nextRowVersion;

        assert result.nextLink() == NULL_LINK : "GC queue is broken, nextLink must be null [nextLink=" + rowVersion.nextLink()
                + ", storage=" + storage.createStorageInfo() + ']';

        // If the found version is a tombstone, then we must remove it as well.
        if (rowVersion.isTombstone()) {
            toRemove = List.of(nextRowVersion, rowVersion);

            // If the found version is the head of the chain, then delete the entire chain.
            if (oldRow.headLink() == link) {
                operationType = OperationType.REMOVE;
            } else if (oldRow.nextLink() == link) {
                operationType = OperationType.PUT;

                // Find the version for which this version is RowVersion#nextLink.
                toUpdate = storage.readRowVersion(oldRow.headLink(), DONT_LOAD_VALUE);

                newRow = oldRow.withNextLink(nextRowVersion.nextLink());
            } else {
                operationType = OperationType.PUT;

                newRow = oldRow;

                // Find the version for which this version is RowVersion#nextLink.
                toUpdate = storage.findRowVersion(oldRow, equalsByNextLink(link), false);
            }

            if (toUpdate != null && toUpdate.isCommitted()) {
                toDropFromQueue = toUpdate;
            }
        } else {
            operationType = OperationType.PUT;

            toRemove = List.of(nextRowVersion);

            toUpdate = rowVersion;

            if (oldRow.headLink() == link) {
                newRow = oldRow.withNextLink(nextRowVersion.nextLink());
            } else {
                newRow = oldRow;
            }
        }
    }

    @Override
    public @Nullable VersionChain newRow() {
        assert (operationType == OperationType.PUT) ^ (newRow == null) : "newRow=" + newRow + ", op=" + operationType;

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
                updateNextLink(toUpdate.link(), NULL_LINK);
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException(
                        "Error updating the next link: [rowId={}, timestamp={}, rowLink={}, nextLink={}, {}]",
                        e,
                        newRow.rowId(), timestamp, toUpdate.link(), result.nextLink(), storage.createStorageInfo()
                );
            }
        }
    }

    private void updateNextLink(long link, long nextLink) throws IgniteInternalCheckedException {
        freeList.updateDataRow(link, updateNextLinkHandler, nextLink);
    }

    private RowVersion readRowVersionWithChecks(VersionChain versionChain) {
        RowVersion rowVersion = storage.readRowVersion(link, DONT_LOAD_VALUE);

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

        if (toDropFromQueue != null) {
            boolean removed = gcQueue.remove(rowId, toDropFromQueue.timestamp(), toDropFromQueue.link());

            assert removed : "Tombstone removal from GC queue should never happen in parallel";
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
