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
import static org.apache.ignite.internal.util.GridUnsafe.pageSize;

import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.freelist.FreeList;
import org.apache.ignite.internal.pagememory.io.DataPageIo;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.tree.IgniteTree.InvokeClosure;
import org.apache.ignite.internal.pagememory.tree.IgniteTree.OperationType;
import org.apache.ignite.internal.pagememory.util.PageHandler;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;
import org.apache.ignite.internal.pagememory.util.PageUtils;
import org.apache.ignite.internal.storage.CommitResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.WriteIntentListNode;
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

    private long updateTimestampLink = NULL_LINK;

    private @Nullable RowVersion toRemove;

    private CommitResult commitResult;

    /**
     * Row version that will be added to the garbage collection queue when the {@link #afterCompletion() closure completes}.
     *
     * <p>Row version must be committed. It will be a {@link PageIdUtils#NULL_LINK} if the current and the previous row versions are
     * tombstones or have only one row version in the version chain.
     */
    private long rowLinkForAddToGcQueue = NULL_LINK;

    private final UpdateTimestampHandler updateTimestampHandler;

    private final UpdatePrevWiLinkHandler updatePrevWiLinkHandler;

    private final UpdateNextWiLinkHandler updateNextWiLinkHandler;

    @Nullable
    private RowVersion currentRowVersion;

    @Nullable
    private RowVersion prevRowVersion;

    CommitWriteInvokeClosure(
            RowId rowId,
            HybridTimestamp timestamp,
            UUID txId,
            UpdateTimestampHandler updateTimestampHandler,
            UpdatePrevWiLinkHandler updatePrevWiLinkHandler,
            UpdateNextWiLinkHandler updateNextWiLinkHandler,
            AbstractPageMemoryMvPartitionStorage storage
    ) {
        this.rowId = rowId;
        this.timestamp = timestamp;
        this.txId = txId;
        this.storage = storage;
        this.updateTimestampHandler = updateTimestampHandler;
        this.updatePrevWiLinkHandler = updatePrevWiLinkHandler;
        this.updateNextWiLinkHandler = updateNextWiLinkHandler;

        RenewablePartitionStorageState localState = storage.renewableState;

        this.freeList = localState.freeList();
        this.gcQueue = localState.gcQueue();
    }

    static class UpdateTimestampHandler implements PageHandler<HybridTimestamp, Object> {

        @Override
        public Object run(
                int groupId,
                long pageId,
                long page,
                long pageAddr,
                PageIo io,
                HybridTimestamp arg,
                int itemId
        ) throws IgniteInternalCheckedException {
            DataPageIo dataIo = (DataPageIo) io;

            int payloadOffset = dataIo.getPayloadOffset(pageAddr, itemId, pageSize(), 0);

            HybridTimestamps.writeTimestampToMemory(pageAddr, payloadOffset + RowVersion.TIMESTAMP_OFFSET, arg);
            PageUtils.putLong(pageAddr, payloadOffset + RowVersion.PREV_WI_LINK_OFFSET, NULL_LINK);
            PageUtils.putLong(pageAddr, payloadOffset + RowVersion.NEXT_WI_LINK_OFFSET, NULL_LINK);

            return true;
        }
    }

    static class UpdatePrevWiLinkHandler implements PageHandler<WriteIntentListNode, WriteIntentListNode> {

        @Override
        public WriteIntentListNode run(
                int groupId,
                long pageId,
                long page,
                long pageAddr,
                PageIo io,
                WriteIntentListNode removedNode,
                int itemId
        ) throws IgniteInternalCheckedException {
            DataPageIo dataIo = (DataPageIo) io;

            int payloadOffset = dataIo.getPayloadOffset(pageAddr, itemId, pageSize(), 0);

            PageUtils.putLong(pageAddr, payloadOffset + RowVersion.PREV_WI_LINK_OFFSET, removedNode.prev);

            long lsb = PageUtils.getLong(pageAddr, payloadOffset + RowVersion.ROW_ID_LSB_OFFSET);
            long msb = PageUtils.getLong(pageAddr, payloadOffset + RowVersion.ROW_ID_MSB_OFFSET);

            RowId rowId = new RowId(removedNode.rowId.partitionId(), msb, lsb);

            long nextWiLink = PageUtils.getLong(pageAddr, payloadOffset + RowVersion.NEXT_WI_LINK_OFFSET);

            return WriteIntentListNode.createNode(rowId, removedNode.next, removedNode.prev, nextWiLink);
        }
    }

    static class UpdateNextWiLinkHandler implements PageHandler<WriteIntentListNode, WriteIntentListNode> {

        @Override
        public WriteIntentListNode run(
                int groupId,
                long pageId,
                long page,
                long pageAddr,
                PageIo io,
                WriteIntentListNode removedNode,
                int itemId
        ) throws IgniteInternalCheckedException {
            DataPageIo dataIo = (DataPageIo) io;

            int payloadOffset = dataIo.getPayloadOffset(pageAddr, itemId, pageSize(), 0);

            PageUtils.putLong(pageAddr, payloadOffset + RowVersion.NEXT_WI_LINK_OFFSET, removedNode.next);

            long lsb = PageUtils.getLong(pageAddr, payloadOffset + RowVersion.ROW_ID_LSB_OFFSET);
            long msb = PageUtils.getLong(pageAddr, payloadOffset + RowVersion.ROW_ID_MSB_OFFSET);

            RowId rowId = new RowId(removedNode.rowId.partitionId(), msb, lsb);

            long prevWiLink = PageUtils.getLong(pageAddr, payloadOffset + RowVersion.PREV_WI_LINK_OFFSET);

            return WriteIntentListNode.createNode(rowId, removedNode.prev, prevWiLink, removedNode.next);
        }
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
            updateTimestampLink = currentRowVersion.link();

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
        assert operationType == OperationType.PUT || updateTimestampLink == NULL_LINK :
                commitWriteInfo() + ", link=" + updateTimestampLink + ", op=" + operationType;

        if (updateTimestampLink != NULL_LINK) {
            assert !currentRowVersion.isCommitted() : commitWriteInfo() + ", currentRowVersion=" + currentRowVersion;

            WriteIntentListNode currentNode = WriteIntentListNode.createNode(currentRowVersion.rowId(),
                    updateTimestampLink, currentRowVersion.getPrev(), currentRowVersion.getNext());

            try {
                if (currentRowVersion.getNext() != NULL_LINK) {
                    freeList.updateDataRow(currentRowVersion.getNext(), updatePrevWiLinkHandler, currentNode);
                }

                WriteIntentListNode prevNode = currentRowVersion.getPrev() != NULL_LINK
                        ? freeList.updateDataRow(currentRowVersion.getPrev(), updateNextWiLinkHandler, currentNode) :
                        WriteIntentListNode.EMPTY;

                if (currentRowVersion.getNext() == NULL_LINK) {
                    storage.updateWiHead(prevNode);
                }

                freeList.updateDataRow(updateTimestampLink, updateTimestampHandler, timestamp);
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException("Error while update timestamp: [link={}, {}]", e, updateTimestampLink, commitWriteInfo());
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
