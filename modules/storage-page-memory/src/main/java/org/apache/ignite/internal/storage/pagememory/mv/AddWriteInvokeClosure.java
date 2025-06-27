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
import static org.apache.ignite.internal.storage.pagememory.mv.AbstractPageMemoryMvPartitionStorage.DONT_LOAD_VALUE;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.freelist.FreeList;
import org.apache.ignite.internal.pagememory.io.DataPageIo;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.tree.IgniteTree.InvokeClosure;
import org.apache.ignite.internal.pagememory.tree.IgniteTree.OperationType;
import org.apache.ignite.internal.pagememory.util.PageHandler;
import org.apache.ignite.internal.pagememory.util.PageUtils;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.AddWriteResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.WriteIntentListNode;
import org.apache.ignite.internal.util.GridUnsafe;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link InvokeClosure} for {@link AbstractPageMemoryMvPartitionStorage#addWrite(RowId, BinaryRow, UUID, int, int)}.
 *
 * <p>See {@link AbstractPageMemoryMvPartitionStorage} about synchronization.
 *
 * <p>Operation may throw {@link StorageException} which will cause form {@link BplusTree#invoke(Object, Object, InvokeClosure)}.
 */
class AddWriteInvokeClosure implements InvokeClosure<VersionChain> {
    private final RowId rowId;

    private final @Nullable BinaryRow row;

    private final UUID txId;

    private final int commitTableOrZoneId;

    private final int commitPartitionId;

    private final AbstractPageMemoryMvPartitionStorage storage;

    private final FreeList freeList;

    private OperationType operationType;

    private @Nullable VersionChain newRow;

    private @Nullable RowVersion newRowVersion;

    private @Nullable RowVersion toRemove;

    private AddWriteResult addWriteResult;

    private AtomicReference<WriteIntentListNode> head;

    AddWriteInvokeClosure(
            RowId rowId,
            @Nullable BinaryRow row,
            UUID txId,
            int commitTableOrZoneId,
            int commitPartitionId,
            AbstractPageMemoryMvPartitionStorage storage
    ) {
        this.rowId = rowId;
        this.row = row;
        this.txId = txId;
        this.commitTableOrZoneId = commitTableOrZoneId;
        this.commitPartitionId = commitPartitionId;
        this.storage = storage;

        RenewablePartitionStorageState localState = storage.renewableState;

        this.freeList = localState.freeList();
    }

    @Override
    public void call(@Nullable VersionChain oldRow) throws IgniteInternalCheckedException {
        WriteIntentListNode oldHead = storage.wiHead();

        if (oldRow == null) {
            RowVersion newVersion = insertRowVersion(row, NULL_LINK, oldHead);

            WriteIntentListNode newHead = WriteIntentListNode.createHeadNode(rowId, newVersion.link(), oldHead.prev);

            newRow = VersionChain.createUncommitted(rowId, txId, commitTableOrZoneId, commitPartitionId, newVersion.link(), NULL_LINK);

            operationType = OperationType.PUT;

            addWriteResult = AddWriteResult.success(null);

            storage.updateWiHead(newHead);

            return;
        }

        if (oldRow.isUncommitted() && !txId.equals(oldRow.transactionId())) {
            addWriteResult = AddWriteResult.txMismatch(
                    oldRow.transactionId(),
                    latestCommitTimestamp(storage, oldRow, this::addWriteInfo)
            );

            operationType = OperationType.NOOP;

            return;
        }

        RowVersion newVersion = insertRowVersion(row, oldRow.newestCommittedLink(), oldHead);

        WriteIntentListNode newHead = WriteIntentListNode.createHeadNode(rowId, newVersion.link(), oldHead.prev);

        if (oldRow.isUncommitted()) {
            RowVersion currentVersion = storage.readRowVersion(oldRow.headLink(), ALWAYS_LOAD_VALUE);

            addWriteResult = AddWriteResult.success(currentVersion.value());

            // As we replace an uncommitted version with new one, we need to remove old uncommitted version.
            toRemove = currentVersion;
        } else {
            addWriteResult = AddWriteResult.success(null);
        }

        newRow = VersionChain.createUncommitted(
                rowId,
                txId,
                commitTableOrZoneId,
                commitPartitionId,
                newVersion.link(),
                newVersion.nextLink()
        );

        operationType = OperationType.PUT;

        storage.updateWiHead(newHead);
    }

    @Override
    public @Nullable VersionChain newRow() {
        assert (operationType == OperationType.PUT) == (newRow != null) :
                addWriteInfo() + ", newRow=" + newRow + ", op=" + operationType;

        return newRow;
    }

    @Override
    public OperationType operationType() {
        assert operationType != null : addWriteInfo();

        return operationType;
    }

    private RowVersion insertRowVersion(@Nullable BinaryRow row, long nextPartitionlessLink, WriteIntentListNode oldHead) {
        RowVersion rowVersion = new RowVersion(rowId, storage.partitionId, nextPartitionlessLink, row, oldHead.link, NULL_LINK);

        storage.insertRowVersion(rowVersion);

        newRowVersion = rowVersion;

        return rowVersion;
    }

    /**
     * Method to call after {@link BplusTree#invoke(Object, Object, InvokeClosure)} has completed.
     */
    void afterCompletion() {
        if (toRemove != null) {
            storage.removeRowVersion(toRemove);
        }
    }

    static @Nullable HybridTimestamp latestCommitTimestamp(
            AbstractPageMemoryMvPartitionStorage storage,
            VersionChain chain,
            Supplier<String> operationInfo
    ) {
        if (!chain.hasCommittedVersions()) {
            return null;
        }

        RowVersion rowVersion = storage.readRowVersion(chain.newestCommittedLink(), DONT_LOAD_VALUE);

        assert rowVersion != null : operationInfo.get() + ", newestCommittedLink=" + chain.newestCommittedLink();

        return rowVersion.timestamp();
    }

    AddWriteResult addWriteResult() {
        return addWriteResult;
    }

    private String addWriteInfo() {
        return storage.addWriteInfo(rowId, row, txId, commitTableOrZoneId, commitPartitionId);
    }

    @Override
    public void onUpdate() {
        if (newRowVersion != null && newRowVersion.getPrev() != NULL_LINK) {
            try {
                freeList.updateDataRow(newRowVersion.getPrev(), new UpdateNextLinkHandler(), newRowVersion.link());
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException("Error while update new WI link: [link={}, {}]", e, newRowVersion.getPrev(), addWriteInfo());
            }
        }
    }

    private static class UpdateNextLinkHandler implements PageHandler<Long, Object> {

        @Override
        public Object run(
                int groupId,
                long pageId,
                long page,
                long pageAddr,
                PageIo io,
                Long arg,
                int itemId
        ) {
            DataPageIo dataIo = (DataPageIo) io;

            int payloadOffset = dataIo.getPayloadOffset(pageAddr, itemId, GridUnsafe.pageSize(), 0);

            PageUtils.putLong(pageAddr, payloadOffset + RowVersion.NEXT_WI_LINK_OFFSET, arg);

            return true;
        }
    }
}
