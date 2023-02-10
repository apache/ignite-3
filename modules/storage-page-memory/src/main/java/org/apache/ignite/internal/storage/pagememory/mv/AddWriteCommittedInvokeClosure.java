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
import static org.apache.ignite.internal.storage.pagememory.mv.AbstractPageMemoryMvPartitionStorage.rowBytes;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.tree.IgniteTree.InvokeClosure;
import org.apache.ignite.internal.pagememory.tree.IgniteTree.OperationType;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link InvokeClosure} for
 * {@link AbstractPageMemoryMvPartitionStorage#addWriteCommitted(RowId, BinaryRow, HybridTimestamp)}.
 *
 * <p>Synchronization between reading and updating the version chain occurs due to the locks (read and write) of the page of the tree on
 * which the version chain is located.
 *
 * <p>Synchronization between update operations for the version chain must be external (by {@link RowId row ID}).
 *
 * <p>Operation may throw {@link StorageException} which will cause form {@link BplusTree#invoke(Object, Object, InvokeClosure)}.
 */
class AddWriteCommittedInvokeClosure implements InvokeClosure<VersionChain> {
    private final RowId rowId;

    private final @Nullable BinaryRow row;

    private final HybridTimestamp commitTimestamp;

    private final AbstractPageMemoryMvPartitionStorage storage;

    private @Nullable VersionChain newRow;

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
    }

    @Override
    public void call(@Nullable VersionChain oldRow) throws IgniteInternalCheckedException {
        if (oldRow != null && oldRow.isUncommitted()) {
            // This means that there is a bug in our code as the caller must make sure that no write intent exists below this write.
            throw new StorageException("Write intent exists: [rowId={}, {}]", oldRow.rowId(), storage.createStorageInfo());
        }

        long nextLink = oldRow == null ? NULL_LINK : oldRow.newestCommittedLink();

        RowVersion newVersion = insertCommittedRowVersion(row, commitTimestamp, nextLink);

        newRow = VersionChain.createCommitted(rowId, newVersion.link(), newVersion.nextLink());
    }

    @Override
    public @Nullable VersionChain newRow() {
        assert newRow != null;

        return newRow;
    }

    @Override
    public OperationType operationType() {
        return OperationType.PUT;
    }

    private RowVersion insertCommittedRowVersion(@Nullable BinaryRow row, HybridTimestamp commitTimestamp, long nextPartitionlessLink) {
        byte[] rowBytes = rowBytes(row);

        RowVersion rowVersion = new RowVersion(storage.partitionId, commitTimestamp, nextPartitionlessLink, ByteBuffer.wrap(rowBytes));

        storage.insertRowVersion(rowVersion);

        return rowVersion;
    }
}
