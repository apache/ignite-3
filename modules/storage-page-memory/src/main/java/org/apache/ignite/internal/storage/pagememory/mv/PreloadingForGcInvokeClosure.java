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

import static org.apache.ignite.internal.storage.pagememory.mv.AbstractPageMemoryMvPartitionStorage.ALWAYS_LOAD_VALUE;
import static org.apache.ignite.internal.storage.pagememory.mv.AbstractPageMemoryMvPartitionStorage.DONT_LOAD_VALUE;
import static org.apache.ignite.internal.storage.pagememory.mv.FindRowVersion.RowVersionFilter.equalsByNextLink;

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.tree.IgniteTree.InvokeClosure;
import org.apache.ignite.internal.pagememory.tree.IgniteTree.OperationType;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link InvokeClosure} to preload row versions before calling {@link MvPartitionStorage#vacuum} to reduce the number of
 * IO operations that may occur when executed in the loop in {@link MvPartitionStorage#runConsistently}. This will ultimately reduce the
 * time to acquire a write lock at a checkpoint.
 *
 * @see RemoveWriteOnGcInvokeClosure
 */
class PreloadingForGcInvokeClosure implements InvokeClosure<VersionChain> {
    private final RowId rowId;

    private final HybridTimestamp timestamp;

    private final long link;

    private final AbstractPageMemoryMvPartitionStorage storage;

    PreloadingForGcInvokeClosure(RowId rowId, HybridTimestamp timestamp, long link, AbstractPageMemoryMvPartitionStorage storage) {
        this.rowId = rowId;
        this.timestamp = timestamp;
        this.link = link;
        this.storage = storage;
    }

    @Override
    public void call(@Nullable VersionChain oldRow) throws IgniteInternalCheckedException {
        if (oldRow == null || !oldRow.hasNextLink()) {
            return;
        }

        RowVersion rowVersion = storage.readRowVersion(link, DONT_LOAD_VALUE);

        if (rowVersion == null || !rowVersion.hasNextLink()) {
            return;
        }

        storage.readRowVersion(rowVersion.nextLink(), ALWAYS_LOAD_VALUE);

        if (rowVersion.isTombstone()) {
            if (oldRow.nextLink() == link) {
                storage.readRowVersion(oldRow.headLink(), DONT_LOAD_VALUE);
            } else if (oldRow.headLink() != link) {
                storage.findRowVersion(oldRow, equalsByNextLink(link), false);
            }
        }
    }

    @Override
    public OperationType operationType() {
        return OperationType.NOOP;
    }

    @Override
    public @Nullable VersionChain newRow() {
        throw new StorageException(
                "No updates should occur: [rowId={}, timestamp={}, link={}, storage={}]",
                rowId, timestamp, link, storage.createStorageInfo()
        );
    }

    @Override
    public void onUpdate() {
        throw new StorageException(
                "No updates should occur: [rowId={}, timestamp={}, link={}, storage={}]",
                rowId, timestamp, link, storage.createStorageInfo()
        );
    }
}
