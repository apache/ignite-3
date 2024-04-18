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

import java.util.NoSuchElementException;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Cursor reading all versions for {@link RowId}.
 *
 * <p>In the constructor, reads all versions of the chain and then iterates over them.
 */
class ScanVersionsCursor implements Cursor<ReadResult> {
    private final AbstractPageMemoryMvPartitionStorage storage;

    private final VersionChain versionChain;

    private @Nullable Boolean hasNext;

    private RowVersion currentRowVersion;

    private long nextLink;

    /**
     * Constructor.
     *
     * @param versionChain Version chain.
     * @param storage Multi-versioned partition storage.
     * @throws StorageException If there is an error when collecting all versions of the chain.
     */
    ScanVersionsCursor(VersionChain versionChain, AbstractPageMemoryMvPartitionStorage storage) {
        this.storage = storage;
        this.versionChain = versionChain;
        this.nextLink = versionChain.headLink();
    }

    @Override
    public void close() {
        // No-op.
    }

    @Override
    public boolean hasNext() {
        return storage.busy(() -> {
            storage.throwExceptionIfStorageNotInRunnableState();

            if (hasNext != null) {
                return hasNext;
            }

            assert AbstractPageMemoryMvPartitionStorage.rowIsLocked(versionChain.rowId());

            hasNext = (nextLink != NULL_LINK);

            if (hasNext) {
                currentRowVersion = storage.readRowVersion(nextLink, ALWAYS_LOAD_VALUE);

                nextLink = currentRowVersion.nextLink();
            }

            return hasNext;
        });
    }

    @Override
    public ReadResult next() {
        return storage.busy(() -> {
            storage.throwExceptionIfStorageNotInRunnableState();

            assert AbstractPageMemoryMvPartitionStorage.rowIsLocked(versionChain.rowId());

            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            hasNext = null;

            return rowVersionToReadResult(currentRowVersion);
        });
    }

    private ReadResult rowVersionToReadResult(RowVersion rowVersion) {
        RowId rowId = versionChain.rowId();

        if (rowVersion.isCommitted()) {
            if (rowVersion.isTombstone()) {
                return ReadResult.empty(rowId);
            } else {
                return ReadResult.createFromCommitted(rowId, rowVersion.value(), rowVersion.timestamp());
            }
        } else {
            return ReadResult.createFromWriteIntent(
                    rowId,
                    rowVersion.value(),
                    versionChain.transactionId(),
                    versionChain.commitTableId(),
                    versionChain.commitPartitionId(),
                    null
            );
        }
    }
}
