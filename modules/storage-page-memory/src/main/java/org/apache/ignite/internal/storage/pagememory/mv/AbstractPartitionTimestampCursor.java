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

import java.util.NoSuchElementException;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.TableRow;
import org.apache.ignite.internal.storage.PartitionTimestampCursor;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.jetbrains.annotations.Nullable;

abstract class AbstractPartitionTimestampCursor implements PartitionTimestampCursor {
    protected final AbstractPageMemoryMvPartitionStorage storage;

    private @Nullable Cursor<VersionChain> versionChainCursor;

    private boolean iterationExhausted;

    private @Nullable ReadResult nextRead;

    private @Nullable RowId currentRowId;

    AbstractPartitionTimestampCursor(AbstractPageMemoryMvPartitionStorage storage) {
        this.storage = storage;
    }

    @Override
    public boolean hasNext() {
        return storage.busy(() -> {
            storage.throwExceptionIfStorageNotInRunnableState();

            if (nextRead != null) {
                return true;
            }

            if (iterationExhausted) {
                return false;
            }

            createVersionChainCursorIfMissing();

            currentRowId = null;

            while (true) {
                if (!versionChainCursor.hasNext()) {
                    iterationExhausted = true;

                    return false;
                }

                RowId rowId = versionChainCursor.next().rowId();

                ReadResult result = storage.inReadLock(rowId, () -> {
                    VersionChain chain = storage.readVersionChain(rowId);

                    return chain == null ? ReadResult.empty(rowId) : findRowVersion(chain);
                });

                if (result.isEmpty() && !result.isWriteIntent()) {
                    continue;
                }

                nextRead = result;
                currentRowId = rowId;

                return true;
            }
        });
    }

    @Override
    public final ReadResult next() {
        return storage.busy(() -> {
            storage.throwExceptionIfStorageNotInRunnableState();

            if (!hasNext()) {
                throw new NoSuchElementException("The cursor is exhausted: " + storage.createStorageInfo());
            }

            assert nextRead != null;

            ReadResult res = nextRead;

            nextRead = null;

            return res;
        });
    }

    @Override
    public void close() {
        if (versionChainCursor != null) {
            versionChainCursor.close();
        }
    }

    @Override
    public @Nullable TableRow committed(HybridTimestamp timestamp) {
        return storage.busy(() -> {
            storage.throwExceptionIfStorageNotInRunnableState();

            RowId rowId = currentRowId;

            if (rowId == null) {
                throw new IllegalStateException("RowId missing: " + storage.createStorageInfo());
            }

            ReadResult result = storage.inReadLock(rowId, () -> {
                VersionChain chain = storage.readVersionChain(rowId);

                return chain == null ? ReadResult.empty(rowId) : storage.findRowVersionByTimestamp(chain, timestamp);
            });

            if (result.isEmpty()) {
                return null;
            }

            // We don't check if row conforms the key filter here, because we've already checked it.
            return result.tableRow();
        });
    }

    /**
     * Finds a {@link RowVersion} in the {@link VersionChain}, depending on the implementation.
     *
     * <p>For example, for a specific timestamp or the very last in the chain.
     *
     * @param versionChain Version chain.
     */
    abstract ReadResult findRowVersion(VersionChain versionChain);

    private void createVersionChainCursorIfMissing() {
        if (versionChainCursor != null) {
            return;
        }

        try {
            versionChainCursor = storage.versionChainTree.find(null, null);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Find failed: " + storage.createStorageInfo(), e);
        }
    }
}
