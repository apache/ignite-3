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

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.schema.BinaryRow;
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

    /** {@link ReadResult} obtained by caching {@link VersionChain} with {@link BplusTree#find(Object, Object, TreeRowClosure, Object)}. */
    private final Map<RowId, ReadResult> readResultByRowId = new HashMap<>();

    private boolean iterationExhausted;

    private @Nullable ReadResult nextRead;

    private @Nullable VersionChain currentChain;

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

            currentChain = null;

            while (true) {
                if (!versionChainCursor.hasNext()) {
                    iterationExhausted = true;

                    return false;
                }

                VersionChain chain = versionChainCursor.next();

                ReadResult result = readResultByRowId.remove(chain.rowId());

                if (result == null) {
                    // TODO: IGNITE-18717 Add lock by rowId
                    chain = storage.readVersionChain(chain.rowId());

                    if (chain == null) {
                        continue;
                    }

                    result = findRowVersion(chain);
                }

                if (result.isEmpty() && !result.isWriteIntent()) {
                    continue;
                }

                nextRead = result;
                currentChain = chain;

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
    public @Nullable BinaryRow committed(HybridTimestamp timestamp) {
        return storage.busy(() -> {
            storage.throwExceptionIfStorageNotInRunnableState();

            if (currentChain == null) {
                throw new IllegalStateException("Version chain missing: " + storage.createStorageInfo());
            }

            // TODO: IGNITE-18717 Add lock by rowId
            VersionChain chain = storage.readVersionChain(currentChain.rowId());

            if (chain == null) {
                return null;
            }

            ReadResult result = storage.findRowVersionByTimestamp(chain, timestamp);

            if (result.isEmpty()) {
                return null;
            }

            // We don't check if row conforms the key filter here, because we've already checked it.
            return result.binaryRow();
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
            versionChainCursor = storage.versionChainTree.find(null, null, (tree, io, pageAddr, idx) -> {
                // Since the BplusTree cursor caches rows that are on the same page, we should try to get actual ReadResult for them in this
                // filter so as not to get into a situation when we read the chain and the links in it are no longer valid.

                VersionChain versionChain = tree.getRow(io, pageAddr, idx);

                // TODO: IGNITE-18717 Perhaps add lock by rowId

                ReadResult readResult = findRowVersion(versionChain);

                if (!readResult.isEmpty()) {
                    readResultByRowId.put(versionChain.rowId(), readResult);
                }

                return true;
            }, null);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Find failed: " + storage.createStorageInfo(), e);
        }
    }
}
