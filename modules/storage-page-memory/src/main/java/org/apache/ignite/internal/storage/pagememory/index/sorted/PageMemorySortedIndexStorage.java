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

package org.apache.ignite.internal.storage.pagememory.index.sorted;

import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionIfStorageInProgressOfRebalance;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionIfStorageNotInCleanupOrRebalancedState;

import java.nio.ByteBuffer;
import java.util.Objects;
import org.apache.ignite.internal.binarytuple.BinaryTupleCommon;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.util.GradualTask;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.IndexRowImpl;
import org.apache.ignite.internal.storage.index.PeekCursor;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.storage.pagememory.index.AbstractPageMemoryIndexStorage;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumns;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumnsFreeList;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMeta;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMetaTree;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of Sorted index storage using Page Memory.
 */
public class PageMemorySortedIndexStorage extends AbstractPageMemoryIndexStorage<SortedIndexRowKey, SortedIndexRow>
        implements SortedIndexStorage {
    /**
     * Index descriptor.
     *
     * <p>Can be {@code null} only during recovery.
     */
    @Nullable
    private final StorageSortedIndexDescriptor descriptor;

    /** Sorted index tree instance. */
    private volatile SortedIndexTree sortedIndexTree;

    /**
     * Constructor.
     *
     * @param indexMeta Index meta.
     * @param descriptor Sorted index descriptor.
     * @param freeList Free list to store index columns.
     * @param sortedIndexTree Sorted index tree instance.
     * @param indexMetaTree Index meta tree instance.
     */
    public PageMemorySortedIndexStorage(
            IndexMeta indexMeta,
            @Nullable StorageSortedIndexDescriptor descriptor,
            IndexColumnsFreeList freeList,
            SortedIndexTree sortedIndexTree,
            IndexMetaTree indexMetaTree,
            boolean isVolatile
    ) {
        super(indexMeta, sortedIndexTree.partitionId(), freeList, indexMetaTree, isVolatile);

        this.descriptor = descriptor;
        this.sortedIndexTree = sortedIndexTree;
    }

    @Override
    public StorageSortedIndexDescriptor indexDescriptor() {
        assert descriptor != null : "This tree must only be used during recovery";

        return descriptor;
    }

    @Override
    public Cursor<RowId> get(BinaryTuple key) throws StorageException {
        return busyRead(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            SortedIndexRowKey lowerBound = toSortedIndexRow(key, lowestRowId);

            return new ScanCursor<RowId>(lowerBound, sortedIndexTree) {
                @Override
                protected RowId map(SortedIndexRow value) {
                    return value.rowId();
                }

                @Override
                protected boolean exceedsUpperBound(SortedIndexRow value) {
                    return !Objects.equals(value.indexColumns().valueBuffer(), key.byteBuffer());
                }
            };
        });
    }

    @Override
    public void put(IndexRow row) {
        busyNonRead(() -> {
            try {
                SortedIndexRow sortedIndexRow = toSortedIndexRow(row.indexColumns(), row.rowId());

                var insert = new InsertSortedIndexRowInvokeClosure(sortedIndexRow, freeList, sortedIndexTree.inlineSize());

                sortedIndexTree.invoke(sortedIndexRow, null, insert);

                return null;
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException("Failed to put value into index", e);
            }
        });
    }

    @Override
    public void remove(IndexRow row) {
        busyNonRead(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            try {
                SortedIndexRow sortedIndexRow = toSortedIndexRow(row.indexColumns(), row.rowId());

                var remove = new RemoveSortedIndexRowInvokeClosure(sortedIndexRow, freeList);

                sortedIndexTree.invoke(sortedIndexRow, null, remove);

                // Performs actual deletion from freeList if necessary.
                remove.afterCompletion();

                return null;
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException("Failed to remove value from index", e);
            }
        });
    }

    @Override
    public PeekCursor<IndexRow> scan(@Nullable BinaryTuplePrefix lowerBound, @Nullable BinaryTuplePrefix upperBound, int flags) {
        return busyRead(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            boolean includeLower = (flags & GREATER_OR_EQUAL) != 0;
            boolean includeUpper = (flags & LESS_OR_EQUAL) != 0;

            SortedIndexRowKey lower = createBound(lowerBound, !includeLower);

            SortedIndexRowKey upper = createBound(upperBound, includeUpper);

            return new ScanCursor<IndexRow>(lower, sortedIndexTree) {
                @Override
                public IndexRow map(SortedIndexRow value) {
                    return toIndexRowImpl(value);
                }

                @Override
                protected boolean exceedsUpperBound(SortedIndexRow value) {
                    return upper != null && 0 <= sortedIndexTree.getBinaryTupleComparator().compare(
                            value.indexColumns().valueBuffer(),
                            upper.indexColumns().valueBuffer()
                    );
                }
            };
        });
    }

    private @Nullable SortedIndexRowKey createBound(@Nullable BinaryTuplePrefix bound, boolean setEqualityFlag) {
        if (bound == null) {
            return null;
        }

        ByteBuffer buffer = bound.byteBuffer();

        if (setEqualityFlag) {
            byte flags = buffer.get(0);

            buffer.put(0, (byte) (flags | BinaryTupleCommon.EQUALITY_FLAG));
        }

        return new SortedIndexRowKey(new IndexColumns(partitionId, buffer));
    }

    private SortedIndexRow toSortedIndexRow(BinaryTuple tuple, RowId rowId) {
        return new SortedIndexRow(new IndexColumns(partitionId, tuple.byteBuffer()), rowId);
    }

    private @Nullable IndexRowImpl toIndexRowImpl(@Nullable SortedIndexRow sortedIndexRow) {
        return sortedIndexRow == null ? null : new IndexRowImpl(
                new BinaryTuple(indexDescriptor().binaryTupleSchema().elementCount(), sortedIndexRow.indexColumns().valueBuffer()),
                sortedIndexRow.rowId()
        );
    }

    @Override
    public void closeStructures() {
        sortedIndexTree.close();
    }

    /**
     * Updates the internal data structures of the storage on rebalance or cleanup.
     *
     * @param freeList Free list to store index columns.
     * @param sortedIndexTree Sorted index tree instance.
     * @throws StorageException If failed.
     */
    public void updateDataStructures(IndexColumnsFreeList freeList, SortedIndexTree sortedIndexTree) {
        throwExceptionIfStorageNotInCleanupOrRebalancedState(state.get(), this::createStorageInfo);

        this.freeList = freeList;
        this.sortedIndexTree = sortedIndexTree;
    }

    @Override
    protected GradualTask createDestructionTask(int maxWorkUnits) throws IgniteInternalCheckedException {
        return sortedIndexTree.startGradualDestruction(
                rowKey -> removeIndexColumns((SortedIndexRow) rowKey),
                false,
                maxWorkUnits
        );
    }

    private void removeIndexColumns(SortedIndexRow indexRow) {
        if (indexRow.indexColumns().link() != PageIdUtils.NULL_LINK) {
            try {
                freeList.removeDataRowByLink(indexRow.indexColumns().link());
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException("Cannot destroy sorted index " + indexDescriptor().id(), e);
            }

            indexRow.indexColumns().link(PageIdUtils.NULL_LINK);
        }
    }
}
