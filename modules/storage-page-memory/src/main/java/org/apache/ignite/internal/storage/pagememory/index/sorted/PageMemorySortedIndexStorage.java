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
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.pagememory.util.GradualTaskExecutor;
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
    private static final IgniteLogger LOG = Loggers.forClass(PageMemorySortedIndexStorage.class);

    /** Index descriptor. */
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
            StorageSortedIndexDescriptor descriptor,
            IndexColumnsFreeList freeList,
            SortedIndexTree sortedIndexTree,
            IndexMetaTree indexMetaTree
    ) {
        super(indexMeta, sortedIndexTree.partitionId(), freeList, indexMetaTree);

        this.descriptor = descriptor;
        this.sortedIndexTree = sortedIndexTree;
    }

    @Override
    public StorageSortedIndexDescriptor indexDescriptor() {
        return descriptor;
    }

    @Override
    public Cursor<RowId> get(BinaryTuple key) throws StorageException {
        return busy(() -> {
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
        busy(() -> {
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
        busy(() -> {
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
        return busy(() -> {
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
                new BinaryTuple(descriptor.binaryTupleSchema().elementCount(), sortedIndexRow.indexColumns().valueBuffer()),
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

    /**
     * Starts destruction of the data stored by this index partition.
     *
     * @param executor {@link GradualTaskExecutor} on which to destroy.
     * @throws StorageException If something goes wrong.
     */
    public void startDestructionOn(GradualTaskExecutor executor) throws StorageException {
        try {
            executor.execute(
                    sortedIndexTree.startGradualDestruction(rowKey -> removeIndexColumns((SortedIndexRow) rowKey), false)
            ).whenComplete((res, ex) -> {
                if (ex != null) {
                    LOG.error("Sorted index " + descriptor.id() + " destruction has failed", ex);
                }
            });
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Cannot destroy sorted index " + indexDescriptor().id(), e);
        }
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
