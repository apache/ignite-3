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
import org.apache.ignite.internal.storage.index.BinaryTupleComparator;
import org.apache.ignite.internal.storage.index.CatalogIndexStatusSupplier;
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
// TODO: IGNITE-22039 реализовать
public class PageMemorySortedIndexStorage extends AbstractPageMemoryIndexStorage<SortedIndexRowKey, SortedIndexRow, SortedIndexTree>
        implements SortedIndexStorage {
    /**
     * Index descriptor.
     *
     * <p>Can be {@code null} only during recovery.
     */
    @Nullable
    private final StorageSortedIndexDescriptor descriptor;

    private final CatalogIndexStatusSupplier indexStatusSupplier;

    /**
     * Constructor.
     *
     * @param indexMeta Index meta.
     * @param descriptor Sorted index descriptor.
     * @param freeList Free list to store index columns.
     * @param indexTree Sorted index tree instance.
     * @param indexMetaTree Index meta tree instance.
     * @param isVolatile {@code True} if this storage is volatile (i.e. stores its data in memory), or {@code false} if it's persistent.
     * @param indexStatusSupplier Catalog index status supplier.
     */
    public PageMemorySortedIndexStorage(
            IndexMeta indexMeta,
            @Nullable StorageSortedIndexDescriptor descriptor,
            IndexColumnsFreeList freeList,
            SortedIndexTree indexTree,
            IndexMetaTree indexMetaTree,
            boolean isVolatile,
            CatalogIndexStatusSupplier indexStatusSupplier
    ) {
        super(indexMeta, indexTree.partitionId(), indexTree, freeList, indexMetaTree, isVolatile);

        this.descriptor = descriptor;
        this.indexStatusSupplier = indexStatusSupplier;
    }

    @Override
    public StorageSortedIndexDescriptor indexDescriptor() {
        assert descriptor != null : "This tree must only be used during recovery";

        return descriptor;
    }

    @Override
    public Cursor<RowId> get(BinaryTuple key) throws StorageException {
        return busyDataRead(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            throwExceptionIfIndexIsNotBuilt();

            SortedIndexRowKey lowerBound = toSortedIndexRow(key, lowestRowId);

            return new ScanCursor<RowId>(lowerBound) {
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
        busyNonDataRead(() -> {
            try {
                SortedIndexRow sortedIndexRow = toSortedIndexRow(row.indexColumns(), row.rowId());

                SortedIndexTree tree = indexTree;

                var insert = new InsertSortedIndexRowInvokeClosure(sortedIndexRow, freeList, tree.inlineSize());

                tree.invoke(sortedIndexRow, null, insert);

                return null;
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException("Failed to put value into index", e);
            }
        });
    }

    @Override
    public void remove(IndexRow row) {
        busyNonDataRead(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            try {
                SortedIndexRow sortedIndexRow = toSortedIndexRow(row.indexColumns(), row.rowId());

                var remove = new RemoveSortedIndexRowInvokeClosure(sortedIndexRow, freeList);

                indexTree.invoke(sortedIndexRow, null, remove);

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
        return scanInternal(lowerBound, upperBound, flags, true);
    }

    @Override
    public Cursor<IndexRow> readOnlyScan(@Nullable BinaryTuplePrefix lowerBound, @Nullable BinaryTuplePrefix upperBound, int flags) {
        return busyDataRead(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            throwExceptionIfIndexIsNotBuilt();

            boolean includeLower = (flags & GREATER_OR_EQUAL) != 0;
            boolean includeUpper = (flags & LESS_OR_EQUAL) != 0;

            SortedIndexRowKey lower = createBound(lowerBound, !includeLower);
            SortedIndexRowKey upper = createBound(upperBound, includeUpper);

            try {
                Cursor<SortedIndexRow> cursor = indexTree.find(lower, upper);

                return new ReadOnlyScanCursor(cursor);
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException("Couldn't get index tree cursor", e);
            }
        });
    }

    @Override
    public PeekCursor<IndexRow> tolerantScan(@Nullable BinaryTuplePrefix lowerBound, @Nullable BinaryTuplePrefix upperBound, int flags) {
        return scanInternal(lowerBound, upperBound, flags, false);
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
    protected GradualTask createDestructionTask(int maxWorkUnits) throws IgniteInternalCheckedException {
        return indexTree.startGradualDestruction(
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

    private PeekCursor<IndexRow> scanInternal(
            @Nullable BinaryTuplePrefix lowerBound,
            @Nullable BinaryTuplePrefix upperBound,
            int flags,
            boolean onlyBuiltIndex
    ) {
        return busyDataRead(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            if (onlyBuiltIndex) {
                throwExceptionIfIndexIsNotBuilt();
            }

            boolean includeLower = (flags & GREATER_OR_EQUAL) != 0;
            boolean includeUpper = (flags & LESS_OR_EQUAL) != 0;

            SortedIndexRowKey lower = createBound(lowerBound, !includeLower);

            SortedIndexRowKey upper = createBound(upperBound, includeUpper);

            return new ScanCursor<IndexRow>(lower) {
                private final BinaryTupleComparator comparator = localTree.getBinaryTupleComparator();

                @Override
                public IndexRow map(SortedIndexRow value) {
                    return toIndexRowImpl(value);
                }

                @Override
                protected boolean exceedsUpperBound(SortedIndexRow value) {
                    return upper != null && 0 <= comparator.compare(
                            value.indexColumns().valueBuffer(),
                            upper.indexColumns().valueBuffer()
                    );
                }
            };
        });
    }

    private class ReadOnlyScanCursor implements Cursor<IndexRow> {
        private final Cursor<SortedIndexRow> treeCursor;

        private ReadOnlyScanCursor(Cursor<SortedIndexRow> treeCursor) {
            this.treeCursor = treeCursor;
        }

        @Override
        public boolean hasNext() {
            return busyDataRead(() -> {
                throwExceptionIfStorageInProgressOfRebalance(state.get(), PageMemorySortedIndexStorage.this::createStorageInfo);

                return treeCursor.hasNext();
            });
        }

        @Override
        public IndexRow next() {
            return busyDataRead(() -> {
                throwExceptionIfStorageInProgressOfRebalance(state.get(), PageMemorySortedIndexStorage.this::createStorageInfo);

                SortedIndexRow next = treeCursor.next();

                return toIndexRowImpl(next);
            });
        }

        @Override
        public void close() {
            treeCursor.close();
        }
    }
}
