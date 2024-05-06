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

package org.apache.ignite.internal.storage.pagememory.index.hash;

import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionIfStorageInProgressOfRebalance;

import java.util.Objects;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.freelist.FreeListImpl;
import org.apache.ignite.internal.pagememory.util.GradualTask;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.pagememory.index.AbstractPageMemoryIndexStorage;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumns;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMeta;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMetaTree;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of Hash index storage using Page Memory.
 */
public class PageMemoryHashIndexStorage extends AbstractPageMemoryIndexStorage<HashIndexRowKey, HashIndexRow, HashIndexTree>
        implements HashIndexStorage {
    /**
     * Index descriptor.
     *
     * <p>Can be {@code null} only during recovery.
     */
    @Nullable
    private final StorageHashIndexDescriptor descriptor;

    /**
     * Constructor.
     *
     * @param indexMeta Index meta.
     * @param descriptor Hash index descriptor.
     * @param freeList Free list.
     * @param indexTree Hash index tree instance.
     * @param indexMetaTree Index meta tree instance.
     */
    public PageMemoryHashIndexStorage(
            IndexMeta indexMeta,
            @Nullable StorageHashIndexDescriptor descriptor,
            FreeListImpl freeList,
            HashIndexTree indexTree,
            IndexMetaTree indexMetaTree,
            boolean isVolatile
    ) {
        super(indexMeta, indexTree.partitionId(), indexTree, freeList, indexMetaTree, isVolatile);

        this.descriptor = descriptor;
    }

    @Override
    public StorageHashIndexDescriptor indexDescriptor() {
        assert descriptor != null : "This tree must only be used during recovery";

        return descriptor;
    }

    @Override
    public Cursor<RowId> get(BinaryTuple key) throws StorageException {
        return busyDataRead(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            throwExceptionIfIndexIsNotBuilt();

            IndexColumns indexColumns = new IndexColumns(partitionId, key.byteBuffer());

            HashIndexRow lowerBound = new HashIndexRow(indexColumns, lowestRowId);

            return new ScanCursor<RowId>(lowerBound) {
                @Override
                protected RowId map(HashIndexRow value) {
                    return value.rowId();
                }

                @Override
                protected boolean exceedsUpperBound(HashIndexRow value) {
                    return !Objects.equals(value.indexColumns().valueBuffer(), key.byteBuffer());
                }
            };
        });
    }

    @Override
    public void put(IndexRow row) throws StorageException {
        busyNonDataRead(() -> {
            try {
                IndexColumns indexColumns = new IndexColumns(partitionId, row.indexColumns().byteBuffer());

                HashIndexRow hashIndexRow = new HashIndexRow(indexColumns, row.rowId());

                HashIndexTree tree = indexTree;

                var insert = new InsertHashIndexRowInvokeClosure(hashIndexRow, freeList, tree.inlineSize());

                tree.invoke(hashIndexRow, null, insert);

                return null;
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException("Failed to put value into index", e);
            }
        });
    }

    @Override
    public void remove(IndexRow row) throws StorageException {
        busyNonDataRead(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            try {
                IndexColumns indexColumns = new IndexColumns(partitionId, row.indexColumns().byteBuffer());

                HashIndexRow hashIndexRow = new HashIndexRow(indexColumns, row.rowId());

                var remove = new RemoveHashIndexRowInvokeClosure(hashIndexRow, freeList);

                indexTree.invoke(hashIndexRow, null, remove);

                // Performs actual deletion from freeList if necessary.
                remove.afterCompletion();

                return null;
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException("Failed to remove value from index", e);
            }
        });
    }

    @Override
    protected GradualTask createDestructionTask(int maxWorkUnits) throws IgniteInternalCheckedException {
        return indexTree.startGradualDestruction(
                rowKey -> removeIndexColumns((HashIndexRow) rowKey),
                false,
                maxWorkUnits
        );
    }

    private void removeIndexColumns(HashIndexRow indexRow) {
        if (indexRow.indexColumns().link() != PageIdUtils.NULL_LINK) {
            try {
                freeList.removeDataRowByLink(indexRow.indexColumns().link());
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException("Cannot destroy hash index " + indexDescriptor().id(), e);
            }

            indexRow.indexColumns().link(PageIdUtils.NULL_LINK);
        }
    }
}
