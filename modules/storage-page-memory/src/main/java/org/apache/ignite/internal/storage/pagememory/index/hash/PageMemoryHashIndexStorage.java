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
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionIfStorageNotInCleanupOrRebalancedState;

import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.pagememory.util.GradualTaskExecutor;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.index.HashIndexDescriptor;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.pagememory.index.AbstractPageMemoryIndexStorage;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumns;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumnsFreeList;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMeta;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMetaTree;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * Implementation of Hash index storage using Page Memory.
 */
public class PageMemoryHashIndexStorage extends AbstractPageMemoryIndexStorage implements HashIndexStorage {
    private static final IgniteLogger LOG = Loggers.forClass(PageMemoryHashIndexStorage.class);

    /** Index descriptor. */
    private final HashIndexDescriptor descriptor;

    /** Hash index tree instance. */
    private volatile HashIndexTree hashIndexTree;

    /**
     * Constructor.
     *
     * @param indexMeta Index meta.
     * @param descriptor Hash index descriptor.
     * @param freeList Free list to store index columns.
     * @param hashIndexTree Hash index tree instance.
     * @param indexMetaTree Index meta tree instance.
     */
    public PageMemoryHashIndexStorage(
            IndexMeta indexMeta,
            HashIndexDescriptor descriptor,
            IndexColumnsFreeList freeList,
            HashIndexTree hashIndexTree,
            IndexMetaTree indexMetaTree
    ) {
        super(indexMeta, hashIndexTree.partitionId(), freeList, indexMetaTree);

        this.descriptor = descriptor;
        this.hashIndexTree = hashIndexTree;
    }

    @Override
    public HashIndexDescriptor indexDescriptor() {
        return descriptor;
    }

    @Override
    public Cursor<RowId> get(BinaryTuple key) throws StorageException {
        return busy(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            try {
                IndexColumns indexColumns = new IndexColumns(partitionId, key.byteBuffer());

                HashIndexRow lowerBound = new HashIndexRow(indexColumns, lowestRowId);
                HashIndexRow upperBound = new HashIndexRow(indexColumns, highestRowId);

                Cursor<HashIndexRow> cursor = hashIndexTree.find(lowerBound, upperBound);

                return new Cursor<RowId>() {
                    @Override
                    public void close() {
                        cursor.close();
                    }

                    @Override
                    public boolean hasNext() {
                        return busy(() -> {
                            throwExceptionIfStorageInProgressOfRebalance(state.get(), PageMemoryHashIndexStorage.this::createStorageInfo);

                            return cursor.hasNext();
                        });
                    }

                    @Override
                    public RowId next() {
                        return busy(() -> {
                            throwExceptionIfStorageInProgressOfRebalance(state.get(), PageMemoryHashIndexStorage.this::createStorageInfo);

                            return cursor.next().rowId();
                        });
                    }
                };
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException("Failed to create scan cursor", e);
            }
        });
    }

    @Override
    public void put(IndexRow row) throws StorageException {
        busy(() -> {
            try {
                IndexColumns indexColumns = new IndexColumns(partitionId, row.indexColumns().byteBuffer());

                HashIndexRow hashIndexRow = new HashIndexRow(indexColumns, row.rowId());

                var insert = new InsertHashIndexRowInvokeClosure(hashIndexRow, freeList, hashIndexTree.inlineSize());

                hashIndexTree.invoke(hashIndexRow, null, insert);

                return null;
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException("Failed to put value into index", e);
            }
        });
    }

    @Override
    public void remove(IndexRow row) throws StorageException {
        busy(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            try {
                IndexColumns indexColumns = new IndexColumns(partitionId, row.indexColumns().byteBuffer());

                HashIndexRow hashIndexRow = new HashIndexRow(indexColumns, row.rowId());

                var remove = new RemoveHashIndexRowInvokeClosure(hashIndexRow, freeList);

                hashIndexTree.invoke(hashIndexRow, null, remove);

                // Performs actual deletion from freeList if necessary.
                remove.afterCompletion();

                return null;
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException("Failed to remove value from index", e);
            }
        });
    }

    @Override
    public void destroy() throws StorageException {
        // TODO: IGNITE-17626 Remove it
        throw new UnsupportedOperationException();
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
                    hashIndexTree.startGradualDestruction(rowKey -> removeIndexColumns((HashIndexRow) rowKey), false)
            ).whenComplete((res, ex) -> {
                if (ex != null) {
                    LOG.error("Hash index " + descriptor.id() + " destruction has failed", ex);
                }
            });
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Cannot destroy hash index " + indexDescriptor().id(), e);
        }
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

    @Override
    public void close0() {
        hashIndexTree.close();
    }

    /**
     * Updates the internal data structures of the storage on rebalance or cleanup.
     *
     * @param freeList Free list to store index columns.
     * @param hashIndexTree Hash index tree instance.
     * @throws StorageException If failed.
     */
    public void updateDataStructures(IndexColumnsFreeList freeList, HashIndexTree hashIndexTree) {
        throwExceptionIfStorageNotInCleanupOrRebalancedState(state.get(), this::createStorageInfo);

        this.freeList = freeList;
        this.hashIndexTree = hashIndexTree;
    }
}
