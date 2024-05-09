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

import static org.apache.ignite.internal.storage.util.StorageUtils.initialRowIdToBuild;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.PageIdAllocator;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.freelist.FreeListImpl;
import org.apache.ignite.internal.pagememory.util.PageLockListenerNoOp;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.storage.pagememory.AbstractPageMemoryTableStorage;
import org.apache.ignite.internal.storage.pagememory.index.hash.HashIndexTree;
import org.apache.ignite.internal.storage.pagememory.index.hash.PageMemoryHashIndexStorage;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMeta;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMeta.IndexType;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMetaTree;
import org.apache.ignite.internal.storage.pagememory.index.sorted.PageMemorySortedIndexStorage;
import org.apache.ignite.internal.storage.pagememory.index.sorted.SortedIndexTree;

/**
 * Class responsible for creating Index B-Trees.
 */
class IndexStorageFactory {
    private final AbstractPageMemoryTableStorage tableStorage;

    private final int partitionId;

    private final IndexMetaTree indexMetaTree;

    private final FreeListImpl freeList;

    @FunctionalInterface
    private interface IndexTreeConstructor<T> {
        T createTree(long metaPageId) throws IgniteInternalCheckedException;
    }

    private static class IndexTreeAndMeta<T> {
        final T indexTree;

        final IndexMeta indexMeta;

        IndexTreeAndMeta(T indexTree, IndexMeta indexMeta) {
            this.indexTree = indexTree;
            this.indexMeta = indexMeta;
        }
    }

    IndexStorageFactory(
            AbstractPageMemoryTableStorage tableStorage,
            int partitionId,
            IndexMetaTree indexMetaTree,
            FreeListImpl freeList
    ) {
        this.tableStorage = tableStorage;
        this.partitionId = partitionId;
        this.indexMetaTree = indexMetaTree;
        this.freeList = freeList;
    }

    /**
     * Creates a new Page Memory-based Hash Index storage.
     */
    PageMemoryHashIndexStorage createHashIndexStorage(StorageHashIndexDescriptor indexDescriptor) {
        IndexTreeAndMeta<HashIndexTree> treeAndMeta = createHashIndexTreeAndMeta(indexDescriptor);

        return new PageMemoryHashIndexStorage(
                treeAndMeta.indexMeta,
                indexDescriptor,
                freeList,
                treeAndMeta.indexTree,
                indexMetaTree,
                tableStorage.isVolatile()
        );
    }

    /**
     * Restores an existing Page Memory-based Hash Index storage.
     */
    PageMemoryHashIndexStorage restoreHashIndexStorage(StorageHashIndexDescriptor indexDescriptor, IndexMeta indexMeta) {
        return new PageMemoryHashIndexStorage(
                indexMeta,
                indexDescriptor,
                freeList,
                restoreHashIndexTree(indexMeta),
                indexMetaTree,
                tableStorage.isVolatile()
        );
    }

    /**
     * Restores an existing Page Memory-based Hash Index storage that will be immediately be destroyed during recovery.
     */
    PageMemoryHashIndexStorage restoreHashIndexStorageForDestroy(IndexMeta indexMeta) {
        return new PageMemoryHashIndexStorage(
                indexMeta,
                null,
                freeList,
                restoreHashIndexTree(indexMeta),
                indexMetaTree,
                tableStorage.isVolatile()
        );
    }

    private IndexTreeAndMeta<HashIndexTree> createHashIndexTreeAndMeta(StorageHashIndexDescriptor indexDescriptor) {
        return createIndexTree(
                indexDescriptor,
                metaPageId -> HashIndexTree.createNew(
                        tableStorage.getTableId(),
                        Integer.toString(tableStorage.getTableId()),
                        partitionId,
                        tableStorage.dataRegion().pageMemory(),
                        PageLockListenerNoOp.INSTANCE,
                        new AtomicLong(),
                        metaPageId,
                        freeList,
                        indexDescriptor
                ));
    }

    private HashIndexTree restoreHashIndexTree(IndexMeta indexMeta) {
        try {
            return HashIndexTree.restoreExisting(
                    tableStorage.getTableId(),
                    Integer.toString(tableStorage.getTableId()),
                    partitionId,
                    tableStorage.dataRegion().pageMemory(),
                    PageLockListenerNoOp.INSTANCE,
                    new AtomicLong(),
                    indexMeta.metaPageId(),
                    freeList
            );
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException(e);
        }
    }

    /**
     * Creates a new Page Memory-based Sorted Index storage.
     */
    PageMemorySortedIndexStorage createSortedIndexStorage(StorageSortedIndexDescriptor indexDescriptor) {
        IndexTreeAndMeta<SortedIndexTree> treeAndMeta = createSortedIndexTreeAndMeta(indexDescriptor);

        return new PageMemorySortedIndexStorage(
                treeAndMeta.indexMeta,
                indexDescriptor,
                freeList,
                treeAndMeta.indexTree,
                indexMetaTree,
                tableStorage.isVolatile()
        );
    }

    /**
     * Restores an existing Page Memory-based Sorted Index storage.
     */
    PageMemorySortedIndexStorage restoreSortedIndexStorage(StorageSortedIndexDescriptor indexDescriptor, IndexMeta indexMeta) {
        return new PageMemorySortedIndexStorage(
                indexMeta,
                indexDescriptor,
                freeList,
                restoreSortedIndexTree(indexDescriptor, indexMeta),
                indexMetaTree,
                tableStorage.isVolatile()
        );
    }

    /**
     * Restores an existing Page Memory-based Sorted Index storage that will be immediately be destroyed during recovery.
     */
    PageMemorySortedIndexStorage restoreSortedIndexStorageForDestroy(IndexMeta indexMeta) {
        return new PageMemorySortedIndexStorage(
                indexMeta,
                null,
                freeList,
                restoreSortedIndexTreeForDestroy(indexMeta),
                indexMetaTree,
                tableStorage.isVolatile()
        );
    }

    private IndexTreeAndMeta<SortedIndexTree> createSortedIndexTreeAndMeta(StorageSortedIndexDescriptor indexDescriptor) {
        return createIndexTree(
                indexDescriptor,
                metaPageId -> SortedIndexTree.createNew(
                        tableStorage.getTableId(),
                        Integer.toString(tableStorage.getTableId()),
                        partitionId,
                        tableStorage.dataRegion().pageMemory(),
                        PageLockListenerNoOp.INSTANCE,
                        new AtomicLong(),
                        metaPageId,
                        freeList,
                        indexDescriptor
                )
        );
    }

    private SortedIndexTree restoreSortedIndexTree(StorageSortedIndexDescriptor indexDescriptor, IndexMeta indexMeta) {
        try {
            return SortedIndexTree.restoreExisting(
                    tableStorage.getTableId(),
                    Integer.toString(tableStorage.getTableId()),
                    partitionId,
                    tableStorage.dataRegion().pageMemory(),
                    PageLockListenerNoOp.INSTANCE,
                    new AtomicLong(),
                    indexMeta.metaPageId(),
                    freeList,
                    indexDescriptor
            );
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException(e);
        }
    }

    private SortedIndexTree restoreSortedIndexTreeForDestroy(IndexMeta indexMeta) {
        try {
            return SortedIndexTree.restoreForDestroy(
                    tableStorage.getTableId(),
                    Integer.toString(tableStorage.getTableId()),
                    partitionId,
                    tableStorage.dataRegion().pageMemory(),
                    PageLockListenerNoOp.INSTANCE,
                    new AtomicLong(),
                    indexMeta.metaPageId(),
                    freeList
            );
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException(e);
        }
    }

    /**
     * Creates a new B-Tree for the given {@code indexStorage}.
     */
    void updateDataStructuresIn(PageMemoryHashIndexStorage indexStorage) {
        HashIndexTree indexTree = createHashIndexTreeAndMeta(indexStorage.indexDescriptor()).indexTree;

        indexStorage.updateDataStructures(indexMetaTree, freeList, indexTree);
    }

    /**
     * Creates a new B-Tree for the given {@code indexStorage}.
     */
    void updateDataStructuresIn(PageMemorySortedIndexStorage indexStorage) {
        SortedIndexTree indexTree = createSortedIndexTreeAndMeta(indexStorage.indexDescriptor()).indexTree;

        indexStorage.updateDataStructures(indexMetaTree, freeList, indexTree);
    }

    private <T> IndexTreeAndMeta<T> createIndexTree(StorageIndexDescriptor descriptor, IndexTreeConstructor<T> treeConstructor) {
        try {
            PageMemory pageMemory = tableStorage.dataRegion().pageMemory();

            long metaPageId = pageMemory.allocatePage(tableStorage.getTableId(), partitionId, PageIdAllocator.FLAG_AUX);

            T tree = treeConstructor.createTree(metaPageId);

            IndexType indexType = descriptor instanceof StorageHashIndexDescriptor ? IndexType.HASH : IndexType.SORTED;

            UUID nextRowIdUuidToBuild = descriptor.isPk() ? null : initialRowIdToBuild(partitionId).uuid();

            var indexMeta = new IndexMeta(descriptor.id(), indexType, metaPageId, nextRowIdUuidToBuild);

            boolean replaced = indexMetaTree.putx(indexMeta);

            assert !replaced : descriptor.id();

            return new IndexTreeAndMeta<>(tree, indexMeta);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException(e);
        }
    }
}
