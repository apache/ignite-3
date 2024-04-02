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

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.pagememory.util.GradualTaskExecutor;
import org.apache.ignite.internal.storage.MvPartitionStorage.WriteClosure;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptorSupplier;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.storage.pagememory.index.AbstractPageMemoryIndexStorage;
import org.apache.ignite.internal.storage.pagememory.index.hash.PageMemoryHashIndexStorage;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMeta;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMetaKey;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMetaTree;
import org.apache.ignite.internal.storage.pagememory.index.sorted.PageMemorySortedIndexStorage;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Class that manages indexes of a single {@link AbstractPageMemoryMvPartitionStorage}.
 */
class PageMemoryIndexes {
    private static final IgniteLogger LOG = Loggers.forClass(PageMemoryIndexes.class);

    private final Consumer<WriteClosure<Void>> runConsistently;

    private final GradualTaskExecutor destructionExecutor;

    private final ConcurrentMap<Integer, PageMemoryHashIndexStorage> hashIndexes = new ConcurrentHashMap<>();

    private final ConcurrentMap<Integer, PageMemorySortedIndexStorage> sortedIndexes = new ConcurrentHashMap<>();

    PageMemoryIndexes(GradualTaskExecutor destructionExecutor, Consumer<WriteClosure<Void>> runConsistently) {
        this.runConsistently = runConsistently;
        this.destructionExecutor = destructionExecutor;
    }

    @Nullable IndexStorage getIndex(int indexId) {
        PageMemoryHashIndexStorage hashIndexStorage = hashIndexes.get(indexId);

        if (hashIndexStorage != null) {
            return hashIndexStorage;
        }

        return sortedIndexes.get(indexId);
    }

    PageMemoryHashIndexStorage getOrCreateHashIndex(
            StorageHashIndexDescriptor indexDescriptor, IndexStorageFactory indexStorageFactory
    ) {
        assert !sortedIndexes.containsKey(indexDescriptor.id()) : indexDescriptor;

        return hashIndexes.computeIfAbsent(
                indexDescriptor.id(),
                id -> indexStorageFactory.createHashIndexStorage(indexDescriptor)
        );
    }

    PageMemorySortedIndexStorage getOrCreateSortedIndex(
            StorageSortedIndexDescriptor indexDescriptor, IndexStorageFactory indexStorageFactory
    ) {
        assert !hashIndexes.containsKey(indexDescriptor.id()) : indexDescriptor;

        return sortedIndexes.computeIfAbsent(
                indexDescriptor.id(),
                id -> indexStorageFactory.createSortedIndexStorage(indexDescriptor)
        );
    }

    void performRecovery(
            IndexMetaTree indexMetaTree,
            IndexStorageFactory indexStorageFactory,
            StorageIndexDescriptorSupplier indexDescriptorSupplier
    ) throws IgniteInternalCheckedException {
        try (Cursor<IndexMeta> cursor = indexMetaTree.find(null, null)) {
            for (IndexMeta indexMeta : cursor) {
                int indexId = indexMeta.indexId();

                StorageIndexDescriptor indexDescriptor = indexDescriptorSupplier.get(indexId);

                if (indexDescriptor == null) {
                    // Index has not been found in the Catalog, we can treat it as destroyed and remove the storage.
                    runConsistently.accept(locker -> {
                        destroyIndexOnRecovery(indexMeta, indexStorageFactory, indexMetaTree)
                                .whenComplete((v, e) -> {
                                    if (e != null) {
                                        LOG.error(
                                                "Unable to destroy existing index {}, that has been removed from the Catalog", e, indexId
                                        );
                                    }
                                });

                        return null;
                    });
                } else if (indexDescriptor instanceof StorageHashIndexDescriptor) {
                    PageMemoryHashIndexStorage indexStorage = indexStorageFactory
                            .restoreHashIndexStorage((StorageHashIndexDescriptor) indexDescriptor, indexMeta);

                    hashIndexes.put(indexId, indexStorage);
                } else if (indexDescriptor instanceof StorageSortedIndexDescriptor) {
                    PageMemorySortedIndexStorage indexStorage = indexStorageFactory
                            .restoreSortedIndexStorage((StorageSortedIndexDescriptor) indexDescriptor, indexMeta);

                    sortedIndexes.put(indexId, indexStorage);
                } else {
                    throw new AssertionError("Unexpected index descriptor type: " + indexDescriptor);
                }
            }
        }
    }

    private CompletableFuture<Void> destroyIndexOnRecovery(
            IndexMeta indexMeta,
            IndexStorageFactory indexStorageFactory,
            IndexMetaTree indexMetaTree
    ) {
        switch (indexMeta.indexType()) {
            case HASH:
                PageMemoryHashIndexStorage hashIndexStorage = indexStorageFactory.restoreHashIndexStorageForDestroy(indexMeta);

                return destroyStorage(indexMeta.indexId(), hashIndexStorage, indexMetaTree);

            case SORTED:
                PageMemorySortedIndexStorage sortedIndexStorage = indexStorageFactory.restoreSortedIndexStorageForDestroy(indexMeta);

                return destroyStorage(indexMeta.indexId(), sortedIndexStorage, indexMetaTree);

            default:
                throw new AssertionError(String.format(
                        "Unexpected index type %s for index %d", indexMeta.indexType(), indexMeta.indexId()
                ));
        }
    }

    CompletableFuture<Void> destroyIndex(int indexId, IndexMetaTree indexMetaTree) {
        PageMemoryHashIndexStorage hashIndexStorage = hashIndexes.remove(indexId);

        if (hashIndexStorage != null) {
            assert !sortedIndexes.containsKey(indexId) : indexId;

            return destroyStorage(indexId, hashIndexStorage, indexMetaTree);
        }

        PageMemorySortedIndexStorage sortedIndexStorage = sortedIndexes.remove(indexId);

        if (sortedIndexStorage != null) {
            return destroyStorage(indexId, sortedIndexStorage, indexMetaTree);
        }

        return nullCompletedFuture();
    }

    private CompletableFuture<Void> destroyStorage(
            int indexId, AbstractPageMemoryIndexStorage<?, ?, ?> storage, IndexMetaTree indexMetaTree
    ) {
        if (!storage.transitionToDestroyedState()) {
            return nullCompletedFuture();
        }

        return storage.startDestructionOn(destructionExecutor)
                .whenComplete((v, e) -> storage.closeStructures())
                .thenRunAsync(() -> runConsistently.accept(locker -> {
                    try {
                        indexMetaTree.removex(new IndexMetaKey(indexId));
                    } catch (IgniteInternalCheckedException e) {
                        throw new StorageException(e);
                    }

                    return null;
                }), destructionExecutor.executorService());
    }

    CompletableFuture<Void> destroyStructures() {
        // We don't clear the maps containing the indexes on purpose, #getResourcesToClose are used for that.
        CompletableFuture<?>[] indexDestroyFutures = Stream.concat(hashIndexes.values().stream(), sortedIndexes.values().stream())
                .map(indexStorage -> indexStorage.startDestructionOn(destructionExecutor))
                .toArray(CompletableFuture[]::new);

        return CompletableFuture.allOf(indexDestroyFutures);
    }

    void startRebalance() {
        forEachIndex(AbstractPageMemoryIndexStorage::startRebalance);
    }

    void completeRebalance() {
        forEachIndex(AbstractPageMemoryIndexStorage::completeRebalance);
    }

    void startCleanup() {
        forEachIndex(AbstractPageMemoryIndexStorage::startCleanup);
    }

    void finishCleanup() {
        forEachIndex(AbstractPageMemoryIndexStorage::finishCleanup);
    }

    void transitionToDestroyedState() {
        forEachIndex(AbstractPageMemoryIndexStorage::transitionToDestroyedState);
    }

    void updateDataStructures(IndexStorageFactory indexStorageFactory) {
        hashIndexes.values().forEach(indexStorageFactory::updateDataStructuresIn);
        sortedIndexes.values().forEach(indexStorageFactory::updateDataStructuresIn);
    }

    List<AutoCloseable> getResourcesToClose() {
        var resources = new ArrayList<AutoCloseable>();

        forEachIndex(index -> resources.add(index::close));

        resources.add(hashIndexes::clear);
        resources.add(sortedIndexes::clear);

        return resources;
    }

    private void forEachIndex(Consumer<AbstractPageMemoryIndexStorage<?, ?, ?>> consumer) {
        hashIndexes.values().forEach(consumer);
        sortedIndexes.values().forEach(consumer);
    }
}
