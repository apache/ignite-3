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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.ForkJoinPool;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.util.GradualTaskExecutor;
import org.apache.ignite.internal.storage.MvPartitionStorage.Locker;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.storage.pagememory.index.hash.PageMemoryHashIndexStorage;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMeta;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMeta.IndexType;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMetaKey;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMetaTree;
import org.apache.ignite.internal.storage.pagememory.index.sorted.PageMemorySortedIndexStorage;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.Cursor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/** Contains tests for {@link PageMemoryIndexes}. */
@ExtendWith(MockitoExtension.class)
class PageMemoryIndexesTest extends BaseIgniteAbstractTest {
    private final GradualTaskExecutor taskExecutor = new GradualTaskExecutor(ForkJoinPool.commonPool());

    private PageMemoryIndexes indexes;

    @BeforeEach
    void setUp(@Mock Locker locker) {
        indexes = new PageMemoryIndexes(taskExecutor, closure -> closure.execute(locker));
    }

    @AfterEach
    void tearDown() {
        taskExecutor.close();
    }

    @Test
    void testIndexesDestroyedOnRecovery(
            @Mock StorageSortedIndexDescriptor sortedIndexDescriptor,
            @Mock PageMemorySortedIndexStorage sortedIndexStorage,
            @Mock StorageHashIndexDescriptor hashIndexDescriptor,
            @Mock PageMemoryHashIndexStorage hashIndexStorage,
            @Mock IndexMetaTree indexMetaTree,
            @Mock(answer = Answers.RETURNS_DEEP_STUBS) IndexStorageFactory indexStorageFactory
    ) throws IgniteInternalCheckedException {
        int sortedIndexId = 0;

        when(sortedIndexDescriptor.id()).thenReturn(sortedIndexId);
        when(sortedIndexStorage.transitionToDestroyedState()).thenReturn(true);
        when(sortedIndexStorage.startDestructionOn(taskExecutor)).thenReturn(nullCompletedFuture());
        when(indexStorageFactory.restoreSortedIndexStorageForDestroy(any())).thenReturn(sortedIndexStorage);

        int hashIndexId = 1;

        when(hashIndexDescriptor.id()).thenReturn(hashIndexId);
        when(hashIndexStorage.transitionToDestroyedState()).thenReturn(true);
        when(hashIndexStorage.startDestructionOn(taskExecutor)).thenReturn(nullCompletedFuture());
        when(indexStorageFactory.restoreHashIndexStorageForDestroy(any())).thenReturn(hashIndexStorage);

        when(indexMetaTree.find(null, null))
                .thenReturn(Cursor.fromIterable(List.of(
                        new IndexMeta(sortedIndexId, IndexType.SORTED, 0, null),
                        new IndexMeta(hashIndexId, IndexType.HASH, 0, null)
                )));

        indexes.getOrCreateSortedIndex(sortedIndexDescriptor, indexStorageFactory);
        indexes.getOrCreateHashIndex(hashIndexDescriptor, indexStorageFactory);

        indexes.performRecovery(indexMetaTree, indexStorageFactory, indexId -> null);

        verify(sortedIndexStorage).startDestructionOn(any());
        verify(hashIndexStorage).startDestructionOn(any());

        verify(indexMetaTree, timeout(1_000)).removex(new IndexMetaKey(sortedIndexId));
        verify(indexMetaTree, timeout(1_000)).removex(new IndexMetaKey(hashIndexId));
    }
}
