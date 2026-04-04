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

package org.apache.ignite.internal.pagememory.persistence.checkpoint;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.pagememory.persistence.CheckpointUrgency.MUST_TRIGGER;
import static org.apache.ignite.internal.pagememory.persistence.CheckpointUrgency.NOT_REQUIRED;
import static org.apache.ignite.internal.pagememory.persistence.CheckpointUrgency.SHOULD_TRIGGER;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointManager.checkpointUrgency;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointManager.pageIndexesForDeltaFilePageStore;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.TestCheckpointUtils.PageIndexesWithPartitionGeneration.pageIndexesWithPartGen;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.TestCheckpointUtils.createDirtyPagesAndPartitions;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.TestCheckpointUtils.dirtyFullPageIds;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageId;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.pagememory.DataRegion;
import org.apache.ignite.internal.pagememory.TestDataRegion;
import org.apache.ignite.internal.pagememory.configuration.CheckpointConfiguration;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.metrics.CollectionMetricSource;
import org.apache.ignite.internal.pagememory.persistence.CheckpointUrgency;
import org.apache.ignite.internal.pagememory.persistence.DirtyFullPageId;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.PartitionMetaManager;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointDirtyPages.CheckpointDirtyPagesView;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.TestCheckpointUtils.PageIndexesWithPartitionGeneration;
import org.apache.ignite.internal.pagememory.persistence.store.DeltaFilePageStoreIo;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStore;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.invocation.InvocationOnMock;

/**
 * For {@link CheckpointManager} testing.
 */
@ExtendWith({ConfigurationExtension.class, ExecutorServiceExtension.class})
public class CheckpointManagerTest extends BaseIgniteAbstractTest {

    @InjectExecutorService
    private ExecutorService executorService;

    @Test
    void testSimple() throws Exception {
        PersistentPageMemory pageMemory = mock(PersistentPageMemory.class);

        DataRegion<PersistentPageMemory> dataRegion = new TestDataRegion<>(pageMemory);

        CheckpointManager checkpointManager = new CheckpointManager(
                "test",
                null,
                mock(FailureManager.class),
                CheckpointConfiguration.builder().build(),
                mock(FilePageStoreManager.class),
                mock(PartitionMetaManager.class),
                List.of(dataRegion),
                mock(PageIoRegistry.class),
                mock(LogSyncer.class),
                executorService,
                new CollectionMetricSource("test", "storage", null),
                1024
        );

        assertDoesNotThrow(checkpointManager::start);

        assertNotNull(checkpointManager.checkpointTimeoutLock());

        CheckpointListener checkpointListener = new CheckpointListener() {
        };

        assertDoesNotThrow(() -> checkpointManager.addCheckpointListener(checkpointListener, dataRegion));
        assertDoesNotThrow(() -> checkpointManager.removeCheckpointListener(checkpointListener));

        assertNotNull(checkpointManager.forceCheckpoint("test"));
        assertNotNull(checkpointManager.forceCheckpoint("test"));

        assertDoesNotThrow(checkpointManager::stop);
    }

    @Test
    void testCheckpointUrgency() {
        assertEquals(NOT_REQUIRED, checkpointUrgency(List.of()));

        AtomicReference<CheckpointUrgency> urgency0 = new AtomicReference<>(MUST_TRIGGER);
        AtomicReference<CheckpointUrgency> urgency1 = new AtomicReference<>(SHOULD_TRIGGER);

        PersistentPageMemory pageMemory0 = mock(PersistentPageMemory.class);
        PersistentPageMemory pageMemory1 = mock(PersistentPageMemory.class);

        when(pageMemory0.checkpointUrgency()).then(answer -> urgency0.get());
        when(pageMemory1.checkpointUrgency()).then(answer -> urgency1.get());

        DataRegion<PersistentPageMemory> dataRegion0 = new TestDataRegion<>(pageMemory0);
        DataRegion<PersistentPageMemory> dataRegion1 = new TestDataRegion<>(pageMemory1);

        assertEquals(MUST_TRIGGER, checkpointUrgency(List.of(dataRegion0)));
        assertEquals(SHOULD_TRIGGER, checkpointUrgency(List.of(dataRegion1)));
        assertEquals(MUST_TRIGGER, checkpointUrgency(List.of(dataRegion0, dataRegion1)));

        urgency0.set(NOT_REQUIRED);

        assertEquals(NOT_REQUIRED, checkpointUrgency(List.of(dataRegion0)));
        assertEquals(SHOULD_TRIGGER, checkpointUrgency(List.of(dataRegion1)));
        assertEquals(SHOULD_TRIGGER, checkpointUrgency(List.of(dataRegion0, dataRegion1)));

        urgency1.set(NOT_REQUIRED);

        assertEquals(NOT_REQUIRED, checkpointUrgency(List.of(dataRegion0)));
        assertEquals(NOT_REQUIRED, checkpointUrgency(List.of(dataRegion1)));
        assertEquals(NOT_REQUIRED, checkpointUrgency(List.of(dataRegion0, dataRegion1)));
    }

    @Test
    void testPageIndexesForDeltaFilePageStore() {
        PersistentPageMemory pageMemory0 = mock(PersistentPageMemory.class);
        PersistentPageMemory pageMemory1 = mock(PersistentPageMemory.class);

        setPartitionGeneration(pageMemory0, 1);
        setPartitionGeneration(pageMemory1, 1);

        var dirtyPages = new CheckpointDirtyPages(List.of(
                createDirtyPagesAndPartitions(pageMemory0, dirtyPageArray(0, 0, 1, 3, 5)),
                createDirtyPagesAndPartitions(pageMemory1, dirtyPageArray(0, 1, 6, 7, 9))
        ));

        assertArrayEquals(
                new int[]{0}, pageIndexesForDeltaFilePageStore(dirtyPages.getPartitionView(pageMemory0, 0, 0), 0, 0, 1)
        );
        assertArrayEquals(
                new int[]{0, 6, 7}, pageIndexesForDeltaFilePageStore(dirtyPages.getPartitionView(pageMemory1, 0, 1), 0, 1, 8)
        );
        assertArrayEquals(
                new int[]{0, 6, 7},
                pageIndexesForDeltaFilePageStore(dirtyPages.getPartitionView(pageMemory1, 0, 1), 0, 1, 9)
        );
        assertArrayEquals(
                new int[]{0, 6, 7, 9},
                pageIndexesForDeltaFilePageStore(dirtyPages.getPartitionView(pageMemory1, 0, 1), 0, 1, 10)
        );
    }

    @Test
    void testPageIndexesForDeltaFilePageStoreWithPartitionMetaPage() {
        PersistentPageMemory pageMemory0 = mock(PersistentPageMemory.class);
        PersistentPageMemory pageMemory1 = mock(PersistentPageMemory.class);
        PersistentPageMemory pageMemory2 = mock(PersistentPageMemory.class);

        setPartitionGeneration(pageMemory0, 1);
        setPartitionGeneration(pageMemory1, 1);
        setPartitionGeneration(pageMemory2, 1);

        var dirtyPages = new CheckpointDirtyPages(List.of(
                createDirtyPagesAndPartitions(pageMemory0, dirtyPageArray(0, 0, 0, 1, 3, 5)),
                createDirtyPagesAndPartitions(pageMemory1, dirtyPageArray(0, 1, 0, 6, 7, 9)),
                createDirtyPagesAndPartitions(pageMemory2, dirtyPageArray(0, 2, 0))
        ));

        assertArrayEquals(
                new int[]{0},
                pageIndexesForDeltaFilePageStore(dirtyPages.getPartitionView(pageMemory2, 0, 2), 0, 2, 1)
        );
        assertArrayEquals(
                new int[]{0},
                pageIndexesForDeltaFilePageStore(dirtyPages.getPartitionView(pageMemory0, 0, 0), 0, 0, 1)
        );
        assertArrayEquals(
                new int[]{0, 6, 7},
                pageIndexesForDeltaFilePageStore(dirtyPages.getPartitionView(pageMemory1, 0, 1), 0, 1, 8)
        );
        assertArrayEquals(
                new int[]{0, 6, 7},
                pageIndexesForDeltaFilePageStore(dirtyPages.getPartitionView(pageMemory1, 0, 1), 0, 1, 9)
        );
        assertArrayEquals(
                new int[]{0, 6, 7, 9},
                pageIndexesForDeltaFilePageStore(dirtyPages.getPartitionView(pageMemory1, 0, 1), 0, 1, 10)
        );
    }

    @Test
    void testPageIndexesForDeltaFilePageStoreWithDifferentPartitionGeneration() {
        PersistentPageMemory pageMemory = mock(PersistentPageMemory.class);

        PageIndexesWithPartitionGeneration[] pageIndexes = {
                pageIndexesWithPartGen(1, 0, 1, 2, 3),
                pageIndexesWithPartGen(2, 4, 5),
                pageIndexesWithPartGen(3, 0),
                pageIndexesWithPartGen(4, 1),
                pageIndexesWithPartGen(5, 0, 1, 3, 4),
        };

        var dirtyPages = new CheckpointDirtyPages(List.of(
                createDirtyPagesAndPartitions(pageMemory, dirtyFullPageIds(0, 0, pageIndexes))
        ));

        // Checks for 1 generation.
        setPartitionGeneration(pageMemory, 1);

        CheckpointDirtyPagesView partitionView = dirtyPages.getPartitionView(pageMemory, 0, 0);
        assertNotNull(partitionView);

        assertArrayEquals(
                new int[]{0}, pageIndexesForDeltaFilePageStore(partitionView, 0, 0, 1)
        );
        assertArrayEquals(
                new int[]{0, 1}, pageIndexesForDeltaFilePageStore(partitionView, 0, 0, 2)
        );
        assertArrayEquals(
                new int[]{0, 1, 2}, pageIndexesForDeltaFilePageStore(partitionView, 0, 0, 3)
        );
        assertArrayEquals(
                new int[]{0, 1, 2, 3}, pageIndexesForDeltaFilePageStore(partitionView, 0, 0, 4)
        );
        assertArrayEquals(
                new int[]{0, 1, 2, 3}, pageIndexesForDeltaFilePageStore(partitionView, 0, 0, 5)
        );

        // Checks for 2 generation.
        setPartitionGeneration(pageMemory, 2);

        assertArrayEquals(
                new int[]{0}, pageIndexesForDeltaFilePageStore(partitionView, 0, 0, 1)
        );
        assertArrayEquals(
                new int[]{0}, pageIndexesForDeltaFilePageStore(partitionView, 0, 0, 2)
        );
        assertArrayEquals(
                new int[]{0}, pageIndexesForDeltaFilePageStore(partitionView, 0, 0, 3)
        );
        assertArrayEquals(
                new int[]{0}, pageIndexesForDeltaFilePageStore(partitionView, 0, 0, 4)
        );
        assertArrayEquals(
                new int[]{0, 4}, pageIndexesForDeltaFilePageStore(partitionView, 0, 0, 5)
        );
        assertArrayEquals(
                new int[]{0, 4, 5}, pageIndexesForDeltaFilePageStore(partitionView, 0, 0, 6)
        );
        assertArrayEquals(
                new int[]{0, 4, 5}, pageIndexesForDeltaFilePageStore(partitionView, 0, 0, 7)
        );

        // Checks for 3 generation.
        setPartitionGeneration(pageMemory, 3);

        assertArrayEquals(
                new int[]{0}, pageIndexesForDeltaFilePageStore(partitionView, 0, 0, 1)
        );
        assertArrayEquals(
                new int[]{0}, pageIndexesForDeltaFilePageStore(partitionView, 0, 0, 2)
        );

        // Checks for 4 generation.
        setPartitionGeneration(pageMemory, 4);

        assertArrayEquals(
                new int[]{0}, pageIndexesForDeltaFilePageStore(partitionView, 0, 0, 1)
        );
        assertArrayEquals(
                new int[]{0, 1}, pageIndexesForDeltaFilePageStore(partitionView, 0, 0, 2)
        );
        assertArrayEquals(
                new int[]{0, 1}, pageIndexesForDeltaFilePageStore(partitionView, 0, 0, 3)
        );

        // Checks for 5 generation.
        setPartitionGeneration(pageMemory, 5);

        assertArrayEquals(
                new int[]{0}, pageIndexesForDeltaFilePageStore(partitionView, 0, 0, 1)
        );
        assertArrayEquals(
                new int[]{0, 1}, pageIndexesForDeltaFilePageStore(partitionView, 0, 0, 2)
        );
        assertArrayEquals(
                new int[]{0, 1}, pageIndexesForDeltaFilePageStore(partitionView, 0, 0, 3)
        );
        assertArrayEquals(
                new int[]{0, 1, 3}, pageIndexesForDeltaFilePageStore(partitionView, 0, 0, 4)
        );
        assertArrayEquals(
                new int[]{0, 1, 3, 4}, pageIndexesForDeltaFilePageStore(partitionView, 0, 0, 5)
        );
        assertArrayEquals(
                new int[]{0, 1, 3, 4}, pageIndexesForDeltaFilePageStore(partitionView, 0, 0, 6)
        );
    }

    @Test
    void testWritePageToDeltaFilePageStore() throws Exception {
        FilePageStoreManager filePageStoreManager = mock(FilePageStoreManager.class);

        DeltaFilePageStoreIo deltaFilePageStoreIo = mock(DeltaFilePageStoreIo.class);

        FilePageStore filePageStore = mock(FilePageStore.class);

        when(filePageStore.checkpointedPageCount()).thenReturn(2);

        AtomicReference<FilePageStore> filePageStoreRef = new AtomicReference<>(filePageStore);

        when(filePageStore.getOrCreateNewDeltaFile(any(IntFunction.class), any(Supplier.class)))
                .thenReturn(completedFuture(deltaFilePageStoreIo));

        doAnswer(InvocationOnMock::callRealMethod).when(filePageStore).markToDestroy();

        when(filePageStore.isMarkedToDestroy()).then(InvocationOnMock::callRealMethod);

        // Will be written to delta file, as file page store already has 2 pages.
        var dirtyPageId = new DirtyFullPageId(pageId(0, (byte) 0, 1), 0, 1);
        // Will be written to main file, as it is newly allocated.
        var dirtyPageId2 = new DirtyFullPageId(pageId(0, (byte) 0, 2), 0, 1);

        when(filePageStoreManager.getStore(eq(new GroupPartitionId(dirtyPageId.groupId(), dirtyPageId.partitionId()))))
                .then(answer -> filePageStoreRef.get());

        CheckpointManager checkpointManager = spy(new CheckpointManager(
                "test",
                null,
                mock(FailureManager.class),
                CheckpointConfiguration.builder().build(),
                filePageStoreManager,
                mock(PartitionMetaManager.class),
                List.of(),
                mock(PageIoRegistry.class),
                mock(LogSyncer.class),
                executorService,
                new CollectionMetricSource("test", "storage", null),
                1024
        ));

        PersistentPageMemory pageMemory = mock(PersistentPageMemory.class);

        CheckpointProgress checkpointProgress = mock(CheckpointProgress.class);

        var dirtyPages = new CheckpointDirtyPages(List.of(createDirtyPagesAndPartitions(pageMemory, dirtyPageId, dirtyPageId2)));

        when(checkpointProgress.inProgress()).thenReturn(true);

        when(checkpointProgress.pagesToWrite()).thenReturn(dirtyPages);

        when(checkpointManager.lastCheckpointProgress()).thenReturn(checkpointProgress);

        // Spying because mocking ByteBuffer does not work on Java 21.
        ByteBuffer pageBuf = spy(ByteBuffer.wrap(new byte[deltaFilePageStoreIo.pageSize()]));

        checkpointManager.writePageToFilePageStore(pageMemory, dirtyPageId, pageBuf);
        checkpointManager.writePageToFilePageStore(pageMemory, dirtyPageId2, pageBuf);

        verify(deltaFilePageStoreIo, times(1)).write(eq(dirtyPageId.pageId()), eq(pageBuf));
        verify(filePageStore, (times(1))).write(eq(dirtyPageId2.pageId()), eq(pageBuf));
    }

    private static DirtyFullPageId[] dirtyPageArray(int grpId, int partId, int... pageIndex) {
        Arrays.sort(pageIndex);

        return IntStream.of(pageIndex)
                .mapToObj(pageIdx -> new DirtyFullPageId(pageId(partId, (byte) 0, pageIdx), grpId, 1))
                .toArray(DirtyFullPageId[]::new);
    }

    private static void setPartitionGeneration(PersistentPageMemory pageMemory, int partitionGeneration) {
        when(pageMemory.partGeneration(anyInt(), anyInt())).thenReturn(partitionGeneration);
    }
}
