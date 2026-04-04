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

import static java.util.Collections.singletonList;
import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_AUX;
import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory.TRY_AGAIN_TAG;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointTestUtils.createPartitionMetaManager;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.TestCheckpointUtils.createDirtyPagesAndPartitions;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageId;
import static org.apache.ignite.internal.util.GridUnsafe.allocateBuffer;
import static org.apache.ignite.internal.util.GridUnsafe.bufferAddress;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.TestPageIoModule.TestPageIo;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.persistence.DirtyFullPageId;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.PageStoreWriter;
import org.apache.ignite.internal.pagememory.persistence.PageWriteTarget;
import org.apache.ignite.internal.pagememory.persistence.PartitionDestructionLockManager;
import org.apache.ignite.internal.pagememory.persistence.PartitionMeta;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.WriteDirtyPage;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteConcurrentMultiPairQueue;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * For {@link CheckpointPagesWriter} testing.
 */
public class CheckpointPagesWriterTest extends BaseIgniteAbstractTest {
    private static final int PAGE_SIZE = 1024;

    private static PageIoRegistry ioRegistry;

    @BeforeAll
    static void beforeAll() {
        ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();
    }

    @AfterAll
    static void afterAll() {
        ioRegistry = null;
    }

    @Test
    void testWritePages() throws Exception {
        PersistentPageMemory pageMemory = createPageMemory(3);

        var fullPageId1 = new DirtyFullPageId(pageId(0, FLAG_DATA, 1), 0, 1);
        var fullPageId2 = new DirtyFullPageId(pageId(0, FLAG_DATA, 2), 0, 1);
        var fullPageId3 = new DirtyFullPageId(pageId(0, FLAG_AUX, 3), 0, 1);
        var fullPageId4 = new DirtyFullPageId(pageId(0, FLAG_AUX, 4), 0, 1);
        var fullPageId5 = new DirtyFullPageId(pageId(0, FLAG_DATA, 5), 0, 1);
        var fullPageId6 = new DirtyFullPageId(pageId(1, FLAG_DATA, 6), 0, 1);

        CheckpointDirtyPages checkpointDirtyPages = new CheckpointDirtyPages(List.of(
                createDirtyPagesAndPartitions(pageMemory, fullPageId1, fullPageId2, fullPageId3, fullPageId4, fullPageId5, fullPageId6)
        ));

        GroupPartitionId groupPartId0 = groupPartId(0, 0);
        GroupPartitionId groupPartId1 = groupPartId(0, 1);

        Runnable beforePageWrite = mock(Runnable.class);

        ThreadLocal<ByteBuffer> threadBuf = createThreadLocalBuffer();

        ArgumentCaptor<DirtyFullPageId> writtenFullPageIds = ArgumentCaptor.forClass(DirtyFullPageId.class);

        WriteDirtyPage pageWriter = createDirtyPageWriter(writtenFullPageIds);

        ConcurrentMap<GroupPartitionId, PartitionWriteStats> updatedPartitions = new ConcurrentHashMap<>();

        CompletableFuture<?> doneFuture = new CompletableFuture<>();

        CheckpointMetricsTracker tracker = new CheckpointMetricsTracker();

        CheckpointProgressImpl progressImpl = new CheckpointProgressImpl(0);
        progressImpl.pagesToWrite(checkpointDirtyPages);

        PartitionMeta partitionMeta0 = mock(PartitionMeta.class);
        PartitionMeta partitionMeta1 = mock(PartitionMeta.class);

        when(partitionMeta0.partitionGeneration()).thenReturn(1);
        when(partitionMeta1.partitionGeneration()).thenReturn(1);

        when(pageMemory.partGeneration(anyInt(), anyInt())).thenReturn(1);

        IgniteConcurrentMultiPairQueue<PersistentPageMemory, GroupPartitionId> dirtyPartitionQueue
                = checkpointDirtyPages.toDirtyPartitionQueue();

        CheckpointPagesWriter pagesWriter = new CheckpointPagesWriter(
                tracker,
                dirtyPartitionQueue,
                singletonList(pageMemory),
                updatedPartitions,
                doneFuture,
                beforePageWrite,
                threadBuf,
                progressImpl,
                pageWriter,
                ioRegistry,
                createPartitionMetaManager(Map.of(groupPartId0, partitionMeta0, groupPartId1, partitionMeta1)),
                () -> false,
                new PartitionDestructionLockManager()
        );

        pagesWriter.run();

        // Checks.

        assertDoesNotThrow(() -> doneFuture.get(1, TimeUnit.SECONDS));

        assertTrue(dirtyPartitionQueue.isEmpty());

        assertThat(updatedPartitions.keySet(), containsInAnyOrder(groupPartId0, groupPartId1));

        assertThat(updatedPartitions.get(groupPartId0).getTotalWrites(), equalTo(6));
        assertThat(updatedPartitions.get(groupPartId1).getTotalWrites(), equalTo(2));

        assertThat(tracker.dataPagesWritten(), equalTo(4));
        assertThat(progressImpl.writtenPagesCounter().get(), equalTo(8));

        assertThat(
                writtenFullPageIds.getAllValues(),
                equalTo(List.of(
                        // At the beginning, we write the partition meta for each new partition.
                        new DirtyFullPageId(pageId(0, FLAG_AUX, 0), 0, 1),
                        // Order is different because the first 3 pages we have to try to write to the page store 2 times.
                        fullPageId4, fullPageId5,
                        // At the beginning, we write the partition meta for each new partition.
                        new DirtyFullPageId(pageId(1, FLAG_AUX, 0), 0, 1),
                        fullPageId6,
                        // Now the retry pages.
                        fullPageId1, fullPageId2, fullPageId3
                ))
        );

        verify(beforePageWrite, times(14)).run();

        verify(threadBuf, times(1)).get();

        verify(partitionMeta0, times(1)).metaSnapshot(any(UUID.class));
        verify(partitionMeta1, times(1)).metaSnapshot(any(UUID.class));
    }

    @Test
    void testFailWritePages() throws Exception {
        CompletableFuture<?> doneFuture = new CompletableFuture<>();

        PersistentPageMemory pageMemory = mock(PersistentPageMemory.class);

        doThrow(new IgniteInternalCheckedException(INTERNAL_ERR))
                .when(pageMemory)
                .checkpointWritePage(
                        any(DirtyFullPageId.class),
                        any(ByteBuffer.class),
                        any(PageStoreWriter.class),
                        any(CheckpointMetricsTracker.class),
                        anyBoolean()
                );

        GroupPartitionId groupPartId = groupPartId(0, 0);

        CheckpointDirtyPages checkpointDirtyPages = new CheckpointDirtyPages(List.of(
                createDirtyPagesAndPartitions(pageMemory, new DirtyFullPageId(pageId(0, FLAG_DATA, 1), 0, 1))
        ));

        CheckpointProgressImpl checkpointProgress = new CheckpointProgressImpl(0);
        checkpointProgress.pagesToWrite(checkpointDirtyPages);

        CheckpointPagesWriter pagesWriter = new CheckpointPagesWriter(
                new CheckpointMetricsTracker(),
                new IgniteConcurrentMultiPairQueue<>(Map.of(pageMemory, List.of(new GroupPartitionId(0, 0)))),
                singletonList(pageMemory),
                new ConcurrentHashMap<>(),
                doneFuture,
                () -> {},
                createThreadLocalBuffer(),
                checkpointProgress,
                createDirtyPageWriter(null),
                ioRegistry,
                createPartitionMetaManager(Map.of(groupPartId, mock(PartitionMeta.class))),
                () -> false,
                new PartitionDestructionLockManager()
        );

        pagesWriter.run();

        ExecutionException exception = assertThrows(ExecutionException.class, () -> doneFuture.get(1, TimeUnit.SECONDS));

        assertThat(exception.getCause(), instanceOf(IgniteInternalCheckedException.class));
    }

    @Test
    void testShutdownNow() throws Exception {
        CompletableFuture<?> doneFuture = new CompletableFuture<>();

        PersistentPageMemory pageMemory = mock(PersistentPageMemory.class);

        AtomicInteger checkpointWritePageCount = new AtomicInteger();

        doAnswer(answer -> {
            checkpointWritePageCount.incrementAndGet();

            return null;
        })
                .when(pageMemory)
                .checkpointWritePage(
                        any(DirtyFullPageId.class),
                        any(ByteBuffer.class),
                        any(PageStoreWriter.class),
                        any(CheckpointMetricsTracker.class),
                        anyBoolean()
                );

        CheckpointDirtyPages checkpointDirtyPages = new CheckpointDirtyPages(List.of(
                createDirtyPagesAndPartitions(pageMemory, dirtyFullPageId(0, 0, 1), dirtyFullPageId(0, 1, 2))
        ));

        GroupPartitionId groupPartId = groupPartId(0, 0);

        ConcurrentMap<GroupPartitionId, PartitionWriteStats> updatedPartitions = new ConcurrentHashMap<>();

        CheckpointProgressImpl checkpointProgress = new CheckpointProgressImpl(0);
        checkpointProgress.pagesToWrite(checkpointDirtyPages);

        IgniteConcurrentMultiPairQueue<PersistentPageMemory, GroupPartitionId> dirtyPartitionQueue
                = checkpointDirtyPages.toDirtyPartitionQueue();

        PartitionMeta partitionMeta = mock(PartitionMeta.class);
        when(partitionMeta.partitionGeneration()).thenReturn(1);

        CheckpointPagesWriter pagesWriter = new CheckpointPagesWriter(
                new CheckpointMetricsTracker(),
                dirtyPartitionQueue,
                singletonList(pageMemory),
                updatedPartitions,
                doneFuture,
                () -> {},
                createThreadLocalBuffer(),
                checkpointProgress,
                createDirtyPageWriter(null),
                ioRegistry,
                createPartitionMetaManager(Map.of(groupPartId, partitionMeta)),
                () -> checkpointWritePageCount.get() > 0,
                new PartitionDestructionLockManager()
        );

        pagesWriter.run();

        assertDoesNotThrow(() -> doneFuture.get(1, TimeUnit.SECONDS));

        assertThat(dirtyPartitionQueue.size(), equalTo(1));
        assertThat(updatedPartitions.keySet(), contains(groupPartId));
    }

    /**
     * Returns mocked instance of {@link PersistentPageMemory}.
     *
     * @param tryAgainTagFirstPageCount Number of first pages for which the tag value will be {@link PersistentPageMemory#TRY_AGAIN_TAG}.
     * @throws Exception If failed.
     */
    private static PersistentPageMemory createPageMemory(int tryAgainTagFirstPageCount) throws Exception {
        PersistentPageMemory pageMemory = mock(PersistentPageMemory.class);

        AtomicInteger pageCount = new AtomicInteger();

        doAnswer(answer -> {
            PageStoreWriter pageStoreWriter = answer.getArgument(2);

            int tag = pageCount.incrementAndGet() > tryAgainTagFirstPageCount ? 0 : TRY_AGAIN_TAG;

            DirtyFullPageId fullPageId = answer.getArgument(0);

            ByteBuffer buffer = answer.getArgument(1);

            new TestPageIo().initNewPage(bufferAddress(buffer), fullPageId.pageId(), PAGE_SIZE);

            pageStoreWriter.writePage(fullPageId, buffer, tag);

            return null;
        })
                .when(pageMemory)
                .checkpointWritePage(
                        any(DirtyFullPageId.class),
                        any(ByteBuffer.class),
                        any(PageStoreWriter.class),
                        any(CheckpointMetricsTracker.class),
                        anyBoolean()
                );

        return pageMemory;
    }

    private static ThreadLocal<ByteBuffer> createThreadLocalBuffer() {
        ThreadLocal<ByteBuffer> threadBuf = mock(ThreadLocal.class);

        ByteBuffer buffer = allocateBuffer(PAGE_SIZE);

        when(threadBuf.get()).thenReturn(buffer.rewind());

        return threadBuf;
    }

    /**
     * Returns mocked instance of {@link WriteDirtyPage}.
     *
     * @param fullPageIdArgumentCaptor Collector of pages that will fall into {@link WriteDirtyPage#write}.
     */
    private static WriteDirtyPage createDirtyPageWriter(
            @Nullable ArgumentCaptor<DirtyFullPageId> fullPageIdArgumentCaptor
    ) throws Exception {
        WriteDirtyPage writer = mock(WriteDirtyPage.class);

        doReturn(PageWriteTarget.MAIN_FILE).when(writer).write(
                any(),
                fullPageIdArgumentCaptor != null ? fullPageIdArgumentCaptor.capture() : any(),
                any()
        );

        return writer;
    }

    private static DirtyFullPageId dirtyFullPageId(int grpId, int partId, int pageIdx) {
        return new DirtyFullPageId(pageId(partId, (byte) 0, pageIdx), grpId, 1);
    }

    private static GroupPartitionId groupPartId(int grpId, int partId) {
        return new GroupPartitionId(grpId, partId);
    }
}
