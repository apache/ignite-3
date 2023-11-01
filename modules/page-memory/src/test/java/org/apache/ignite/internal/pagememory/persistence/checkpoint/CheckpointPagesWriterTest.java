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

import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_AUX;
import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory.TRY_AGAIN_TAG;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointTestUtils.createPartitionMetaManager;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageId;
import static org.apache.ignite.internal.util.GridUnsafe.allocateBuffer;
import static org.apache.ignite.internal.util.GridUnsafe.bufferAddress;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
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
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.TestPageIoModule.TestPageIo;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.PageStoreWriter;
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

        FullPageId fullPageId1 = new FullPageId(pageId(0, FLAG_DATA, 1), 0);
        FullPageId fullPageId2 = new FullPageId(pageId(0, FLAG_DATA, 2), 0);
        FullPageId fullPageId3 = new FullPageId(pageId(0, FLAG_AUX, 3), 0);
        FullPageId fullPageId4 = new FullPageId(pageId(0, FLAG_AUX, 4), 0);
        FullPageId fullPageId5 = new FullPageId(pageId(0, FLAG_DATA, 5), 0);
        FullPageId fullPageId6 = new FullPageId(pageId(1, FLAG_DATA, 6), 0);

        IgniteConcurrentMultiPairQueue<PersistentPageMemory, FullPageId> writePageIds = new IgniteConcurrentMultiPairQueue<>(
                Map.of(pageMemory, List.of(fullPageId1, fullPageId2, fullPageId3, fullPageId4, fullPageId5, fullPageId6))
        );

        GroupPartitionId groupPartId0 = groupPartId(0, 0);
        GroupPartitionId groupPartId1 = groupPartId(0, 1);

        Runnable beforePageWrite = mock(Runnable.class);

        ThreadLocal<ByteBuffer> threadBuf = createThreadLocalBuffer();

        ArgumentCaptor<FullPageId> writtenFullPageIds = ArgumentCaptor.forClass(FullPageId.class);

        WriteDirtyPage pageWriter = createDirtyPageWriter(writtenFullPageIds);

        ConcurrentMap<GroupPartitionId, LongAdder> updatedPartitions = new ConcurrentHashMap<>();

        CompletableFuture<?> doneFuture = new CompletableFuture<>();

        CheckpointMetricsTracker tracker = new CheckpointMetricsTracker();

        CheckpointProgressImpl progressImpl = new CheckpointProgressImpl(0);

        PartitionMeta partitionMeta0 = mock(PartitionMeta.class);
        PartitionMeta partitionMeta1 = mock(PartitionMeta.class);

        CheckpointPagesWriter pagesWriter = new CheckpointPagesWriter(
                tracker,
                writePageIds,
                updatedPartitions,
                doneFuture,
                beforePageWrite,
                threadBuf,
                progressImpl,
                pageWriter,
                ioRegistry,
                createPartitionMetaManager(Map.of(groupPartId0, partitionMeta0, groupPartId1, partitionMeta1)),
                () -> false
        );

        pagesWriter.run();

        // Checks.

        assertDoesNotThrow(() -> doneFuture.get(1, TimeUnit.SECONDS));

        assertTrue(writePageIds.isEmpty());

        assertThat(updatedPartitions.keySet(), containsInAnyOrder(groupPartId0, groupPartId1));

        assertThat(updatedPartitions.get(groupPartId0).sum(), equalTo(6L));
        assertThat(updatedPartitions.get(groupPartId1).sum(), equalTo(2L));

        assertThat(tracker.dataPagesWritten(), equalTo(4));
        assertThat(progressImpl.writtenPagesCounter().get(), equalTo(8));

        assertThat(
                writtenFullPageIds.getAllValues(),
                equalTo(List.of(
                        // At the beginning, we write the partition meta for each new partition.
                        fullPageId(0, 0, 0),
                        // Order is different because the first 3 pages we have to try to write to the page store 2 times.
                        fullPageId4, fullPageId5,
                        fullPageId(0, 1, 0),
                        fullPageId6, fullPageId1, fullPageId2, fullPageId3
                ))
        );

        verify(beforePageWrite, times(9)).run();

        verify(threadBuf, times(2)).get();

        verify(partitionMeta0, times(1)).metaSnapshot(any(UUID.class));
        verify(partitionMeta0, times(1)).metaSnapshot(any(UUID.class));
    }

    @Test
    void testFailWritePages() throws Exception {
        CompletableFuture<?> doneFuture = new CompletableFuture<>();

        PersistentPageMemory pageMemory = mock(PersistentPageMemory.class);

        doThrow(IgniteInternalCheckedException.class)
                .when(pageMemory)
                .checkpointWritePage(
                        any(FullPageId.class),
                        any(ByteBuffer.class),
                        any(PageStoreWriter.class),
                        any(CheckpointMetricsTracker.class)
                );

        GroupPartitionId groupPartId = groupPartId(0, 0);

        CheckpointPagesWriter pagesWriter = new CheckpointPagesWriter(
                new CheckpointMetricsTracker(),
                new IgniteConcurrentMultiPairQueue<>(Map.of(pageMemory, List.of(fullPageId(0, 0, 1)))),
                new ConcurrentHashMap<>(),
                doneFuture,
                () -> {},
                createThreadLocalBuffer(),
                new CheckpointProgressImpl(0),
                createDirtyPageWriter(null),
                ioRegistry,
                createPartitionMetaManager(Map.of(groupPartId, mock(PartitionMeta.class))),
                () -> false
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
                        any(FullPageId.class),
                        any(ByteBuffer.class),
                        any(PageStoreWriter.class),
                        any(CheckpointMetricsTracker.class)
                );

        IgniteConcurrentMultiPairQueue<PersistentPageMemory, FullPageId> writePageIds = new IgniteConcurrentMultiPairQueue<>(
                Map.of(pageMemory, List.of(fullPageId(0, 0, 1), fullPageId(0, 0, 2)))
        );

        GroupPartitionId groupPartId = groupPartId(0, 0);

        ConcurrentMap<GroupPartitionId, LongAdder> updatedPartitions = new ConcurrentHashMap<>();

        CheckpointPagesWriter pagesWriter = new CheckpointPagesWriter(
                new CheckpointMetricsTracker(),
                writePageIds,
                updatedPartitions,
                doneFuture,
                () -> {},
                createThreadLocalBuffer(),
                new CheckpointProgressImpl(0),
                createDirtyPageWriter(null),
                ioRegistry,
                createPartitionMetaManager(Map.of(groupPartId, mock(PartitionMeta.class))),
                () -> checkpointWritePageCount.get() > 0
        );

        pagesWriter.run();

        assertDoesNotThrow(() -> doneFuture.get(1, TimeUnit.SECONDS));

        assertThat(writePageIds.size(), equalTo(1));
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

            FullPageId fullPageId = answer.getArgument(0);

            ByteBuffer buffer = answer.getArgument(1);

            new TestPageIo().initNewPage(bufferAddress(buffer), fullPageId.pageId(), PAGE_SIZE);

            pageStoreWriter.writePage(fullPageId, buffer, tag);

            return null;
        })
                .when(pageMemory)
                .checkpointWritePage(
                        any(FullPageId.class),
                        any(ByteBuffer.class),
                        any(PageStoreWriter.class),
                        any(CheckpointMetricsTracker.class)
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
            @Nullable ArgumentCaptor<FullPageId> fullPageIdArgumentCaptor
    ) throws Exception {
        WriteDirtyPage writer = mock(WriteDirtyPage.class);

        if (fullPageIdArgumentCaptor != null) {
            doNothing().when(writer).write(any(PersistentPageMemory.class), fullPageIdArgumentCaptor.capture(), any(ByteBuffer.class));
        }

        return writer;
    }

    private static FullPageId fullPageId(int grpId, int partId, int pageIdx) {
        return new FullPageId(pageId(partId, (byte) 0, pageIdx), grpId);
    }

    private static GroupPartitionId groupPartId(int grpId, int partId) {
        return new GroupPartitionId(grpId, partId);
    }
}
