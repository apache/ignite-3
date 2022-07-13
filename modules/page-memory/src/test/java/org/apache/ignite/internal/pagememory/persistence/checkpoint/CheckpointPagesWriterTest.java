/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointTestUtils.createPartitionFilePageStoreManager;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageId;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageIndex;
import static org.apache.ignite.internal.util.GridUnsafe.allocateBuffer;
import static org.apache.ignite.internal.util.GridUnsafe.bufferAddress;
import static org.apache.ignite.internal.util.GridUnsafe.zeroMemory;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.TestPageIoModule.TestPageIo;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.PageStoreWriter;
import org.apache.ignite.internal.pagememory.persistence.PartitionMeta;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.io.PartitionMetaIo;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStore;
import org.apache.ignite.internal.pagememory.persistence.store.PageStore;
import org.apache.ignite.internal.pagememory.persistence.store.PartitionFilePageStore;
import org.apache.ignite.internal.util.IgniteConcurrentMultiPairQueue;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;

/**
 * For {@link CheckpointPagesWriter} testing.
 */
public class CheckpointPagesWriterTest {
    private static final int PAGE_SIZE = 1024;

    private final IgniteLogger log = Loggers.forClass(CheckpointPagesWriterTest.class);

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
        FullPageId fullPageId6 = new FullPageId(pageId(0, FLAG_DATA, 6), 0);

        IgniteConcurrentMultiPairQueue<PersistentPageMemory, FullPageId> writePageIds = new IgniteConcurrentMultiPairQueue<>(
                Map.of(pageMemory, List.of(fullPageId1, fullPageId2, fullPageId3, fullPageId4, fullPageId5, fullPageId6))
        );

        GroupPartitionId groupPartId0 = groupPartId(0, 0);
        GroupPartitionId groupPartId1 = groupPartId(0, 1);

        IgniteConcurrentMultiPairQueue<PersistentPageMemory, GroupPartitionId> updatePartitionIds = new IgniteConcurrentMultiPairQueue<>(
                Map.of(pageMemory, List.of(groupPartId0, groupPartId1))
        );

        Runnable beforePageWrite = mock(Runnable.class);

        ThreadLocal<ByteBuffer> threadBuf = createThreadLocalBuffer();

        PageStore pageStore = mock(PageStore.class);

        ArgumentCaptor<FullPageId> writtenFullPageIds = ArgumentCaptor.forClass(FullPageId.class);

        CheckpointPageWriter pageWriter = createCheckpointPageWriter(pageStore, writtenFullPageIds);

        ConcurrentMap<PageStore, LongAdder> updStores = new ConcurrentHashMap<>();

        CompletableFuture<?> doneFuture = new CompletableFuture<>();

        CheckpointMetricsTracker tracker = new CheckpointMetricsTracker();

        CheckpointProgressImpl progressImpl = new CheckpointProgressImpl(0);

        PartitionFilePageStore partitionPageStore0 = spy(new PartitionFilePageStore(
                createFilePageStore(true),
                mock(PartitionMeta.class)
        ));

        PartitionFilePageStore partitionPageStore1 = spy(new PartitionFilePageStore(
                createFilePageStore(false),
                mock(PartitionMeta.class)
        ));

        CheckpointPagesWriter pagesWriter = new CheckpointPagesWriter(
                log,
                tracker,
                writePageIds,
                updatePartitionIds,
                updStores,
                doneFuture,
                beforePageWrite,
                threadBuf,
                progressImpl,
                pageWriter,
                ioRegistry,
                createPartitionFilePageStoreManager(Map.of(groupPartId0, partitionPageStore0, groupPartId1, partitionPageStore1)),
                () -> false
        );

        pagesWriter.run();

        // Checks.

        assertDoesNotThrow(() -> doneFuture.get(1, TimeUnit.SECONDS));

        assertTrue(writePageIds.isEmpty());
        assertTrue(updatePartitionIds.isEmpty());

        assertThat(updStores.keySet(), equalTo(Set.of(pageStore)));
        assertThat(updStores.get(pageStore).sum(), equalTo(8L));

        assertThat(tracker.dataPagesWritten(), equalTo(4));
        assertThat(progressImpl.writtenPagesCounter().get(), equalTo(8));

        assertThat(
                writtenFullPageIds.getAllValues(),
                equalTo(List.of(
                        // Order is different because the first 3 pages we have to try to write to the page store 2 times.
                        fullPageId4, fullPageId5, fullPageId6, fullPageId1, fullPageId2, fullPageId3,
                        // First pages (PartitionMetaIo) in the partition.
                        fullPageId(0, 0, 0), fullPageId(0, 1, 0)
                ))
        );

        verify(beforePageWrite, times(9)).run();

        verify(threadBuf, times(4)).get();

        verify(partitionPageStore0, times(1)).meta();
        verify(partitionPageStore1, times(1)).meta();
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

        PartitionFilePageStore partitionPageStore = spy(new PartitionFilePageStore(mock(FilePageStore.class), mock(PartitionMeta.class)));

        CheckpointPagesWriter pagesWriter = new CheckpointPagesWriter(
                log,
                new CheckpointMetricsTracker(),
                new IgniteConcurrentMultiPairQueue<>(Map.of(pageMemory, List.of(fullPageId(0, 0, 1)))),
                new IgniteConcurrentMultiPairQueue<>(Map.of(pageMemory, List.of(groupPartId))),
                new ConcurrentHashMap<>(),
                doneFuture,
                () -> {
                },
                createThreadLocalBuffer(),
                new CheckpointProgressImpl(0),
                mock(CheckpointPageWriter.class),
                ioRegistry,
                createPartitionFilePageStoreManager(Map.of(groupPartId, partitionPageStore)),
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

        IgniteConcurrentMultiPairQueue<PersistentPageMemory, GroupPartitionId> updatePartitionIds = new IgniteConcurrentMultiPairQueue<>(
                Map.of(pageMemory, List.of(groupPartId))
        );

        PartitionFilePageStore partitionPageStore = spy(new PartitionFilePageStore(mock(FilePageStore.class), mock(PartitionMeta.class)));

        CheckpointPagesWriter pagesWriter = new CheckpointPagesWriter(
                log,
                new CheckpointMetricsTracker(),
                writePageIds,
                updatePartitionIds,
                new ConcurrentHashMap<>(),
                doneFuture,
                () -> {
                },
                createThreadLocalBuffer(),
                new CheckpointProgressImpl(0),
                mock(CheckpointPageWriter.class),
                ioRegistry,
                createPartitionFilePageStoreManager(Map.of(groupPartId, partitionPageStore)),
                () -> checkpointWritePageCount.get() > 0
        );

        pagesWriter.run();

        assertDoesNotThrow(() -> doneFuture.get(1, TimeUnit.SECONDS));

        assertThat(writePageIds.size(), equalTo(1));
        assertThat(updatePartitionIds.size(), equalTo(1));
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

        when(pageMemory.partitionMetaPageId(anyInt(), anyInt())).then(InvocationOnMock::callRealMethod);

        return pageMemory;
    }

    private static ThreadLocal<ByteBuffer> createThreadLocalBuffer() {
        ThreadLocal<ByteBuffer> threadBuf = mock(ThreadLocal.class);

        ByteBuffer buffer = allocateBuffer(PAGE_SIZE);

        when(threadBuf.get()).thenReturn(buffer.rewind());

        return threadBuf;
    }

    /**
     * Returns mocked instance of {@link CheckpointPageWriter}.
     *
     * @param pageStore Return value for {@link CheckpointPageWriter#write}.
     * @param fullPageIdArgumentCaptor Collector of pages that will fall into {@link CheckpointPageWriter#write}.
     */
    private static CheckpointPageWriter createCheckpointPageWriter(
            PageStore pageStore,
            ArgumentCaptor<FullPageId> fullPageIdArgumentCaptor
    ) throws Exception {
        CheckpointPageWriter pageWriter = mock(CheckpointPageWriter.class);

        when(pageWriter.write(fullPageIdArgumentCaptor.capture(), any(ByteBuffer.class), anyInt())).thenReturn(pageStore);

        return pageWriter;
    }

    private static FullPageId fullPageId(int grpId, int partId, int pageIdx) {
        return new FullPageId(pageId(partId, (byte) 0, pageIdx), grpId);
    }

    private static GroupPartitionId groupPartId(int grpId, int partId) {
        return new GroupPartitionId(grpId, partId);
    }

    private static FilePageStore createFilePageStore(boolean readEmptyPartitionMetaIo) throws Exception {
        FilePageStore filePageStore = mock(FilePageStore.class);

        when(filePageStore.read(anyLong(), any(ByteBuffer.class), anyBoolean())).then(answer -> {
            long pageId = answer.getArgument(0);

            assertEquals(0, pageIndex(pageId));

            ByteBuffer buffer = answer.getArgument(1);

            long pageAddr = bufferAddress(buffer);

            if (readEmptyPartitionMetaIo) {
                zeroMemory(pageAddr, PAGE_SIZE);
            } else {
                PartitionMetaIo.VERSIONS.latest().initNewPage(pageAddr, pageId, PAGE_SIZE);
            }

            return true;
        });

        return filePageStore;
    }
}
