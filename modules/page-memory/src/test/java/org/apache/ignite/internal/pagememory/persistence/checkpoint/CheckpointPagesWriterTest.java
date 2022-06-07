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
import static org.apache.ignite.internal.pagememory.persistence.PageMemoryImpl.TRY_AGAIN_TAG;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageId;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
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
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.persistence.PageMemoryImpl;
import org.apache.ignite.internal.pagememory.persistence.PageStoreWriter;
import org.apache.ignite.internal.pagememory.persistence.store.PageStore;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteLogger;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * For {@link CheckpointPagesWriter} testing.
 */
public class CheckpointPagesWriterTest {
    private final IgniteLogger log = IgniteLogger.forClass(CheckpointPagesWriterTest.class);

    @Test
    void testWritePages() throws Exception {
        PageMemoryImpl pageMemory = createPageMemoryImpl(3);

        FullPageId fullPageId0 = new FullPageId(pageId(0, FLAG_DATA, 0), 0);
        FullPageId fullPageId1 = new FullPageId(pageId(0, FLAG_DATA, 1), 0);
        FullPageId fullPageId2 = new FullPageId(pageId(0, FLAG_AUX, 2), 0);
        FullPageId fullPageId3 = new FullPageId(pageId(0, FLAG_AUX, 3), 0);
        FullPageId fullPageId4 = new FullPageId(pageId(1, FLAG_DATA, 4), 0);
        FullPageId fullPageId5 = new FullPageId(pageId(1, FLAG_DATA, 5), 0);

        IgniteConcurrentMultiPairQueue<PageMemoryImpl, FullPageId> writePageIds = new IgniteConcurrentMultiPairQueue<>(
                Map.of(pageMemory, List.of(fullPageId0, fullPageId1, fullPageId2, fullPageId3, fullPageId4, fullPageId5))
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

        CheckpointPagesWriter pagesWriter = new CheckpointPagesWriter(
                log,
                tracker,
                writePageIds,
                updStores,
                doneFuture,
                beforePageWrite,
                threadBuf,
                progressImpl,
                pageWriter,
                () -> false
        );

        pagesWriter.run();

        // Checks.

        assertDoesNotThrow(() -> doneFuture.get(1, TimeUnit.SECONDS));

        assertTrue(writePageIds.isEmpty());

        assertThat(updStores.keySet(), equalTo(Set.of(pageStore)));
        assertThat(updStores.get(pageStore).sum(), equalTo(6L));

        assertThat(tracker.dataPagesWritten(), equalTo(4));
        assertThat(progressImpl.writtenPagesCounter().get(), equalTo(6));

        assertThat(
                writtenFullPageIds.getAllValues(),
                // Order is different because the first 3 pages we have to try to write to the page store 2 times.
                equalTo(List.of(fullPageId3, fullPageId4, fullPageId5, fullPageId0, fullPageId1, fullPageId2))
        );

        verify(beforePageWrite, times(9)).run();

        verify(threadBuf, times(2)).get();
    }

    @Test
    void testFailWritePages() throws Exception {
        CompletableFuture<?> doneFuture = new CompletableFuture<>();

        PageMemoryImpl pageMemory = mock(PageMemoryImpl.class);

        doThrow(IgniteInternalCheckedException.class)
                .when(pageMemory)
                .checkpointWritePage(
                        any(FullPageId.class),
                        any(ByteBuffer.class),
                        any(PageStoreWriter.class),
                        any(CheckpointMetricsTracker.class)
                );

        CheckpointPagesWriter pagesWriter = new CheckpointPagesWriter(
                log,
                new CheckpointMetricsTracker(),
                new IgniteConcurrentMultiPairQueue<>(Map.of(pageMemory, List.of(new FullPageId(0, 0)))),
                new ConcurrentHashMap<>(),
                doneFuture,
                () -> {
                },
                createThreadLocalBuffer(),
                new CheckpointProgressImpl(0),
                mock(CheckpointPageWriter.class),
                () -> false
        );

        pagesWriter.run();

        ExecutionException exception = assertThrows(ExecutionException.class, () -> doneFuture.get(1, TimeUnit.SECONDS));

        assertThat(exception.getCause(), instanceOf(IgniteInternalCheckedException.class));
    }

    @Test
    void testShutdownNow() throws Exception {
        CompletableFuture<?> doneFuture = new CompletableFuture<>();

        PageMemoryImpl pageMemory = mock(PageMemoryImpl.class);

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

        IgniteConcurrentMultiPairQueue<PageMemoryImpl, FullPageId> writePageIds = new IgniteConcurrentMultiPairQueue<>(
                Map.of(pageMemory, List.of(new FullPageId(0, 0), new FullPageId(1, 0)))
        );

        CheckpointPagesWriter pagesWriter = new CheckpointPagesWriter(
                log,
                new CheckpointMetricsTracker(),
                writePageIds,
                new ConcurrentHashMap<>(),
                doneFuture,
                () -> {
                },
                createThreadLocalBuffer(),
                new CheckpointProgressImpl(0),
                mock(CheckpointPageWriter.class),
                () -> checkpointWritePageCount.get() > 0
        );

        pagesWriter.run();

        assertDoesNotThrow(() -> doneFuture.get(1, TimeUnit.SECONDS));

        assertThat(writePageIds.size(), equalTo(1));
    }

    /**
     * Returns mocked instance of {@link PageMemoryImpl}.
     *
     * @param tryAgainTagFirstPageCount Number of first pages for which the tag value will be {@link PageMemoryImpl#TRY_AGAIN_TAG}.
     * @throws Exception If failed.
     */
    private static PageMemoryImpl createPageMemoryImpl(int tryAgainTagFirstPageCount) throws Exception {
        PageMemoryImpl pageMemory = mock(PageMemoryImpl.class);

        AtomicInteger pageCount = new AtomicInteger();

        doAnswer(answer -> {
            PageStoreWriter pageStoreWriter = answer.getArgument(2);

            int tag = pageCount.incrementAndGet() > tryAgainTagFirstPageCount ? 0 : TRY_AGAIN_TAG;

            pageStoreWriter.writePage(answer.getArgument(0), answer.getArgument(1), tag);

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

        ByteBuffer buffer = ByteBuffer.allocate(4);

        buffer.putInt(-1);

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
}
