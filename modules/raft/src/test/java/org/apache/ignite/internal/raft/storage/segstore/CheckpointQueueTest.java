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

package org.apache.ignite.internal.raft.storage.segstore;

import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runRace;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.RunnableX;
import org.apache.ignite.internal.raft.storage.segstore.CheckpointQueue.Entry;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@ExtendWith(ExecutorServiceExtension.class)
class CheckpointQueueTest extends BaseIgniteAbstractTest {
    private static final int MAX_QUEUE_SIZE = 10;

    private final CheckpointQueue queue = new CheckpointQueue(MAX_QUEUE_SIZE);

    @AfterEach
    void tearDown() {
        queue.close();
    }

    @Test
    void testAddPeekRemove(
            @Mock SegmentFile segmentFile1,
            @Mock SegmentFile segmentFile2,
            @Mock ReadModeIndexMemTable memTable1,
            @Mock ReadModeIndexMemTable memTable2
    ) throws InterruptedException {
        queue.add(segmentFile1, memTable1);
        queue.add(segmentFile2, memTable2);

        Entry entry = queue.peekHead();

        assertThat(entry.segmentFile(), is(segmentFile1));
        assertThat(entry.memTable(), is(memTable1));

        // Head remains the same if we didn't remove it.
        entry = queue.peekHead();

        assertThat(entry.segmentFile(), is(segmentFile1));
        assertThat(entry.memTable(), is(memTable1));

        queue.removeHead();

        entry = queue.peekHead();

        assertThat(entry.segmentFile(), is(segmentFile2));
        assertThat(entry.memTable(), is(memTable2));
    }

    @Test
    void testBlockingPeek(
            @InjectExecutorService(threadCount = 1) ExecutorService executor,
            @Mock SegmentFile segmentFile,
            @Mock ReadModeIndexMemTable memTable
    ) throws InterruptedException {
        CompletableFuture<Entry> peekFuture = supplyAsync(() -> {
            try {
                return queue.peekHead();
            } catch (InterruptedException e) {
                throw new CompletionException(e);
            }
        }, executor);

        assertThat(peekFuture, willTimeoutFast());

        queue.add(segmentFile, memTable);

        assertThat(peekFuture, willCompleteSuccessfully());

        assertThat(peekFuture.join().segmentFile(), is(segmentFile));
        assertThat(peekFuture.join().memTable(), is(memTable));
    }

    @Test
    void testBlockingAdd(
            @InjectExecutorService(threadCount = 1) ExecutorService executor,
            @Mock SegmentFile segmentFile,
            @Mock ReadModeIndexMemTable memTable
    ) throws InterruptedException {
        for (int i = 0; i < MAX_QUEUE_SIZE; i++) {
            queue.add(segmentFile, memTable);
        }

        CompletableFuture<Void> addFuture = runAsync(() -> {
            try {
                queue.add(segmentFile, memTable);
            } catch (InterruptedException e) {
                throw new CompletionException(e);
            }
        }, executor);

        assertThat(addFuture, willTimeoutFast());

        queue.removeHead();

        assertThat(addFuture, willCompleteSuccessfully());
    }

    @Test
    void testEmptyQueueSearch() {
        Iterator<Entry> iterator = queue.tailIterator();

        assertThat(iterator.hasNext(), is(false));
        assertThrows(NoSuchElementException.class, iterator::next);
    }

    @Test
    void testSingleThreadedSearch(
            @Mock SegmentFile segmentFile1,
            @Mock SegmentFile segmentFile2,
            @Mock SegmentFile segmentFile3,
            @Mock ReadModeIndexMemTable memTable1,
            @Mock ReadModeIndexMemTable memTable2,
            @Mock ReadModeIndexMemTable memTable3
    ) throws InterruptedException {
        queue.add(segmentFile1, memTable1);
        queue.add(segmentFile2, memTable2);
        queue.add(segmentFile3, memTable3);

        Iterator<Entry> iterator = queue.tailIterator();

        Entry entry3 = iterator.next();
        assertThat(entry3.segmentFile(), is(segmentFile3));
        assertThat(entry3.memTable(), is(memTable3));

        Entry entry2 = iterator.next();
        assertThat(entry2.segmentFile(), is(segmentFile2));
        assertThat(entry2.memTable(), is(memTable2));

        Entry entry1 = iterator.next();
        assertThat(entry1.segmentFile(), is(segmentFile1));
        assertThat(entry1.memTable(), is(memTable1));
    }

    @Test
    void testMultithreadedAddPeek(@Mock SegmentFile segmentFile) {
        int numEntries = 10_000;

        RunnableX producerTask = () -> {
            for (int i = 1; i <= numEntries; i++) {
                ReadModeIndexMemTable mockTable = mock(ReadModeIndexMemTable.class);

                var segmentInfo = new SegmentInfo(0);

                segmentInfo.addOffset(0, i);

                when(mockTable.segmentInfo(anyLong())).thenReturn(segmentInfo);

                queue.add(segmentFile, mockTable);
            }
        };

        RunnableX consumerTask = () -> {
            for (int i = 1; i <= numEntries; i++) {
                Entry entry = queue.peekHead();

                assertThat(entry.memTable().segmentInfo(0).getOffset(0), is(i));

                queue.removeHead();
            }
        };

        runRace(producerTask, consumerTask);
    }

    @Test
    void testMultiThreadedSearch(@Mock SegmentFile segmentFile) {
        var isDone = new AtomicBoolean(false);

        int numEntries = 10_000;

        RunnableX producerTask = () -> {
            for (int i = 1; i <= numEntries; i++) {
                ReadModeIndexMemTable mockTable = mock(ReadModeIndexMemTable.class);

                var segmentInfo = new SegmentInfo(0);

                segmentInfo.addOffset(0, i);

                when(mockTable.segmentInfo(anyLong())).thenReturn(segmentInfo);

                queue.add(segmentFile, mockTable);
            }
        };

        RunnableX consumerTask = () -> {
            for (int i = 1; i <= numEntries; i++) {
                Entry entry = queue.peekHead();

                assertThat(entry.memTable().segmentInfo(0).getOffset(0), is(i));

                queue.removeHead();
            }

            isDone.set(true);
        };

        RunnableX searchTask = () -> {
            while (!isDone.get()) {
                Iterator<Entry> iterator = queue.tailIterator();

                int prevOffset = 0;

                while (iterator.hasNext()) {
                    Entry entry = iterator.next();

                    int offset = entry.memTable().segmentInfo(0).getOffset(0);

                    // Offsets must be in sequential decreasing order.
                    if (prevOffset != 0) {
                        assertThat(offset, is(prevOffset - 1));
                    }

                    prevOffset = offset;
                }
            }
        };

        runRace(producerTask, consumerTask, searchTask, searchTask, searchTask);
    }

    @Test
    void testCloseUnblocksWaitingThreadsOnAdd(
            @InjectExecutorService(threadCount = 1) ExecutorService executor,
            @Mock SegmentFile segmentFile,
            @Mock ReadModeIndexMemTable memTable
    ) throws InterruptedException {
        for (int i = 0; i < MAX_QUEUE_SIZE; i++) {
            queue.add(segmentFile, memTable);
        }

        CompletableFuture<Void> addFuture = runAsync(() -> {
            try {
                queue.add(segmentFile, memTable);
            } catch (InterruptedException e) {
                throw new CompletionException(e);
            }
        }, executor);

        assertThat(addFuture, willTimeoutFast());

        queue.close();

        assertThat(addFuture, willThrow(IgniteInternalException.class));
    }

    @Test
    void testCloseUnblocksWaitingThreadsOnRemove(@InjectExecutorService(threadCount = 1) ExecutorService executor) {
        CompletableFuture<Void> removeFuture = runAsync(() -> {
            try {
                queue.peekHead();
            } catch (InterruptedException e) {
                throw new CompletionException(e);
            }
        }, executor);

        assertThat(removeFuture, willTimeoutFast());

        queue.close();

        assertThat(removeFuture, willThrow(IgniteInternalException.class));
    }
}
