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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.FINISHED;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.LOCK_TAKEN;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.IgniteConcurrentMultiPairQueue.EMPTY;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.persistence.PageMemoryImpl;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.NodeStoppingException;
import org.junit.jupiter.api.Test;

/**
 * For {@link Checkpointer} testing.
 */
public class CheckpointerTest {
    private final IgniteLogger log = IgniteLogger.forClass(CheckpointerTest.class);

    @Test
    void testStartAndStop() throws Exception {
        Checkpointer checkpointer = new Checkpointer(
                log,
                "test",
                null,
                null,
                createCheckpointWorkflow(EMPTY),
                createCheckpointPagesWriterFactory(mock(CheckpointPageWriter.class)),
                1,
                () -> 1_000
        );

        assertNull(checkpointer.runner());

        assertFalse(checkpointer.isShutdownNow());
        assertFalse(checkpointer.isCancelled());
        assertFalse(checkpointer.isDone());

        checkpointer.start();

        assertTrue(waitForCondition(() -> checkpointer.runner() != null, 10, 100));

        checkpointer.stop();

        assertTrue(waitForCondition(() -> checkpointer.runner() == null, 10, 100));

        assertTrue(checkpointer.isShutdownNow());
        assertTrue(checkpointer.isCancelled());
        assertTrue(checkpointer.isDone());
    }

    @Test
    void testScheduleCheckpoint() {
        Checkpointer checkpointer = new Checkpointer(
                log,
                "test",
                null,
                null,
                mock(CheckpointWorkflow.class),
                mock(CheckpointPagesWriterFactory.class),
                1,
                () -> 1_000
        );

        assertNull(checkpointer.currentProgress());

        CheckpointProgressImpl scheduledProgress = (CheckpointProgressImpl) checkpointer.scheduledProgress();

        long nextCheckpointDelayNanos = scheduledProgress.nextCheckpointDelayNanos();
        String reason = scheduledProgress.reason();

        assertThat(
                nextCheckpointDelayNanos,
                allOf(greaterThanOrEqualTo(MILLISECONDS.toNanos(995)), lessThanOrEqualTo(MILLISECONDS.toNanos(1005)))
        );

        assertSame(scheduledProgress, checkpointer.scheduleCheckpoint(3000, "test0"));

        assertNull(checkpointer.currentProgress());

        assertEquals(nextCheckpointDelayNanos, scheduledProgress.nextCheckpointDelayNanos());
        assertEquals(reason, scheduledProgress.reason());

        assertSame(scheduledProgress, checkpointer.scheduleCheckpoint(100, "test1"));

        assertNull(checkpointer.currentProgress());

        assertEquals(MILLISECONDS.toNanos(100), scheduledProgress.nextCheckpointDelayNanos());
        assertEquals("test1", scheduledProgress.reason());

        // Checks after the start of a checkpoint.

        checkpointer.startCheckpointProgress();

        CheckpointProgressImpl currentProgress = (CheckpointProgressImpl) checkpointer.currentProgress();

        assertSame(scheduledProgress, currentProgress);

        assertNotSame(scheduledProgress, checkpointer.scheduledProgress());

        scheduledProgress = (CheckpointProgressImpl) checkpointer.scheduledProgress();

        assertThat(
                scheduledProgress.nextCheckpointDelayNanos(),
                allOf(greaterThanOrEqualTo(MILLISECONDS.toNanos(995)), lessThanOrEqualTo(MILLISECONDS.toNanos(1005)))
        );

        assertSame(currentProgress, checkpointer.scheduleCheckpoint(90, "test2"));
        assertSame(currentProgress, checkpointer.currentProgress());
        assertSame(scheduledProgress, checkpointer.scheduledProgress());

        assertEquals(MILLISECONDS.toNanos(100), currentProgress.nextCheckpointDelayNanos());
        assertEquals("test1", currentProgress.reason());

        currentProgress.transitTo(LOCK_TAKEN);

        assertSame(scheduledProgress, checkpointer.scheduleCheckpoint(90, "test3"));
        assertSame(currentProgress, checkpointer.currentProgress());
        assertSame(scheduledProgress, checkpointer.scheduledProgress());

        assertEquals(MILLISECONDS.toNanos(90), scheduledProgress.nextCheckpointDelayNanos());
        assertEquals("test3", scheduledProgress.reason());

        // Checks the listener.

        BiConsumer<Void, Throwable> finishFutureListener = mock(BiConsumer.class);

        assertSame(scheduledProgress, checkpointer.scheduleCheckpoint(0, "test4", finishFutureListener));
        assertSame(currentProgress, checkpointer.currentProgress());
        assertSame(scheduledProgress, checkpointer.scheduledProgress());

        assertEquals(MILLISECONDS.toNanos(0), scheduledProgress.nextCheckpointDelayNanos());
        assertEquals("test4", scheduledProgress.reason());

        verify(finishFutureListener, times(0)).accept(null, null);

        currentProgress.transitTo(FINISHED);

        verify(finishFutureListener, times(0)).accept(null, null);

        scheduledProgress.transitTo(FINISHED);

        verify(finishFutureListener, times(1)).accept(null, null);
    }

    @Test
    void testWaitCheckpointEvent() throws Exception {
        Checkpointer checkpointer = new Checkpointer(
                log,
                "test",
                null,
                null,
                mock(CheckpointWorkflow.class),
                mock(CheckpointPagesWriterFactory.class),
                1,
                () -> 200
        );

        CompletableFuture<?> waitCheckpointEventFuture = runAsync(checkpointer::waitCheckpointEvent);

        assertThrows(TimeoutException.class, () -> waitCheckpointEventFuture.get(100, MILLISECONDS));

        waitCheckpointEventFuture.get(200, MILLISECONDS);

        ((CheckpointProgressImpl) checkpointer.scheduledProgress()).nextCheckpointNanos(MILLISECONDS.toNanos(10_000));

        checkpointer.stop();

        runAsync(checkpointer::waitCheckpointEvent).get(100, MILLISECONDS);
    }

    @Test
    void testCheckpointBody() throws Exception {
        LongSupplier checkpointFrequencySupplier = mock(LongSupplier.class);

        when(checkpointFrequencySupplier.getAsLong())
                .thenReturn(100L)
                .thenReturn(10_000L);

        Checkpointer checkpointer = spy(new Checkpointer(
                log,
                "test",
                null,
                null,
                createCheckpointWorkflow(EMPTY),
                createCheckpointPagesWriterFactory(mock(CheckpointPageWriter.class)),
                1,
                checkpointFrequencySupplier
        ));

        ((CheckpointProgressImpl) checkpointer.scheduledProgress())
                .futureFor(FINISHED)
                .whenComplete((unused, throwable) -> {
                    try {
                        verify(checkpointer, times(1)).doCheckpoint();

                        checkpointer.shutdownCheckpointer(false);
                    } catch (IgniteInternalCheckedException e) {
                        fail(e);
                    }
                });

        runAsync(checkpointer::body).get(200, MILLISECONDS);

        verify(checkpointer, times(2)).doCheckpoint();

        ExecutionException exception = assertThrows(
                ExecutionException.class,
                () -> checkpointer.scheduledProgress().futureFor(FINISHED).get(100, MILLISECONDS)
        );

        assertThat(exception.getCause(), instanceOf(NodeStoppingException.class));

        // Checks cancelled checkpointer.

        runAsync(checkpointer::body).get(200, MILLISECONDS);

        verify(checkpointer, times(3)).doCheckpoint();

        exception = assertThrows(
                ExecutionException.class,
                () -> checkpointer.scheduledProgress().futureFor(FINISHED).get(100, MILLISECONDS)
        );

        assertThat(exception.getCause(), instanceOf(NodeStoppingException.class));

        // Checks shutdowned checkpointer.

        checkpointer.shutdownCheckpointer(true);

        runAsync(checkpointer::body).get(200, MILLISECONDS);

        verify(checkpointer, times(3)).doCheckpoint();

        exception = assertThrows(
                ExecutionException.class,
                () -> checkpointer.scheduledProgress().futureFor(FINISHED).get(100, MILLISECONDS)
        );

        assertThat(exception.getCause(), instanceOf(NodeStoppingException.class));
    }

    @Test
    void testDoCheckpoint() throws Exception {
        IgniteConcurrentMultiPairQueue<PageMemoryImpl, FullPageId> dirtyPages = dirtyPages(
                mock(PageMemoryImpl.class),
                new FullPageId(0, 0), new FullPageId(1, 0), new FullPageId(2, 0)
        );

        Checkpointer checkpointer = spy(new Checkpointer(
                log,
                "test",
                null,
                null,
                createCheckpointWorkflow(dirtyPages),
                createCheckpointPagesWriterFactory(mock(CheckpointPageWriter.class)),
                1,
                () -> 1_000
        ));

        assertDoesNotThrow(checkpointer::doCheckpoint);

        assertTrue(dirtyPages.isEmpty());

        verify(checkpointer, times(1)).startCheckpointProgress();

        assertEquals(checkpointer.currentProgress().currentCheckpointPagesCount(), 3);
    }

    private IgniteConcurrentMultiPairQueue<PageMemoryImpl, FullPageId> dirtyPages(
            PageMemoryImpl pageMemory,
            FullPageId... fullPageIds
    ) {
        return fullPageIds.length == 0 ? EMPTY : new IgniteConcurrentMultiPairQueue<>(Map.of(pageMemory, List.of(fullPageIds)));
    }

    private CheckpointWorkflow createCheckpointWorkflow(
            IgniteConcurrentMultiPairQueue<PageMemoryImpl, FullPageId> dirtyPages
    ) throws Exception {
        CheckpointWorkflow mock = mock(CheckpointWorkflow.class);

        when(mock.markCheckpointBegin(anyLong(), any(CheckpointProgressImpl.class), any(CheckpointMetricsTracker.class)))
                .then(answer -> new Checkpoint(dirtyPages, answer.getArgument(1)));

        doAnswer(answer -> {
            ((Checkpoint) answer.getArgument(0)).progress.transitTo(FINISHED);

            return null;
        })
                .when(mock)
                .markCheckpointEnd(any(Checkpoint.class));

        return mock;
    }

    private CheckpointPagesWriterFactory createCheckpointPagesWriterFactory(CheckpointPageWriter checkpointPageWriter) {
        return new CheckpointPagesWriterFactory(log, checkpointPageWriter, 1024);
    }
}
