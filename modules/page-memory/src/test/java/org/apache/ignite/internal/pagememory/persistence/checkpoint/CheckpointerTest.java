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

import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.FINISHED;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.LOCK_TAKEN;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.IgniteConcurrentMultiPairQueue.EMPTY;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
                () -> 1_000,
                () -> 0
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
        Checkpointer checkpointer = spy(new Checkpointer(
                log,
                "test",
                null,
                null,
                mock(CheckpointWorkflow.class),
                mock(CheckpointPagesWriterFactory.class),
                1,
                () -> 1_000,
                () -> 0
        ));

        assertNull(checkpointer.currentProgress());

        CheckpointProgressImpl scheduledProgress = (CheckpointProgressImpl) checkpointer.scheduledProgress();

        long onCreateCheckpointerNextCheckpointNanos = scheduledProgress.nextCheckpointNanos();
        String onCreateCheckpointerReason = scheduledProgress.reason();

        assertThat(
                onCreateCheckpointerNextCheckpointNanos - nanoTime(),
                allOf(greaterThan(0L), lessThanOrEqualTo(MILLISECONDS.toNanos(1_000)))
        );

        assertNull(onCreateCheckpointerReason);

        assertSame(scheduledProgress, checkpointer.scheduleCheckpoint(3000, "test0"));

        assertNull(checkpointer.currentProgress());

        assertEquals(onCreateCheckpointerNextCheckpointNanos, scheduledProgress.nextCheckpointNanos());
        assertEquals(onCreateCheckpointerReason, scheduledProgress.reason());

        assertSame(scheduledProgress, checkpointer.scheduleCheckpoint(100, "test1"));

        assertNull(checkpointer.currentProgress());

        assertNotEquals(onCreateCheckpointerNextCheckpointNanos, scheduledProgress.nextCheckpointNanos());
        assertNotEquals(onCreateCheckpointerReason, scheduledProgress.reason());

        assertThat(
                scheduledProgress.nextCheckpointNanos() - nanoTime(),
                allOf(greaterThan(0L), lessThanOrEqualTo(MILLISECONDS.toNanos(100)))
        );

        assertEquals("test1", scheduledProgress.reason());

        long scheduledNextCheckpointNanos = scheduledProgress.nextCheckpointNanos();
        String scheduledReason = scheduledProgress.reason();

        // Checks after the start of a checkpoint.

        checkpointer.startCheckpointProgress();

        CheckpointProgressImpl currentProgress = (CheckpointProgressImpl) checkpointer.currentProgress();

        assertSame(scheduledProgress, currentProgress);

        assertNotSame(scheduledProgress, checkpointer.scheduledProgress());

        verify(checkpointer, times(1)).nextCheckpointInterval();

        scheduledProgress = (CheckpointProgressImpl) checkpointer.scheduledProgress();

        assertThat(
                scheduledProgress.nextCheckpointNanos() - nanoTime(),
                allOf(greaterThan(0L), lessThanOrEqualTo(MILLISECONDS.toNanos(1_000)))
        );

        assertEquals(scheduledNextCheckpointNanos, currentProgress.nextCheckpointNanos());
        assertEquals(scheduledReason, currentProgress.reason());

        assertSame(currentProgress, checkpointer.scheduleCheckpoint(90, "test2"));
        assertSame(currentProgress, checkpointer.currentProgress());
        assertSame(scheduledProgress, checkpointer.scheduledProgress());

        assertEquals(scheduledNextCheckpointNanos, currentProgress.nextCheckpointNanos());
        assertEquals(scheduledReason, currentProgress.reason());

        currentProgress.transitTo(LOCK_TAKEN);

        assertSame(scheduledProgress, checkpointer.scheduleCheckpoint(90, "test3"));
        assertSame(currentProgress, checkpointer.currentProgress());
        assertSame(scheduledProgress, checkpointer.scheduledProgress());

        assertThat(
                scheduledProgress.nextCheckpointNanos() - nanoTime(),
                allOf(greaterThan(0L), lessThanOrEqualTo(MILLISECONDS.toNanos(90)))
        );

        assertEquals("test3", scheduledProgress.reason());

        assertEquals(scheduledNextCheckpointNanos, currentProgress.nextCheckpointNanos());
        assertEquals(scheduledReason, currentProgress.reason());

        // Checks the listener.

        BiConsumer<Void, Throwable> finishFutureListener = mock(BiConsumer.class);

        assertSame(scheduledProgress, checkpointer.scheduleCheckpoint(0, "test4", finishFutureListener));
        assertSame(currentProgress, checkpointer.currentProgress());
        assertSame(scheduledProgress, checkpointer.scheduledProgress());

        assertThat(scheduledProgress.nextCheckpointNanos() - nanoTime(), lessThan(0L));

        assertEquals("test4", scheduledProgress.reason());

        assertEquals(scheduledNextCheckpointNanos, currentProgress.nextCheckpointNanos());
        assertEquals(scheduledReason, currentProgress.reason());

        verify(finishFutureListener, times(0)).accept(null, null);

        currentProgress.transitTo(FINISHED);

        verify(finishFutureListener, times(0)).accept(null, null);

        scheduledProgress.transitTo(FINISHED);

        verify(finishFutureListener, times(1)).accept(null, null);

        verify(checkpointer, times(1)).nextCheckpointInterval();
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
                () -> 200,
                () -> 0
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
                checkpointFrequencySupplier,
                () -> 0
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

        checkpointer.shutdownCheckpointer(false);

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
                () -> 1_000,
                () -> 0
        ));

        assertDoesNotThrow(checkpointer::doCheckpoint);

        assertTrue(dirtyPages.isEmpty());

        verify(checkpointer, times(1)).startCheckpointProgress();

        assertEquals(checkpointer.currentProgress().currentCheckpointPagesCount(), 3);
    }

    @Test
    void testNextCheckpointInterval() {
        AtomicLong checkpointFrequency = new AtomicLong();
        AtomicInteger checkpointFrequencyDeviation = new AtomicInteger();

        Checkpointer checkpointer = new Checkpointer(
                log,
                "test",
                null,
                null,
                mock(CheckpointWorkflow.class),
                mock(CheckpointPagesWriterFactory.class),
                1,
                checkpointFrequency::get,
                checkpointFrequencyDeviation::get
        );

        // Checks case 0 deviation.

        checkpointFrequencyDeviation.set(0);

        checkpointFrequency.set(1_000);
        assertEquals(1_000, checkpointer.nextCheckpointInterval());

        checkpointFrequency.set(2_000);
        assertEquals(2_000, checkpointer.nextCheckpointInterval());

        // Checks for non-zero deviation.

        checkpointFrequencyDeviation.set(10);

        assertThat(
                checkpointer.nextCheckpointInterval(),
                allOf(greaterThanOrEqualTo(1_900L), lessThanOrEqualTo(2_100L))
        );

        checkpointFrequencyDeviation.set(20);

        assertThat(
                checkpointer.nextCheckpointInterval(),
                allOf(greaterThanOrEqualTo(1_800L), lessThanOrEqualTo(2_200L))
        );
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
