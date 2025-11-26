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

import static java.lang.Thread.currentThread;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.pagememory.persistence.CheckpointUrgency.MUST_TRIGGER;
import static org.apache.ignite.internal.pagememory.persistence.CheckpointUrgency.NOT_REQUIRED;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.LOCK_RELEASED;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedIn;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.apache.ignite.lang.ErrorGroups.CriticalWorkers.SYSTEM_CRITICAL_OPERATION_TIMEOUT_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.pagememory.persistence.CheckpointUrgency;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * For {@link CheckpointTimeoutLock} testing.
 */
@ExtendWith(ExecutorServiceExtension.class)
public class CheckpointTimeoutLockTest extends BaseIgniteAbstractTest {
    @Nullable
    private CheckpointTimeoutLock timeoutLock;

    @InjectExecutorService
    private ExecutorService executorService;

    @AfterEach
    void tearDown() {
        if (timeoutLock != null) {
            readUnlock(timeoutLock);

            timeoutLock.stop();
        }
    }

    @Test
    void testCheckpointReadLockTimeout() {
        timeoutLock = new CheckpointTimeoutLock(
                newReadWriteLock(), Long.MAX_VALUE, () -> NOT_REQUIRED, mock(Checkpointer.class), mock(FailureManager.class));

        timeoutLock.start();

        assertEquals(Long.MAX_VALUE, timeoutLock.checkpointReadLockTimeout());

        timeoutLock.checkpointReadLockTimeout(Long.MIN_VALUE);

        assertEquals(Long.MIN_VALUE, timeoutLock.checkpointReadLockTimeout());
    }

    @Test
    void testCheckpointReadLock() throws Exception {
        var timeoutLock0 = new CheckpointTimeoutLock(
                newReadWriteLock(), 0, () -> NOT_REQUIRED, mock(Checkpointer.class), mock(FailureManager.class));
        var timeoutLock1 = new CheckpointTimeoutLock(
                newReadWriteLock(), 1_000, () -> NOT_REQUIRED, mock(Checkpointer.class), mock(FailureManager.class));

        try {
            timeoutLock0.start();
            timeoutLock1.start();

            assertDoesNotThrow(timeoutLock0::checkpointReadLock);
            assertDoesNotThrow(timeoutLock1::checkpointReadLock);

            // Checks reentrant lock.

            assertDoesNotThrow(timeoutLock0::checkpointReadLock);
            assertDoesNotThrow(timeoutLock1::checkpointReadLock);
        } finally {
            readUnlock(timeoutLock0);
            readUnlock(timeoutLock1);

            closeAll(timeoutLock0::stop, timeoutLock1::stop);
        }
    }

    @Test
    void testCheckpointReadLockWithWriteLockHeldByCurrentThread() {
        CheckpointReadWriteLock readWriteLock = newReadWriteLock();

        timeoutLock = new CheckpointTimeoutLock(
                readWriteLock, 1_000, () -> NOT_REQUIRED, mock(Checkpointer.class), mock(FailureManager.class));

        timeoutLock.start();

        readWriteLock.writeLock();

        try {
            assertDoesNotThrow(timeoutLock::checkpointReadLock);

            // Check reentrant lock.

            assertDoesNotThrow(timeoutLock::checkpointReadLock);
        } finally {
            writeUnlock(readWriteLock);
        }
    }

    @Test
    void testCheckpointReadLockFailOnNodeStop() {
        timeoutLock = new CheckpointTimeoutLock(
                newReadWriteLock(), Long.MAX_VALUE, () -> NOT_REQUIRED, mock(Checkpointer.class), mock(FailureManager.class));

        timeoutLock.stop();

        IgniteInternalException exception = assertThrows(IgniteInternalException.class, timeoutLock::checkpointReadLock);

        assertThat(exception.getCause(), instanceOf(NodeStoppingException.class));
    }

    @Test
    void testCheckpointReadLockTimeoutFail() throws Exception {
        CheckpointReadWriteLock readWriteLock0 = newReadWriteLock();
        CheckpointReadWriteLock readWriteLock1 = newReadWriteLock();

        FailureManager failureManager = mock(FailureManager.class);
        doAnswer(invocation -> true).when(failureManager).process(any());

        var timeoutLock0 = new CheckpointTimeoutLock(
                readWriteLock0, 0, () -> NOT_REQUIRED, mock(Checkpointer.class), failureManager);
        var timeoutLock1 = new CheckpointTimeoutLock(
                readWriteLock1, 100, () -> NOT_REQUIRED, mock(Checkpointer.class), failureManager);

        try {
            timeoutLock0.start();
            timeoutLock1.start();

            readWriteLock0.writeLock();
            readWriteLock1.writeLock();

            CountDownLatch startThreadLatch = new CountDownLatch(2);

            CompletableFuture<?> readLockFuture0 = runAsync(() -> checkpointReadLock(startThreadLatch, timeoutLock0));
            CompletableFuture<?> readLockFuture1 = runAsync(() -> checkpointReadLock(startThreadLatch, timeoutLock1));

            assertTrue(startThreadLatch.await(1_000, MILLISECONDS));

            // For the Windows case, getting a read lock can take up to 100 ms.
            assertThrows(TimeoutException.class, () -> readLockFuture0.get(1_000, MILLISECONDS));

            ExecutionException exception = assertThrows(ExecutionException.class, () -> readLockFuture1.get(1_000, MILLISECONDS));

            assertThat(exception.getCause(), instanceOf(IgniteInternalException.class));

            IgniteInternalException cause = (IgniteInternalException) exception.getCause();

            assertThat(cause.code(), is(SYSTEM_CRITICAL_OPERATION_TIMEOUT_ERR));
        } finally {
            writeUnlock(readWriteLock0);
            writeUnlock(readWriteLock1);

            closeAll(timeoutLock0::stop, timeoutLock1::stop);
        }
    }

    @Test
    void testCheckpointReadLockTimeoutFail2() throws Exception {
        CheckpointReadWriteLock readWriteLock0 = newReadWriteLock();
        CheckpointReadWriteLock readWriteLock1 = newReadWriteLock();

        var timeoutLock0 = new CheckpointTimeoutLock(
                readWriteLock0, 0, () -> NOT_REQUIRED, mock(Checkpointer.class), mock(FailureManager.class));
        var timeoutLock1 = new CheckpointTimeoutLock(
                readWriteLock1, 100, () -> NOT_REQUIRED, mock(Checkpointer.class), mock(FailureManager.class));

        try {
            timeoutLock0.start();
            timeoutLock1.start();

            readWriteLock0.writeLock();
            readWriteLock1.writeLock();

            CountDownLatch startThreadLatch = new CountDownLatch(2);

            CompletableFuture<?> readLockFuture0 = runAsync(() -> checkpointReadLock(startThreadLatch, timeoutLock0));
            CompletableFuture<?> readLockFuture1 = runAsync(() -> checkpointReadLock(startThreadLatch, timeoutLock1));

            assertTrue(startThreadLatch.await(1_000, MILLISECONDS));

            // For the Windows case, getting a read lock can take up to 100 ms.
            assertThrows(TimeoutException.class, () -> readLockFuture0.get(500, MILLISECONDS));
            assertThrows(TimeoutException.class, () -> readLockFuture1.get(500, MILLISECONDS));

            writeUnlock(readWriteLock1);

            assertThat(readLockFuture1, willSucceedIn(1, SECONDS));
        } finally {
            writeUnlock(readWriteLock0);
            writeUnlock(readWriteLock1);

            closeAll(timeoutLock0::stop, timeoutLock1::stop);
        }
    }

    @Test
    void testCheckpointReadLockTimeoutWithInterruptionFail() throws Exception {
        CheckpointReadWriteLock readWriteLock0 = newReadWriteLock();
        CheckpointReadWriteLock readWriteLock1 = newReadWriteLock();

        var timeoutLock0 = new CheckpointTimeoutLock(
                readWriteLock0, 0, () -> NOT_REQUIRED, mock(Checkpointer.class), mock(FailureManager.class));
        var timeoutLock1 = new CheckpointTimeoutLock(
                readWriteLock1, 100, () -> NOT_REQUIRED, mock(Checkpointer.class), mock(FailureManager.class));

        try {
            timeoutLock0.start();
            timeoutLock1.start();

            readWriteLock0.writeLock();
            readWriteLock1.writeLock();

            CountDownLatch startThreadLatch = new CountDownLatch(2);
            CountDownLatch interruptedThreadLatch = new CountDownLatch(2);

            CompletableFuture<?> readLockFuture0 = runAsync(() -> {
                currentThread().interrupt();

                try {
                    checkpointReadLock(startThreadLatch, timeoutLock0);
                } finally {
                    interruptedThreadLatch.countDown();
                }
            });

            CompletableFuture<?> readLockFuture1 = runAsync(() -> {
                currentThread().interrupt();

                try {
                    checkpointReadLock(startThreadLatch, timeoutLock1);
                } finally {
                    interruptedThreadLatch.countDown();
                }
            });

            assertTrue(startThreadLatch.await(100, MILLISECONDS));

            // For the Windows case, getting a read lock can take up to 100 ms.
            assertThrows(TimeoutException.class, () -> readLockFuture0.get(1_000, MILLISECONDS));

            assertThrows(TimeoutException.class, () -> readLockFuture1.get(1_000, MILLISECONDS));

            writeUnlock(readWriteLock0);
            writeUnlock(readWriteLock1);

            assertTrue(interruptedThreadLatch.await(1_000, MILLISECONDS));
        } finally {
            writeUnlock(readWriteLock0);
            writeUnlock(readWriteLock1);

            closeAll(timeoutLock0::stop, timeoutLock1::stop);
        }
    }

    @Test
    void testScheduleCheckpoint() throws Exception {
        CompletableFuture<?> lockRealiseFuture = mock(CompletableFuture.class);

        CountDownLatch latch = new CountDownLatch(1);

        when(lockRealiseFuture.get()).then(a -> {
            latch.countDown();

            return null;
        });

        Checkpointer checkpointer = newCheckpointer(currentThread(), lockRealiseFuture);

        AtomicReference<CheckpointUrgency> urgency = new AtomicReference<>(MUST_TRIGGER);

        timeoutLock = new CheckpointTimeoutLock(
                newReadWriteLock(), 0, urgency::get, checkpointer, mock(FailureManager.class));

        timeoutLock.start();

        CompletableFuture<?> readLockFuture = runAsync(() -> {
            timeoutLock.checkpointReadLock();

            timeoutLock.checkpointReadUnlock();
        });

        assertTrue(latch.await(100, MILLISECONDS));

        urgency.set(NOT_REQUIRED);

        readLockFuture.get(100, MILLISECONDS);
    }

    @Test
    void testFailureLockReleasedFuture() {
        Checkpointer checkpointer = newCheckpointer(currentThread(), failedFuture(new Exception("test")));

        timeoutLock = new CheckpointTimeoutLock(
                newReadWriteLock(), 0, () -> MUST_TRIGGER, checkpointer, mock(FailureManager.class));

        timeoutLock.start();

        IgniteInternalException exception = assertThrows(IgniteInternalException.class, timeoutLock::checkpointReadLock);

        assertThat(exception.getCause(), instanceOf(Exception.class));
        assertThat(exception.getCause().getMessage(), equalTo("test"));
    }

    @Test
    void testCanceledLockReleasedFuture() {
        CompletableFuture<?> future = new CompletableFuture<>();

        future.cancel(true);

        Checkpointer checkpointer = newCheckpointer(currentThread(), future);

        timeoutLock = new CheckpointTimeoutLock(
                newReadWriteLock(), 0, () -> MUST_TRIGGER, checkpointer, mock(FailureManager.class));

        timeoutLock.start();

        IgniteInternalException exception = assertThrows(IgniteInternalException.class, timeoutLock::checkpointReadLock);

        assertThat(exception.getCause(), instanceOf(CancellationException.class));
    }

    private void checkpointReadLock(CountDownLatch latch, CheckpointTimeoutLock lock) {
        latch.countDown();

        lock.checkpointReadLock();

        lock.checkpointReadUnlock();
    }

    private void writeUnlock(CheckpointReadWriteLock lock) {
        if (lock.isWriteLockHeldByCurrentThread()) {
            lock.writeUnlock();
        }
    }

    private void readUnlock(CheckpointTimeoutLock lock) {
        while (lock.checkpointLockIsHeldByThread()) {
            lock.checkpointReadUnlock();
        }
    }

    private CheckpointReadWriteLock newReadWriteLock() {
        return new CheckpointReadWriteLock(new ReentrantReadWriteLockWithTracking(log, 5_000), executorService);
    }

    private CheckpointProgress newCheckpointProgress(CompletableFuture<?> future) {
        CheckpointProgress progress = mock(CheckpointProgress.class);

        when(progress.futureFor(LOCK_RELEASED)).then(a -> future);

        return progress;
    }

    private Checkpointer newCheckpointer(Thread runner, CompletableFuture<?> future) {
        Checkpointer checkpointer = mock(Checkpointer.class);

        when(checkpointer.runner()).thenReturn(runner);

        CheckpointProgress checkpointProgress = newCheckpointProgress(future);

        when(checkpointer.scheduleCheckpoint(0, "too many dirty pages")).thenReturn(checkpointProgress);

        return checkpointer;
    }
}
