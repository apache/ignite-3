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

import static java.lang.Thread.currentThread;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.LOCK_RELEASED;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.pagememory.PageMemoryDataRegion;
import org.apache.ignite.internal.pagememory.persistence.PageMemoryImpl;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.NodeStoppingException;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * For {@link CheckpointTimeoutLock} testing.
 */
public class CheckpointTimeoutLockTest {
    private final IgniteLogger log = IgniteLogger.forClass(CheckpointTimeoutLockTest.class);

    @Nullable
    private CheckpointTimeoutLock timeoutLock;

    @AfterEach
    void tearDown() {
        if (timeoutLock != null) {
            readUnlock(timeoutLock);

            timeoutLock.stop();
        }
    }

    @Test
    void testCheckpointReadLockTimeout() {
        timeoutLock = new CheckpointTimeoutLock(log, newReadWriteLock(), Long.MAX_VALUE, () -> true, mock(Checkpointer.class));

        timeoutLock.start();

        assertEquals(Long.MAX_VALUE, timeoutLock.checkpointReadLockTimeout());

        timeoutLock.checkpointReadLockTimeout(Long.MIN_VALUE);

        assertEquals(Long.MIN_VALUE, timeoutLock.checkpointReadLockTimeout());
    }

    @Test
    void testCheckpointReadLock() throws Exception {
        CheckpointTimeoutLock timeoutLock0 = new CheckpointTimeoutLock(log, newReadWriteLock(), 0, () -> true, mock(Checkpointer.class));
        CheckpointTimeoutLock timeoutLock1 = new CheckpointTimeoutLock(log, newReadWriteLock(), 1, () -> true, mock(Checkpointer.class));

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

        timeoutLock = new CheckpointTimeoutLock(log, readWriteLock, 1, () -> true, mock(Checkpointer.class));

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
        timeoutLock = new CheckpointTimeoutLock(log, newReadWriteLock(), Long.MAX_VALUE, () -> true, mock(Checkpointer.class));

        timeoutLock.stop();

        IgniteInternalException exception = assertThrows(IgniteInternalException.class, timeoutLock::checkpointReadLock);

        assertThat(exception.getCause(), instanceOf(NodeStoppingException.class));
    }

    @Test
    void testCheckpointReadLockTimeoutFail() throws Exception {
        CheckpointReadWriteLock readWriteLock0 = newReadWriteLock();
        CheckpointReadWriteLock readWriteLock1 = newReadWriteLock();

        CheckpointTimeoutLock timeoutLock0 = new CheckpointTimeoutLock(log, readWriteLock0, 0, () -> true, mock(Checkpointer.class));
        CheckpointTimeoutLock timeoutLock1 = new CheckpointTimeoutLock(log, readWriteLock1, 1, () -> true, mock(Checkpointer.class));

        try {
            timeoutLock0.start();
            timeoutLock1.start();

            readWriteLock0.writeLock();
            readWriteLock1.writeLock();

            CountDownLatch startThreadLatch = new CountDownLatch(2);

            CompletableFuture<?> readLockFuture0 = runAsync(() -> checkpointReadLock(startThreadLatch, timeoutLock0));
            CompletableFuture<?> readLockFuture1 = runAsync(() -> checkpointReadLock(startThreadLatch, timeoutLock1));

            assertTrue(startThreadLatch.await(100, MILLISECONDS));

            // For the Windows case, getting a read lock can take up to 100 ms.
            assertThrows(TimeoutException.class, () -> readLockFuture0.get(100, MILLISECONDS));

            ExecutionException exception = assertThrows(ExecutionException.class, () -> readLockFuture1.get(100, MILLISECONDS));

            assertThat(exception.getCause().getCause(), instanceOf(CheckpointReadLockTimeoutException.class));
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

        CheckpointTimeoutLock timeoutLock0 = new CheckpointTimeoutLock(log, readWriteLock0, 0, () -> true, mock(Checkpointer.class));
        CheckpointTimeoutLock timeoutLock1 = new CheckpointTimeoutLock(log, readWriteLock1, 1, () -> true, mock(Checkpointer.class));

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
            assertThrows(TimeoutException.class, () -> readLockFuture0.get(100, MILLISECONDS));

            ExecutionException exception = assertThrows(ExecutionException.class, () -> readLockFuture1.get(100, MILLISECONDS));

            assertThat(exception.getCause().getCause(), instanceOf(CheckpointReadLockTimeoutException.class));

            writeUnlock(readWriteLock0);
            writeUnlock(readWriteLock1);

            assertTrue(interruptedThreadLatch.await(100, MILLISECONDS));
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

        AtomicBoolean safeToUpdate = new AtomicBoolean();

        timeoutLock = new CheckpointTimeoutLock(log, newReadWriteLock(), 0, safeToUpdate::get, checkpointer);

        timeoutLock.start();

        CompletableFuture<?> readLockFuture = runAsync(() -> {
            timeoutLock.checkpointReadLock();

            timeoutLock.checkpointReadUnlock();
        });

        assertTrue(latch.await(100, MILLISECONDS));

        safeToUpdate.set(true);

        readLockFuture.get(100, MILLISECONDS);
    }

    @Test
    void testFailureLockReleasedFuture() {
        Checkpointer checkpointer = newCheckpointer(currentThread(), failedFuture(new Exception("test")));

        timeoutLock = new CheckpointTimeoutLock(log, newReadWriteLock(), 0, () -> false, checkpointer);

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

        PageMemoryDataRegion dataRegion = newPageMemoryDataRegion(true, new AtomicBoolean());

        timeoutLock = new CheckpointTimeoutLock(log, newReadWriteLock(), 0, () -> false, checkpointer);

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
        return new CheckpointReadWriteLock(new ReentrantReadWriteLockWithTracking(log, 5_000));
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

    private PageMemoryDataRegion newPageMemoryDataRegion(boolean persistent, AtomicBoolean safeToUpdate) {
        PageMemoryDataRegion dataRegion = mock(PageMemoryDataRegion.class);

        when(dataRegion.persistent()).thenReturn(persistent);

        PageMemoryImpl pageMemory = mock(PageMemoryImpl.class);

        when(pageMemory.safeToUpdate()).then(a -> safeToUpdate.get());

        when(dataRegion.pageMemory()).thenReturn(pageMemory);

        return dataRegion;
    }
}
