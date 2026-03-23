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

package org.apache.ignite.internal.util;

import static java.lang.Thread.currentThread;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.anyOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runRace;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutIn;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.lang.RunnableX;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Enum;

/**
 * Tests for {@link StripedVersatileReadWriteLock}.
 */
@Timeout(20)
@ExtendWith(ExecutorServiceExtension.class)
class StripedVersatileReadWriteLockTest {
    private static final String ASYNC_CONTINUATION_THREAD_PREFIX = "ace";

    @InjectExecutorService(threadPrefix = ASYNC_CONTINUATION_THREAD_PREFIX)
    private ExecutorService asyncContinuationExecutor;

    /** The lock under test. */
    private StripedVersatileReadWriteLock lock;

    /** Executor service used to run tasks in threads different from the main test thread. */
    @InjectExecutorService
    private ExecutorService executor;

    @BeforeEach
    void createLock() {
        lock = new StripedVersatileReadWriteLock(asyncContinuationExecutor);
    }

    /**
     * Cleans up after a test.
     */
    @AfterEach
    void cleanup() {
        releaseReadLocks();
        releaseWriteLocks();
    }

    private void releaseReadLocks() {
        while (true) {
            try {
                lock.readUnlock();
            } catch (IllegalMonitorStateException e) {
                // Released our read lock completely.
                break;
            }
        }
    }

    private void releaseWriteLocks() {
        while (true) {
            try {
                lock.writeUnlock();
            } catch (IllegalMonitorStateException e) {
                // Released our write lock completely.
                break;
            }
        }
    }

    @ParameterizedTest
    @EnumSource(BlockingWriteLockAcquisition.class)
    void readLockDoesNotAllowWriteLockToBeAcquired(BlockingWriteLockAcquisition acquisition) {
        lock.readLock();

        assertThatWriteLockAcquireAttemptBlocksForever(acquisition);

        lock.readUnlock();
    }

    @ParameterizedTest
    @EnumSource(BlockingWriteLockAcquisition.class)
    void readLockDoesNotAllowWriteLockToBeAcquiredBySameThread(BlockingWriteLockAcquisition acquisition) {
        lock.readLock();

        assertThatActionBlocksForever(() -> acquisition.acquire(lock));

        lock.readUnlock();
    }

    private void assertThatWriteLockAcquireAttemptBlocksForever(BlockingWriteLockAcquisition acquisition) {
        assertThatActionBlocksForever(() -> acquisition.acquire(lock));
    }

    private void assertThatActionBlocksForever(Runnable action) {
        CompletableFuture<?> future = runAsync(action, executor);

        assertThat(future, willTimeoutIn(100, MILLISECONDS));
    }

    @Test
    void readLockAllowsReadLockToBeAcquired() {
        lock.readLock();

        assertThatReadLockCanBeAcquired();
    }

    private void assertThatReadLockCanBeAcquired() {
        runWithTimeout(lock::readLock);
    }

    private <T> T callWithTimeout(Callable<T> call) throws ExecutionException, InterruptedException, TimeoutException {
        return executor.submit(call).get(10, SECONDS);
    }

    private void runWithTimeout(Runnable runnable) {
        assertThat(runAsync(runnable, executor), willCompleteSuccessfully());
    }

    @ParameterizedTest
    @EnumSource(BlockingWriteLockAcquisition.class)
    void writeLockDoesNotAllowReadLockToBeAcquired(BlockingWriteLockAcquisition acquisition) {
        acquisition.acquire(lock);

        assertThatReadLockAcquireAttemptBlocksForever();

        lock.writeUnlock();
    }

    private void assertThatReadLockAcquireAttemptBlocksForever() {
        assertThatActionBlocksForever(lock::readLock);
    }

    @ParameterizedTest
    @EnumSource(BlockingWriteLockAcquisition.class)
    void writeLockDoesNotAllowReadLockToBeAcquiredBySameThread(BlockingWriteLockAcquisition acquisition) {
        assertThatActionBlocksForever(() -> {
            acquisition.acquire(lock);
            lock.readLock();
        });

        lock.writeUnlock();
    }

    @CartesianTest
    @EnumSource(BlockingWriteLockAcquisition.class)
    void writeLockDoesNotAllowWriteLockToBeAcquired(
            @Enum(BlockingWriteLockAcquisition.class) BlockingWriteLockAcquisition firstAttempt,
            @Enum(BlockingWriteLockAcquisition.class) BlockingWriteLockAcquisition secondAttempt
    ) {
        firstAttempt.acquire(lock);

        assertThatWriteLockAcquireAttemptBlocksForever(secondAttempt);

        lock.writeUnlock();
    }

    @CartesianTest
    @EnumSource(BlockingWriteLockAcquisition.class)
    void writeLockDoesNotAllowWriteLockToBeAcquiredBySameThread(
            @Enum(BlockingWriteLockAcquisition.class) BlockingWriteLockAcquisition firstAttempt,
            @Enum(BlockingWriteLockAcquisition.class) BlockingWriteLockAcquisition secondAttempt
    ) {
        assertThatActionBlocksForever(() -> {
            firstAttempt.acquire(lock);
            secondAttempt.acquire(lock);
        });

        lock.writeUnlock();
    }

    @Test
    void readUnlockReleasesTheLock() {
        lock.readLock();
        lock.readUnlock();

        runWithTimeout(lock::writeLock);
    }

    @ParameterizedTest
    @EnumSource(BlockingWriteLockAcquisition.class)
    void writeUnlockReleasesTheLock(BlockingWriteLockAcquisition acquisition) {
        acquisition.acquire(lock);
        lock.writeUnlock();

        assertThatReadLockCanBeAcquired();
    }

    @Test
    void shouldNotAllowInterleavingHoldingReadAndWriteLocks() {
        lock.writeLock();

        assertFalse(lock.tryReadLock());

        lock.writeUnlock();

        lock.readLock();

        assertFalse(lock.tryWriteLock());

        lock.readUnlock();

        // Test that we can operate with write locks now.
        lock.writeLock();
        lock.writeUnlock();
    }

    @Test
    void readLockReleasedLessTimesThanAcquiredShouldStillBeTaken() {
        lock.readLock();

        CompletableFuture<?> future = runAsync(() -> {
            lock.readLock();
            lock.readUnlock();
        }, executor);
        assertThat(future, willCompleteSuccessfully());

        assertThatWriteLockAcquireAttemptBlocksForever(BlockingWriteLockAcquisition.WRITE_LOCK);

        lock.readUnlock();
    }

    @Test
    void shouldThrowOnReadUnlockingWhenNotReadLocked() {
        assertThrows(IllegalMonitorStateException.class, lock::readUnlock);
    }

    @Test
    void shouldThrowOnWriteUnlockingWhenNotWriteLocked() {
        assertThrows(IllegalMonitorStateException.class, lock::writeUnlock);
    }

    @ParameterizedTest
    @EnumSource(BlockingWriteLockAcquisition.class)
    void readLockAcquiredWithTryReadLockDoesNotAllowWriteLockToBeAcquired(BlockingWriteLockAcquisition acquisition) {
        lock.tryReadLock();

        assertThatWriteLockAcquireAttemptBlocksForever(acquisition);

        lock.readUnlock();
    }

    @ParameterizedTest
    @EnumSource(BlockingWriteLockAcquisition.class)
    void readLockAcquiredWithTryReadLockDoesNotAllowWriteLockToBeAcquiredBySameThread(BlockingWriteLockAcquisition acquisition) {
        lock.tryReadLock();

        assertThatActionBlocksForever(() -> acquisition.acquire(lock));

        lock.readUnlock();
    }

    @Test
    void tryReadLockShouldReturnTrueWhenReadLockWasAcquiredSuccessfully() {
        assertTrue(lock.tryReadLock());
    }

    @Test
    void tryReadLockShouldReturnFalseWhenReadLockCouldNotBeAcquired() throws Exception {
        lock.writeLock();

        Boolean acquired = callWithTimeout(lock::tryReadLock);

        assertThat(acquired, is(false));
    }

    @Test
    void writeLockAcquiredWithTryWriteLockDoesNotAllowWriteLockToBeAcquired() {
        lock.tryWriteLock();

        assertThatReadLockAcquireAttemptBlocksForever();

        lock.writeUnlock();
    }

    @Test
    void writeLockAcquiredWithTryWriteLockDoesNotAllowWriteLockToBeAcquiredBySameThread() {
        assertThatActionBlocksForever(() -> {
            lock.tryWriteLock();
            lock.readLock();
        });

        lock.writeUnlock();
    }

    @Test
    void tryWriteLockShouldReturnTrueWhenWriteLockWasAcquiredSuccessfully() {
        assertTrue(lock.tryWriteLock());
    }

    @Test
    void tryWriteLockShouldReturnFalseWhenWriteLockCouldNotBeAcquired() throws Exception {
        lock.writeLock();

        Boolean acquired = callWithTimeout(lock::tryWriteLock);

        assertThat(acquired, is(false));
    }

    @Test
    void inReadLockAsyncExecutesClosureAfterTakingReadLock() {
        assertThat(lock.inReadLockAsync(() -> completedFuture(lock.tryWriteLock())), willBe(false));
    }

    @Test
    void inReadLockAsyncReleasesReadLockInTheEnd() {
        assertThat(lock.inReadLockAsync(CompletableFutures::nullCompletedFuture), willCompleteSuccessfully());

        assertThatNoReadLockIsHeld();
    }

    @Test
    void inReadLockAsyncReleasesReadLockInTheEndInCaseOfExceptionInSyncPart() {
        assertThat(lock.inReadLockAsync(() -> {
            throw new RuntimeException("Oops");
        }), willThrow(Exception.class));

        assertThatNoReadLockIsHeld();
    }

    @Test
    void inReadLockAsyncReleasesReadLockInTheEndInCaseOfExceptionInAsyncPart() {
        assertThat(lock.inReadLockAsync(() -> failedFuture(new Exception("Oops"))), willThrow(Exception.class));

        assertThatNoReadLockIsHeld();
    }

    private void assertThatNoReadLockIsHeld() {
        assertThat("Read locks are still held", lock.readLocksHeld(), is(0));
    }

    private void assertThatNoWriteLockIsHeld() {
        assertFalse(lock.isWriteLocked(), "Write lock is still held");
    }

    @Test
    void inReadLockAsyncTakesReadLockAfterWriteLockGetsReleased() {
        lock.writeLock();

        CompletableFuture<Boolean> future1 = lock.inReadLockAsync(CompletableFutures::nullCompletedFuture);
        CompletableFuture<Boolean> future2 = lock.inReadLockAsync(CompletableFutures::nullCompletedFuture);
        CompletableFuture<Boolean> future3 = lock.inReadLockAsync(CompletableFutures::nullCompletedFuture);

        assertThat(anyOf(future1, future2, future3), willTimeoutIn(100, MILLISECONDS));

        lock.writeUnlock();

        assertThat(allOf(future1, future2, future3), willCompleteSuccessfully());

        assertThatNoReadLockIsHeld();
    }

    @Test
    void inReadLockAsyncTakesReadLockInExecutorAfterWriteLockGetsReleased() {
        lock.writeLock();

        AtomicReference<Thread> threadRef = new AtomicReference<>();
        CompletableFuture<?> future = lock.inReadLockAsync(CompletableFutures::nullCompletedFuture)
                .whenComplete((res, ex) -> threadRef.set(currentThread()));

        lock.writeUnlock();
        assertThat(future, willCompleteSuccessfully());

        assertThat(threadRef.get().getName(), startsWith(ASYNC_CONTINUATION_THREAD_PREFIX));
    }

    @Test
    void concurrentInReadLockAsyncAndWriteLockWorkCorrectly() throws Exception {
        RunnableX readLocker = () -> {
            for (int i = 0; i < 300; i++) {
                lock.inReadLockAsync(CompletableFutures::nullCompletedFuture).get(10, SECONDS);
            }
        };
        RunnableX writeLocker = () -> {
            for (int i = 0; i < 300; i++) {
                lock.writeLock();
                lock.writeUnlock();
            }
        };

        runRace(10_000, readLocker, writeLocker);

        assertThatReadLocksHeldReachesZero();
        assertThatWriteLockGetsUnlocked();
    }

    private void assertThatReadLocksHeldReachesZero() throws InterruptedException {
        assertTrue(
                waitForCondition(() -> lock.readLocksHeld() == 0, SECONDS.toMillis(10)),
                () -> "Read locks are still held " + lock.readLocksHeld()
        );
    }

    private void assertThatWriteLockGetsUnlocked() throws InterruptedException {
        assertTrue(
                waitForCondition(() -> !lock.isWriteLocked(), SECONDS.toMillis(10)),
                "Still write locked"
        );
    }

    @Test
    void inWriteLockAsyncExecutesClosureAfterTakingWriteLock() {
        assertThat(lock.inWriteLockAsync(() -> completedFuture(lock.tryWriteLock())), willBe(false));
    }

    @Test
    void inWriteLockAsyncReleasesWriteLockInTheEnd() {
        assertThat(lock.inWriteLockAsync(CompletableFutures::nullCompletedFuture), willCompleteSuccessfully());

        assertThatNoWriteLockIsHeld();
    }

    @Test
    void inWriteLockAsyncReleasesWriteLockInTheEndInCaseOfExceptionInSyncPart() {
        assertThat(lock.inWriteLockAsync(() -> {
            throw new RuntimeException("Oops");
        }), willThrow(Exception.class));

        assertThatNoWriteLockIsHeld();
    }

    @Test
    void inWriteLockAsyncReleasesWriteLockInTheEndInCaseOfExceptionInAsyncPart() {
        assertThat(lock.inWriteLockAsync(() -> failedFuture(new Exception("Oops"))), willThrow(Exception.class));

        assertThatNoWriteLockIsHeld();
    }

    @ParameterizedTest
    @EnumSource(WriteLockImpeder.class)
    void inWriteLockAsyncTakesWriteLockAfterImpedingLockGetsReleased(WriteLockImpeder impeder) {
        impeder.impede(lock);

        CompletableFuture<Void> future = lock.inWriteLockAsync(CompletableFutures::nullCompletedFuture);

        assertThat(future, willTimeoutIn(100, MILLISECONDS));

        impeder.stopImpeding(lock);

        assertThat(future, willCompleteSuccessfully());

        assertThatNoWriteLockIsHeld();
    }

    @ParameterizedTest
    @EnumSource(WriteLockImpeder.class)
    void multipleInWriteLockAsyncAttemptsTakeWriteLockAfterImpedingLocksGetReleased(WriteLockImpeder impeder) {
        impeder.impede(lock);

        CompletableFuture<Void> future1 = lock.inWriteLockAsync(CompletableFutures::nullCompletedFuture);
        CompletableFuture<Void> future2 = lock.inWriteLockAsync(CompletableFutures::nullCompletedFuture);
        CompletableFuture<Void> future3 = lock.inWriteLockAsync(CompletableFutures::nullCompletedFuture);

        assertThat(anyOf(future1, future2, future3), willTimeoutIn(100, MILLISECONDS));

        impeder.stopImpeding(lock);

        assertThat(allOf(future1, future2, future3), willCompleteSuccessfully());

        assertThatNoWriteLockIsHeld();
    }

    @ParameterizedTest
    @EnumSource(WriteLockImpeder.class)
    void inWriteLockAsyncTakesWriteLockInExecutorAfterImpedingLockGetsReleased(WriteLockImpeder impeder) {
        impeder.impede(lock);

        AtomicReference<Thread> threadRef = new AtomicReference<>();
        CompletableFuture<?> future = lock.inWriteLockAsync(CompletableFutures::nullCompletedFuture)
                .whenComplete((res, ex) -> threadRef.set(currentThread()));

        impeder.stopImpeding(lock);
        assertThat(future, willCompleteSuccessfully());

        assertThat(threadRef.get().getName(), startsWith(ASYNC_CONTINUATION_THREAD_PREFIX));
    }

    @Test
    void inWriteLockAsyncSetsPendingWriteLocks() {
        lock.readLock();

        // This will wait till read lock is released.
        lock.inWriteLockAsync(CompletableFutures::nullCompletedFuture);

        assertFalse(lock.tryReadLock());

        lock.readUnlock();
    }

    private enum BlockingWriteLockAcquisition {
        WRITE_LOCK {
            @Override
            void acquire(StripedVersatileReadWriteLock lock) {
                lock.writeLock();
            }
        };

        abstract void acquire(StripedVersatileReadWriteLock lock);
    }

    private enum WriteLockImpeder {
        READ_LOCK {
            @Override
            void impede(StripedVersatileReadWriteLock lock) {
                lock.readLock();
            }

            @Override
            void stopImpeding(StripedVersatileReadWriteLock lock) {
                lock.readUnlock();
            }
        },
        WRITE_LOCK {
            @Override
            void impede(StripedVersatileReadWriteLock lock) {
                lock.writeLock();
            }

            @Override
            void stopImpeding(StripedVersatileReadWriteLock lock) {
                lock.writeUnlock();
            }
        };

        abstract void impede(StripedVersatileReadWriteLock lock);

        abstract void stopImpeding(StripedVersatileReadWriteLock lock);
    }
}
