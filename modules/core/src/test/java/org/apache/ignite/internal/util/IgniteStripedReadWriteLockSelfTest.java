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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Test for {@link IgniteStripedReadWriteLockSelfTest}.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
public class IgniteStripedReadWriteLockSelfTest {
    private static final int CONCURRENCY = 16;

    /** The lock under test. */
    private final IgniteStripedReadWriteLock lock = new IgniteStripedReadWriteLock(CONCURRENCY);

    /** Executor service used to run tasks in threads different from the main test thread. */
    private final ExecutorService executor = Executors.newCachedThreadPool();

    /**
     * Cleans up after a test.
     */
    @AfterEach
    void cleanup() {
        releaseReadLockHeldByCurrentThread();
        releaseWriteLockHeldByCurrentThread();

        IgniteUtils.shutdownAndAwaitTermination(executor, 3, TimeUnit.SECONDS);
    }

    private void releaseReadLockHeldByCurrentThread() {
        while (true) {
            try {
                lock.readLock().unlock();
            } catch (IllegalMonitorStateException e) {
                // released our read lock completely
                break;
            }
        }
    }

    private void releaseWriteLockHeldByCurrentThread() {
        while (lock.isWriteLockedByCurrentThread()) {
            lock.writeLock().unlock();
        }
    }

    @Test
    void readLockDoesNotAllowWriteLockToBeAcquired() {
        lock.readLock().lock();

        assertThatWriteLockAcquireAttemptBlocksForever();

        lock.readLock().unlock();
    }

    private void assertThatWriteLockAcquireAttemptBlocksForever() {
        Future<?> future = executor.submit(() -> lock.writeLock().lock());

        assertThrows(TimeoutException.class, () -> future.get(100, TimeUnit.MILLISECONDS));
    }

    @Test
    void readLockDoesNotAllowWriteLockToBeAcquiredWithTimeout() throws Exception {
        lock.readLock().lock();

        Boolean acquired = callWithTimeout(() -> lock.writeLock().tryLock(1, TimeUnit.MILLISECONDS));
        assertThat(acquired, is(false));

        lock.readLock().unlock();
    }

    @Test
    void readLockAllowsReadLockToBeAcquired() throws Exception {
        lock.readLock().lock();

        assertThatReadLockCanBeAcquired();
    }

    private void assertThatReadLockCanBeAcquired() throws InterruptedException, ExecutionException, TimeoutException {
        runWithTimeout(() -> lock.readLock().lock());
    }

    private <T> T callWithTimeout(Callable<T> call) throws ExecutionException, InterruptedException, TimeoutException {
        Future<T> future = executor.submit(call);
        return getWithTimeout(future);
    }

    private void runWithTimeout(Runnable runnable) throws ExecutionException, InterruptedException, TimeoutException {
        Future<?> future = executor.submit(runnable);
        getWithTimeout(future);
    }

    private static <T> T getWithTimeout(Future<? extends T> future) throws ExecutionException,
            InterruptedException, TimeoutException {
        return future.get(10, TimeUnit.SECONDS);
    }

    @Test
    void writeLockDoesNotAllowReadLockToBeAcquired() {
        lock.writeLock().lock();

        assertThatReadLockAcquireAttemptBlocksForever();

        lock.writeLock().unlock();
    }

    private void assertThatReadLockAcquireAttemptBlocksForever() {
        Future<?> readLockAttemptFuture = executor.submit(() -> lock.readLock().lock());

        assertThrows(TimeoutException.class, () -> readLockAttemptFuture.get(100, TimeUnit.MILLISECONDS));
    }

    @Test
    void writeLockDoesNotAllowWriteLockToBeAcquired() {
        lock.writeLock().lock();

        assertThatWriteLockAcquireAttemptBlocksForever();

        lock.writeLock().unlock();
    }

    @Test
    void readUnlockReleasesTheLock() throws Exception {
        lock.readLock().lock();
        lock.readLock().unlock();

        runWithTimeout(lock::writeLock);
    }

    @Test
    void writeUnlockReleasesTheLock() throws Exception {
        lock.writeLock().lock();
        lock.writeLock().unlock();

        assertThatReadLockCanBeAcquired();
    }

    @Test
    void testWriteLockReentry() {
        lock.writeLock();

        lock.writeLock();

        assertTrue(lock.writeLock().tryLock());
    }

    @Test
    void testWriteLockReentryWithTryWriteLock() {
        lock.writeLock().tryLock();

        assertTrue(lock.writeLock().tryLock());
    }

    @Test
    void testWriteLockReentryWithTimeout() throws Exception {
        lock.writeLock().tryLock(1, TimeUnit.MILLISECONDS);
        lock.writeLock().tryLock(1, TimeUnit.MILLISECONDS);

        assertTrue(lock.writeLock().tryLock());
    }

    @Test
    void testReadLockReentry() {
        lock.readLock();

        lock.readLock();

        assertTrue(lock.readLock().tryLock());
    }

    @Test
    void shouldAllowAcquireAndReleaseReadLockWhileHoldingWriteLock() {
        lock.writeLock().lock();

        lock.readLock().lock();
        lock.readLock().unlock();

        lock.writeLock().unlock();
    }

    @Test
    void shouldAllowInterleavingHoldingReadAndWriteLocks() {
        lock.writeLock().lock();

        lock.readLock().lock();

        lock.writeLock().unlock();

        assertFalse(lock.writeLock().tryLock());

        lock.readLock().unlock();

        // Test that we can operate with write locks now.
        lock.writeLock().lock();
        lock.writeLock().unlock();
    }

    @Test
    void readLockReleasedLessTimesThanAcquiredShouldStillBeTaken() {
        lock.readLock().lock();
        lock.readLock().lock();
        lock.readLock().unlock();

        assertThatWriteLockAcquireAttemptBlocksForever();

        lock.readLock().lock();
    }

    @Test
    void writeLockReleasedLessTimesThanAcquiredShouldStillBeTaken() {
        lock.writeLock().lock();
        lock.writeLock().lock();
        lock.writeLock().unlock();

        assertThatReadLockAcquireAttemptBlocksForever();

        lock.writeLock().unlock();
    }

    @Test
    void shouldThrowOnReadUnlockingWhenNotHoldingReadLock() {
        assertThrows(IllegalMonitorStateException.class, () -> lock.readLock().unlock());
    }

    @Test
    void shouldThrowOnWriteUnlockingWhenNotHoldingWriteLock() {
        assertThrows(IllegalMonitorStateException.class,  () -> lock.writeLock().unlock());
    }

    @Test
    void readLockAcquiredWithTryReadLockDoesNotAllowWriteLockToBeAcquired() {
        lock.readLock().tryLock();

        assertThatWriteLockAcquireAttemptBlocksForever();

        lock.readLock().unlock();
    }

    @Test
    void tryReadLockShouldReturnTrueWhenReadLockWasAcquiredSuccessfully() {
        assertTrue(lock.readLock().tryLock());
    }

    @Test
    void tryReadLockShouldReturnFalseWhenReadLockCouldNotBeAcquired() throws Exception {
        lock.writeLock().lock();

        Boolean acquired = callWithTimeout(() -> lock.readLock().tryLock());

        assertThat(acquired, is(false));
    }

    @Test
    void writeLockAcquiredWithTryWriteLockDoesNotAllowWriteLockToBeAcquired() {
        lock.writeLock().tryLock();

        assertThatReadLockAcquireAttemptBlocksForever();

        lock.writeLock().unlock();
    }

    @Test
    void tryWriteLockShouldReturnTrueWhenWriteLockWasAcquiredSuccessfully() {
        assertTrue(lock.writeLock().tryLock());
    }

    @Test
    void tryWriteLockShouldReturnFalseWhenWriteLockCouldNotBeAcquired() throws Exception {
        lock.writeLock().lock();

        Boolean acquired = callWithTimeout(() -> lock.writeLock().tryLock());

        assertThat(acquired, is(false));
    }

    @Test
    void writeLockedByCurrentThreadShouldReturnTrueWhenLockedByCurrentThread() {
        lock.writeLock().lock();

        assertTrue(lock.isWriteLockedByCurrentThread());
    }

    @Test
    void writeLockedByCurrentThreadShouldReturnFalseWhenNotLocked() {
        assertFalse(lock.isWriteLockedByCurrentThread());
    }

    @Test
    void writeLockedByCurrentThreadShouldReturnFalseWhenLockedByAnotherThread() throws Exception {
        lock.writeLock();

        Boolean lockedByCaller = callWithTimeout(lock::isWriteLockedByCurrentThread);
        assertThat(lockedByCaller, is(false));
    }
}
