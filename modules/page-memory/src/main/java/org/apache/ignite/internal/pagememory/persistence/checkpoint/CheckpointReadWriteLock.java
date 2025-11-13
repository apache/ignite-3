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

import static org.apache.ignite.internal.util.FastTimestamps.coarseCurrentTimeMillis;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteThrottledLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapper of the classic read write lock with checkpoint features.
 */
public class CheckpointReadWriteLock {
    /**
     * Any thread with a such prefix is managed by the checkpoint.
     *
     * <p>So some conditions can rely on it(ex. we don't need a checkpoint lock there because the lock is already taken).
     */
    static final String CHECKPOINT_RUNNER_THREAD_PREFIX = "checkpoint-runner";

    private static final long LONG_LOCK_THRESHOLD_MILLIS = 1000;

    private static final String LONG_LOCK_THROTTLE_KEY = "long-lock";

    private final IgniteThrottledLogger log;

    private final ThreadLocal<Integer> checkpointReadLockHoldCount = ThreadLocal.withInitial(() -> 0);

    /** Checkpoint lock. */
    private final ReentrantReadWriteLockWithTracking checkpointLock;

    /** Current write lock holder thread. */
    private volatile @Nullable Thread currentWriteLockHolder;

    /**
     * Constructor.
     *
     * @param checkpointLock Checkpoint lock.
     * @param throttledLogExecutor Executor for the throttled logger.
     */
    public CheckpointReadWriteLock(ReentrantReadWriteLockWithTracking checkpointLock, Executor throttledLogExecutor) {
        this.checkpointLock = checkpointLock;
        this.log = Loggers.toThrottledLogger(Loggers.forClass(CheckpointReadWriteLock.class), throttledLogExecutor);
    }

    /**
     * Gets the checkpoint read lock.
     *
     * @throws IgniteInternalException If failed.
     */
    public void readLock() {
        if (isWriteLockHeldByCurrentThread()) {
            return;
        }

        long start = coarseCurrentTimeMillis();

        checkpointLock.readLock().lock();

        onReadLock(start, true);
    }

    /**
     * Tries to get a checkpoint read lock.
     *
     * @param timeout – Time to wait for the read lock.
     * @param unit – Time unit of the timeout argument.
     * @throws IgniteInternalException If failed.
     */
    public boolean tryReadLock(long timeout, TimeUnit unit) throws InterruptedException {
        if (isWriteLockHeldByCurrentThread()) {
            return true;
        }

        long start = coarseCurrentTimeMillis();

        boolean res = checkpointLock.readLock().tryLock(timeout, unit);

        onReadLock(start, res);

        return res;
    }

    /**
     * Tries to get a checkpoint read lock.
     *
     * @return {@code True} if the checkpoint read lock is acquired.
     */
    public boolean tryReadLock() {
        if (isWriteLockHeldByCurrentThread()) {
            return true;
        }

        long start = coarseCurrentTimeMillis();

        boolean res = checkpointLock.readLock().tryLock();

        onReadLock(start, res);

        return res;
    }

    /**
     * Returns {@code true} if checkpoint lock is held by current thread.
     */
    public boolean checkpointLockIsHeldByThread() {
        return isWriteLockHeldByCurrentThread()
                || checkpointReadLockHoldCount.get() > 0
                || Thread.currentThread() instanceof IgniteCheckpointThread;
    }

    /**
     * Releases the checkpoint read lock.
     */
    public void readUnlock() {
        if (isWriteLockHeldByCurrentThread()) {
            return;
        }

        checkpointLock.readLock().unlock();

        checkpointReadLockHoldCount.set(checkpointReadLockHoldCount.get() - 1);
    }

    /**
     * Takes the checkpoint write lock.
     */
    public void writeLock() {
        checkpointLock.writeLock().lock();

        this.currentWriteLockHolder = Thread.currentThread();
    }

    /**
     * Releases the checkpoint write lock.
     */
    public void writeUnlock() {
        this.currentWriteLockHolder = null;

        checkpointLock.writeLock().unlock();
    }

    /**
     * Returns {@code true} if current thread hold write lock.
     */
    public boolean isWriteLockHeldByCurrentThread() {
        return currentWriteLockHolder == Thread.currentThread();
    }

    /**
     * Returns the number of reentrant read holds on this lock by the current thread. A reader thread has a hold on a lock for each lock
     * action that is not matched by an unlock action.
     */
    public int getReadHoldCount() {
        return checkpointLock.getReadHoldCount();
    }

    /**
     * Returns {@code true} if there are threads waiting to acquire the write lock.
     *
     * @return {@code true} if there are threads waiting to acquire the write lock, {@code false} otherwise.
     */
    public boolean hasQueuedWriters() {
        return checkpointLock.hasQueuedWriters();
    }

    private void onReadLock(long start, boolean taken) {
        long elapsed = coarseCurrentTimeMillis() - start;

        if (taken) {
            checkpointReadLockHoldCount.set(checkpointReadLockHoldCount.get() + 1);
        }

        if (elapsed > LONG_LOCK_THRESHOLD_MILLIS) {
            log.warn(LONG_LOCK_THROTTLE_KEY, "Checkpoint read lock took {} ms to acquire.", elapsed);
        }
    }
}
