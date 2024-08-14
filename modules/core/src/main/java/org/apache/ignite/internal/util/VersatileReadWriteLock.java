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

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.TestOnly;

/**
 * A versatile read-write lock that can be used both in synchronous and asynchronous contexts.
 * Its blocking methods use the spinwait strategy. When they do so, they are not interruptible (that is, they do not break their loop on
 * interruption signal).
 *
 * <p>The locks are NOT reentrant (that is, the same thread can NOT acquire the same lock a few times without releasing it).
 *
 * <p>Write lock acquire requests are prioritized over read lock acquire requests. That is, if both read and write lock
 * acquire requests are received when the write lock is held by someone else, then, on its release, the write lock attempt will be served
 * first.
 *
 * <p>Lock owners are not tracked.
 *
 * <p>Asynchronous locking methods may complete the futures either in the calling thread (if they were able to immediately acquire
 * the requested lock) or in the supplied pool (if they had to wait for a release to happen before being able to satisfy the request).
 *
 * <p>Asynchronous locking methods never use spin loops. They do use CAS loops, but these are mostly very short.
 */
public class VersatileReadWriteLock {
    /** The highest bit of the state signals that the write lock is acquired. */
    private static final int WRITE_LOCK_BITS = 1 << 31;

    /** 31 lower bits represent a counter of read locks that qre acquired. */
    private static final int READ_LOCK_BITS = ~WRITE_LOCK_BITS;

    /**
     * State 0 means that both read and write locks are available for acquiring (as no locks are acquired).
     *
     * @see #state
     */
    private static final int AVAILABLE = 0;

    /** How much time to sleep on each iteration of a spin loop (milliseconds). */
    private static final int SLEEP_MILLIS = 10;

    /** {@link VarHandle} used to access the {@code pendingWLocks} field. */
    private static final VarHandle PENDING_WLOCKS_VH;

    /** {@link VarHandle} used to access the {@code state} field. */
    private static final VarHandle STATE_VH;

    static {
        try {
            STATE_VH = MethodHandles.lookup()
                .findVarHandle(VersatileReadWriteLock.class, "state", int.class);

            PENDING_WLOCKS_VH = MethodHandles.lookup()
                .findVarHandle(VersatileReadWriteLock.class, "pendingWriteLocks", int.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Main state of the lock.
     * <ul>
     *     <li>The highest bit is for whether the write lock is acquired or not.</li>
     *     <li>The remaining bits store a counter representing the acquired read locks.</li>
     * </ul>
     */
    @SuppressWarnings("unused")
    private volatile int state;

    /**
     * Number of pending write attempts to acquire the write lock. It is used to prioritize write lock attempts over read
     * lock attempts when the write lock has been released (so, if both an attempt to acquire the write lock and an attempt to acquire the
     * read lock are waiting for write lock to be released, a write lock attempt will be served first when the release happens).
     */
    @SuppressWarnings("unused")
    private volatile int pendingWriteLocks;

    /** Futures to be completed when read locks (one per future) are acquired after a write lock is releasaed. */
    private final Set<CompletableFuture<Void>> readLockSolicitors = ConcurrentHashMap.newKeySet();

    /** Futures to be completed when a write lock (one per future) is acquired after an impeding lock is released. */
    private final Set<CompletableFuture<Void>> writeLockSolicitors = ConcurrentHashMap.newKeySet();

    /** In this pool {@link #readLockSolicitors} and {@link #writeLockSolicitors} will be completed. */
    private final Executor asyncContinuationExecutor;

    /**
     * Constructor.
     */
    public VersatileReadWriteLock(Executor asyncContinuationExecutor) {
        this.asyncContinuationExecutor = asyncContinuationExecutor;
    }

    /**
     * Acquires the read lock. If the write lock is already held, this blocks until the write lock is released (and until all
     * concurrent write locks are acquired and released, as this class prioritizes write lock attempts over read lock attempts).
     */
    @SuppressWarnings("BusyWait")
    public void readLock() {
        boolean interrupted = false;

        while (true) {
            int curState = state;

            if (writeLockedOrGoingToBe(curState)) {
                try {
                    Thread.sleep(SLEEP_MILLIS);
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }

                continue;
            }

            if (tryAdvanceStateToReadLocked(curState)) {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }

                break;
            }
        }
    }

    private static int state(boolean writeLocked, int readLocks) {
        assert readLocks >= 0 && readLocks <= READ_LOCK_BITS : readLocks;

        return (writeLocked ? WRITE_LOCK_BITS : 0) | (readLocks & READ_LOCK_BITS);
    }

    private static boolean writeLocked(int curState) {
        return (curState & WRITE_LOCK_BITS) != 0;
    }

    private static int readLocks(int state) {
        return state & READ_LOCK_BITS;
    }

    private boolean writeLockedOrGoingToBe(int curState) {
        return writeLocked(curState) || pendingWriteLocks > 0;
    }

    private boolean tryAdvanceStateToReadLocked(int curState) {
        assert !writeLocked(curState);

        int newState = state(false, readLocks(curState) + 1);

        return compareAndSet(STATE_VH, curState, newState);
    }

    /**
     * Tries to acquire the read lock. No spinwait is used if the lock cannot be acquired immediately.
     *
     * @return {@code true} if acquired, {@code false} if write lock is already held by someone else (or someone is waiting to acquire
     *     the write lock).
     */
    public boolean tryReadLock() {
        while (true) {
            int curState = state;

            if (writeLockedOrGoingToBe(curState)) {
                return false;
            }

            if (tryAdvanceStateToReadLocked(curState)) {
                return true;
            }
        }
    }

    /**
     * Releases the read lock.
     *
     * @throws IllegalMonitorStateException thrown if the read lock is not acquired by anyone.
     */
    public void readUnlock() {
        while (true) {
            int curState = state;

            // We allow a write lock to be held here as someone could have taken it forcefully.
            boolean writeLocked = writeLocked(curState);
            int readLocks = readLocks(curState);
            if (readLocks < 1) {
                throw new IllegalMonitorStateException();
            }

            if (compareAndSet(STATE_VH, curState, state(writeLocked, readLocks - 1))) {
                if (readLocks == 1) {
                    // We released the final read lock.
                    notifyWriteLockSolicitors();
                }

                return;
            }
        }
    }

    /**
     * Acquires the write lock waiting, if needed. The thread will block until all other read and write locks are released.
     */
    @SuppressWarnings("BusyWait")
    public void writeLock() {
        boolean interrupted = false;

        incrementPendingWriteLocks();
        try {
            while (!trySwitchStateToWriteLocked()) {
                try {
                    Thread.sleep(SLEEP_MILLIS);
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }
        } finally {
            decrementPendingWriteLocks();
        }

        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private void incrementPendingWriteLocks() {
        while (true) {
            int curPendingWriteLocks = pendingWriteLocks;

            if (compareAndSet(PENDING_WLOCKS_VH, curPendingWriteLocks, curPendingWriteLocks + 1)) {
                break;
            }
        }
    }

    private boolean trySwitchStateToWriteLocked() {
        return compareAndSet(STATE_VH, AVAILABLE, state(true, 0));
    }

    private void decrementPendingWriteLocks() {
        while (true) {
            int curPendingWriteLocks = pendingWriteLocks;

            assert curPendingWriteLocks > 0;

            if (compareAndSet(PENDING_WLOCKS_VH, curPendingWriteLocks, curPendingWriteLocks - 1)) {
                break;
            }
        }
    }

    /**
     * Acquires the write lock without sleeping between unsuccessful attempts. Instead, the spinwait eats cycles of the core it gets at full
     * speed. It is non-interruptible as its {@link #writeLock()} cousin.
     */
    @SuppressWarnings("PMD.EmptyControlStatement")
    public void writeLockBusy() {
        incrementPendingWriteLocks();
        try {
            while (!trySwitchStateToWriteLocked()) {
                // No-op.
            }
        } finally {
            decrementPendingWriteLocks();
        }
    }

    /**
     * Tries to acquire the write lock. Never blocks: if any lock has already been acquired by someone else, returns {@code false}
     * immediately.
     *
     * @return {@code true} if the write lock has been acquired, {@code false} otherwise
     */
    public boolean tryWriteLock() {
        return trySwitchStateToWriteLocked();
    }

    /**
     * Tries to acquire the write lock with timeout. If it gets the write lock before the timeout expires, then returns {@code true}. If the
     * timeout expires before the lock becomes available, returns {@code false}.
     *
     * @param timeout Timeout.
     * @param unit    Unit.
     * @return {@code true} if the write lock has been acquired in time; {@code false} otherwise
     * @throws InterruptedException If interrupted.
     */
    @SuppressWarnings("BusyWait")
    public boolean tryWriteLock(long timeout, TimeUnit unit) throws InterruptedException {
        incrementPendingWriteLocks();
        try {
            long startNanos = System.nanoTime();

            long timeoutNanos = unit.toNanos(timeout);

            while (true) {
                if (trySwitchStateToWriteLocked()) {
                    return true;
                }

                Thread.sleep(SLEEP_MILLIS);

                if (System.nanoTime() - startNanos >= timeoutNanos) {
                    return false;
                }
            }
        } finally {
            decrementPendingWriteLocks();
        }
    }

    /**
     * Releases the write lock.
     *
     * @throws IllegalMonitorStateException thrown if the write lock is not acquired.
     */
    public void writeUnlock() {
        int curState = state;
        // There could still be some read locks if the write lock was taken forcefully.
        int readLocks = readLocks(curState);

        if (!writeLocked(curState)) {
            throw new IllegalMonitorStateException();
        }

        boolean b = compareAndSet(STATE_VH, state, state(false, readLocks));

        assert b;

        notifyWriteLockSolicitors();
        notifyReadLockSolicitors();
    }

    private void notifyWriteLockSolicitors() {
        if (writeLockSolicitors.isEmpty()) {
            return;
        }

        for (Iterator<CompletableFuture<Void>> iterator = writeLockSolicitors.iterator(); iterator.hasNext(); ) {
            CompletableFuture<Void> future = iterator.next();

            if (!tryWriteLock()) {
                // Someone has already acquired an impeding lock, we're too late, let's wait for next opportunity.
                break;
            }

            decrementPendingWriteLocks();

            asyncContinuationExecutor.execute(() -> future.complete(null));

            iterator.remove();
        }
    }

    private void notifyReadLockSolicitors() {
        if (readLockSolicitors.isEmpty()) {
            return;
        }

        for (Iterator<CompletableFuture<Void>> iterator = readLockSolicitors.iterator(); iterator.hasNext(); ) {
            CompletableFuture<Void> future = iterator.next();

            if (!tryReadLock()) {
                // Someone has already acquired a write lock, we're too late, let's wait for next opportunity.
                break;
            }

            asyncContinuationExecutor.execute(() -> {
                if (!future.complete(null)) {
                    // The one who added this future has already taken the read lock after adding the future; as they have completed the
                    // future, this is us who needs to unlock the excess.
                    readUnlock();
                }
            });

            iterator.remove();
        }
    }

    /**
     * Returns {@code true} on success.
     *
     * @param varHandle VarHandle.
     * @param expect    Expected.
     * @param update    Update.
     * @return {@code True} on success.
     */
    private boolean compareAndSet(VarHandle varHandle, int expect, int update) {
        return varHandle.compareAndSet(this, expect, update);
    }

    /**
     * Executes the provided asynchronous action under protection of a read lock: that is, it first obtains a read lock
     * asynchronously, then executes the action, and then releases the lock.
     *
     * @param action Action to execute.
     * @return Action result.
     */
    public <T> CompletableFuture<T> inReadLockAsync(Supplier<? extends CompletableFuture<T>> action) {
        return readLockAsync()
                .thenCompose(unused -> action.get())
                .whenComplete((res, ex) -> readUnlock());
    }

    private CompletableFuture<Void> readLockAsync() {
        if (tryReadLock()) {
            return nullCompletedFuture();
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        readLockSolicitors.add(future);

        // Let's check again as the lock might have been released before we added the future.
        if (tryReadLock()) {
            readLockSolicitors.remove(future);

            if (!future.complete(null)) {
                // The one who processes the solicitors set has already taken the read lock for us; as they have completed the
                // future, this is us who needs to unlock the excess.
                readUnlock();
            }
        }

        return future;
    }

    /**
     * Executes the provided asynchronous action under protection of a write lock: that is, it first obtains the write lock
     * asynchronously, then executes the action, and then releases the lock.
     *
     * @param action Action to execute.
     * @return Action result.
     */
    public <T> CompletableFuture<T> inWriteLockAsync(Supplier<? extends CompletableFuture<T>> action) {
        return writeLockAsync()
                .thenCompose(unused -> action.get())
                .whenComplete((res, ex) -> writeUnlock());
    }

    private CompletableFuture<Void> writeLockAsync() {
        if (tryWriteLock()) {
            return nullCompletedFuture();
        }

        incrementPendingWriteLocks();

        CompletableFuture<Void> future = new CompletableFuture<>();
        writeLockSolicitors.add(future);

        // Let's check again as the lock might have been released before we added the future.
        if (tryWriteLock()) {
            decrementPendingWriteLocks();
            writeLockSolicitors.remove(future);

            future.complete(null);
        }

        return future;
    }

    /**
     * Returns the count of pending write lock requests count. Only used by tests, should not be used in production code.
     *
     * @return count of pending requests to get the write lock
     */
    @TestOnly
    int pendingWriteLocksCount() {
        return pendingWriteLocks;
    }

    @Override
    public String toString() {
        return S.toString(VersatileReadWriteLock.class, this);
    }
}
