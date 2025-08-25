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

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

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
 *
 * <p>Synchronous read lock acquisitions and releases MUST be made in the same thread. There is no such requirement for write lock and
 * for asynchronous locking.
 *
 * <p>The lock is striped. It allows to reduce contention (as there are as many contention points as there are stripes).
 */
public class StripedVersatileReadWriteLock {
    /** Default concurrency. */
    private static final int DEFAULT_CONCURRENCY = Math.max(1, Runtime.getRuntime().availableProcessors() / 2);

    private final VersatileReadWriteLock[] locks;

    /** Index generator. */
    private static final AtomicInteger INDEX_GENERATOR = new AtomicInteger();

    /** Index. */
    private static final ThreadLocal<Integer> INDEX = ThreadLocal.withInitial(INDEX_GENERATOR::incrementAndGet);

    /**
     * Constructor.
     */
    public StripedVersatileReadWriteLock(Executor asyncContinuationExecutor) {
        this(asyncContinuationExecutor, DEFAULT_CONCURRENCY);
    }

    private StripedVersatileReadWriteLock(Executor asyncContinuationExecutor, int concurrency) {
        assert concurrency > 0 : "Concurrency must be positive, but was: " + concurrency;

        locks = new VersatileReadWriteLock[concurrency];
        for (int i = 0; i < concurrency; i++) {
            locks[i] = new VersatileReadWriteLock(asyncContinuationExecutor);
        }
    }

    /**
     * Gets current stripe index.
     *
     * @return Index of current thread stripe.
     */
    private int currentIndex() {
        int index = INDEX.get();

        return index % locks.length;
    }

    private VersatileReadWriteLock currentLock() {
        return locks[currentIndex()];
    }

    /**
     * Acquires the read lock. If the write lock is already held, this blocks until the write lock is released (and until all
     * concurrent write locks are acquired and released, as this class prioritizes write lock attempts over read lock attempts).
     *
     * <p>In contrast with async locking, invocations of this method MUST be invoked in the same thread in which the corresponding
     * {@link #readUnlock()} will be invoked.
     */
    public void readLock() {
        currentLock().readLock();
    }

    /**
     * Tries to acquire the read lock. No spinwait is used if the lock cannot be acquired immediately.
     *
     * <p>In contrast with async locking, invocations of this method MUST be invoked in the same thread in which the corresponding
     * {@link #readUnlock()} will be invoked.
     *
     * @return {@code true} if acquired, {@code false} if write lock is already held by someone else (or someone is waiting to acquire
     *     the write lock).
     */
    public boolean tryReadLock() {
        return currentLock().tryReadLock();
    }

    /**
     * Releases the read lock.
     *
     * <p>In contrast with async locking, invocations of this method MUST be invoked in the same thread in which the corresponding
     * {@link #readLock()} is invoked.
     *
     * @throws IllegalMonitorStateException thrown if the read lock is not acquired by anyone.
     */
    public void readUnlock() {
        currentLock().readUnlock();
    }

    private void readUnlock(int idx) {
        locks[idx].readUnlock();
    }

    /**
     * Acquires the write lock waiting, if needed. The thread will block until all other read and write locks are released.
     */
    public void writeLock() {
        // Locks must be acquired in order to avoid deadlocks.
        for (VersatileReadWriteLock lock : locks) {
            lock.writeLock();
        }
    }

    private void writeUnlock0(int fromIndex) {
        for (int i = fromIndex; i >= 0; i--) {
            locks[i].writeUnlock();
        }
    }

    /**
     * Tries to acquire the write lock. Never blocks: if any lock has already been acquired by someone else, returns {@code false}
     * immediately.
     *
     * @return {@code true} if the write lock has been acquired, {@code false} otherwise
     */
    public boolean tryWriteLock() {
        int i = 0;

        try {
            for (; i < locks.length; i++) {
                if (!locks[i].tryWriteLock()) {
                    break;
                }
            }
        } finally {
            if (0 < i && i < locks.length) {
                writeUnlock0(i - 1);
            }
        }

        return i == locks.length;
    }

    /**
     * Releases the write lock.
     *
     * @throws IllegalMonitorStateException thrown if the write lock is not acquired.
     */
    public void writeUnlock() {
        writeUnlock0(locks.length - 1);
    }

    /**
     * Executes the provided asynchronous action under protection of a read lock: that is, it first obtains a read lock
     * asynchronously, then executes the action, and then releases the lock.
     *
     * @param action Action to execute.
     * @return Action result.
     */
    public <T> CompletableFuture<T> inReadLockAsync(Supplier<? extends CompletableFuture<T>> action) {
        int index = currentIndex();

        return locks[index].readLockAsync()
                .thenCompose(unused -> action.get())
                .whenComplete((res, ex) -> readUnlock(index));
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
        CompletableFuture<Void> future = nullCompletedFuture();

        // Locks must be acquired in order to avoid deadlocks, hence the chain.
        for (VersatileReadWriteLock lock : locks) {
            future = future.thenCompose(unused -> lock.writeLockAsync());
        }

        return future;
    }

    int readLocksHeld() {
        return Arrays.stream(locks).mapToInt(VersatileReadWriteLock::readLocksHeld).sum();
    }

    boolean isWriteLocked() {
        return Arrays.stream(locks).anyMatch(VersatileReadWriteLock::isWriteLocked);
    }
}
