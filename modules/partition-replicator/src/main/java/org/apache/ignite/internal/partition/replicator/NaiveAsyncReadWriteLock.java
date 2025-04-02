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

package org.apache.ignite.internal.partition.replicator;

import static java.util.concurrent.CompletableFuture.completedFuture;

import it.unimi.dsi.fastutil.longs.Long2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap.Entry;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.StampedLock;

/**
 * Asynchronous analogue of read-write lock. It has the following properties:
 *
 * <ul>
 *     <li>Write lock is exclusive; if the lock is write-locked, other attempts to acquire any lock waits for the write lock to be released
 *     </li>
 *     <li>Read lock is non-exclusive: if the lock is read-locked (and there are no waiting write lock attempts), other read locks are
 *     acquired immediately, but attempts to acquire write locks wait for all read locks to be releasaed</li>
 *     <li>Write locks have priority over read locks: if the lock is read-locked, and there is a waiting write lock attempt, read lock
 *     attempts will queue until all write lock attempts are satisfied and released</li>
 *     <li>Lock holder is not bound to any thread; instead, a lock holder gets a stamp that can be used to release the lock</li>
 * </ul>
 *
 * <p>This implementation is naive because it implies that time-to-hold the locks can be pretty long and there will be no
 * high contension on the acquiring side; this simplifies the implementation.</p>
 */
public class NaiveAsyncReadWriteLock {
    /** Executor in which the waiting lock attempts' futures are completed. */
    private final Executor futureCompletionExecutor;

    /** Used to manage the lock state (including issuing and using stamps). */
    private final StampedLock stampedLock = new StampedLock();

    /** Used to linearize access to waiters collections. */
    private final Object mutex = new Object();

    /** Queue of futures waiting for write lock to be acquired; served in the order of appearance. */
    private final Queue<CompletableFuture<Long>> writeLockWaiters = new ArrayDeque<>();

    /** Queue of futures waiting for read locks to be acquired; served in the order of appearance. */
    private final Queue<CompletableFuture<Long>> readLockWaiters = new ArrayDeque<>();

    public NaiveAsyncReadWriteLock(Executor futureCompletionExecutor) {
        this.futureCompletionExecutor = futureCompletionExecutor;
    }

    /**
     * Attempts to acquire the write lock.
     *
     * @return Future completed with the stamp of the acquired lock; completed when the lock is acquired.
     */
    public CompletableFuture<Long> writeLock() {
        synchronized (mutex) {
            long stamp = stampedLock.tryWriteLock();
            if (stamp != 0) {
                return completedFuture(stamp);
            }

            CompletableFuture<Long> lockFuture = new CompletableFuture<>();

            writeLockWaiters.add(lockFuture);

            return lockFuture;
        }
    }

    /**
     * Unlocks write lock previously obtained via {@link #writeLock()}.
     *
     * @param stamp Stamp returned via write lock future.
     */
    public void unlockWrite(long stamp) {
        synchronized (mutex) {
            stampedLock.unlockWrite(stamp);

            CompletableFuture<Long> writeLockWaiter = writeLockWaiters.poll();

            if (writeLockWaiter != null) {
                // Someone is waiting for a write lock, satisfy the request.
                long newWriteStamp = stampedLock.tryWriteLock();
                assert newWriteStamp != 0;

                futureCompletionExecutor.execute(() -> writeLockWaiter.complete(newWriteStamp));
            } else {
                // Someone might be waiting for read locks.
                satisfyReadLockWaiters();
            }
        }
    }

    private void satisfyReadLockWaiters() {
        Long2ObjectMap<CompletableFuture<Long>> readLockWaitersMap = null;

        for (CompletableFuture<Long> readLockWaiter : readLockWaiters) {
            if (readLockWaitersMap == null) {
                readLockWaitersMap = new Long2ObjectAVLTreeMap<>();
            }

            long newReadStamp = stampedLock.tryReadLock();
            assert newReadStamp != 0;
            readLockWaitersMap.put(newReadStamp, readLockWaiter);

            for (Entry<CompletableFuture<Long>> entry : readLockWaitersMap.long2ObjectEntrySet()) {
                futureCompletionExecutor.execute(() -> entry.getValue().complete(entry.getLongKey()));
            }
        }

        readLockWaiters.clear();
    }

    /**
     * Attempts to acquire a read lock.
     *
     * @return Future completed with the stamp of the acquired lock; completed when the lock is acquired.
     */
    public CompletableFuture<Long> readLock() {
        synchronized (mutex) {
            // Write lock attempts have priority over read lock attempts, so first check whether someone waits for write lock.
            if (writeLockWaiters.isEmpty()) {
                long stamp = stampedLock.tryReadLock();
                if (stamp != 0) {
                    return completedFuture(stamp);
                }
            }

            CompletableFuture<Long> lockFuture = new CompletableFuture<>();

            readLockWaiters.add(lockFuture);

            return lockFuture;
        }
    }

    /**
     * Unlocks read lock previously obtained via {@link #readLock()}.
     *
     * @param stamp Stamp returned via read lock future.
     */
    public void unlockRead(long stamp) {
        synchronized (mutex) {
            stampedLock.unlockRead(stamp);

            CompletableFuture<Long> writeLockWaiter = writeLockWaiters.peek();

            if (writeLockWaiter != null) {
                long newWriteStamp = stampedLock.tryWriteLock();
                if (newWriteStamp != 0) {
                    writeLockWaiters.remove();

                    futureCompletionExecutor.execute(() -> writeLockWaiter.complete(newWriteStamp));
                }
            }
        }
    }

    boolean isReadLocked() {
        return stampedLock.isReadLocked();
    }
}
