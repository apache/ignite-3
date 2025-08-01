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

package org.apache.ignite.internal.pagememory.util;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;
import java.util.function.LongPredicate;
import org.apache.ignite.internal.util.OffheapReadWriteLock;
import org.jetbrains.annotations.Nullable;

/**
 * Off-heap read-write lock that allows to sequence the lock acquisition order if necessary.
 */
public class SequencedOffheapReadWriteLock extends OffheapReadWriteLock {
    /** Number of threads that are actively waiting on {@link Sequencer#await()} or {@link Sequencer#complete()} right now. */
    private final AtomicInteger activelyWaiting = new AtomicInteger();

    /** {@link Sequencer} instance that will be used to order the locks. Might be {@code null}. */
    private volatile @Nullable Sequencer sequencer;

    /** All the locks acquired by all the threads. Used for a deadlock prevention. */
    private volatile LongList @Nullable [] acquiredLocks;

    /**
     * Constructor.
     */
    public SequencedOffheapReadWriteLock() {
        super(OffheapReadWriteLock.DEFAULT_CONCURRENCY_LEVEL);
    }

    /**
     * Starts sequencing the lock acquisition order, using passed generator and number of threads. Caller must guarantee that no one else
     * using this lock instance while calling this method.
     *
     * @see Sequencer
     */
    public void startSequencing(IntSupplier threadIdGenerator, int threads) {
        sequencer = new Sequencer(threadIdGenerator, threads);

        LongList[] acquiredLocks = new LongList[threads];
        for (int i = 0; i < threads; i++) {
            // We don't hold too many locks at the same time, 4 should be enough for the majority of cases.
            acquiredLocks[i] = new LongArrayList(4);
        }

        this.acquiredLocks = acquiredLocks;
    }

    /**
     * Stops sequencing the lock acquisition order.
     */
    public void stopSequencing() {
        sequencer = null;
        acquiredLocks = null;
    }

    @Override
    public boolean readLock(long lock, int tag) {
        Sequencer s = sequencer;

        if (s == null) {
            return super.readLock(lock, tag);
        }

        return doLock(lock, s, l -> super.readLock(l, tag));
    }

    @Override
    public void readUnlock(long lock) {
        super.readUnlock(lock);

        doUnlock(lock);
    }

    @Override
    public boolean tryWriteLock(long lock, int tag) {
        Sequencer s = sequencer;

        if (s == null) {
            return super.tryWriteLock(lock, tag);
        }

        return doLock(lock, s, l -> super.tryWriteLock(l, tag));
    }

    @Override
    public boolean writeLock(long lock, int tag) {
        Sequencer s = sequencer;

        if (s == null) {
            return super.writeLock(lock, tag);
        }

        return doLock(lock, s, l -> super.writeLock(l, tag));
    }

    @Override
    public void writeUnlock(long lock, int tag) {
        super.writeUnlock(lock, tag);

        doUnlock(lock);
    }

    /**
     * Sets current thread ID to the sequencer.
     *
     * @throws NullPointerException If sequencer is not started.
     * @see #startSequencing(IntSupplier, int)
     * @see #stopSequencing()
     * @see Sequencer#setCurrentThreadId(int)
     */
    public void setCurrentThreadId(int threadId) {
        Sequencer s = sequencerNotNull();

        s.setCurrentThreadId(threadId);
    }

    /**
     * Waits until it's current thread's turn to proceed. This method must be called after {@link #setCurrentThreadId(int)}.
     *
     * @throws NullPointerException If sequencer is not started.
     * @see Sequencer#await()
     */
    public void await() {
        Sequencer s = sequencerNotNull();

        activelyWaiting.incrementAndGet();
        s.await();
        activelyWaiting.decrementAndGet();
    }

    /**
     * Notifies the sequencer that current thread completed its operations.
     *
     * @throws NullPointerException If sequencer is not started.
     * @see Sequencer#release()
     */
    public void release() {
        sequencerNotNull().release();
    }

    /**
     * Notifies the sequencer that current thread completed its operations.
     *
     * @throws NullPointerException If sequencer is not started.
     * @see Sequencer#complete()
     */
    public void complete() {
        Sequencer s = sequencerNotNull();

        activelyWaiting.incrementAndGet();
        s.complete();
        activelyWaiting.decrementAndGet();
    }

    private boolean doLock(long lock, Sequencer s, LongPredicate lockFunction) {
        mainLoop: while (true) {
            activelyWaiting.incrementAndGet();
            s.await();
            activelyWaiting.decrementAndGet();

            LongList[] acquiredLocks = this.acquiredLocks;
            assert acquiredLocks != null : "Sequencer is started, but acquiredLocks are not initialized";

            int threads = acquiredLocks.length;

            // Wait until the rest of threads is waiting for the sequencer. This state guarantees that they won't unlock anything until we
            // call "s.release()", which as a side effect guarantees that the content of "acquiredLocks" won't change, and it's safe to do a
            // deadlock prevention routine.
            while (activelyWaiting.get() != threads - 1) {
                // Spin-wait is the simplest way to wait for something here. It is guaranteed that this loop will eventually be completed.
                Thread.onSpinWait();
            }

            int currentThreadId = s.getCurrentThreadId();

            try {
                // Deadlock prevention itself. If any of the threads already holds the lock that we want to acquire, we must not do that.
                // That thread is currently stuck at a sequencer, and we're holding a sequencer right now. If we attempt to proceed with
                // acquiring "lock", we'll get a classic deadlock situation. Instead, we must skip our turn and try locking the "lock" again
                // next time.
                for (int threadId = 0; threadId < threads; threadId++) {
                    // Loop should be faster than stream, this is a hot code.
                    if (threadId != currentThreadId) {
                        LongList locks = acquiredLocks[threadId];

                        if (!locks.isEmpty() && locks.contains(lock)) {
                            continue mainLoop;
                        }
                    }
                }

                if (lockFunction.test(lock)) {
                    // Only mark lock as acquired if it was successfully locked.
                    acquiredLocks[currentThreadId].add(lock);

                    return true;
                } else {
                    return false;
                }
            } finally {
                s.release();
            }
        }
    }

    private void doUnlock(long lock) {
        Sequencer s = sequencer;

        if (s != null) {
            int currentThreadId = s.getCurrentThreadId();

            LongList[] acquiredLocks = this.acquiredLocks;
            assert acquiredLocks != null : "Sequencer is started, but acquiredLocks are not initialized";

            boolean released = acquiredLocks[currentThreadId].rem(lock);
            assert released : "Release of a lock that has not been taken: " + lock;
        }
    }

    private Sequencer sequencerNotNull() {
        return Objects.requireNonNull(sequencer, "sequencer");
    }
}
