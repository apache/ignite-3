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

import static java.lang.ThreadLocal.withInitial;
import static org.apache.ignite.internal.util.FastTimestamps.coarseCurrentTimeMillis;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;

/**
 * ReentrantReadWriteLock adapter with readLock tracking.
 */
public class ReentrantReadWriteLockWithTracking implements ReadWriteLock {
    /** Delegate instance. */
    private final ReentrantReadWriteLock delegate = new ReentrantReadWriteLock();

    /** Read lock holder. */
    private final ReentrantReadWriteLock.ReadLock readLock;

    /** Write lock holder. */
    private final ReentrantReadWriteLock.WriteLock writeLock = new ReentrantReadWriteLock.WriteLock(delegate) {
    };

    /**
     * ReentrantReadWriteLock wrapper, provides additional trace info on {@link ReadLockWithTracking#unlock()} method, if someone holds the
     * lock more than {@code readLockThreshold}.
     *
     * @param log Ignite logger.
     * @param readLockThreshold ReadLock threshold timeout in milliseconds.
     */
    public ReentrantReadWriteLockWithTracking(IgniteLogger log, long readLockThreshold) {
        readLock = new ReadLockWithTracking(delegate, log, readLockThreshold);
    }

    /**
     * Delegator implementation.
     */
    public ReentrantReadWriteLockWithTracking() {
        readLock = new ReentrantReadWriteLock.ReadLock(delegate) {
        };
    }

    /** {@inheritDoc} */
    @Override
    public ReentrantReadWriteLock.ReadLock readLock() {
        return readLock;
    }

    /** {@inheritDoc} */
    @Override
    public ReentrantReadWriteLock.WriteLock writeLock() {
        return writeLock;
    }

    /**
     * Returns {@code true} if the current thread holds write lock and {@code false} otherwise.
     */
    public boolean isWriteLockedByCurrentThread() {
        return delegate.isWriteLockedByCurrentThread();
    }

    /**
     * Returns the number of reentrant read holds on this lock by the current thread. A reader thread has a hold on a lock for each lock
     * action that is not matched by an unlock action.
     */
    public int getReadHoldCount() {
        return delegate.getReadHoldCount();
    }

    /**
     * Returns the number of read locks held for this lock. This method is designed for use in monitoring system state, not for
     * synchronization control.
     */
    public int getReadLockCount() {
        return delegate.getReadLockCount();
    }

    /**
     * Returns {@code true} if there are threads waiting to acquire the write lock.
     * This method is designed for use in monitoring system state, not for synchronization control.
     *
     * @return {@code true} if there may be threads waiting to acquire the write lock, {@code false} otherwise.
     */
    public boolean hasQueuedWriters() {
        // ReentrantReadWriteLock doesn't expose a direct method to check for queued writers only.
        // We use hasQueuedThreads() as an approximation - it returns true if there are
        // ANY queued threads, including threads waiting on read lock.
        // This is acceptable for our use case since readers won't queue each other.
        return delegate.hasQueuedThreads();
    }

    /**
     * Tracks long read lock holders.
     */
    private static class ReadLockWithTracking extends ReentrantReadWriteLock.ReadLock {
        private static final long serialVersionUID = 0L;

        private final ThreadLocal<IgniteBiTuple<Integer, Long>> readLockHolderTs = withInitial(() -> new IgniteBiTuple<>(0, 0L));

        private final IgniteLogger log;

        private final long readLockThreshold;

        /**
         * Constructor.
         *
         * @param lock Outer lock object.
         * @param log Logger.
         * @param readLockThreshold Lock print threshold in milliseconds.
         */
        protected ReadLockWithTracking(ReentrantReadWriteLock lock, IgniteLogger log, long readLockThreshold) {
            super(lock);

            this.log = log;

            this.readLockThreshold = readLockThreshold;
        }

        private void inc() {
            IgniteBiTuple<Integer, Long> val = readLockHolderTs.get();

            int cntr = val.get1();

            if (cntr == 0) {
                val.set2(coarseCurrentTimeMillis());
            }

            val.set1(++cntr);

            readLockHolderTs.set(val);
        }

        private void dec() {
            IgniteBiTuple<Integer, Long> val = readLockHolderTs.get();

            int cntr = val.get1();

            if (--cntr == 0) {
                long timeout = coarseCurrentTimeMillis() - val.get2();

                if (timeout > readLockThreshold) {
                    log.warn("ReadLock held for too long [heldFor={}ms]", new IgniteInternalException(), timeout);
                }
            }

            val.set1(cntr);

            readLockHolderTs.set(val);
        }

        /** {@inheritDoc} */
        @Override
        public void lock() {
            super.lock();

            inc();
        }

        /** {@inheritDoc} */
        @Override
        public void lockInterruptibly() throws InterruptedException {
            super.lockInterruptibly();

            inc();
        }

        /** {@inheritDoc} */
        @Override
        public boolean tryLock() {
            if (super.tryLock()) {
                inc();

                return true;
            } else {
                return false;
            }
        }

        /** {@inheritDoc} */
        @Override
        public boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException {
            if (super.tryLock(timeout, unit)) {
                inc();

                return true;
            } else {
                return false;
            }
        }

        /** {@inheritDoc} */
        @Override
        public void unlock() {
            super.unlock();

            dec();
        }
    }
}
