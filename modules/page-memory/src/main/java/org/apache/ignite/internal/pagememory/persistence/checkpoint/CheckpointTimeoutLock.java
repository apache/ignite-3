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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.LOCK_RELEASED;
import static org.apache.ignite.internal.util.FastTimestamps.coarseCurrentTimeMillis;
import static org.apache.ignite.internal.util.IgniteUtils.getUninterruptibly;

import java.util.Collection;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.pagememory.PageMemoryDataRegion;
import org.apache.ignite.internal.pagememory.persistence.PageMemoryImpl;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.NodeStoppingException;

/**
 * Checkpoint lock for outer usage which should be used to protect data during writing to memory. It contains complex logic for the correct
 * taking of inside checkpoint lock(timeout, force checkpoint, etc.).
 */
public class CheckpointTimeoutLock implements IgniteComponent {
    /** Ignite logger. */
    protected final IgniteLogger log;

    /** Data regions which should be covered by this lock. */
    private final Supplier<Collection<PageMemoryDataRegion>> dataRegionsSupplier;

    /** Internal checkpoint lock. */
    private final CheckpointReadWriteLock checkpointReadWriteLock;

    /** Service for triggering the checkpoint. */
    private final Checkpointer checkpointer;

    /** Timeout for checkpoint read lock acquisition in milliseconds. */
    private volatile long checkpointReadLockTimeout;

    /** Stop flag. */
    private boolean stop;

    /**
     * Constructor.
     *
     * @param log Logger.
     * @param checkpointReadWriteLock Checkpoint read-write lock.
     * @param checkpointReadLockTimeout Timeout for checkpoint read lock acquisition in milliseconds.
     * @param dataRegionsSupplier Data regions which should be covered by this lock.
     */
    public CheckpointTimeoutLock(
            IgniteLogger log,
            CheckpointReadWriteLock checkpointReadWriteLock,
            long checkpointReadLockTimeout,
            Supplier<Collection<PageMemoryDataRegion>> dataRegionsSupplier,
            Checkpointer checkpointer
    ) {
        this.log = log;
        this.checkpointReadWriteLock = checkpointReadWriteLock;
        this.checkpointReadLockTimeout = checkpointReadLockTimeout;
        this.dataRegionsSupplier = dataRegionsSupplier;
        this.checkpointer = checkpointer;
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        stop = false;
    }

    /** {@inheritDoc} */
    @Override
    public void stop() {
        checkpointReadWriteLock.writeLock();

        try {
            stop = true;
        } finally {
            checkpointReadWriteLock.writeUnlock();
        }
    }

    /**
     * Gets the checkpoint read lock.
     *
     * @throws IgniteInternalException If failed.
     * @throws CheckpointReadLockTimeoutException If failed to get checkpoint read lock by timeout.
     */
    public void checkpointReadLock() {
        if (checkpointReadWriteLock.isWriteLockHeldByCurrentThread()) {
            return;
        }

        long timeout = checkpointReadLockTimeout;

        long start = coarseCurrentTimeMillis();

        boolean interrupted = false;

        try {
            for (; ; ) {
                try {
                    if (timeout > 0 && (coarseCurrentTimeMillis() - start) >= timeout) {
                        failCheckpointReadLock();
                    }

                    try {
                        if (timeout > 0) {
                            long timeout1 = timeout - (coarseCurrentTimeMillis() - start);

                            if (!checkpointReadWriteLock.tryReadLock(timeout1, MILLISECONDS)) {
                                failCheckpointReadLock();
                            }
                        } else {
                            checkpointReadWriteLock.readLock();
                        }
                    } catch (InterruptedException e) {
                        interrupted = true;

                        continue;
                    }

                    if (stop) {
                        checkpointReadWriteLock.readUnlock();

                        throw new IgniteInternalException(new NodeStoppingException("Failed to get checkpoint read lock"));
                    }

                    if (checkpointReadWriteLock.getReadHoldCount() > 1 || safeToUpdateAllPageMemories() || checkpointer.runner() == null) {
                        break;
                    } else {
                        // If the checkpoint is triggered outside the lock,
                        // it could cause the checkpoint to fire again for the same reason
                        // (due to a data race between collecting dirty pages and triggering the checkpoint).
                        CheckpointProgress checkpoint = checkpointer.scheduleCheckpoint(0, "too many dirty pages");

                        checkpointReadWriteLock.readUnlock();

                        if (timeout > 0 && coarseCurrentTimeMillis() - start >= timeout) {
                            failCheckpointReadLock();
                        }

                        try {
                            getUninterruptibly(checkpoint.futureFor(LOCK_RELEASED));
                        } catch (ExecutionException e) {
                            throw new IgniteInternalException("Failed to wait for checkpoint begin", e.getCause());
                        } catch (CancellationException e) {
                            throw new IgniteInternalException("Failed to wait for checkpoint begin", e);
                        }
                    }
                } catch (CheckpointReadLockTimeoutException e) {
                    log.error(e.getMessage(), e);

                    throw e;

                    // TODO: IGNITE-16899 Should reset the timeout and try again
                    //timeout = 0;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Tries to get a checkpoint read lock.
     *
     * @return {@code True} if the checkpoint read lock is acquired.
     */
    public boolean tryCheckpointReadLock() {
        return checkpointReadWriteLock.tryReadLock();
    }

    /**
     * Releases the checkpoint read lock.
     */
    public void checkpointReadUnlock() {
        checkpointReadWriteLock.readUnlock();
    }

    /**
     * Returns timeout for checkpoint read lock acquisition in milliseconds.
     */
    public long checkpointReadLockTimeout() {
        return checkpointReadLockTimeout;
    }

    /**
     * Sets timeout for checkpoint read lock acquisition.
     *
     * @param val New timeout in milliseconds, non-positive value denotes infinite timeout.
     */
    public void checkpointReadLockTimeout(long val) {
        checkpointReadLockTimeout = val;
    }

    /**
     * Returns {@code true} if checkpoint lock is held by current thread.
     */
    public boolean checkpointLockIsHeldByThread() {
        return checkpointReadWriteLock.checkpointLockIsHeldByThread();
    }

    private boolean safeToUpdateAllPageMemories() {
        for (PageMemoryDataRegion memPlc : dataRegionsSupplier.get()) {
            if (!memPlc.persistent()) {
                continue;
            }

            PageMemoryImpl pageMemEx = (PageMemoryImpl) memPlc.pageMemory();

            if (!pageMemEx.safeToUpdate()) {
                return false;
            }
        }

        return true;
    }

    private void failCheckpointReadLock() throws CheckpointReadLockTimeoutException {
        // TODO: IGNITE-16899 I think they should fail the node or something like in 2.0

        throw new CheckpointReadLockTimeoutException("Checkpoint read lock acquisition has been timed out");
    }
}
