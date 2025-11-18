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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.failure.FailureType.SYSTEM_CRITICAL_OPERATION_TIMEOUT;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.LOCK_RELEASED;
import static org.apache.ignite.internal.util.FastTimestamps.coarseCurrentTimeMillis;
import static org.apache.ignite.internal.util.IgniteUtils.getUninterruptibly;
import static org.apache.ignite.lang.ErrorGroups.CriticalWorkers.SYSTEM_CRITICAL_OPERATION_TIMEOUT_ERR;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.pagememory.persistence.CheckpointUrgency;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.jetbrains.annotations.Nullable;

/**
 * Checkpoint lock for outer usage which should be used to protect data during writing to memory. It contains complex logic for the correct
 * taking of inside checkpoint lock(timeout, force checkpoint, etc.).
 */
public class CheckpointTimeoutLock {
    /** Logger. */
    protected static final IgniteLogger LOG = Loggers.forClass(CheckpointTimeoutLock.class);

    /**
     * {@link PersistentPageMemory#checkpointUrgency()}  Checkpoint urgency check} for all page memories.
     */
    private final Supplier<CheckpointUrgency> urgencySupplier;

    /** Internal checkpoint lock. */
    private final CheckpointReadWriteLock checkpointReadWriteLock;

    /** Service for triggering the checkpoint. */
    private final Checkpointer checkpointer;

    /** Timeout for checkpoint read lock acquisition in milliseconds. */
    private volatile long checkpointReadLockTimeout;

    /** Stop flag. */
    private boolean stop;

    /** Failure processor. */
    private final FailureManager failureManager;

    /** Checkpoint read lock metrics, nullable if metrics not enabled. */
    private final @Nullable CheckpointReadLockMetrics metrics;

    /** ThreadLocal to track lock acquisition timestamp for hold time calculation. */
    private static final ThreadLocal<Long> lockAcquisitionTime = new ThreadLocal<>();

    /** Counter for threads currently waiting for checkpoint read lock. */
    private final AtomicInteger waitingThreads = new AtomicInteger(0);

    /**
     * Constructor.
     *
     * @param checkpointReadWriteLock Checkpoint read-write lock.
     * @param checkpointReadLockTimeout Timeout for checkpoint read lock acquisition in milliseconds.
     * @param urgencySupplier {@link PersistentPageMemory#checkpointUrgency()}  Checkpoint urgency check} for all page memories.
     * @param checkpointer Service for triggering the checkpoint.
     * @param failureManager Failure manager.
     */
    public CheckpointTimeoutLock(
            CheckpointReadWriteLock checkpointReadWriteLock,
            long checkpointReadLockTimeout,
            Supplier<CheckpointUrgency> urgencySupplier,
            Checkpointer checkpointer,
            FailureManager failureManager
    ) {
        this(checkpointReadWriteLock, checkpointReadLockTimeout, urgencySupplier, checkpointer, failureManager, null);
    }

    /**
     * Constructor with metrics.
     *
     * @param checkpointReadWriteLock Checkpoint read-write lock.
     * @param checkpointReadLockTimeout Timeout for checkpoint read lock acquisition in milliseconds.
     * @param urgencySupplier {@link PersistentPageMemory#checkpointUrgency()}  Checkpoint urgency check} for all page memories.
     * @param checkpointer Service for triggering the checkpoint.
     * @param failureManager Failure manager.
     * @param metrics Checkpoint read lock metrics, or null to disable metrics.
     */
    public CheckpointTimeoutLock(
            CheckpointReadWriteLock checkpointReadWriteLock,
            long checkpointReadLockTimeout,
            Supplier<CheckpointUrgency> urgencySupplier,
            Checkpointer checkpointer,
            FailureManager failureManager,
            @Nullable CheckpointReadLockMetrics metrics
    ) {
        this.checkpointReadWriteLock = checkpointReadWriteLock;
        this.checkpointReadLockTimeout = checkpointReadLockTimeout;
        this.urgencySupplier = urgencySupplier;
        this.checkpointer = checkpointer;
        this.failureManager = failureManager;
        this.metrics = metrics;
    }

    /**
     * Starts a checkpoint lock.
     */
    public void start() {
        stop = false;
    }

    /**
     * Stops a checkpoint lock.
     */
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

        // Track metrics: start time for acquisition duration
        long acquisitionStartNanos = metrics != null ? System.nanoTime() : 0;
        boolean lockAcquired = false;
        boolean hadContention = false;

        long timeout = checkpointReadLockTimeout;

        long start = coarseCurrentTimeMillis();

        boolean interrupted = false;

        // Track metrics: increment waiting threads counter
        if (metrics != null) {
            waitingThreads.incrementAndGet();
        }

        try {
            // Track metrics: increment acquisitions counter
            if (metrics != null) {
                metrics.acquisitions().increment();
            }

            for (; ; ) {
                try {
                    if (timeout > 0 && (coarseCurrentTimeMillis() - start) >= timeout) {
                        failCheckpointReadLock();
                    }

                    try {
                        if (timeout > 0) {
                            if (!checkpointReadWriteLock.tryReadLock(timeout - (coarseCurrentTimeMillis() - start), MILLISECONDS)) {
                                // Track metrics: lock not immediately available, mark as contention
                                if (metrics != null && !hadContention) {
                                    metrics.contentionCount().increment();
                                    hadContention = true;
                                }
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

                    CheckpointUrgency urgency;

                    if (checkpointReadWriteLock.getReadHoldCount() > 1
                            || checkpointer.runner() == null
                            || (urgency = urgencySupplier.get()) == CheckpointUrgency.NOT_REQUIRED
                    ) {
                        // Track metrics: lock successfully acquired, record acquisition time and store timestamp for hold time
                        if (metrics != null && !lockAcquired) {
                            long acquisitionDurationNanos = System.nanoTime() - acquisitionStartNanos;
                            metrics.acquisitionTime().add(acquisitionDurationNanos);
                            lockAcquisitionTime.set(System.nanoTime());
                            lockAcquired = true;
                        }
                        return;
                    } else {
                        // If the checkpoint is triggered outside the lock,
                        // it could cause the checkpoint to fire again for the same reason
                        // (due to a data race between collecting dirty pages and triggering the checkpoint).
                        CheckpointProgress checkpoint = checkpointer.scheduleCheckpoint(0, "too many dirty pages");

                        if (urgency != CheckpointUrgency.MUST_TRIGGER) {
                            // Allow to take the checkpoint read lock, if urgency is not "must trigger". We optimistically assume that
                            // triggered checkpoint will start soon, without us having to explicitly wait for it and without page memory
                            // overflow.

                            // Track metrics: lock successfully acquired
                            if (metrics != null && !lockAcquired) {
                                long acquisitionDurationNanos = System.nanoTime() - acquisitionStartNanos;
                                metrics.acquisitionTime().add(acquisitionDurationNanos);
                                lockAcquisitionTime.set(System.nanoTime());
                                lockAcquired = true;
                            }
                            return;
                        }

                        checkpointReadWriteLock.readUnlock();

                        // Track metrics: had to release and wait, mark as contention
                        if (metrics != null && !hadContention) {
                            metrics.contentionCount().increment();
                            hadContention = true;
                        }

                        long elapsed = coarseCurrentTimeMillis() - start;
                        if (timeout > 0 && elapsed >= timeout) {
                            failCheckpointReadLock();
                        }

                        try {
                            if (timeout > 0) {
                                getUninterruptibly(checkpoint.futureFor(LOCK_RELEASED), timeout - elapsed);
                            } else {
                                getUninterruptibly(checkpoint.futureFor(LOCK_RELEASED));
                            }
                        } catch (ExecutionException e) {
                            throw new IgniteInternalException("Failed to wait for checkpoint begin", e.getCause());
                        } catch (CancellationException e) {
                            throw new IgniteInternalException("Failed to wait for checkpoint begin", e);
                        } catch (TimeoutException e) {
                            failCheckpointReadLock();
                        }
                    }
                } catch (CheckpointReadLockTimeoutException e) {
                    LOG.debug(e.getMessage(), e);

                    timeout = 0;
                }
            }
        } finally {
            // Track metrics: decrement waiting threads counter
            if (metrics != null) {
                waitingThreads.decrementAndGet();
            }

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
        // Track metrics: calculate hold time if we have the acquisition timestamp
        if (metrics != null) {
            Long acquisitionTimeNanos = lockAcquisitionTime.get();
            if (acquisitionTimeNanos != null) {
                long holdDurationNanos = System.nanoTime() - acquisitionTimeNanos;
                metrics.holdTime().add(holdDurationNanos);
                lockAcquisitionTime.remove(); // Clean up ThreadLocal
            }
        }

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

    /**
     * Returns {@code true} if there are threads waiting to acquire the checkpoint write lock.
     * This can be used by user code to determine if it should stop execution preemptively
     * to allow checkpointing to proceed.
     *
     * @return {@code true} if checkpoint is waiting for the write lock, {@code false} otherwise.
     */
    public boolean shouldReleaseReadLock() {
        return checkpointReadWriteLock.hasQueuedWriters();
    }

    /**
     * Returns the current number of threads waiting for checkpoint read lock.
     * Used for metrics tracking.
     *
     * @return Number of waiting threads.
     */
    public int waitingThreadsCount() {
        return waitingThreads.get();
    }

    private void failCheckpointReadLock() throws CheckpointReadLockTimeoutException {
        String msg = "Checkpoint read lock acquisition has been timed out.";

        IgniteInternalException e = new IgniteInternalException(SYSTEM_CRITICAL_OPERATION_TIMEOUT_ERR, msg);

        // either fail the node or try acquire read lock again by throwing an CheckpointReadLockTimeoutException
        if (failureManager.process(new FailureContext(SYSTEM_CRITICAL_OPERATION_TIMEOUT, e))) {
            throw e;
        }

        throw new CheckpointReadLockTimeoutException(msg);
    }
}
