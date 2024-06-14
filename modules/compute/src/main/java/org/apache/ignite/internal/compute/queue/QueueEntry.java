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

package org.apache.ignite.internal.compute.queue;

import static org.apache.ignite.internal.util.CompletableFutures.copyStateTo;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.jetbrains.annotations.Nullable;

/**
 * Class for queue's entries.
 * This class implement comparable mechanism for {@link BoundedPriorityBlockingQueue}
 * which used as queue in {@link java.util.concurrent.ThreadPoolExecutor}.
 * Each entry has unique seqNum which is used for comparing entries with identical priority.
 * It means that entries with same priority has FIFO resolving strategy in queue.
 *
 * @param <R> Compute job return type.
 */
class QueueEntry<R> implements Runnable, Comparable<QueueEntry<R>> {
    private static final AtomicLong seq = new AtomicLong(Long.MIN_VALUE);

    private final CompletableFuture<R> future = new CompletableFuture<>();

    private final Callable<CompletableFuture<R>> jobAction;

    private final int priority;

    private final long seqNum;

    /** Thread used to run the job, initialized once the job starts executing. */
    private @Nullable Thread workerThread;

    private final Lock lock = new ReentrantLock();

    private volatile boolean isInterrupted;

    /**
     * Constructor.
     *
     * @param jobAction Compute job callable.
     * @param priority Job priority.
     */
    QueueEntry(Callable<CompletableFuture<R>> jobAction, int priority) {
        this.jobAction = jobAction;
        this.priority = priority;
        seqNum = seq.getAndIncrement();
    }

    @Override
    public void run() {
        lock.lock();
        try {
            workerThread = Thread.currentThread();
        } finally {
            lock.unlock();
        }

        try {
            CompletableFuture<R> jobFut = jobAction.call();

            if (jobFut == null) {
                // Allow null futures for synchronous jobs.
                future.complete(null);
            } else {
                jobFut.whenComplete(copyStateTo(future));
            }
        } catch (Throwable e) {
            future.completeExceptionally(e);
        } finally {
            lock.lock();
            try {
                workerThread = null;
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * To {@link CompletableFuture} transformer.
     *
     * @return Completable future that will be finished when Compute job is finished.
     */
    CompletableFuture<R> toFuture() {
        return future;
    }

    /**
     * Sets interrupt status of the worker thread.
     */
    void interrupt() {
        // Interrupt under the lock to prevent interrupting thread used by the pool for another task
        lock.lock();
        try {
            if (workerThread != null) {
                // Set the interrupted flag first since it's used to determine the final status of the job.
                // Job could handle interruption and exit before this flag is set moving the job to completed state rather than canceled.
                isInterrupted = true;
                workerThread.interrupt();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Indicates whether the execution was interrupted externally.
     *
     * @return {@code true} when the execution was interrupted externally.
     */
    boolean isInterrupted() {
        return isInterrupted;
    }

    @Override
    public int compareTo(QueueEntry o) {
        int compare = Integer.compare(o.priority, this.priority);
        if (compare == 0 && this != o) {
            return seqNum < o.seqNum ? -1 : 1;
        }
        return compare;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        QueueEntry<?> that = (QueueEntry<?>) o;

        return seqNum == that.seqNum;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(seqNum);
    }
}
