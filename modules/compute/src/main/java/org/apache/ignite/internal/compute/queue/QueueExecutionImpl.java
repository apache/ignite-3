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

import static org.apache.ignite.lang.ErrorGroups.Compute.QUEUE_OVERFLOW_ERR;

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.compute.ComputeException;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.internal.compute.state.ComputeStateMachine;
import org.apache.ignite.internal.compute.state.IllegalJobStateTransition;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.lang.ErrorGroups.Compute;
import org.jetbrains.annotations.Nullable;

/**
 * Queue execution object implementation.
 *
 * @param <R> Job result type.
 */
class QueueExecutionImpl<R> implements QueueExecution<R> {
    private static final IgniteLogger LOG = Loggers.forClass(QueueExecutionImpl.class);

    private final UUID jobId;
    private final Callable<R> job;
    private final AtomicInteger priority;
    private final Lock changePriorityLock = new ReentrantLock();
    private final ThreadPoolExecutor executor;
    private final ComputeStateMachine stateMachine;

    private final CompletableFuture<R> result = new CompletableFuture<>();

    private final AtomicReference<QueueEntry<R>> queueEntry = new AtomicReference<>();

    /**
     * Constructor.
     *
     * @param jobId Job id.
     * @param job Execute job callable.
     * @param priority Job priority.
     * @param executor Executor on which the queue entry is running.
     * @param stateMachine State machine.
     */
    QueueExecutionImpl(
            UUID jobId,
            Callable<R> job,
            int priority,
            ThreadPoolExecutor executor,
            ComputeStateMachine stateMachine
    ) {
        this.jobId = jobId;
        this.job = job;
        this.priority = new AtomicInteger(priority);
        this.executor = executor;
        this.stateMachine = stateMachine;
    }

    @Override
    public CompletableFuture<R> resultAsync() {
        return result;
    }

    @Override
    public @Nullable JobStatus status() {
        return stateMachine.currentStatus(jobId);
    }

    @Override
    public void cancel() {
        changePriorityLock.lock();
        try {
            stateMachine.cancelingJob(jobId);

            QueueEntry<R> queueEntry = this.queueEntry.get();
            if (queueEntry != null) {
                executor.remove(queueEntry);
                queueEntry.interrupt();
            }
        } catch (IllegalJobStateTransition e) {
            LOG.info("Cannot cancel the job", e);
            throw new CancellingException(jobId);
        } finally {
            changePriorityLock.unlock();
        }
    }

    @Override
    public void changePriority(int newPriority) {
        if (newPriority == priority.get()) {
            return;
        }
        if (changePriorityLock.tryLock()
                && stateMachine.currentStatus(jobId) != null
                && stateMachine.currentStatus(jobId).state() == JobState.QUEUED) {
            try {
                QueueEntry<R> queueEntry = this.queueEntry.get();
                // remove job from queue
                if (queueEntry != null) {
                    executor.remove(queueEntry);
                    queueEntry.interrupt();
                }
                // new priority will be used on queue entry run.
                this.priority.set(newPriority);
                this.queueEntry.get().setPriority(newPriority);
                try {
                    executor.execute(this.queueEntry.get());
                } catch (QueueOverflowException e) {
                    result.completeExceptionally(new ComputeException(QUEUE_OVERFLOW_ERR, e));
                }
            } finally {
                changePriorityLock.unlock();
            }
        } else {
            throw new ComputeException(Compute.CHANGE_JOB_PRIORITY_JOB_EXECUTING_ERR, "Can not change job priority,"
                    + " job already processing. job id " + jobId);
        }
    }

    /**
     * Runs the job, completing the result future and retrying the execution in case of failure at most {@code numRetries} times.
     *
     * @param numRetries Number of times to retry failed execution.
     */
    void run(int numRetries) {
        QueueEntry<R> queueEntry;
        changePriorityLock.lock();
        try {
            queueEntry = new QueueEntry<>(() -> {
                stateMachine.executeJob(jobId);
                return job.call();
            }, priority.get());

            // Ignoring previous value since it can't be running because we are calling run
            // either after the construction or after the failure.
            this.queueEntry.set(queueEntry);

            try {
                executor.execute(queueEntry);
            } catch (QueueOverflowException e) {
                result.completeExceptionally(new ComputeException(QUEUE_OVERFLOW_ERR, e));
                return;
            }
        } finally {
            changePriorityLock.unlock();
        }

        queueEntry.toFuture().whenComplete((r, throwable) -> {
            if (throwable != null) {
                if (numRetries > 0) {
                    stateMachine.queueJob(jobId);
                    run(numRetries - 1);
                } else {
                    if (queueEntry.isInterrupted()) {
                        stateMachine.cancelJob(jobId);
                    } else {
                        stateMachine.failJob(jobId);
                    }
                    result.completeExceptionally(throwable);
                }
            } else {
                if (queueEntry.isInterrupted()) {
                    stateMachine.cancelJob(jobId);
                } else {
                    stateMachine.completeJob(jobId);
                }
                result.complete(r);
            }
        });
    }

}
