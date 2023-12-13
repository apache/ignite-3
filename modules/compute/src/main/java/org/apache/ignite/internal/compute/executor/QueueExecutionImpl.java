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

package org.apache.ignite.internal.compute.executor;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.internal.compute.queue.QueueEntry;
import org.apache.ignite.internal.compute.queue.QueueExecution;
import org.apache.ignite.internal.compute.state.ComputeStateMachine;
import org.apache.ignite.internal.compute.state.IllegalJobStateTransition;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;

/**
 * Queue execution object implementation.
 *
 * @param <R> Job result type.
 */
public class QueueExecutionImpl<R> implements QueueExecution<R> {
    private static final IgniteLogger LOG = Loggers.forClass(QueueExecutionImpl.class);

    private final UUID jobId;
    private final CompletableFuture<R> future;
    private final QueueEntry<R> queueEntry;
    private final ThreadPoolExecutor executor;
    private final ComputeStateMachine stateMachine;

    /**
     * Constructor.
     *
     * @param jobId Job id.
     * @param queueEntry Queue entry.
     * @param future Result future.
     * @param executor Executor on which the queue entry is running.
     * @param stateMachine State machine.
     */
    public QueueExecutionImpl(
            UUID jobId,
            QueueEntry<R> queueEntry,
            CompletableFuture<R> future,
            ThreadPoolExecutor executor,
            ComputeStateMachine stateMachine
    ) {
        this.jobId = jobId;
        this.future = future;
        this.queueEntry = queueEntry;
        this.executor = executor;
        this.stateMachine = stateMachine;
    }

    @Override
    public CompletableFuture<R> resultAsync() {
        return future;
    }

    @Override
    public JobStatus status() {
        return stateMachine.currentStatus(jobId);
    }

    @Override
    public void cancel() {
        try {
            stateMachine.cancelingJob(jobId);
            executor.remove(queueEntry);
            queueEntry.interrupt();
        } catch (IllegalJobStateTransition e) {
            LOG.info("Cannot cancel the job", e);
        }
    }
}
