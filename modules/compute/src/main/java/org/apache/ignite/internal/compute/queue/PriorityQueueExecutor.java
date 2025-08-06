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

import static org.apache.ignite.internal.compute.events.ComputeEventsFactory.logJobQueuedEvent;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.compute.configuration.ComputeConfiguration;
import org.apache.ignite.internal.compute.events.ComputeEventMetadata;
import org.apache.ignite.internal.compute.state.ComputeStateMachine;
import org.apache.ignite.internal.eventlog.api.EventLog;

/**
 * Compute job executor with priority mechanism.
 */
public class PriorityQueueExecutor {
    private final ComputeThreadPoolExecutor executor;

    private final ComputeStateMachine stateMachine;

    private final EventLog eventLog;

    private final String nodeName;

    /**
     * Constructor.
     *
     * @param configuration Compute configuration.
     * @param threadFactory Thread factory.
     * @param eventLog Event log.
     * @param nodeName Local node name.
     */
    public PriorityQueueExecutor(
            ComputeConfiguration configuration,
            ThreadFactory threadFactory,
            ComputeStateMachine stateMachine,
            EventLog eventLog,
            String nodeName
    ) {
        this.stateMachine = stateMachine;
        this.eventLog = eventLog;
        this.nodeName = nodeName;

        BlockingQueue<Runnable> workQueue = new BoundedPriorityBlockingQueue<>(() -> configuration.queueMaxSize().value());
        executor = new ComputeThreadPoolExecutor(
                configuration.threadPoolSize().value(),
                configuration.threadPoolSize().value(),
                0L,
                TimeUnit.SECONDS,
                workQueue,
                threadFactory
        );
    }

    /**
     * Submit job for execution. Job can be started immediately if queue is empty or will be added to queue with provided priority.
     *
     * @param <R> Job result type.
     * @param job Execute job callable.
     * @param priority Job priority.
     * @param maxRetries Number of retries of the execution after failure, {@code 0} means the execution will not be retried.
     * @return Completable future which will be finished when compute job finished.
     */
    public <R> QueueExecution<R> submit(Callable<CompletableFuture<R>> job, int priority, int maxRetries) {
        return submit(job, priority, maxRetries, ComputeEventMetadata.builder());
    }

    /**
     * Submit job for execution. Job can be started immediately if queue is empty or will be added to queue with provided priority.
     *
     * @param <R> Job result type.
     * @param job Execute job callable.
     * @param priority Job priority.
     * @param maxRetries Number of retries of the execution after failure, {@code 0} means the execution will not be retried.
     * @param metadataBuilder Event metadata builder.
     * @return Completable future which will be finished when compute job finished.
     */
    public <R> QueueExecution<R> submit(
            Callable<CompletableFuture<R>> job,
            int priority,
            int maxRetries,
            ComputeEventMetadata.Builder metadataBuilder
    ) {
        Objects.requireNonNull(job);

        UUID jobId = stateMachine.initJob();

        ComputeEventMetadata eventMetadata = metadataBuilder
                .jobId(jobId)
                .nodeName(nodeName)
                .build();
        logJobQueuedEvent(eventLog, eventMetadata);

        QueueExecutionImpl<R> execution = new QueueExecutionImpl<>(jobId, job, priority, executor, stateMachine, eventLog, eventMetadata);
        execution.run(maxRetries);
        return execution;
    }

    /**
     * Submit job for execution. Job can be started immediately if queue is empty or will be added to queue with default priority and no
     * retries.
     *
     * @param job Execute job callable.
     * @param <R> Job result type.
     * @return Completable future which will be finished when compute job finished.
     */
    public <R> QueueExecution<R> submit(Callable<CompletableFuture<R>> job) {
        return submit(job, 0, 0);
    }

    /**
     * Shutdown executor. After shutdown executor is not usable anymore.
     */
    public void shutdown() {
        executor.shutdown();
    }
}
