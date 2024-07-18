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

import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_READ;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_WRITE;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.task.MapReduceTask;
import org.apache.ignite.compute.task.TaskExecutionContext;
import org.apache.ignite.internal.compute.ComputeUtils;
import org.apache.ignite.internal.compute.ExecutionOptions;
import org.apache.ignite.internal.compute.JobExecutionContextImpl;
import org.apache.ignite.internal.compute.configuration.ComputeConfiguration;
import org.apache.ignite.internal.compute.loader.JobClassLoader;
import org.apache.ignite.internal.compute.queue.PriorityQueueExecutor;
import org.apache.ignite.internal.compute.queue.QueueExecution;
import org.apache.ignite.internal.compute.state.ComputeStateMachine;
import org.apache.ignite.internal.compute.task.JobSubmitter;
import org.apache.ignite.internal.compute.task.TaskExecutionContextImpl;
import org.apache.ignite.internal.compute.task.TaskExecutionInternal;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.marshaling.Marshaler;
import org.jetbrains.annotations.Nullable;

/**
 * Base implementation of {@link ComputeExecutor}.
 */
public class ComputeExecutorImpl implements ComputeExecutor {
    private static final IgniteLogger LOG = Loggers.forClass(ComputeExecutorImpl.class);

    private final Ignite ignite;

    private final ComputeConfiguration configuration;

    private final ComputeStateMachine stateMachine;

    private PriorityQueueExecutor executorService;

    /**
     * Constructor.
     *
     * @param ignite Ignite instance for public API access.
     * @param stateMachine Compute jobs state machine.
     * @param configuration Compute configuration.
     */
    public ComputeExecutorImpl(
            Ignite ignite,
            ComputeStateMachine stateMachine,
            ComputeConfiguration configuration
    ) {
        this.ignite = ignite;
        this.configuration = configuration;
        this.stateMachine = stateMachine;
    }

    @Override
    public <T, R> JobExecutionInternal<Object> executeJob(
            ExecutionOptions options,
            Class<? extends ComputeJob<T, R>> jobClass,
            JobClassLoader classLoader,
            T input
    ) {
        assert executorService != null;

        AtomicBoolean isInterrupted = new AtomicBoolean();
        JobExecutionContext context = new JobExecutionContextImpl(ignite, isInterrupted, classLoader);
        ComputeJob<T, R> jobInstance = ComputeUtils.instantiateJob(jobClass);
        Marshaler<T, byte[]> inputMarshaller = jobInstance.inputMarshaler();
        Marshaler<R, byte[]> resultMarshaller = jobInstance.resultMarshaler();

        QueueExecution<Object> execution = executorService.submit(
                () -> {
                    var fut = jobInstance.executeAsync(context, unmarshallOrNotIfNull(inputMarshaller, input));
                    if (fut != null) {
                        return fut.thenApply(res -> marshallOrNull(res, resultMarshaller));
                    }
                    return null;
                },
                options.priority(),
                options.maxRetries()
        );

        return new JobExecutionInternal<>(execution, isInterrupted);
    }


    private static <R> Object marshallOrNull(R res, @Nullable Marshaler<R, byte[]> marshaler) {
        if (marshaler == null) {
            return res;
        }

        return marshaler.marshal(res);
    }

    private static <T> @Nullable T unmarshallOrNotIfNull(@Nullable Marshaler<T, byte[]> marshaler, Object input) {
        if (marshaler == null) {
            return (T) input;
        }

        if (input instanceof byte[]) {
            return marshaler.unmarshal((byte[]) input);
        }

        return (T) input;
    }

    @Override
    public <I, M, T, R> TaskExecutionInternal<I, M, T, R> executeTask(
            JobSubmitter<M, T> jobSubmitter,
            Class<? extends MapReduceTask<I, M, T, R>> taskClass,
            I input
    ) {
        assert executorService != null;

        AtomicBoolean isCancelled = new AtomicBoolean();
        TaskExecutionContext context = new TaskExecutionContextImpl(ignite, isCancelled);

        return new TaskExecutionInternal<>(executorService, jobSubmitter, taskClass, context, isCancelled, input);
    }

    @Override
    public void start() {
        stateMachine.start();
        executorService = new PriorityQueueExecutor(
                configuration,
                IgniteThreadFactory.create(ignite.name(), "compute", LOG, STORAGE_READ, STORAGE_WRITE),
                stateMachine
        );
    }

    @Override
    public void stop() {
        stateMachine.stop();
        executorService.shutdown();
    }
}
