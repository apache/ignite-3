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

import static org.apache.ignite.internal.compute.ComputeUtils.getJobExecuteArgumentType;
import static org.apache.ignite.internal.compute.ComputeUtils.jobClass;
import static org.apache.ignite.internal.compute.ComputeUtils.unmarshalOrNotIfNull;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_READ;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_WRITE;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.JobExecutorType;
import org.apache.ignite.compute.task.MapReduceTask;
import org.apache.ignite.compute.task.TaskExecutionContext;
import org.apache.ignite.internal.compute.ComputeJobDataHolder;
import org.apache.ignite.internal.compute.ComputeJobDataType;
import org.apache.ignite.internal.compute.ComputeUtils;
import org.apache.ignite.internal.compute.ExecutionOptions;
import org.apache.ignite.internal.compute.JobExecutionContextImpl;
import org.apache.ignite.internal.compute.SharedComputeUtils;
import org.apache.ignite.internal.compute.configuration.ComputeConfiguration;
import org.apache.ignite.internal.compute.events.ComputeEventMetadata;
import org.apache.ignite.internal.compute.executor.platform.PlatformComputeTransport;
import org.apache.ignite.internal.compute.executor.platform.dotnet.DotNetComputeExecutor;
import org.apache.ignite.internal.compute.loader.JobClassLoader;
import org.apache.ignite.internal.compute.queue.PriorityQueueExecutor;
import org.apache.ignite.internal.compute.queue.QueueExecution;
import org.apache.ignite.internal.compute.state.ComputeStateMachine;
import org.apache.ignite.internal.compute.task.JobSubmitter;
import org.apache.ignite.internal.compute.task.TaskExecutionContextImpl;
import org.apache.ignite.internal.compute.task.TaskExecutionInternal;
import org.apache.ignite.internal.deployunit.DisposableDeploymentUnit;
import org.apache.ignite.internal.eventlog.api.EventLog;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.marshalling.Marshaller;
import org.jetbrains.annotations.Nullable;

/**
 * Base implementation of {@link ComputeExecutor}.
 */
public class ComputeExecutorImpl implements ComputeExecutor {
    private static final IgniteLogger LOG = Loggers.forClass(ComputeExecutorImpl.class);

    private final Ignite ignite;

    private final ComputeConfiguration configuration;

    private final ComputeStateMachine stateMachine;

    private final TopologyService topologyService;

    private final ClockService clockService;

    private final EventLog eventLog;

    private PriorityQueueExecutor executorService;

    private @Nullable DotNetComputeExecutor dotNetComputeExecutor;

    /**
     * Constructor.
     *
     * @param ignite Ignite instance for public API access.
     * @param stateMachine Compute jobs state machine.
     * @param configuration Compute configuration.
     * @param topologyService Topology service.
     * @param eventLog Event log.
     */
    public ComputeExecutorImpl(
            Ignite ignite,
            ComputeStateMachine stateMachine,
            ComputeConfiguration configuration,
            TopologyService topologyService,
            ClockService clockService,
            EventLog eventLog
    ) {
        this.ignite = ignite;
        this.configuration = configuration;
        this.stateMachine = stateMachine;
        this.topologyService = topologyService;
        this.clockService = clockService;
        this.eventLog = eventLog;
    }

    public void setPlatformComputeTransport(PlatformComputeTransport transport) {
        this.dotNetComputeExecutor = new DotNetComputeExecutor(transport);
    }

    @Override
    public JobExecutionInternal<ComputeJobDataHolder> executeJob(
            ExecutionOptions options,
            String jobClassName,
            JobClassLoader classLoader,
            ComputeEventMetadata.Builder metadataBuilder,
            ComputeJobDataHolder input
    ) {
        assert executorService != null;

        AtomicBoolean isInterrupted = new AtomicBoolean();
        JobExecutionContext context = new JobExecutionContextImpl(ignite, isInterrupted, classLoader, options.partition());

        metadataBuilder.jobClassName(jobClassName);

        Callable<CompletableFuture<ComputeJobDataHolder>> jobCallable = getJobCallable(
                options.executorType(), jobClassName, classLoader, input, context);

        jobCallable = addObservableTimestamp(jobCallable, clockService);

        QueueExecution<ComputeJobDataHolder> execution = executorService.submit(
                jobCallable,
                options.priority(),
                options.maxRetries(),
                metadataBuilder
        );

        return new JobExecutionInternal<>(execution, isInterrupted, null, false, topologyService.localMember());
    }

    private static Callable<CompletableFuture<ComputeJobDataHolder>> addObservableTimestamp(
            Callable<CompletableFuture<ComputeJobDataHolder>> jobCallable,
            ClockService clockService) {
        return () -> {
            CompletableFuture<ComputeJobDataHolder> jobFut = jobCallable.call();

            if (jobFut == null) {
                return CompletableFuture.completedFuture(
                        new ComputeJobDataHolder(ComputeJobDataType.NATIVE, null, clockService.currentLong()));
            }

            return jobFut.thenApply(holder -> {
                if (holder == null) {
                    return new ComputeJobDataHolder(ComputeJobDataType.NATIVE, null, clockService.currentLong());
                }

                return new ComputeJobDataHolder(holder.type(), holder.data(), clockService.currentLong());
            });
        };
    }

    private Callable<CompletableFuture<ComputeJobDataHolder>> getJobCallable(
            JobExecutorType executorType,
            String jobClassName,
            JobClassLoader classLoader,
            ComputeJobDataHolder input,
            JobExecutionContext context
    ) {
        executorType = executorType == null ? JobExecutorType.JAVA_EMBEDDED : executorType;

        switch (executorType) {
            case JAVA_EMBEDDED:
                return getJavaJobCallable(jobClassName, classLoader, input, context);

            case DOTNET_SIDECAR:
                DotNetComputeExecutor dotNetExec = dotNetComputeExecutor;

                if (dotNetExec == null) {
                    throw new IllegalStateException("DotNetComputeExecutor is not set");
                }

                return dotNetExec.getJobCallable(getDeploymentUnitPaths(classLoader), jobClassName, input, context);

            default:
                throw new IllegalArgumentException("Unsupported executor type: " + executorType);
        }
    }

    private static ArrayList<String> getDeploymentUnitPaths(JobClassLoader classLoader) {
        ArrayList<String> unitPaths = new ArrayList<>();

        for (DisposableDeploymentUnit unit : classLoader.units()) {
            try {
                unitPaths.add(unit.path().toRealPath().toString());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return unitPaths;
    }

    private static Callable<CompletableFuture<ComputeJobDataHolder>> getJavaJobCallable(
            String jobClassName,
            JobClassLoader classLoader,
            ComputeJobDataHolder input,
            JobExecutionContext context
    ) {
        Class<ComputeJob<Object, Object>> jobClass = jobClass(classLoader, jobClassName);
        ComputeJob<Object, Object> jobInstance = ComputeUtils.instantiateJob(jobClass);

        Marshaller<Object, byte[]> inputMarshaller = jobInstance.inputMarshaller();
        Marshaller<Object, byte[]> resultMarshaller = jobInstance.resultMarshaller();

        return unmarshalExecMarshal(input, jobClass, jobInstance, context, inputMarshaller, resultMarshaller);
    }

    private static <T, R> Callable<CompletableFuture<ComputeJobDataHolder>> unmarshalExecMarshal(
            ComputeJobDataHolder input,
            Class<? extends ComputeJob<T, R>> jobClass,
            ComputeJob<T, R> jobInstance,
            JobExecutionContext context,
            @Nullable Marshaller<T, byte[]> inputMarshaller,
            @Nullable Marshaller<R, byte[]> resultMarshaller
    ) {
        return () -> {
            CompletableFuture<R> userJobFut = jobInstance.executeAsync(
                    context, unmarshalOrNotIfNull(inputMarshaller, input, getJobExecuteArgumentType(jobClass)));

            return userJobFut == null
                    ? null
                    : userJobFut.thenApply(r -> SharedComputeUtils.marshalArgOrResult(r, resultMarshaller));
        };
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
        IgniteThreadFactory threadFactory = IgniteThreadFactory.create(ignite.name(), "compute", LOG, STORAGE_READ, STORAGE_WRITE);
        executorService = new PriorityQueueExecutor(configuration, threadFactory, stateMachine, eventLog, ignite.name());
    }

    @Override
    public void stop() {
        stateMachine.stop();
        executorService.shutdown();

        DotNetComputeExecutor dotNetExec = dotNetComputeExecutor;
        if (dotNetExec != null) {
            dotNetExec.stop();
        }
    }

    /**
     * Handles the removal of a deployment unit.
     *
     * @param unitPath Path to the deployment unit that is being removed.
     */
    public void onUnitRemoving(Path unitPath) {
        DotNetComputeExecutor dotNetExec = dotNetComputeExecutor;

        if (dotNetExec != null) {
            // Start async undeployment, do not wait. Won't throw.
            dotNetExec.beginUndeployUnit(unitPath);
        }
    }
}
