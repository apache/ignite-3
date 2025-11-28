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

package org.apache.ignite.internal.compute;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.compute.ClassLoaderExceptionsMapper.mapClassLoaderExceptions;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.compute.configuration.ComputeConfiguration;
import org.apache.ignite.internal.compute.events.ComputeEventMetadataBuilder;
import org.apache.ignite.internal.compute.executor.ComputeExecutor;
import org.apache.ignite.internal.compute.executor.JobExecutionInternal;
import org.apache.ignite.internal.compute.messaging.ComputeMessaging;
import org.apache.ignite.internal.compute.messaging.RemoteJobExecution;
import org.apache.ignite.internal.compute.task.DelegatingTaskExecution;
import org.apache.ignite.internal.compute.task.JobSubmitter;
import org.apache.ignite.internal.compute.task.TaskExecutionInternal;
import org.apache.ignite.internal.deployunit.loader.UnitsContext;
import org.apache.ignite.internal.deployunit.loader.UnitsContextManager;
import org.apache.ignite.internal.eventlog.api.EventLog;
import org.apache.ignite.internal.future.InFlightFutures;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.systemview.api.SystemView;
import org.apache.ignite.internal.systemview.api.SystemViewProvider;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.CancelHandleHelper;
import org.apache.ignite.lang.CancellationToken;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Implementation of {@link ComputeComponent}.
 */
public class ComputeComponentImpl implements ComputeComponent, SystemViewProvider {
    private static final IgniteLogger LOG = Loggers.forClass(ComputeComponentImpl.class);

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping of the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    private final InFlightFutures inFlightFutures = new InFlightFutures();

    private final TopologyService topologyService;

    private final LogicalTopologyService logicalTopologyService;

    private final UnitsContextManager jobContextManager;

    private final ComputeExecutor executor;

    private final EventLog eventLog;

    private final ComputeMessaging messaging;

    private final ExecutionManager executionManager;

    private final ExecutorService failoverExecutor;

    private final ComputeViewProvider computeViewProvider = new ComputeViewProvider();

    /**
     * Creates a new instance.
     */
    public ComputeComponentImpl(
            String nodeName,
            MessagingService messagingService,
            TopologyService topologyService,
            LogicalTopologyService logicalTopologyService,
            UnitsContextManager jobContextManager,
            ComputeExecutor executor,
            ComputeConfiguration computeConfiguration,
            EventLog eventLog
    ) {
        this.topologyService = topologyService;
        this.logicalTopologyService = logicalTopologyService;
        this.jobContextManager = jobContextManager;
        this.executor = executor;
        this.eventLog = eventLog;
        executionManager = new ExecutionManager(computeConfiguration, topologyService);
        messaging = new ComputeMessaging(executionManager, messagingService, topologyService);
        failoverExecutor = Executors.newSingleThreadExecutor(
                IgniteThreadFactory.create(nodeName, "compute-job-failover", LOG)
        );
    }

    @Override
    public CompletableFuture<CancellableJobExecution<ComputeJobDataHolder>> executeLocally(
            ExecutionContext executionContext,
            @Nullable CancellationToken cancellationToken
    ) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException()));
        }

        try {
            CompletableFuture<UnitsContext> classLoaderFut = jobContextManager.acquireClassLoader(executionContext.units());

            CompletableFuture<CancellableJobExecution<ComputeJobDataHolder>> future =
                    mapClassLoaderExceptions(classLoaderFut, executionContext.jobClassName())
                            .thenApply(context -> {
                                JobExecutionInternal<ComputeJobDataHolder> execution = execJob(context, executionContext);
                                execution.resultAsync().whenComplete((result, e) -> context.close());
                                inFlightFutures.registerFuture(execution.resultAsync());

                                if (cancellationToken != null) {
                                    CancelHandleHelper.addCancelAction(cancellationToken, execution::cancel, execution.resultAsync());
                                }

                                DelegatingJobExecution delegatingExecution = new DelegatingJobExecution(execution);

                                //noinspection DataFlowIssue Not null since the job was just submitted
                                executionManager.addLocalExecution(execution.state().id(), delegatingExecution);

                                return delegatingExecution;
                            });

            inFlightFutures.registerFuture(future);
            inFlightFutures.registerFuture(classLoaderFut);

            if (cancellationToken != null) {
                CancelHandleHelper.addCancelAction(cancellationToken, classLoaderFut);
                CancelHandleHelper.addCancelAction(cancellationToken, future);
            }

            return future;
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public <I, M, T, R> CancellableTaskExecution<R> executeTask(
            JobSubmitter<M, T> jobSubmitter,
            List<DeploymentUnit> units,
            String taskClassName,
            ComputeEventMetadataBuilder metadataBuilder,
            @Nullable I arg
    ) {
        if (!busyLock.enterBusy()) {
            return new DelegatingTaskExecution<>(
                    failedFuture(new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException()))
            );
        }

        try {
            CompletableFuture<TaskExecutionInternal<I, M, T, R>> taskFuture =
                    mapClassLoaderExceptions(jobContextManager.acquireClassLoader(units), taskClassName)
                            .thenApply(context -> {
                                TaskExecutionInternal<I, M, T, R> execution = execTask(
                                        context,
                                        jobSubmitter,
                                        taskClassName,
                                        metadataBuilder,
                                        arg
                                );
                                execution.resultAsync().whenComplete((r, e) -> context.close());
                                inFlightFutures.registerFuture(execution.resultAsync());
                                return execution;
                            });

            inFlightFutures.registerFuture(taskFuture);

            DelegatingTaskExecution<I, M, T, R> result = new DelegatingTaskExecution<>(taskFuture);

            result.idAsync().thenAccept(jobId -> executionManager.addLocalExecution(
                    jobId,
                    new TaskToJobExecutionWrapper<>(result, topologyService.localMember())
            ));
            return result;
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public CompletableFuture<CancellableJobExecution<ComputeJobDataHolder>> executeRemotely(
            InternalClusterNode remoteNode,
            ExecutionContext executionContext,
            @Nullable CancellationToken cancellationToken
    ) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException()));
        }

        try {
            CompletableFuture<UUID> jobIdFuture = messaging.remoteExecuteRequestAsync(remoteNode, executionContext);

            inFlightFutures.registerFuture(jobIdFuture);

            return jobIdFuture.thenApply(jobId -> {
                RemoteJobExecution execution = new RemoteJobExecution(
                        remoteNode, jobId, inFlightFutures, messaging
                );

                if (cancellationToken != null) {
                    CancelHandleHelper.addCancelAction(cancellationToken, execution::cancelAsync, execution.resultAsync());
                }

                executionManager.addRemoteExecution(jobId, execution);

                return execution;
            });
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public CompletableFuture<JobExecution<ComputeJobDataHolder>> executeRemotelyWithFailover(
            InternalClusterNode remoteNode,
            NextWorkerSelector nextWorkerSelector,
            ExecutionContext executionContext,
            @Nullable CancellationToken cancellationToken
    ) {
        return ComputeJobFailover.failSafeExecute(
                        this, logicalTopologyService, topologyService, failoverExecutor, eventLog,
                        remoteNode, nextWorkerSelector, executionContext
                )
                .thenApply(execution -> {
                    // Do not add cancel action to the underlying jobs, let the FailSafeJobExecution handle it.
                    if (cancellationToken != null) {
                        CancelHandleHelper.addCancelAction(cancellationToken, execution::cancelAsync, execution.resultAsync());
                    }

                    execution.idAsync().thenAccept(jobId -> executionManager.addLocalExecution(jobId, execution));

                    return execution;
                });
    }

    @Override
    public CompletableFuture<Collection<JobState>> statesAsync() {
        return messaging.broadcastStatesAsync();
    }

    @Override
    public CompletableFuture<@Nullable JobState> stateAsync(UUID jobId) {
        return executionManager.stateAsync(jobId).thenCompose(state -> {
            if (state != null) {
                return completedFuture(state);
            }
            return messaging.broadcastStateAsync(jobId);
        });
    }

    @Override
    public CompletableFuture<@Nullable Boolean> cancelAsync(UUID jobId) {
        return executionManager.cancelAsync(jobId).thenCompose(result -> {
            if (result != null) {
                return completedFuture(result);
            }
            return messaging.broadcastCancelAsync(jobId);
        });
    }

    @Override
    public CompletableFuture<@Nullable Boolean> changePriorityAsync(UUID jobId, int newPriority) {
        return executionManager.changePriorityAsync(jobId, newPriority).thenCompose(result -> {
            if (result != null) {
                return completedFuture(result);
            }
            return messaging.broadcastChangePriorityAsync(jobId, newPriority);
        });
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        executor.start();
        executionManager.start();
        computeViewProvider.init(executionManager);

        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        if (!stopGuard.compareAndSet(false, true)) {
            return nullCompletedFuture();
        }

        busyLock.block();
        inFlightFutures.cancelInFlightFutures();

        executionManager.stop();
        messaging.stop();
        executor.stop();
        computeViewProvider.stop();
        IgniteUtils.shutdownAndAwaitTermination(failoverExecutor, 10, TimeUnit.SECONDS);

        return nullCompletedFuture();
    }

    /**
     * Enables compute messages handling.
     */
    public void enable() {
        messaging.start(executionContext -> executeLocally(executionContext, null));
    }

    private JobExecutionInternal<ComputeJobDataHolder> execJob(UnitsContext context, ExecutionContext executionContext) {
        try {
            return executor.executeJob(
                    executionContext.options(),
                    executionContext.jobClassName(),
                    context.classLoader(),
                    executionContext.metadataBuilder(),
                    executionContext.arg()
            );
        } catch (Throwable e) {
            context.close();
            throw e;
        }
    }

    private <I, M, T, R> TaskExecutionInternal<I, M, T, R> execTask(
            UnitsContext context,
            JobSubmitter<M, T> jobSubmitter,
            String taskClassName,
            ComputeEventMetadataBuilder metadataBuilder,
            @Nullable I arg
    ) {
        try {
            return executor.executeTask(jobSubmitter, taskClassName, context.classLoader(), metadataBuilder, arg);
        } catch (Throwable e) {
            context.close();
            throw e;
        }
    }

    @TestOnly
    public ExecutionManager executionManager() {
        return executionManager;
    }

    @Override
    public List<SystemView<?>> systemViews() {
        return List.of(computeViewProvider.get());
    }
}
