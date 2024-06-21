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
import static org.apache.ignite.internal.compute.ComputeUtils.jobClass;
import static org.apache.ignite.internal.compute.ComputeUtils.taskClass;
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
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.compute.task.TaskExecution;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.compute.configuration.ComputeConfiguration;
import org.apache.ignite.internal.compute.executor.ComputeExecutor;
import org.apache.ignite.internal.compute.executor.JobExecutionInternal;
import org.apache.ignite.internal.compute.loader.JobContext;
import org.apache.ignite.internal.compute.loader.JobContextManager;
import org.apache.ignite.internal.compute.messaging.ComputeMessaging;
import org.apache.ignite.internal.compute.messaging.RemoteJobExecution;
import org.apache.ignite.internal.compute.task.DelegatingTaskExecution;
import org.apache.ignite.internal.compute.task.JobSubmitter;
import org.apache.ignite.internal.compute.task.TaskExecutionInternal;
import org.apache.ignite.internal.future.InFlightFutures;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Implementation of {@link ComputeComponent}.
 */
public class ComputeComponentImpl implements ComputeComponent {
    private static final IgniteLogger LOG = Loggers.forClass(ComputeComponentImpl.class);

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping of the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    private final InFlightFutures inFlightFutures = new InFlightFutures();

    private final TopologyService topologyService;

    private final LogicalTopologyService logicalTopologyService;

    private final JobContextManager jobContextManager;

    private final ComputeExecutor executor;

    private final ComputeMessaging messaging;

    private final ExecutionManager executionManager;

    private final ExecutorService failoverExecutor;

    /**
     * Creates a new instance.
     */
    public ComputeComponentImpl(
            String nodeName,
            MessagingService messagingService,
            TopologyService topologyService,
            LogicalTopologyService logicalTopologyService,
            JobContextManager jobContextManager,
            ComputeExecutor executor,
            ComputeConfiguration computeConfiguration
    ) {
        this.topologyService = topologyService;
        this.logicalTopologyService = logicalTopologyService;
        this.jobContextManager = jobContextManager;
        this.executor = executor;
        executionManager = new ExecutionManager(computeConfiguration, topologyService);
        messaging = new ComputeMessaging(executionManager, messagingService, topologyService);
        failoverExecutor = Executors.newSingleThreadExecutor(
                NamedThreadFactory.create(nodeName, "compute-job-failover", LOG)
        );
    }

    /** {@inheritDoc} */
    @Override
     public <R> JobExecution<R> executeLocally(
            ExecutionOptions options,
            List<DeploymentUnit> units,
            String jobClassName,
            Object input
    ) {
        if (!busyLock.enterBusy()) {
            return new DelegatingJobExecution<>(
                    failedFuture(new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException()))
            );
        }

        try {
            CompletableFuture<JobExecutionInternal<R>> future =
                    mapClassLoaderExceptions(jobContextManager.acquireClassLoader(units), jobClassName)
                            .thenApply(context -> {
                                JobExecutionInternal<R> execution = execJob(context, options, jobClassName, input);
                                execution.resultAsync().whenComplete((result, e) -> context.close());
                                inFlightFutures.registerFuture(execution.resultAsync());
                                return execution;
                            });

            inFlightFutures.registerFuture(future);

            JobExecution<R> result = new DelegatingJobExecution<>(future);
            result.idAsync().thenAccept(jobId -> executionManager.addExecution(jobId, result));
            return result;
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public <R> TaskExecution<R> executeTask(
            JobSubmitter jobSubmitter,
            List<DeploymentUnit> units,
            String taskClassName,
            Object input
    ) {
        if (!busyLock.enterBusy()) {
            return new DelegatingTaskExecution<>(
                    failedFuture(new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException()))
            );
        }

        try {
            CompletableFuture<TaskExecutionInternal<?, R>> taskFuture =
                    mapClassLoaderExceptions(jobContextManager.acquireClassLoader(units), taskClassName)
                            .thenApply(context -> {
                                TaskExecutionInternal<?, R> execution = execTask(context, jobSubmitter, taskClassName, input);
                                execution.resultAsync().whenComplete((r, e) -> context.close());
                                inFlightFutures.registerFuture(execution.resultAsync());
                                return execution;
                            });

            inFlightFutures.registerFuture(taskFuture);

            DelegatingTaskExecution<R> result = new DelegatingTaskExecution<>(taskFuture);
            result.idAsync().thenAccept(jobId -> executionManager.addExecution(jobId, result));
            return result;
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override
    public <R> JobExecution<R> executeRemotely(
            ExecutionOptions options,
            ClusterNode remoteNode,
            List<DeploymentUnit> units,
            String jobClassName,
            Object input
    ) {
        if (!busyLock.enterBusy()) {
            return new DelegatingJobExecution<>(
                    failedFuture(new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException()))
            );
        }

        try {
            CompletableFuture<UUID> jobIdFuture = messaging.remoteExecuteRequestAsync(options, remoteNode, units, jobClassName, input);
            CompletableFuture<R> resultFuture = jobIdFuture.thenCompose(jobId -> messaging.remoteJobResultRequestAsync(remoteNode, jobId));

            inFlightFutures.registerFuture(jobIdFuture);
            inFlightFutures.registerFuture(resultFuture);

            JobExecution<R> result = new RemoteJobExecution<>(remoteNode, jobIdFuture, resultFuture, inFlightFutures, messaging);
            jobIdFuture.thenAccept(jobId -> executionManager.addExecution(jobId, result));
            return result;
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public <R> JobExecution<R> executeRemotelyWithFailover(
            ClusterNode remoteNode,
            NextWorkerSelector nextWorkerSelector,
            List<DeploymentUnit> units,
            String jobClassName,
            ExecutionOptions options,
            Object input
    ) {
        JobExecution<R> result = new ComputeJobFailover<R>(
                this, logicalTopologyService, topologyService,
                remoteNode, nextWorkerSelector, failoverExecutor, units,
                jobClassName, options, input
        ).failSafeExecute();

        result.idAsync().thenAccept(jobId -> executionManager.addExecution(jobId, result));
        return result;
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

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        executor.start();
        messaging.start(this::executeLocally);
        executionManager.start();

        return nullCompletedFuture();
    }

    /** {@inheritDoc} */
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
        IgniteUtils.shutdownAndAwaitTermination(failoverExecutor, 10, TimeUnit.SECONDS);

        return nullCompletedFuture();
    }


    private <T, R> JobExecutionInternal<R> execJob(JobContext context, ExecutionOptions options, String jobClassName, Object args) {
        try {
            return executor.executeJob(options, jobClass(context.classLoader(), jobClassName), context.classLoader(), args);
        } catch (Throwable e) {
            context.close();
            throw e;
        }
    }

    private <T, R> TaskExecutionInternal<T, R> execTask(
            JobContext context,
            JobSubmitter jobSubmitter,
            String taskClassName,
            T input
    ) {
        try {
            return executor.executeTask(jobSubmitter, taskClass(context.classLoader(), taskClassName), input);
        } catch (Throwable e) {
            context.close();
            throw e;
        }
    }

    @TestOnly
    ExecutionManager executionManager() {
        return executionManager;
    }
}
