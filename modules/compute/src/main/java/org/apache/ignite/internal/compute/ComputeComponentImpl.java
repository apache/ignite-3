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
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.compute.configuration.ComputeConfiguration;
import org.apache.ignite.internal.compute.executor.ComputeExecutor;
import org.apache.ignite.internal.compute.executor.JobExecutionInternal;
import org.apache.ignite.internal.compute.loader.JobContext;
import org.apache.ignite.internal.compute.loader.JobContextManager;
import org.apache.ignite.internal.compute.messaging.ComputeMessaging;
import org.apache.ignite.internal.compute.messaging.RemoteJobExecution;
import org.apache.ignite.internal.future.InFlightFutures;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.TopologyService;
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
            Object... args
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
                                JobExecutionInternal<R> execution = exec(context, options, jobClassName, args);
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

    /** {@inheritDoc} */
    @Override
    public <R> JobExecution<R> executeRemotely(
            ExecutionOptions options,
            ClusterNode remoteNode,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        if (!busyLock.enterBusy()) {
            return new DelegatingJobExecution<>(
                    failedFuture(new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException()))
            );
        }

        try {
            CompletableFuture<UUID> jobIdFuture = messaging.remoteExecuteRequestAsync(options, remoteNode, units, jobClassName, args);
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
            Object... args
    ) {
        JobExecution<R> result = new ComputeJobFailover<R>(
                this, logicalTopologyService, topologyService,
                remoteNode, nextWorkerSelector, failoverExecutor, units,
                jobClassName, options, args
        ).failSafeExecute();

        result.idAsync().thenAccept(jobId -> executionManager.addExecution(jobId, result));
        return result;
    }

    @Override
    public CompletableFuture<Collection<JobStatus>> statusesAsync() {
        return messaging.broadcastStatusesAsync();
    }

    @Override
    public CompletableFuture<@Nullable JobStatus> statusAsync(UUID jobId) {
        return messaging.broadcastStatusAsync(jobId);
    }

    @Override
    public CompletableFuture<@Nullable Boolean> cancelAsync(UUID jobId) {
        return messaging.broadcastCancelAsync(jobId);
    }

    @Override
    public CompletableFuture<@Nullable Boolean> changePriorityAsync(UUID jobId, int newPriority) {
        return messaging.broadcastChangePriorityAsync(jobId, newPriority);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> start() {
        executor.start();
        messaging.start(this::executeLocally);
        executionManager.start();

        return nullCompletedFuture();
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();
        inFlightFutures.cancelInFlightFutures();

        executionManager.stop();
        messaging.stop();
        executor.stop();
        IgniteUtils.shutdownAndAwaitTermination(failoverExecutor, 10, TimeUnit.SECONDS);
    }

    private <R> JobExecutionInternal<R> exec(JobContext context, ExecutionOptions options, String jobClassName, Object[] args) {
        return executor.executeJob(
                options,
                ComputeUtils.jobClass(context.classLoader(), jobClassName),
                args
        );
    }

    @TestOnly
    ExecutionManager executionManager() {
        return executionManager;
    }
}
