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
import static org.apache.ignite.lang.ErrorGroups.Compute.RESULT_NOT_FOUND_ERR;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.compute.ComputeException;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.internal.compute.configuration.ComputeConfiguration;
import org.apache.ignite.internal.compute.executor.ComputeExecutor;
import org.apache.ignite.internal.compute.executor.JobExecutionInternal;
import org.apache.ignite.internal.compute.loader.JobContext;
import org.apache.ignite.internal.compute.loader.JobContextManager;
import org.apache.ignite.internal.compute.messaging.ComputeMessaging;
import org.apache.ignite.internal.compute.messaging.RemoteJobExecution;
import org.apache.ignite.internal.compute.queue.CancellingException;
import org.apache.ignite.internal.future.InFlightFutures;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.TopologyService;

/**
 * Implementation of {@link ComputeComponent}.
 */
public class ComputeComponentImpl implements ComputeComponent {
    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping of the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    private final InFlightFutures inFlightFutures = new InFlightFutures();

    private final JobContextManager jobContextManager;

    private final ComputeExecutor executor;

    private final ComputeMessaging messaging;

    private final Cleaner<JobExecution<?>> cleaner;

    private final Map<UUID, JobExecution<?>> executions = new ConcurrentHashMap<>();

    /**
     * Creates a new instance.
     */
    public ComputeComponentImpl(
            MessagingService messagingService,
            TopologyService topologyService,
            JobContextManager jobContextManager,
            ComputeExecutor executor,
            ComputeConfiguration computeCfg
    ) {
        this.jobContextManager = jobContextManager;
        this.executor = executor;
        messaging = new ComputeMessaging(this, messagingService, topologyService);
        cleaner = new Cleaner<>(computeCfg);
    }

    /** {@inheritDoc} */
    @Override
    public <R> JobExecution<R> executeLocally(
            ExecutionOptions options,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        JobExecution<R> result = start(options, units, jobClassName, args);
        result.idAsync().thenAccept(jobId -> {
            executions.put(jobId, result);
            result.resultAsync().whenComplete((r, throwable) -> cleaner.scheduleRemove(jobId));
        });
        return result;
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
            CompletableFuture<R> resultFuture = jobIdFuture.thenCompose(jobId -> messaging.<R>remoteJobResultRequestAsync(remoteNode, jobId)
                    .whenComplete((r, throwable) -> cleaner.scheduleRemove(jobId)));

            inFlightFutures.registerFuture(jobIdFuture);
            inFlightFutures.registerFuture(resultFuture);

            JobExecution<R> result = new RemoteJobExecution<>(remoteNode, jobIdFuture, resultFuture, inFlightFutures, messaging);
            jobIdFuture.thenAccept(jobId -> executions.put(jobId, result));
            return result;
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public CompletableFuture<?> resultAsync(UUID jobId) {
        JobExecution<?> execution = executions.get(jobId);
        if (execution != null) {
            return execution.resultAsync();
        }
        return failedFuture(new ComputeException(RESULT_NOT_FOUND_ERR, "Job result not found for the job id " + jobId));
    }

    @Override
    public CompletableFuture<JobStatus> broadcastStatusAsync(UUID jobId) {
        return messaging.broadcastStatusAsync(jobId);
    }

    @Override
    public CompletableFuture<JobStatus> localStatusAsync(UUID jobId) {
        JobExecution<?> execution = executions.get(jobId);
        return execution != null ? execution.statusAsync() : nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> broadcastCancelAsync(UUID jobId) {
        return messaging.broadcastCancelAsync(jobId);
    }

    @Override
    public CompletableFuture<Void> localCancelAsync(UUID jobId) {
        JobExecution<?> execution = executions.get(jobId);
        return execution != null ? execution.cancelAsync() : failedFuture(new CancellingException(jobId));
    }

    private <R> JobExecution<R> start(
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

            return new DelegatingJobExecution<>(future);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> start() {
        executor.start();
        messaging.start();
        cleaner.start(executions::remove);

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

        cleaner.stop();
        messaging.stop();
        executor.stop();
    }

    private <R> JobExecutionInternal<R> exec(JobContext context, ExecutionOptions options, String jobClassName, Object[] args) {
        return executor.executeJob(
                options,
                ComputeUtils.jobClass(context.classLoader(), jobClassName),
                args
        );
    }
}
