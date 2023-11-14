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
import static org.apache.ignite.internal.compute.ComputeUtils.instantiateJob;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.compute.loader.JobContext;
import org.apache.ignite.internal.compute.loader.JobContextManager;
import org.apache.ignite.internal.compute.message.DeploymentUnitMsg;
import org.apache.ignite.internal.compute.message.ExecuteRequest;
import org.apache.ignite.internal.compute.message.ExecuteResponse;
import org.apache.ignite.internal.compute.queue.ComputeExecutor;
import org.apache.ignite.internal.future.InFlightFutures;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.MessagingService;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link ComputeComponent}.
 */
public class ComputeComponentImpl implements ComputeComponent {
    private static final long NETWORK_TIMEOUT_MILLIS = Long.MAX_VALUE;

    private final ComputeMessagesFactory messagesFactory = new ComputeMessagesFactory();

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping of the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    private final InFlightFutures inFlightFutures = new InFlightFutures();

    private final MessagingService messagingService;

    private final JobContextManager jobContextManager;

    private final ComputeExecutor executor;

    /**
     * Creates a new instance.
     */
    public ComputeComponentImpl(
            MessagingService messagingService,
            JobContextManager jobContextManager,
            ComputeExecutor executor
    ) {
        this.messagingService = messagingService;
        this.jobContextManager = jobContextManager;
        this.executor = executor;
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        executor.start();

        messagingService.addMessageHandler(ComputeMessageTypes.class, (message, senderConsistentId, correlationId) -> {
            assert correlationId != null;

            if (message instanceof ExecuteRequest) {
                processExecuteRequest((ExecuteRequest) message, senderConsistentId, correlationId);
            }
        });
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        executor.stop();

        inFlightFutures.cancelInFlightFutures();
    }

    /** {@inheritDoc} */
    @Override
    public <R> CompletableFuture<R> executeLocally(
            ExecutionOptions options,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            CompletableFuture<JobContext> jobContextCompletableFuture = mapClassLoaderExceptions(
                    jobContextManager.acquireClassLoader(units), jobClassName);
            return jobContextCompletableFuture
                    .thenCompose(context ->
                            doExecuteLocally(options, ComputeUtils.<R>instantiateJob(context.classLoader(), jobClassName), args)
                                    .whenComplete((r, e) -> context.close())
                    );
        } finally {
            busyLock.leaveBusy();
        }
    }

    private <R> CompletableFuture<R> doExecuteLocally(ExecutionOptions options, ComputeJob<R> jobInstance, Object[] args) {
        CompletableFuture<R> future = executor.executeJob(options, jobInstance, args);
        inFlightFutures.registerFuture(future);

        return future;
    }

    /** {@inheritDoc} */
    @Override
    public <R> CompletableFuture<R> executeRemotely(
            ExecutionOptions options,
            ClusterNode remoteNode,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            return doExecuteRemotely(options, remoteNode, units, jobClassName, args);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private <R> CompletableFuture<R> doExecuteRemotely(
            ExecutionOptions options,
            ClusterNode remoteNode,
            List<DeploymentUnit> units,
            String jobClassName,
            Object[] args
    ) {
        List<DeploymentUnitMsg> deploymentUnitMsgs = units.stream()
                .map(this::toDeploymentUnitMsg)
                .collect(Collectors.toList());

        ExecuteRequest executeRequest = messagesFactory.executeRequest()
                .executeOptions(options)
                .deploymentUnits(deploymentUnitMsgs)
                .jobClassName(jobClassName)
                .args(args)
                .build();

        CompletableFuture<R> future = messagingService.invoke(remoteNode, executeRequest, NETWORK_TIMEOUT_MILLIS)
                .thenCompose(message -> resultFromExecuteResponse((ExecuteResponse) message));
        inFlightFutures.registerFuture(future);
        return future;
    }

    private void processExecuteRequest(ExecuteRequest executeRequest, String senderConsistentId, long correlationId) {
        if (!busyLock.enterBusy()) {
            sendExecuteResponse(null, new NodeStoppingException(), senderConsistentId, correlationId);
            return;
        }

        try {
            List<DeploymentUnit> units = toDeploymentUnit(executeRequest.deploymentUnits());

            mapClassLoaderExceptions(jobContextManager.acquireClassLoader(units), executeRequest.jobClassName())
                    .whenComplete((context, err) -> {
                        if (err != null) {
                            if (context != null) {
                                context.close();
                            }

                            sendExecuteResponse(null, err, senderConsistentId, correlationId);
                        }

                        doExecuteLocally(
                                executeRequest.executeOptions(),
                                instantiateJob(context.classLoader(), executeRequest.jobClassName()),
                                executeRequest.args()
                        ).whenComplete((r, e) -> context.close())
                                .handle((result, ex) -> sendExecuteResponse(result, ex, senderConsistentId, correlationId));
                    });
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Nullable
    private Object sendExecuteResponse(@Nullable Object result, @Nullable Throwable ex, String senderConsistentId, Long correlationId) {
        ExecuteResponse executeResponse = messagesFactory.executeResponse()
                .result(result)
                .throwable(ex)
                .build();

        messagingService.respond(senderConsistentId, executeResponse, correlationId);

        return null;
    }

    private DeploymentUnitMsg toDeploymentUnitMsg(DeploymentUnit unit) {
        return messagesFactory.deploymentUnitMsg()
                .name(unit.name())
                .version(unit.version().toString())
                .build();
    }

    private static List<DeploymentUnit> toDeploymentUnit(List<DeploymentUnitMsg> unitMsgs) {
        return unitMsgs.stream()
                .map(it -> new DeploymentUnit(it.name(), Version.parseVersion(it.version())))
                .collect(Collectors.toList());
    }

    private static <R> CompletableFuture<R> resultFromExecuteResponse(ExecuteResponse executeResponse) {
        Throwable throwable = executeResponse.throwable();
        if (throwable != null) {
            return failedFuture(throwable);
        }

        return completedFuture((R) executeResponse.result());
    }
}
