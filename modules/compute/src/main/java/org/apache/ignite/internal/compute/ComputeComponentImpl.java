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

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.compute.configuration.ComputeConfiguration;
import org.apache.ignite.internal.compute.loader.JobContext;
import org.apache.ignite.internal.compute.loader.JobContextManager;
import org.apache.ignite.internal.compute.message.DeploymentUnitMsg;
import org.apache.ignite.internal.compute.message.ExecuteRequest;
import org.apache.ignite.internal.compute.message.ExecuteResponse;
import org.apache.ignite.internal.future.InFlightFutures;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.MessagingService;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link ComputeComponent}.
 */
public class ComputeComponentImpl implements ComputeComponent {
    private static final IgniteLogger LOG = Loggers.forClass(ComputeComponentImpl.class);

    private static final long NETWORK_TIMEOUT_MILLIS = Long.MAX_VALUE;

    private static final long THREAD_KEEP_ALIVE_SECONDS = 60;

    private final ComputeMessagesFactory messagesFactory = new ComputeMessagesFactory();

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping of the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    private final InFlightFutures inFlightFutures = new InFlightFutures();

    private final Ignite ignite;

    private final MessagingService messagingService;

    private final ComputeConfiguration configuration;

    private final JobContextManager jobContextManager;

    private ExecutorService jobExecutorService;

    /**
     * Creates a new instance.
     */
    public ComputeComponentImpl(
            Ignite ignite,
            MessagingService messagingService,
            ComputeConfiguration configuration,
            JobContextManager jobContextManager) {
        this.ignite = ignite;
        this.messagingService = messagingService;
        this.configuration = configuration;
        this.jobContextManager = jobContextManager;
    }

    /** {@inheritDoc} */
    @Override
    public <R> CompletableFuture<R> executeLocally(List<DeploymentUnit> units, String jobClassName, Object... args) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            return mapClassLoaderExceptions(jobClassLoader(units), jobClassName)
                    .thenCompose(context -> doExecuteLocally(this.<R, ComputeJob<R>>jobClass(context.classLoader(), jobClassName), args)
                            .whenComplete((r, e) -> context.close())
                    );
        } finally {
            busyLock.leaveBusy();
        }
    }

    private <R> CompletableFuture<R> doExecuteLocally(Class<? extends ComputeJob<R>> jobClass, Object[] args) {
        assert jobExecutorService != null : "Not started yet!";

        CompletableFuture<R> future = startLocalExecution(jobClass, args);
        inFlightFutures.registerFuture(future);

        return future;
    }

    private <R> CompletableFuture<R> startLocalExecution(Class<? extends ComputeJob<R>> jobClass, Object[] args) {
        try {
            return CompletableFuture.supplyAsync(() -> executeJob(jobClass, args), jobExecutorService);
        } catch (RejectedExecutionException e) {
            return failedFuture(e);
        }
    }

    private <R> R executeJob(Class<? extends ComputeJob<R>> jobClass, Object[] args) {
        ComputeJob<R> job = instantiateJob(jobClass);
        JobExecutionContext context = new JobExecutionContextImpl(ignite);
        // TODO: IGNITE-16746 - translate NodeStoppingException to a public exception
        return job.execute(context, args);
    }

    private <R> ComputeJob<R> instantiateJob(Class<? extends ComputeJob<R>> jobClass) {
        if (!(ComputeJob.class.isAssignableFrom(jobClass))) {
            throw new IgniteInternalException("'" + jobClass.getName() + "' does not implement ComputeJob interface");
        }

        try {
            Constructor<? extends ComputeJob<R>> constructor = jobClass.getDeclaredConstructor();

            if (!constructor.canAccess(null)) {
                constructor.setAccessible(true);
            }

            return constructor.newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IgniteInternalException("Cannot instantiate job", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public <R> CompletableFuture<R> executeRemotely(ClusterNode remoteNode, List<DeploymentUnit> units, String jobClassName,
            Object... args) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            return doExecuteRemotely(remoteNode, units, jobClassName, args);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private <R> CompletableFuture<R> doExecuteRemotely(ClusterNode remoteNode, List<DeploymentUnit> units, String jobClassName,
            Object[] args) {
        List<DeploymentUnitMsg> deploymentUnitMsgs = units.stream()
                .map(this::toDeploymentUnitMsg)
                .collect(Collectors.toList());

        ExecuteRequest executeRequest = messagesFactory.executeRequest()
                .deploymentUnits(deploymentUnitMsgs)
                .jobClassName(jobClassName)
                .args(args)
                .build();

        CompletableFuture<R> future = messagingService.invoke(remoteNode, executeRequest, NETWORK_TIMEOUT_MILLIS)
                .thenCompose(message -> resultFromExecuteResponse((ExecuteResponse) message));
        inFlightFutures.registerFuture(future);
        return future;
    }

    private <R> CompletableFuture<R> resultFromExecuteResponse(ExecuteResponse executeResponse) {
        if (executeResponse.throwable() != null) {
            return failedFuture(executeResponse.throwable());
        }

        return completedFuture((R) executeResponse.result());
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void start() {
        jobExecutorService = new ThreadPoolExecutor(
                configuration.threadPoolSize().value(),
                configuration.threadPoolSize().value(),
                THREAD_KEEP_ALIVE_SECONDS,
                TimeUnit.SECONDS,
                newExecutorServiceTaskQueue(),
                new NamedThreadFactory(NamedThreadFactory.threadPrefix(ignite.name(), "compute"), LOG)
        );

        messagingService.addMessageHandler(ComputeMessageTypes.class, (message, senderConsistentId, correlationId) -> {
            assert correlationId != null;

            if (message instanceof ExecuteRequest) {
                processExecuteRequest((ExecuteRequest) message, senderConsistentId, correlationId);

                return;
            }

            throw new IgniteInternalException("Unexpected message type " + message.getClass());
        });
    }

    BlockingQueue<Runnable> newExecutorServiceTaskQueue() {
        return new LinkedBlockingQueue<>();
    }

    private void processExecuteRequest(ExecuteRequest executeRequest, String senderConsistentId, long correlationId) {
        if (!busyLock.enterBusy()) {
            sendExecuteResponse(null, new NodeStoppingException(), senderConsistentId, correlationId);
            return;
        }

        try {
            List<DeploymentUnit> units = toDeploymentUnit(executeRequest.deploymentUnits());

            mapClassLoaderExceptions(jobClassLoader(units), executeRequest.jobClassName())
                    .whenComplete((context, err) -> {
                        if (err != null) {
                            if (context != null) {
                                context.close();
                            }

                            sendExecuteResponse(null, err, senderConsistentId, correlationId);
                        }

                        doExecuteLocally(jobClass(context.classLoader(), executeRequest.jobClassName()), executeRequest.args())
                                .whenComplete((r, e) -> context.close())
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

    private <R, J extends ComputeJob<R>> Class<J> jobClass(ClassLoader jobClassLoader, String jobClassName) {
        try {
            return (Class<J>) Class.forName(jobClassName, true, jobClassLoader);
        } catch (ClassNotFoundException e) {
            throw new IgniteInternalException("Cannot load job class by name '" + jobClassName + "'", e);
        }
    }

    private CompletableFuture<JobContext> jobClassLoader(List<DeploymentUnit> units) {
        return jobContextManager.acquireClassLoader(units);
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        IgniteUtils.shutdownAndAwaitTermination(jobExecutorService, stopTimeoutMillis(), TimeUnit.MILLISECONDS);

        inFlightFutures.cancelInFlightFutures();
    }

    long stopTimeoutMillis() {
        return configuration.threadPoolStopTimeoutMillis().value();
    }

    private DeploymentUnitMsg toDeploymentUnitMsg(DeploymentUnit unit) {
        return messagesFactory.deploymentUnitMsg()
                .name(unit.name())
                .version(unit.version().toString())
                .build();
    }

    private List<DeploymentUnit> toDeploymentUnit(List<DeploymentUnitMsg> unitMsgs) {
        return unitMsgs.stream()
                .map(it -> new DeploymentUnit(it.name(), Version.parseVersion(it.version())))
                .collect(Collectors.toList());
    }
}
