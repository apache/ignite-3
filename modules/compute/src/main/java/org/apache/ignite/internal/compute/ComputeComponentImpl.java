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

import java.lang.reflect.Constructor;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.internal.compute.configuration.ComputeConfiguration;
import org.apache.ignite.internal.compute.message.ExecuteRequest;
import org.apache.ignite.internal.compute.message.ExecuteResponse;
import org.apache.ignite.internal.future.InFlightFutures;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.NodeStoppingException;
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

    private final Ignite ignite;
    private final MessagingService messagingService;
    private final ComputeConfiguration configuration;

    private ExecutorService jobExecutorService;

    private final ClassLoader jobClassLoader = Thread.currentThread().getContextClassLoader();

    private final ComputeMessagesFactory messagesFactory = new ComputeMessagesFactory();

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping of the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    private final InFlightFutures inFlightFutures = new InFlightFutures();

    /**
     * Creates a new instance.
     */
    public ComputeComponentImpl(Ignite ignite, MessagingService messagingService, ComputeConfiguration configuration) {
        this.ignite = ignite;
        this.messagingService = messagingService;
        this.configuration = configuration;
    }

    /** {@inheritDoc} */
    @Override
    public <R> CompletableFuture<R> executeLocally(Class<? extends ComputeJob<R>> jobClass, Object... args) {
        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(new NodeStoppingException());
        }

        try {
            return doExecuteLocally(jobClass, args);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override
    public <R> CompletableFuture<R> executeLocally(String jobClassName, Object... args) {
        return completedFuture(null).thenCompose(ignore -> executeLocally(jobClass(jobClassName), args));
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
            return CompletableFuture.failedFuture(e);
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
    public <R> CompletableFuture<R> executeRemotely(ClusterNode remoteNode, Class<? extends ComputeJob<R>> jobClass, Object... args) {
        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(new NodeStoppingException());
        }

        try {
            return doExecuteRemotely(remoteNode, jobClass, args);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override
    public <R> CompletableFuture<R> executeRemotely(ClusterNode remoteNode, String jobClassName, Object... args) {
        return completedFuture(null).thenCompose(ignored -> executeRemotely(remoteNode, jobClass(jobClassName), args));
    }

    private <R> CompletableFuture<R> doExecuteRemotely(ClusterNode remoteNode, Class<? extends ComputeJob<R>> jobClass, Object[] args) {
        ExecuteRequest executeRequest = messagesFactory.executeRequest()
                .jobClassName(jobClass.getName())
                .args(args)
                .build();

        CompletableFuture<R> future = messagingService.invoke(remoteNode, executeRequest, NETWORK_TIMEOUT_MILLIS)
                .thenCompose(message -> resultFromExecuteResponse((ExecuteResponse) message));
        inFlightFutures.registerFuture(future);
        return future;
    }

    @SuppressWarnings("unchecked")
    private <R> CompletableFuture<R> resultFromExecuteResponse(ExecuteResponse executeResponse) {
        if (executeResponse.throwable() != null) {
            return CompletableFuture.failedFuture(executeResponse.throwable());
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

        messagingService.addMessageHandler(ComputeMessageTypes.class, (message, sender, correlationId) -> {
            assert correlationId != null;

            if (message instanceof ExecuteRequest) {
                processExecuteRequest((ExecuteRequest) message, sender, correlationId);

                return;
            }

            throw new IgniteInternalException("Unexpected message type " + message.getClass());
        });
    }

    BlockingQueue<Runnable> newExecutorServiceTaskQueue() {
        return new LinkedBlockingQueue<>();
    }

    private void processExecuteRequest(ExecuteRequest executeRequest, ClusterNode sender, long correlationId) {
        if (!busyLock.enterBusy()) {
            sendExecuteResponse(null, new NodeStoppingException(), sender, correlationId);
            return;
        }

        try {
            Class<ComputeJob<Object>> jobClass = jobClass(executeRequest.jobClassName());

            doExecuteLocally(jobClass, executeRequest.args())
                    .handle((result, ex) -> sendExecuteResponse(result, ex, sender, correlationId));
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Nullable
    private Object sendExecuteResponse(Object result, Throwable ex, ClusterNode sender, Long correlationId) {
        ExecuteResponse executeResponse = messagesFactory.executeResponse()
                .result(result)
                .throwable(ex)
                .build();

        messagingService.respond(sender, executeResponse, correlationId);

        return null;
    }

    @SuppressWarnings("unchecked")
    private <R, J extends ComputeJob<R>> Class<J> jobClass(String jobClassName) {
        try {
            return (Class<J>) Class.forName(jobClassName, true, jobClassLoader);
        } catch (ClassNotFoundException e) {
            throw new IgniteInternalException("Cannot load job class by name '" + jobClassName + "'", e);
        }
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
}
