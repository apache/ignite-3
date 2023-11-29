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
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.internal.compute.executor.ComputeExecutor;
import org.apache.ignite.internal.compute.loader.JobContext;
import org.apache.ignite.internal.compute.loader.JobContextManager;
import org.apache.ignite.internal.compute.messaging.ComputeMessaging;
import org.apache.ignite.internal.future.InFlightFutures;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.MessagingService;

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

    /**
     * Creates a new instance.
     */
    public ComputeComponentImpl(
            MessagingService messagingService,
            JobContextManager jobContextManager,
            ComputeExecutor executor
    ) {
        this.jobContextManager = jobContextManager;
        this.executor = executor;
        messaging = new ComputeMessaging(messagingService);
    }

    /** {@inheritDoc} */
    @Override
    public <R> CompletableFuture<R> executeLocally(
            ExecutionOptions options,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        return start(options, units, jobClassName, args);
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
            return failedFuture(new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException()));
        }

        try {
            CompletableFuture<R> future = messaging.remoteExecuteRequest(options, remoteNode, units, jobClassName, args);
            inFlightFutures.registerFuture(future);
            return future;
        } finally {
            busyLock.leaveBusy();
        }
    }

    private <R> CompletableFuture<R> start(
            ExecutionOptions options,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException()));
        }

        try {
            CompletableFuture<R> future = mapClassLoaderExceptions(jobContextManager.acquireClassLoader(units), jobClassName)
                    .thenCompose(context -> this.<R>exec(context, options, jobClassName, args)
                            .whenComplete((r, e) -> context.close()));

            inFlightFutures.registerFuture(future);

            return future;
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        executor.start();
        messaging.start(this::start);
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();
        inFlightFutures.cancelInFlightFutures();

        messaging.stop();
        executor.stop();
    }

    private <R> CompletableFuture<R> exec(JobContext context, ExecutionOptions options, String jobClassName, Object[] args) {
        return executor.executeJob(
                options,
                ComputeUtils.jobClass(context.classLoader(), jobClassName),
                args
        );
    }
}
