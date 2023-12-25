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

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * This is a helper class for {@link ComputeComponent} to handle job failover.
 * The usage of this class is following:
 *
 * <p>If you want to execute a job on node1 and use node2 and node3 as failover candidates,
 * then you should create an instance of this class with workerNode = node1, failoverCandidates = [node2, node3] as arguments.
 *
 * @param <T> the type of the result of the job.
 */
class ComputeJobFailover<T> {

    private final ComputeComponent computeComponent;

    private final NodeLeftEventsSource nodeLeftEventsSource;

    private final Set<ClusterNode> failoverCandidates;

    private final AtomicReference<ClusterNode> runningWorkerNode;

    private final List<DeploymentUnit> units;

    private final String jobClassName;

    private final Object[] args;

    private volatile RemoteExecutionContext<T> jobContext;

    /**
     * Creates a per-job instance.
     *
     * @param computeComponent compute component.
     * @param nodeLeftEventsSource node left events source, used as dynamic topology service (allows to remove handlers).
     * @param workerNode the node to execute the job on.
     * @param failoverCandidates the set of nodes where the job can be restarted if the worker node leaves the cluster.
     * @param units deployment units.
     * @param jobClassName the name of the job class.
     * @param args the arguments of the job.
     */
    ComputeJobFailover(
            ComputeComponent computeComponent,
            NodeLeftEventsSource nodeLeftEventsSource,
            ClusterNode workerNode,
            @Nullable
            Set<ClusterNode> failoverCandidates,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        this.computeComponent = computeComponent;
        this.nodeLeftEventsSource = nodeLeftEventsSource;
        this.runningWorkerNode = new AtomicReference<>(workerNode);
        this.failoverCandidates = failoverCandidates;
        this.units = units;
        this.jobClassName = jobClassName;
        this.args = args;
    }

    /**
     * Executes a job on the worker node and restarts the job on one of the
     * failover candidates if the worker node leaves the cluster.
     *
     * @return CompletableFuture with the result of the job.
     */
    CompletableFuture<T> failSafeExecuteOnOneNode() {
        // Save the context to be able to restart the job on a failover candidate.
        jobContext = new RemoteExecutionContext<>();
        jobContext.units = units;
        jobContext.jobClassName = jobClassName;
        jobContext.args = args;

        RemoteCompletableFutureWrapper<T> futureWrapper = new RemoteCompletableFutureWrapper<>(
                computeComponent.executeRemotely(runningWorkerNode.get(), units, jobClassName, args)
        );
        jobContext.future = futureWrapper;

        UUID jobId = UUID.randomUUID(); //Todo
        nodeLeftEventsSource.addEventHandler(jobId, new OnNodeLeft());

        futureWrapper.onDispose(
                () -> nodeLeftEventsSource.removeEventHandler(jobId)
        );

        return futureWrapper;
    }

    private static class RemoteExecutionContext<T> {
        List<DeploymentUnit> units;
        String jobClassName;
        Object[] args;
        RemoteCompletableFutureWrapper<T> future;
    }

    class OnNodeLeft implements Consumer<ClusterNode> {
        @Override
        public void accept(ClusterNode clusterNode) {
            if (!runningWorkerNode.get().equals(clusterNode)) {
                return;
            }
            if (failoverCandidates == null) {
                jobContext.future.cancel(true);
                return;
            }

            var futureWrapper = jobContext.future;
            if (failoverCandidates.isEmpty()) {
                futureWrapper.completeExceptionally(new IgniteInternalException("No failover candidates left"));
                return;
            }

            ClusterNode candidate = failoverCandidates.stream().findFirst().get();
            runningWorkerNode.set(candidate);
            failoverCandidates.remove(candidate);
            CompletableFuture<T> remoteFuture = computeComponent.executeRemotely(
                    candidate, jobContext.units, jobContext.jobClassName, jobContext.args
            );
            futureWrapper.setFuture(remoteFuture);
        }
    }
}
