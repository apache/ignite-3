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
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.lang.ErrorGroups.Compute;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * This is a helper class for {@link ComputeComponent} to handle job failures. You can think about this class as a "retryable compute job
 * with captured context". Retry logic is applied ONLY if the worker node leaves the cluster. If the job itself is failing, then the
 * exception is propagated to the caller and this class does not handle it.
 *
 * <p>If you want to execute a job on node1 and use node2 and node3 as failover candidates,
 * then you should create an instance of this class with workerNode = node1, failoverCandidates = [node2, node3] as arguments and
 * call {@link #failSafeExecute()}.
 *
 * @param <T> the type of the result of the job.
 */
class ComputeJobFailover<T> {
    private static final IgniteLogger LOG = Loggers.forClass(ComputeJobFailover.class);

    /**
     * Compute component that is called when the {@link #runningWorkerNode} has left the cluster.
     */
    private final ComputeComponent computeComponent;

    /**
     * The failover listens this event source to know when the {@link #runningWorkerNode} has left the cluster.
     */
    private final NodeLeftEventsSource nodeLeftEventsSource;

    /**
     * Set of worker candidates in case {@link #runningWorkerNode} has left the cluster.
     */
    private final ConcurrentLinkedDeque<ClusterNode> failoverCandidates;

    /**
     * The node where the job is being executed at a given moment. If node leaves the cluster, then the job is restarted on one of the
     * {@link #failoverCandidates} and the reference is CASed to the new node.
     */
    private final AtomicReference<ClusterNode> runningWorkerNode;

    /**
     * Context of the called job. Captures deployment units, jobClassName and arguments.
     */
    private final RemoteExecutionContext<T> jobContext;

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
            Set<ClusterNode> failoverCandidates,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        this.computeComponent = computeComponent;
        this.nodeLeftEventsSource = nodeLeftEventsSource;
        this.runningWorkerNode = new AtomicReference<>(workerNode);
        this.failoverCandidates = new ConcurrentLinkedDeque<>(failoverCandidates);
        this.jobContext = new RemoteExecutionContext<>(units, jobClassName, args);
    }

    /**
     * Executes a job on the worker node and restarts the job on one of the failover candidates if the worker node leaves the cluster.
     *
     * @return JobExecution with the result of the job and the status of the job.
     */
    JobExecution<T> failSafeExecute() {
        JobExecution<T> remoteJobExecution = launchJobOn(runningWorkerNode);
        jobContext.initJobExecution(new FailSafeJobExecution<>(remoteJobExecution));

        Consumer<ClusterNode> handler = new OnNodeLeft();
        nodeLeftEventsSource.addEventHandler(handler);
        remoteJobExecution.resultAsync().whenComplete((r, e) -> nodeLeftEventsSource.removeEventHandler(handler));

        return jobContext.failSafeJobExecution();
    }

    private JobExecution<T> launchJobOn(AtomicReference<ClusterNode> runningWorkerNode) {
        return computeComponent.executeRemotely(runningWorkerNode.get(), jobContext.units(), jobContext.jobClassName(), jobContext.args());
    }

    class OnNodeLeft implements Consumer<ClusterNode> {
        @Override
        public void accept(ClusterNode leftNode) {
            if (!runningWorkerNode.get().equals(leftNode)) {
                return;
            }

            ClusterNode nextWorkerCandidate = takeCandidate();
            if (nextWorkerCandidate == null) {
                LOG.warn("No more worker nodes to restart the job. Failing the job {}.", jobContext.jobClassName());

                FailSafeJobExecution<?> failSafeJobExecution = jobContext.failSafeJobExecution();
                failSafeJobExecution.completeExceptionally(
                        new IgniteInternalException(Compute.COMPUTE_JOB_FAILED_ERR)
                );
                return;
            }

            LOG.warn(
                    "Worker node {} has left the cluster. Restarting the job {} on node {}.",
                    leftNode, jobContext.jobClassName(), nextWorkerCandidate
            );

            runningWorkerNode.set(nextWorkerCandidate);
            JobExecution<T> jobExecution = launchJobOn(runningWorkerNode);
            jobContext.updateJobExecution(jobExecution);
        }

        @Nullable
        private ClusterNode takeCandidate() {
            try {
                return failoverCandidates.pop();
            } catch (NoSuchElementException ex) {
                return null;
            }
        }
    }
}
