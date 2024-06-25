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
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.lang.ErrorGroups.Compute;
import org.apache.ignite.network.ClusterNode;

/**
 * This is a helper class for {@link ComputeComponent} to handle job failures. You can think about this class as a "retryable compute job
 * with captured context". Retry logic is applied ONLY if the worker node leaves the cluster. If the job itself is failing, then the
 * exception is propagated to the caller and this class does not handle it.
 *
 * <p>If you want to execute a job on node1 and use node2 and node3 as failover candidates,
 * then you should create an instance of this class with workerNode = node1, failoverCandidates = [node2, node3] as arguments and call
 * {@link #failSafeExecute()}.
 *
 * @param <R> the type of the result of the job.
 */
class ComputeJobFailover<R> {
    private static final IgniteLogger LOG = Loggers.forClass(ComputeJobFailover.class);

    /**
     * Thread to run failover logic. We can not perform time-consuming operations in the same thread where we discover topology changes (it
     * is network id thread).
     */
    private final Executor executor;

    /**
     * Compute component that is called when the {@link #runningWorkerNode} has left the cluster.
     */
    private final ComputeComponent computeComponent;

    /**
     * Topology service is needed to listen logical topology events. This is how we get know that the node has left the cluster.
     */
    private final LogicalTopologyService logicalTopologyService;

    /**
     * Physical topology service that helps to figure out if we want to restart job on the local node or on the remote one.
     */
    private final TopologyService topologyService;

    /**
     * The selector that returns the next worker node to execute job on.
     */
    private final NextWorkerSelector nextWorkerSelector;

    /**
     * The node where the job is being executed at a given moment. If node leaves the cluster, then the job is restarted on one of the
     * worker node returned by {@link #nextWorkerSelector} and the reference is CASed to the new node.
     */
    private final AtomicReference<ClusterNode> runningWorkerNode;

    /**
     * Context of the called job. Captures deployment units, jobClassName and arguments.
     */
    private final RemoteExecutionContext<?, R> jobContext;

    /**
     * Creates a per-job instance.
     *
     * @param computeComponent compute component.
     * @param logicalTopologyService logical topology service.
     * @param topologyService physical topology service.
     * @param workerNode the node to execute the job on.
     * @param nextWorkerSelector the selector that returns the next worker to execute job on.
     * @param executor the thread pool where the failover should run on.
     * @param units deployment units.
     * @param jobClassName the name of the job class.
     * @param executionOptions execution options like priority or max retries.
     * @param args the arguments of the job.
     */
    ComputeJobFailover(
            ComputeComponent computeComponent,
            LogicalTopologyService logicalTopologyService,
            TopologyService topologyService,
            ClusterNode workerNode,
            NextWorkerSelector nextWorkerSelector,
            Executor executor,
            List<DeploymentUnit> units,
            String jobClassName,
            ExecutionOptions executionOptions,
            Object args
    ) {
        this.computeComponent = computeComponent;
        this.runningWorkerNode = new AtomicReference<>(workerNode);
        this.logicalTopologyService = logicalTopologyService;
        this.topologyService = topologyService;
        this.nextWorkerSelector = nextWorkerSelector;
        this.jobContext = new RemoteExecutionContext<>(units, jobClassName, executionOptions, args);
        this.executor = executor;
    }

    /**
     * Executes a job on the worker node and restarts the job on one of the failover candidates if the worker node leaves the cluster.
     *
     * @return JobExecution with the result of the job and the status of the job.
     */
    JobExecution<R> failSafeExecute() {
        JobExecution<R> jobExecution = launchJobOn(runningWorkerNode.get());
        jobContext.initJobExecution(new FailSafeJobExecution<>(jobExecution));

        LogicalTopologyEventListener nodeLeftEventListener = new OnNodeLeft();
        logicalTopologyService.addEventListener(nodeLeftEventListener);
        jobExecution.resultAsync().whenComplete((r, e) -> logicalTopologyService.removeEventListener(nodeLeftEventListener));

        return jobContext.failSafeJobExecution();
    }

    private JobExecution<R> launchJobOn(ClusterNode runningWorkerNode) {
        if (runningWorkerNode.equals(topologyService.localMember())) {
            return computeComponent.executeLocally(jobContext.executionOptions(), jobContext.units(), jobContext.jobClassName(),
                    jobContext.arg());
        } else {
            return computeComponent.executeRemotely(
                    jobContext.executionOptions(), runningWorkerNode, jobContext.units(), jobContext.jobClassName(), jobContext.arg()
            );
        }
    }

    class OnNodeLeft implements LogicalTopologyEventListener {
        @Override
        public void onNodeLeft(LogicalNode leftNode, LogicalTopologySnapshot newTopology) {
            if (!runningWorkerNode.get().id().equals(leftNode.id())) {
                return;
            }

            LOG.info("Worker node {} has left the cluster.", leftNode.name());
            executor.execute(this::selectNewWorker);
        }

        private void selectNewWorker() {
            nextWorkerSelector.next()
                    .thenAccept(nextWorker -> {
                        if (nextWorker == null) {
                            LOG.warn("No more worker nodes to restart the job. Failing the job {}.", jobContext.jobClassName());

                            FailSafeJobExecution<?> failSafeJobExecution = jobContext.failSafeJobExecution();
                            failSafeJobExecution.completeExceptionally(
                                    new IgniteInternalException(Compute.COMPUTE_JOB_FAILED_ERR)
                            );
                            return;
                        }

                        if (topologyService.getByConsistentId(nextWorker.name()) == null) {
                            LOG.warn("Worker node {} is not found in the cluster", nextWorker.name());
                            // Restart next worker selection
                            executor.execute(this::selectNewWorker);
                            return;
                        }

                        LOG.info("Restarting the job {} on node {}.", jobContext.jobClassName(), nextWorker.name());

                        runningWorkerNode.set(nextWorker);
                        JobExecution<R> jobExecution = launchJobOn(runningWorkerNode.get());
                        jobContext.updateJobExecution(jobExecution);
                    });
        }
    }
}
