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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.compute.events.ComputeEventMetadata;
import org.apache.ignite.internal.compute.events.ComputeEventsFactory;
import org.apache.ignite.internal.eventlog.api.EventLog;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.lang.ErrorGroups.Compute;

/**
 * This is a helper class for {@link ComputeComponent} to handle job failures. You can think about this class as a "retryable compute job
 * with captured context". Retry logic is applied ONLY if the worker node leaves the cluster. If the job itself is failing, then the
 * exception is propagated to the caller and this class does not handle it.
 *
 * <p>If you want to execute a job on node1 and use node2 and node3 as failover candidates,
 * then you should create an instance of this class with workerNode = node1, failoverCandidates = [node2, node3] as arguments and call
 * {@link #failSafeExecute()}.
 */
class ComputeJobFailover {
    private static final IgniteLogger LOG = Loggers.forClass(ComputeJobFailover.class);

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
     * Thread to run failover logic. We can not perform time-consuming operations in the same thread where we discover topology changes (it
     * is network id thread).
     */
    private final Executor executor;

    private final EventLog eventLog;
    /**
     * The node where the job is being executed at a given moment. If node leaves the cluster, then the job is restarted on one of the
     * worker node returned by {@link #nextWorkerSelector} and the reference is CASed to the new node.
     */
    private final AtomicReference<InternalClusterNode> runningWorkerNode;

    /**
     * The selector that returns the next worker node to execute job on.
     */
    private final NextWorkerSelector nextWorkerSelector;

    /**
     * Context of the called job. Captures deployment units, jobClassName and arguments.
     */
    private final ExecutionContext jobContext;

    /**
     * Job id of the execution.
     */
    private final UUID jobId = UUID.randomUUID();

    private FailSafeJobExecution failSafeExecution;

    /**
     * Creates a per-job instance.
     *
     * @param computeComponent compute component.
     * @param logicalTopologyService logical topology service.
     * @param topologyService physical topology service.
     * @param executor the thread pool where the failover should run on.
     * @param eventLog Event log.
     * @param workerNode the node to execute the job on.
     * @param nextWorkerSelector the selector that returns the next worker to execute job on.
     * @param executionOptions execution options like priority or max retries.
     * @param units deployment units.
     * @param jobClassName the name of the job class.
     * @param metadataBuilder Event metadata builder.
     * @param arg the arguments of the job.
     */
    private ComputeJobFailover(
            ComputeComponent computeComponent,
            LogicalTopologyService logicalTopologyService,
            TopologyService topologyService,
            Executor executor,
            EventLog eventLog,
            InternalClusterNode workerNode,
            NextWorkerSelector nextWorkerSelector,
            ExecutionContext executionContext
    ) {
        this.computeComponent = computeComponent;
        this.logicalTopologyService = logicalTopologyService;
        this.topologyService = topologyService;
        this.executor = executor;
        this.eventLog = eventLog;
        this.runningWorkerNode = new AtomicReference<>(workerNode);
        this.nextWorkerSelector = nextWorkerSelector;

        // Assign failover job id so that it is consistent for any remote job.
        executionContext.metadataBuilder().jobId(jobId);

        this.jobContext = executionContext;
    }

    static CompletableFuture<CancellableJobExecution<ComputeJobDataHolder>> failSafeExecute(
            ComputeComponent computeComponent,
            LogicalTopologyService logicalTopologyService,
            TopologyService topologyService,
            Executor executor,
            EventLog eventLog,
            InternalClusterNode workerNode,
            NextWorkerSelector nextWorkerSelector,
            ExecutionContext executionContext
    ) {
        return new ComputeJobFailover(
                computeComponent,
                logicalTopologyService,
                topologyService,
                executor,
                eventLog,
                workerNode,
                nextWorkerSelector,
                executionContext
        ).execute();
    }

    /**
     * Executes a job on the worker node and restarts the job on one of the failover candidates if the worker node leaves the cluster.
     *
     * @return JobExecution with the result of the job and the status of the job.
     */
    private CompletableFuture<CancellableJobExecution<ComputeJobDataHolder>> execute() {
        return launchJobOn(runningWorkerNode.get())
                .thenApply(execution -> {
                    failSafeExecution = new FailSafeJobExecution(execution, jobId);

                    LogicalTopologyEventListener nodeLeftEventListener = new OnNodeLeft();
                    logicalTopologyService.addEventListener(nodeLeftEventListener);
                    failSafeExecution.resultAsync()
                            .whenComplete((r, e) -> logicalTopologyService.removeEventListener(nodeLeftEventListener));

                    return failSafeExecution;
                });
    }

    private CompletableFuture<CancellableJobExecution<ComputeJobDataHolder>> launchJobOn(InternalClusterNode runningWorkerNode) {
        if (runningWorkerNode.name().equals(topologyService.localMember().name())) {
            return computeComponent.executeLocally(jobContext, null);
        } else {
            return computeComponent.executeRemotely(runningWorkerNode, jobContext, null);
        }
    }

    private class OnNodeLeft implements LogicalTopologyEventListener {
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

                            logJobFailedEvent();

                            failSafeExecution.completeExceptionally(new IgniteInternalException(Compute.COMPUTE_JOB_FAILED_ERR));
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

                        launchJobOn(nextWorker).thenAccept(execution -> failSafeExecution.updateJobExecution(execution));
                    });
        }

        private void logJobFailedEvent() {
            // Fill missing fields
            ComputeEventMetadata eventMetadata = jobContext.metadataBuilder()
                    .jobClassName(jobContext.jobClassName())
                    .targetNode(runningWorkerNode.get().name()) // Use last worker node
                    .build();

            ComputeEventsFactory.logJobFailedEvent(eventLog, eventMetadata);
        }
    }
}
