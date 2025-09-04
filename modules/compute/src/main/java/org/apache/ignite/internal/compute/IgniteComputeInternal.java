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

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobExecutionOptions;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.compute.TaskDescriptor;
import org.apache.ignite.compute.task.MapReduceTask;
import org.apache.ignite.compute.task.TaskExecution;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.internal.compute.events.ComputeEventMetadataBuilder;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.lang.CancellationToken;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;

/**
 * Internal compute facade.
 */
public interface IgniteComputeInternal extends IgniteCompute {
    /**
     * Executes a {@link ComputeJob} of the given class on a single node. If the node leaves the cluster, it will be restarted on one of the
     * candidate nodes.
     *
     * @param nodes Candidate nodes; In case target node left the cluster, the job will be restarted on one of them.
     * @param executionContext Execution context.
     * @param cancellationToken Cancellation token or {@code null}.
     * @return CompletableFuture Job result.
     */
    CompletableFuture<JobExecution<ComputeJobDataHolder>> executeAsyncWithFailover(
            Set<InternalClusterNode> nodes,
            ExecutionContext executionContext,
            @Nullable CancellationToken cancellationToken
    );

    /**
     * Submits a job of the given class for the execution on the node where the given key is located. The node is a leader of the
     * corresponding RAFT group.
     *
     * @param table Table whose key is used to determine the node to execute the job on.
     * @param key Key that identifies the node to execute the job on.
     * @param executionContext Execution context.
     * @param cancellationToken Cancellation token or {@code null}.
     * @return Job execution object.
     */
    CompletableFuture<JobExecution<ComputeJobDataHolder>> submitColocatedInternal(
            TableViewInternal table,
            Tuple key,
            ExecutionContext executionContext,
            @Nullable CancellationToken cancellationToken
    );

    /**
     * Submits a job of the given class for the execution on the node where the given partition's primary replica is located.
     *
     * @param table Table whose partition is used to determine the node to execute the job on.
     * @param partitionId Partition identifier.
     * @param units Deployment units. Can be empty.
     * @param jobClassName Name of the job class to execute.
     * @param options job execution options (priority, max retries).
     * @param metadataBuilder Event metadata builder.
     * @param arg Argument of the job.
     * @param cancellationToken Cancellation token or {@code null}.
     * @return Job execution object.
     */
    CompletableFuture<JobExecution<ComputeJobDataHolder>> submitPartitionedInternal(
            TableViewInternal table,
            int partitionId,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            ComputeEventMetadataBuilder metadataBuilder,
            @Nullable ComputeJobDataHolder arg,
            @Nullable CancellationToken cancellationToken
    );

    /**
     * Submits a {@link MapReduceTask} of the given class for an execution.
     *
     * @param <T> Job argument (T)ype.
     * @param <R> Job (R)esult type.
     * @param taskDescriptor Map reduce task descriptor.
     * @param metadataBuilder Event metadata builder.
     * @param arg Task argument.
     * @param cancellationToken Cancellation token or {@code null}.
     * @return Task execution interface.
     */
    <T, R> TaskExecution<R> submitMapReduceInternal(
            TaskDescriptor<T, R> taskDescriptor,
            ComputeEventMetadataBuilder metadataBuilder,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    );

    /**
     * Retrieves the current state of all jobs on all nodes in the cluster.
     *
     * @return The collection of job states.
     */
    CompletableFuture<Collection<JobState>> statesAsync();

    /**
     * Gets job state by id.
     *
     * @param jobId Job id.
     * @return Job state or {@code null} if there's no state registered for this id.
     */
    CompletableFuture<@Nullable JobState> stateAsync(UUID jobId);

    /**
     * Cancels compute job.
     *
     * @param jobId Job id.
     * @return The future which will be completed with {@code true} when the job is cancelled, {@code false} when the job couldn't be
     *         cancelled (either it's not yet started, or it's already completed), or {@code null} if there's no job with the specified id.
     */
    CompletableFuture<@Nullable Boolean> cancelAsync(UUID jobId);

    /**
     * Changes compute job priority.
     *
     * @param jobId Job id.
     * @param newPriority New priority.
     * @return The future which will be completed with {@code true} when the priority is changed, {@code false} when the priority couldn't
     *         be changed (it's already executing or completed), or {@code null} if there's no job with the specified id.
     */
    CompletableFuture<@Nullable Boolean> changePriorityAsync(UUID jobId, int newPriority);
}
