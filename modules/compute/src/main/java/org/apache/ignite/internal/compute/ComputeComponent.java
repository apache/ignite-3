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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.compute.task.TaskExecution;
import org.apache.ignite.internal.compute.task.JobSubmitter;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * Compute functionality.
 */
public interface ComputeComponent extends IgniteComponent {
    /**
     * Executes a job of the given class on the current node.
     *
     * @param options Job execution options.
     * @param units Deployment units which will be loaded for execution.
     * @param jobClassName Name of the job class.
     * @param input Job args.
     * @param <R> Job result type.
     * @return Job execution object.
     */
    <T, R> JobExecution<R> executeLocally(
            ExecutionOptions options,
            List<DeploymentUnit> units,
            String jobClassName,
            T input
    );

    /**
     * Executes a job of the given class on the current node with default execution options {@link ExecutionOptions#DEFAULT}.
     *
     * @param units Deployment units which will be loaded for execution.
     * @param jobClassName Name of the job class.
     * @param input Job args.
     * @param <R> Job result type.
     * @return Job execution object.
     */
    default <T, R> JobExecution<R> executeLocally(
            List<DeploymentUnit> units,
            String jobClassName,
            T input
    ) {
        return executeLocally(ExecutionOptions.DEFAULT, units, jobClassName, input);
    }

    /**
     * Executes a job of the given class on a remote node.
     *
     * @param options Job execution options.
     * @param remoteNode Remote node name.
     * @param units Deployment units which will be loaded for execution.
     * @param jobClassName Name of the job class.
     * @param input Job args.
     * @param <R> Job result type.
     * @return Job execution object.
     */
    <T, R> JobExecution<R> executeRemotely(
            ExecutionOptions options,
            ClusterNode remoteNode,
            List<DeploymentUnit> units,
            String jobClassName,
            T input
    );

    /**
     * Executes a job of the given class on a remote node with default execution options {@link ExecutionOptions#DEFAULT}.
     *
     * @param remoteNode Remote node name.
     * @param units Deployment units which will be loaded for execution.
     * @param jobClassName Name of the job class.
     * @param input Job args.
     * @param <R> Job result type.
     * @return Job execution object.
     */
    default <T, R> JobExecution<R> executeRemotely(
            ClusterNode remoteNode,
            List<DeploymentUnit> units,
            String jobClassName,
            T input
    ) {
        return executeRemotely(ExecutionOptions.DEFAULT, remoteNode, units, jobClassName,input );
    }

    /**
     * Executes a job of the given class on a remote node. If the node leaves the cluster, it will be restarted on the node given by the
     * {@code nextWorkerSelector}.
     *
     * @param remoteNode Remote node name.
     * @param nextWorkerSelector The selector that returns the next worker to execute job on.
     * @param options Job execution options.
     * @param units Deployment units which will be loaded for execution.
     * @param jobClassName Name of the job class.
     * @param input Job args.
     * @param <R> Job result type.
     * @return Job execution object.
     */
    <T, R> JobExecution<R> executeRemotelyWithFailover(
            ClusterNode remoteNode,
            NextWorkerSelector nextWorkerSelector,
            List<DeploymentUnit> units,
            String jobClassName,
            ExecutionOptions options,
            T input
    );

    /**
     * Executes a task of the given class.
     *
     * @param jobSubmitter Function which submits a job with specified parameters for the execution.
     * @param units Deployment units which will be loaded for execution.
     * @param taskClassName Name of the task class.
     * @param input Task args.
     * @param <R> Task result type.
     * @return Task execution object.
     */
    <T, R> TaskExecution<R> executeTask(
            JobSubmitter jobSubmitter,
            List<DeploymentUnit> units,
            String taskClassName,
            T input
    );

    /**
     * Retrieves the current status of all jobs on all nodes in the cluster.
     *
     * @return The collection of job statuses.
     */
    CompletableFuture<Collection<JobStatus>> statusesAsync();

    /**
     * Retrieves the current status of the job on any node in the cluster. The job status may be deleted and thus return {@code null} if the
     * time for retaining job status has been exceeded.
     *
     * @param jobId Job id.
     * @return The current status of the job, or {@code null} if the job status no longer exists due to exceeding the retention time limit.
     */
    CompletableFuture<@Nullable JobStatus> statusAsync(UUID jobId);

    /**
     * Cancels the job running on any node in the cluster.
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
