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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.network.ClusterNode;

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
     * @param args Job args.
     * @param <R> Job result type.
     * @return Future execution result.
     */
    <R> JobExecution<R> executeLocally(
            ExecutionOptions options,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    );

    /**
     * Executes a job of the given class on the current node with default execution options {@link ExecutionOptions#DEFAULT}.
     *
     * @param units Deployment units which will be loaded for execution.
     * @param jobClassName Name of the job class.
     * @param args Job args.
     * @param <R> Job result type.
     * @return Future execution result.
     */
    default <R> JobExecution<R> executeLocally(
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        return executeLocally(ExecutionOptions.DEFAULT, units, jobClassName, args);
    }

    /**
     * Executes a job of the given class on a remote node.
     *
     * @param options Job execution options.
     * @param remoteNode Name of the job class.
     * @param units Deployment units which will be loaded for execution.
     * @param jobClassName Name of the job class.
     * @param args Job args.
     * @param <R> Job result type.
     * @return Future execution result.
     */
    <R> JobExecution<R> executeRemotely(
            ExecutionOptions options,
            ClusterNode remoteNode,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    );

    /**
     * Executes a job of the given class on a remote node with default execution options {@link ExecutionOptions#DEFAULT}.
     *
     * @param remoteNode Name of the job class.
     * @param units Deployment units which will be loaded for execution.
     * @param jobClassName Name of the job class.
     * @param args Job args.
     * @param <R> Job result type.
     * @return Future execution result.
     */
    default <R> JobExecution<R> executeRemotely(
            ClusterNode remoteNode,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        return executeRemotely(ExecutionOptions.DEFAULT, remoteNode, units, jobClassName, args);
    }

    /**
     * Returns job's execution result.
     *
     * @param jobId Job id.
     * @return Job's execution result future.
     */
    CompletableFuture<?> resultAsync(UUID jobId);

    /**
     * Retrieves the current status of the job on any node in the cluster. The job status may be deleted and thus return {@code null} if the
     * time for retaining job status has been exceeded.
     *
     * @param jobId Job id.
     * @return The current status of the job, or {@code null} if the job status no longer exists due to exceeding the retention time limit.
     */
    CompletableFuture<JobStatus> broadcastStatusAsync(UUID jobId);

    /**
     * Retrieves the current status of the job from local node. The job status may be deleted and thus return {@code null} if the time for
     * retaining job status has been exceeded.
     *
     * @param jobId Job id.
     * @return The current status of the job, or {@code null} if the job status no longer exists due to exceeding the retention time limit.
     */
    CompletableFuture<JobStatus> localStatusAsync(UUID jobId);

    /**
     * Cancels the job running on any node in the cluster.
     *
     * @param jobId Job id.
     * @return The future which will be completed when cancel request is processed.
     */
    CompletableFuture<Void> broadcastCancelAsync(UUID jobId);

    /**
     * Cancels the locally running job.
     *
     * @param jobId Job id.
     * @return The future which will be completed when cancel request is processed.
     */
    CompletableFuture<Void> localCancelAsync(UUID jobId);
}
