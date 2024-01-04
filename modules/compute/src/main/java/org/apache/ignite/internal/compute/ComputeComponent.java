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
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobStatus;
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
     * Returns job status by ID.
     *
     * @param jobId Job ID.
     * @return Job status. {@code null} if job with the given ID does not exist.
     */
    @Nullable
    JobStatus getJobStatus(UUID jobId);
}
