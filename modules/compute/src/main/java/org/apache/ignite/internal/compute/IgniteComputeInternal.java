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
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.marshalling.Marshaller;
import org.apache.ignite.network.ClusterNode;
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
     * @param <R> Job result type.
     * @param nodes Candidate nodes; In case target node left the cluster, the job will be restarted on one of them.
     * @param units Deployment units. Can be empty.
     * @param jobClassName Name of the job class to execute.
     * @param options Job execution options.
     * @param argumentMarshaller Marshaller for the job argument.
     * @param resultMarshaller Marshaller for the job result.
     * @param payload Arguments of the job.
     * @return CompletableFuture Job result.
     */
    <R> JobExecution<R> executeAsyncWithFailover(
            Set<ClusterNode> nodes,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            @Nullable Marshaller<Object, byte[]> argumentMarshaller,
            @Nullable Marshaller<R, byte[]> resultMarshaller,
            Object payload
    );

    /**
     * Submits a job of the given class for the execution on the node where the given key is located. The node is a leader of the
     * corresponding RAFT group.
     *
     * @param table Table whose key is used to determine the node to execute the job on.
     * @param key Key that identifies the node to execute the job on.
     * @param units Deployment units. Can be empty.
     * @param jobClassName Name of the job class to execute.
     * @param options job execution options (priority, max retries).
     * @param argumentMarshaller Marshaller for the job argument.
     * @param resultMarshaller Marshaller for the job result.
     * @param payload Arguments of the job.
     * @param <R> Job result type.
     * @return Job execution object.
     */
    <R> CompletableFuture<JobExecution<R>> submitColocatedInternal(
            TableViewInternal table,
            Tuple key,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            @Nullable Marshaller<Object, byte[]> argumentMarshaller,
            @Nullable Marshaller<R, byte[]> resultMarshaller,
            Object payload);

    /**
     * Wraps the given future into a job execution object.
     *
     * @param fut Future to wrap.
     * @param <R> Job result type.
     * @return Job execution object.
     */
    default <R> JobExecution<R> wrapJobExecutionFuture(CompletableFuture<JobExecution<R>> fut) {
        return new JobExecutionFutureWrapper<>(fut);
    }

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
