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

package org.apache.ignite.compute;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.task.MapReduceTask;
import org.apache.ignite.compute.task.TaskExecution;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * Provides the ability to execute Compute jobs.
 *
 * @see ComputeJob
 * @see ComputeJob#executeAsync(JobExecutionContext, Object...)
 */
public interface IgniteCompute {
    /**
     * Submits a {@link ComputeJob} of the given class for an execution on a single node from a set of candidate nodes.
     *
     * @param <R> Job result type.
     * @param target Execution target.
     * @param descriptor Job descriptor.
     * @param args Arguments of the job.
     * @return Job execution object.
     */
    <T, R> JobExecution<R> submit(
            JobTarget target,
            JobDescriptor descriptor,
            T args
    );

    /**
     * Submits a {@link ComputeJob} of the given class for an execution on a single node from a set of candidate nodes. A shortcut for
     * {@code submit(...).resultAsync()}.
     *
     * @param <R> Job result type.
     * @param target Execution target.
     * @param descriptor Job descriptor.
     * @param args Arguments of the job.
     * @return Job result future.
     */
    default <T, R> CompletableFuture<R> executeAsync(
            JobTarget target,
            JobDescriptor descriptor,
            T args
    ) {
        return this.<T, R>submit(target, descriptor, args).resultAsync();
    }

    /**
     * Executes a {@link ComputeJob} of the given class on a single node from a set of candidate nodes.
     *
     * @param <R> Job result type
     * @param target Execution target.
     * @param descriptor Job descriptor.
     * @param args Arguments of the job.
     * @return Job result.
     * @throws ComputeException If there is any problem executing the job.
     */
    <T, R> R execute(
            JobTarget target,
            JobDescriptor descriptor,
            @Nullable T args
    );

    /**
     * Submits a {@link ComputeJob} of the given class for an execution on all nodes in the given node set.
     *
     * @param <R> Job result type.
     * @param nodes Nodes to execute the job on.
     * @param descriptor Job descriptor.
     * @param args Arguments of the job.
     * @return Map from node to job execution object.
     */
    <T, R> Map<ClusterNode, JobExecution<R>> submitBroadcast(
            Set<ClusterNode> nodes,
            JobDescriptor descriptor,
            T args
    );

    /**
     * Executes a {@link ComputeJob} of the given class on all nodes in the given node set.
     *
     * @param <R> Job result type.
     * @param nodes Nodes to execute the job on.
     * @param descriptor Job descriptor.
     * @param args Arguments of the job.
     * @return Map from node to job result.
     */
    default <T, R> CompletableFuture<Map<ClusterNode, R>> executeBroadcastAsync(
            Set<ClusterNode> nodes,
            JobDescriptor descriptor,
            @Nullable T args
    ) {
        Map<ClusterNode, CompletableFuture<R>> futures = nodes.stream()
                .collect(toMap(identity(), node -> this.executeAsync(JobTarget.node(node), descriptor, args)));

        return allOf(futures.values().toArray(CompletableFuture[]::new))
                .thenApply(ignored -> {
                            Map<ClusterNode, R> map = new HashMap<>();

                            for (Entry<ClusterNode, CompletableFuture<R>> entry : futures.entrySet()) {
                                map.put(entry.getKey(), entry.getValue().join());
                            }

                            return map;
                        }
                );
    }

    /**
     * Executes a {@link ComputeJob} of the given class on all nodes in the given node set.
     *
     * @param <R> Job result type.
     * @param nodes Nodes to execute the job on.
     * @param descriptor Job descriptor.
     * @param args Arguments of the job.
     * @return Map from node to job result.
     * @throws ComputeException If there is any problem executing the job.
     */
    default <T, R> Map<ClusterNode, R> executeBroadcast(
            Set<ClusterNode> nodes,
            JobDescriptor descriptor,
            @Nullable T args
    ) {
        Map<ClusterNode, R> map = new HashMap<>();

        for (ClusterNode node : nodes) {
            map.put(node, execute(JobTarget.node(node), descriptor, args));
        }

        return map;
    }

    /**
     * Submits a {@link MapReduceTask} of the given class for an execution.
     *
     * @param units Deployment units.
     * @param taskClassName Map reduce task class name.
     * @param args Task arguments.
     * @param <R> Task result type.
     * @return Task execution interface.
     */
    <T, R> TaskExecution<R> submitMapReduce(List<DeploymentUnit> units, String taskClassName, @Nullable T args);

    /**
     * Submits a {@link MapReduceTask} of the given class for an execution. A shortcut for {@code submitMapReduce(...).resultAsync()}.
     *
     * @param units Deployment units.
     * @param taskClassName Map reduce task class name.
     * @param input Task arguments.
     * @param <R> Task result type.
     * @return Task result future.
     */
    default <T, R> CompletableFuture<R> executeMapReduceAsync(List<DeploymentUnit> units, String taskClassName, T input) {
        return this.<T, R>submitMapReduce(units, taskClassName, input).resultAsync();
    }

    /**
     * Executes a {@link MapReduceTask} of the given class.
     *
     * @param units Deployment units.
     * @param taskClassName Map reduce task class name.
     * @param args Task arguments.
     * @param <R> Task result type.
     * @return Task result.
     * @throws ComputeException If there is any problem executing the task.
     */
    <T, R> R executeMapReduce(List<DeploymentUnit> units, String taskClassName, @Nullable T args);
}
