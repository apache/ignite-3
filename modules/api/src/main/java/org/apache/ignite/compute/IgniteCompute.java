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
import static org.apache.ignite.compute.JobExecutionOptions.DEFAULT;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.task.MapReduceTask;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;

/**
 * Provides the ability to execute Compute jobs.
 *
 * @see ComputeJob
 * @see ComputeJob#execute(JobExecutionContext, Object...)
 */
public interface IgniteCompute {
    /**
     * Submits a {@link ComputeJob} of the given class for an execution on a single node from a set of candidate nodes.
     *
     * @param <R> Job result type.
     * @param nodes Candidate nodes; the job will be executed on one of them.
     * @param descriptor Job descriptor.
     * @param args Arguments of the job.
     * @return Job execution object.
     */
    <R> JobExecution<R> submit(
            Set<ClusterNode> nodes,
            JobDescriptor descriptor,
            Object... args
    );

    /**
     * Submits a {@link ComputeJob} of the given class for an execution on a single node from a set of candidate nodes. A shortcut for
     * {@code submit(...).resultAsync()}.
     *
     * @param <R> Job result type.
     * @param nodes Candidate nodes; the job will be executed on one of them.
     * @param descriptor Job descriptor.
     * @param args Arguments of the job.
     * @return Job result future.
     */
    default <R> CompletableFuture<R> executeAsync(
            Set<ClusterNode> nodes,
            JobDescriptor descriptor,
            Object... args
    ) {
        return this.<R>submit(nodes, descriptor, args).resultAsync();
    }

    /**
     * Executes a {@link ComputeJob} of the given class on a single node from a set of candidate nodes.
     *
     * @param <R> Job result type
     * @param nodes Candidate nodes; the job will be executed on one of them.
     * @param descriptor Job descriptor.
     * @param args Arguments of the job.
     * @return Job result.
     * @throws ComputeException If there is any problem executing the job.
     */
    <R> R execute(
            Set<ClusterNode> nodes,
            JobDescriptor descriptor,
            Object... args
    );

    /**
     * Submits a job of the given class for the execution on the node where the given key is located. The node is a leader of the
     * corresponding RAFT group.
     *
     * @param tableName Name of the table whose key is used to determine the node to execute the job on.
     * @param key Key that identifies the node to execute the job on.
     * @param descriptor Job descriptor.
     * @param args Arguments of the job.
     * @param <R> Job result type.
     * @return Job execution object.
     */
    <R> JobExecution<R> submitColocated(
            String tableName,
            Tuple key,
            JobDescriptor descriptor,
            Object... args
    );

    /**
     * Submits a job of the given class for the execution on the node where the given key is located. The node is a leader of the
     * corresponding RAFT group.
     *
     * @param tableName Name of the table whose key is used to determine the node to execute the job on.
     * @param key Key that identifies the node to execute the job on.
     * @param keyMapper Mapper used to map the key to a binary representation.
     * @param descriptor Job descriptor.
     * @param args Arguments of the job.
     * @param <R> Job result type.
     * @return Job execution object.
     */
    <K, R> JobExecution<R> submitColocated(
            String tableName,
            K key,
            Mapper<K> keyMapper,
            JobDescriptor descriptor,
            Object... args
    );

    /**
     * Submits a job of the given class for the execution on the node where the given key is located. The node is a leader of the
     * corresponding RAFT group.
     *
     * @param tableName Name of the table whose key is used to determine the node to execute the job on.
     * @param key Key that identifies the node to execute the job on.
     * @param keyMapper Mapper used to map the key to a binary representation.
     * @param units Deployment units. Can be empty.
     * @param jobClassName Name of the job class to execute.
     * @param args Arguments of the job.
     * @param options Job execution options (priority, max retries).
     * @param <R> Job result type.
     * @return Job execution object.
     */
    default <K, R> JobExecution<R> submitColocated(
            String tableName,
            K key,
            Mapper<K> keyMapper,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            Object... args
    ) {
        return submitColocated(tableName, key, keyMapper, JobDescriptor.builder()
                .jobClassName(jobClassName)
                .units(units)
                .options(options)
                .build(), args);
    }

    /**
     * Submits a job of the given class for the execution on the node where the given key is located with default execution options
     * {@link JobExecutionOptions#DEFAULT}. The node is a leader of the corresponding RAFT group.
     *
     * @param tableName Name of the table whose key is used to determine the node to execute the job on.
     * @param key Key that identifies the node to execute the job on.
     * @param keyMapper Mapper used to map the key to a binary representation.
     * @param units Deployment units. Can be empty.
     * @param jobClassName Name of the job class to execute.
     * @param args Arguments of the job.
     * @param <R> Job result type.
     * @return Job execution object.
     */
    default <K, R> JobExecution<R> submitColocated(
            String tableName,
            K key,
            Mapper<K> keyMapper,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        return submitColocated(tableName, key, keyMapper, units, jobClassName, DEFAULT, args);
    }

    /**
     * Submits a job of the given class for the execution on the node where the given key is located. The node is a leader of the
     * corresponding RAFT group. A shortcut for {@code submitColocated(...).resultAsync()}.
     *
     * @param tableName Name of the table whose key is used to determine the node to execute the job on.
     * @param key Key that identifies the node to execute the job on.
     * @param units Deployment units. Can be empty.
     * @param jobClassName Name of the job class to execute.
     * @param options Job execution options (priority, max retries).
     * @param args Arguments of the job.
     * @param <R> Job result type.
     * @return Job result future.
     */
    default <R> CompletableFuture<R> executeColocatedAsync(
            String tableName,
            Tuple key,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            Object... args
    ) {
        return this.<R>submitColocated(tableName, key, JobDescriptor.builder()
                .jobClassName(jobClassName)
                .units(units)
                .options(options)
                .build(), args).resultAsync();
    }

    /**
     * Submits a job of the given class for the execution on the node where the given key is located with default execution options
     * {@link JobExecutionOptions#DEFAULT}. The node is a leader of the corresponding RAFT group. A shortcut for
     * {@code submitColocated(...).resultAsync()}.
     *
     * @param tableName Name of the table whose key is used to determine the node to execute the job on.
     * @param key Key that identifies the node to execute the job on.
     * @param units Deployment units. Can be empty.
     * @param jobClassName Name of the job class to execute.
     * @param args Arguments of the job.
     * @param <R> Job result type.
     * @return Job result future.
     */
    default <R> CompletableFuture<R> executeColocatedAsync(
            String tableName,
            Tuple key,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        return this.<R>submitColocated(tableName, key, JobDescriptor.builder()
                .jobClassName(jobClassName)
                .units(units)
                .options(DEFAULT)
                .build(), args).resultAsync();
    }

    /**
     * Submits a job of the given class for the execution on the node where the given key is located. The node is a leader of the
     * corresponding RAFT group. A shortcut for {@code submitColocated(...).resultAsync()}.
     *
     * @param tableName Name of the table whose key is used to determine the node to execute the job on.
     * @param key Key that identifies the node to execute the job on.
     * @param keyMapper Mapper used to map the key to a binary representation.
     * @param units Deployment units. Can be empty.
     * @param jobClassName Name of the job class to execute.
     * @param args Arguments of the job.
     * @param options Job execution options (priority, max retries).
     * @param <R> Job result type.
     * @return Job result future.
     */
    default <K, R> CompletableFuture<R> executeColocatedAsync(
            String tableName,
            K key,
            Mapper<K> keyMapper,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            Object... args
    ) {
        return this.<K, R>submitColocated(tableName, key, keyMapper, units, jobClassName, options, args).resultAsync();
    }

    /**
     * Submits a job of the given class for the execution on the node where the given key is located with default execution options
     * {@link JobExecutionOptions#DEFAULT}. The node is a leader of the corresponding RAFT group. A shortcut for
     * {@code submitColocated(...).resultAsync()}.
     *
     * @param tableName Name of the table whose key is used to determine the node to execute the job on.
     * @param key Key that identifies the node to execute the job on.
     * @param keyMapper Mapper used to map the key to a binary representation.
     * @param units Deployment units. Can be empty.
     * @param jobClassName Name of the job class to execute.
     * @param args Arguments of the job.
     * @param <R> Job result type.
     * @return Job result future.
     */
    default <K, R> CompletableFuture<R> executeColocatedAsync(
            String tableName,
            K key,
            Mapper<K> keyMapper,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        return this.<K, R>submitColocated(tableName, key, keyMapper, units, jobClassName, args).resultAsync();
    }

    /**
     * Executes a job of the given class on the node where the given key is located. The node is a leader of the corresponding RAFT group.
     *
     * @param <R> Job result type.
     * @param tableName Name of the table whose key is used to determine the node to execute the job on.
     * @param key Key that identifies the node to execute the job on.
     * @param units Deployment units. Can be empty.
     * @param jobClassName Name of the job class to execute.
     * @param options Job execution options (priority, max retries).
     * @param args Arguments of the job.
     * @return Job result.
     * @throws ComputeException If there is any problem executing the job.
     */
    <R> R executeColocated(
            String tableName,
            Tuple key,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            Object... args);

    /**
     * Executes a job of the given class on the node where the given key is located
     * with default execution options {@link JobExecutionOptions#DEFAULT}.
     * The node is a leader of the corresponding RAFT group.
     *
     * @param tableName Name of the table whose key is used to determine the node to execute the job on.
     * @param key Key that identifies the node to execute the job on.
     * @param units Deployment units. Can be empty.
     * @param jobClassName Name of the job class to execute.
     * @param args Arguments of the job.
     * @param <R> Job result type.
     * @return Job result.
     * @throws ComputeException If there is any problem executing the job.
     */
    default <R> R executeColocated(
            String tableName,
            Tuple key,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        return executeColocated(tableName, key, units, jobClassName, DEFAULT, args);
    }

    /**
     * Executes a job of the given class on the node where the given key is located. The node is a leader of the corresponding RAFT group.
     *
     * @param <R> Job result type.
     * @param tableName Name of the table whose key is used to determine the node to execute the job on.
     * @param key Key that identifies the node to execute the job on.
     * @param keyMapper Mapper used to map the key to a binary representation.
     * @param units Deployment units. Can be empty.
     * @param jobClassName Name of the job class to execute.
     * @param options Job execution options (priority, max retries).
     * @param args Arguments of the job.
     * @return Job result.
     * @throws ComputeException If there is any problem executing the job.
     */
    <K, R> R executeColocated(
            String tableName,
            K key,
            Mapper<K> keyMapper,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            Object... args);

    /**
     * Executes a job of the given class on the node where the given key is located. The node is a leader
     * of the corresponding RAFT group.
     *
     * @param tableName Name of the table whose key is used to determine the node to execute the job on.
     * @param key Key that identifies the node to execute the job on.
     * @param keyMapper Mapper used to map the key to a binary representation.
     * @param units Deployment units. Can be empty.
     * @param jobClassName Name of the job class to execute.
     * @param args Arguments of the job.
     * @param <R> Job result type.
     * @return Job result.
     * @throws ComputeException If there is any problem executing the job.
     */
    default <K, R> R executeColocated(
            String tableName,
            K key,
            Mapper<K> keyMapper,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        return executeColocated(tableName, key, keyMapper, units, jobClassName, DEFAULT, args);
    }

    /**
     * Submits a {@link ComputeJob} of the given class for an execution on all nodes in the given node set.
     *
     * @param <R> Job result type.
     * @param nodes Nodes to execute the job on.
     * @param units Deployment units. Can be empty.
     * @param jobClassName Name of the job class to execute.
     * @param options Job execution options (priority, max retries).
     * @param args Arguments of the job.
     * @return Map from node to job execution object.
     */
    <R> Map<ClusterNode, JobExecution<R>> submitBroadcast(
            Set<ClusterNode> nodes,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            Object... args
    );

    /**
     * Submits a {@link ComputeJob} of the given class for an execution on all nodes in the given node set
     * with default execution options {@link JobExecutionOptions#DEFAULT}.
     *
     * @param nodes Nodes to execute the job on.
     * @param units Deployment units. Can be empty.
     * @param jobClassName Name of the job class to execute.
     * @param args Arguments of the job.
     * @param <R> Job result type.
     * @return Map from node to job execution object.
     */
    default <R> Map<ClusterNode, JobExecution<R>> submitBroadcast(
            Set<ClusterNode> nodes,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        return submitBroadcast(nodes, units, jobClassName, DEFAULT, args);
    }

    /**
     * Executes a {@link ComputeJob} of the given class on all nodes in the given node set.
     *
     * @param <R> Job result type.
     * @param nodes Nodes to execute the job on.
     * @param units Deployment units. Can be empty.
     * @param jobClassName Name of the job class to execute.
     * @param options Job execution options (priority, max retries).
     * @param args Arguments of the job.
     * @return Map from node to job result.
     */
    default <R> CompletableFuture<Map<ClusterNode, R>> executeBroadcastAsync(
            Set<ClusterNode> nodes,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            Object... args
    ) {
        Map<ClusterNode, CompletableFuture<R>> futures = nodes.stream()
                .collect(toMap(identity(), node -> this.executeAsync(Set.of(node), JobDescriptor.builder()
                        .jobClassName(jobClassName)
                        .units(units)
                        .options(options)
                        .build(), args)));

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
     * Executes a {@link ComputeJob} of the given class on all nodes in the given node set
     * with default execution options {@link JobExecutionOptions#DEFAULT}.
     *
     * @param nodes Nodes to execute the job on.
     * @param units Deployment units. Can be empty.
     * @param jobClassName Name of the job class to execute.
     * @param args Arguments of the job.
     * @param <R> Job result type.
     * @return Map from node to job result.
     */
    default <R> CompletableFuture<Map<ClusterNode, R>> executeBroadcastAsync(
            Set<ClusterNode> nodes,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        return executeBroadcastAsync(nodes, units, jobClassName, DEFAULT, args);
    }

    /**
     * Executes a {@link ComputeJob} of the given class on all nodes in the given node set.
     *
     * @param <R> Job result type.
     * @param nodes Nodes to execute the job on.
     * @param units Deployment units. Can be empty.
     * @param jobClassName Name of the job class to execute.
     * @param options Job execution options (priority, max retries).
     * @param args Arguments of the job.
     * @return Map from node to job result.
     * @throws ComputeException If there is any problem executing the job.
     */
    default <R> Map<ClusterNode, R> executeBroadcast(
            Set<ClusterNode> nodes,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            Object... args
    ) {
        Map<ClusterNode, R> map = new HashMap<>();

        for (ClusterNode node : nodes) {
            map.put(node, execute(Set.of(node), JobDescriptor.builder()
                    .jobClassName(jobClassName)
                    .units(units)
                    .options(options)
                    .build(), args));
        }

        return map;
    }

    /**
     * Executes a {@link ComputeJob} of the given class on all nodes in the given node set
     * with default execution options {@link JobExecutionOptions#DEFAULT}.
     *
     * @param nodes Nodes to execute the job on.
     * @param units Deployment units. Can be empty.
     * @param jobClassName Name of the job class to execute.
     * @param args Arguments of the job.
     * @param <R> Job result type.
     * @return Map from node to job result.
     * @throws ComputeException If there is any problem executing the job.
     */
    default <R> Map<ClusterNode, R> executeBroadcast(
            Set<ClusterNode> nodes,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        return executeBroadcast(nodes, units, jobClassName, DEFAULT, args);
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
    <R> TaskExecution<R> submitMapReduce(List<DeploymentUnit> units, String taskClassName, Object... args);

    /**
     * Submits a {@link MapReduceTask} of the given class for an execution. A shortcut for {@code submitMapReduce(...).resultAsync()}.
     *
     * @param units Deployment units.
     * @param taskClassName Map reduce task class name.
     * @param args Task arguments.
     * @param <R> Task result type.
     * @return Task result future.
     */
    default <R> CompletableFuture<R> executeMapReduceAsync(List<DeploymentUnit> units, String taskClassName, Object... args) {
        return this.<R>submitMapReduce(units, taskClassName, args).resultAsync();
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
    <R> R executeMapReduce(List<DeploymentUnit> units, String taskClassName, Object... args);
}
