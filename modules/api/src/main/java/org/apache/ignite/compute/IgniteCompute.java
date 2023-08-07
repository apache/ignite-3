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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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
     * Executes a {@link ComputeJob} of the given class on a single node from a set of candidate nodes.
     *
     * @param nodes    Candidate nodes; the job will be executed on one of them.
     * @param units    Deployment units. Can be empty.
     * @param jobClassName Name of the job class to execute.
     * @param args     Arguments of the job.
     * @param <R>      Job result type
     * @return CompletableFuture Job result.
     */
    <R> CompletableFuture<R> executeAsync(
            Set<ClusterNode> nodes,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    );

    /**
     * Executes a {@link ComputeJob} of the given class on a single node from a set of candidate nodes.
     *
     * @param nodes    Candidate nodes; the job will be executed on one of them.
     * @param units    Deployment units. Can be empty.
     * @param jobClassName Name of the job class to execute.
     * @param args     Arguments of the job.
     * @param <R>      Job result type
     * @return Job result.
     */
    <R> R execute(
            Set<ClusterNode> nodes,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args);

    /**
     * Executes a job of the given class on the node where the given key is located. The node is a leader
     * of the corresponding RAFT group.
     *
     * @param tableName Name of the table whose key is used to determine the node to execute the job on.
     * @param key Key that identifies the node to execute the job on.
     * @param units Deployment units. Can be empty.
     * @param jobClassName Name of the job class to execute.
     * @param args Arguments of the job.
     * @param <R> Job result type.
     * @return CompletableFuture Job result.
     */
    <R> CompletableFuture<R> executeColocatedAsync(
            String tableName,
            Tuple key,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    );

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
     * @return CompletableFuture Job result.
     */
    <K, R> CompletableFuture<R> executeColocatedAsync(
            String tableName,
            K key,
            Mapper<K> keyMapper,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    );

    /**
     * Executes a job of the given class on the node where the given key is located. The node is a leader
     * of the corresponding RAFT group.
     *
     * @param tableName Name of the table whose key is used to determine the node to execute the job on.
     * @param key Key that identifies the node to execute the job on.
     * @param units Deployment units. Can be empty.
     * @param jobClassName Name of the job class to execute.
     * @param args Arguments of the job.
     * @param <R> Job result type.
     * @return Job result.
     */
    <R> R executeColocated(
            String tableName,
            Tuple key,
            List<DeploymentUnit> units,
            String jobClassName,
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
     */
    <K, R> R executeColocated(
            String tableName,
            K key,
            Mapper<K> keyMapper,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args);

    /**
     * Executes a {@link ComputeJob} of the given class on all nodes in the given node set.
     *
     * @param nodes Nodes to execute the job on.
     * @param units Deployment units. Can be empty.
     * @param jobClassName Name of the job class to execute.
     * @param args     Arguments of the job.
     * @param <R>      Job result type.
     * @return Map from node to job result future.
     */
    <R> Map<ClusterNode, CompletableFuture<R>> broadcastAsync(
            Set<ClusterNode> nodes,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    );
}
