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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;

/**
 * Provides access to the Compute functionality - the ability to execute Compute jobs.
 *
 * @see ComputeJob
 * @see ComputeJob#execute(JobExecutionContext, Object...)
 */
public interface IgniteCompute {
    /**
     * Executes a {@link ComputeJob} represented by a given class on one node from a set of nodes.
     *
     * @param nodes    Nodes on which to execute the job.
     * @param jobClass Class of the job to execute.
     * @param args     Arguments of the job.
     * @param <R>      Job result type.
     * @return future Job result.
     */
    <R> CompletableFuture<R> execute(Set<ClusterNode> nodes, Class<? extends ComputeJob<R>> jobClass, Object... args);

    /**
     * Executes a {@link ComputeJob} represented by a given class on one node from a set of nodes.
     *
     * @param nodes    Candidate nodes; the job is executed on one of them.
     * @param jobClass Name Name of the job class to execute.
     * @param args     Arguments of the job.
     * @param <R>      Job result type.
     * @return future Job result.
     */
    <R> CompletableFuture<R> execute(Set<ClusterNode> nodes, String jobClassName, Object... args);

    /**
     * Executes a job represented by a given class on a node where the given key is located. The node is the leader
     * of the corresponding RAFT group.
     *
     * @param tableName Name of the table whose key is used to determine the node to execute the job on.
     * @param key Key to be used to determine the node to execute the job on.
     * @param jobClass Class of the job to execute.
     * @param args Arguments of the job.
     * @param <R> Job result type.
     * @return future Job result.
     */
    <R> CompletableFuture<R> executeColocated(String tableName, Tuple key, Class<? extends ComputeJob<R>> jobClass, Object... args);

    /**
     * Executes a job represented by a given class on a node where the given key is located. The node is the leader
     * of the corresponding RAFT group.
     *
     * @param tableName Name of the table whose key is used to determine the node to execute the job on.
     * @param key Key to be used to determine the node to execute the job on.
     * @param jobClass Class of the job to execute.
     * @param args Arguments of the job.
     * @param <R> Job result type.
     * @return future Job result.
     */
    <K, R> CompletableFuture<R> executeColocated(String tableName, K key, Mapper<K> keyMapper,
                                                 Class<? extends ComputeJob<R>> jobClass, Object... args);

    /**
     * Executes a job represented by a given class on a node where a given key is located. The node is the leader
     * of the corresponding RAFT group.
     *
     * @param tableName Name of the table whose key is used to determine the node to execute the job on.
     * @param key Key to be used to determine the node to execute the job on.
     * @param jobClass Class of the job to execute.
     * @param args Arguments of the job.
     * @param <R> Job result type.
     * @return future Job result.
     */
    <R> CompletableFuture<R> executeColocated(String tableName, Tuple key, String jobClassName, Object... args);

    /**
     * Executes a job represented by a given class on a node where the given key is located. The node is the leader
     * of the corresponding RAFT group.
     *
     * @param tableName name of the table which key is used to determine the node on which the job will be executed
     * @param key Key to be used to determine the node to execute the job on.
     * @param keyMapper Mapper to be used to map the key to a binary representation.
     * @param jobClassName Name of the job class to execute.
     * @param args Arguments of the job.
     * @param <R> Job result type.
     * @return future Job result.
     */
    <K, R> CompletableFuture<R> executeColocated(String tableName, K key, Mapper<K> keyMapper, String jobClassName, Object... args);

    /**
     * Executes a {@link ComputeJob} represented by a given class on all nodes from the given node set.
     *
     * @param nodes Nodes to execute the job on.
     * @param jobClass Class of the job to execute.
     * @param args     Arguments of the job.
     * @param <R>      Job result type.
     * @return future Job results.
     */
    <R> Map<ClusterNode, CompletableFuture<R>> broadcast(Set<ClusterNode> nodes, Class<? extends ComputeJob<R>> jobClass, Object... args);

    /**
     * Executes a {@link ComputeJob} represented by a given class on all nodes from the given node set.
     *
     * @param nodes Nodes to execute the job on.
     * @param jobClass Class of the job to execute.
     * @param args     Arguments of the job.
     * @param <R>      Job result type.
     * @return future Job results.
     */
    <R> Map<ClusterNode, CompletableFuture<R>> broadcast(Set<ClusterNode> nodes, String jobClassName, Object... args);
}
