/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
 * Provides access to the Compute functionality: the ability to execute compute jobs.
 *
 * @see ComputeJob
 * @see ComputeJob#execute(JobExecutionContext, Object...)
 */
public interface IgniteCompute {
    /**
     * Executes a {@link ComputeJob} represented by the given class on one node from the nodes set.
     *
     * @param nodes    nodes on which to execute the job
     * @param jobClass class of the job to execute
     * @param args     arguments of the job
     * @param <R>      job result type
     * @return future job result
     */
    <R> CompletableFuture<R> execute(Set<ClusterNode> nodes, Class<? extends ComputeJob<R>> jobClass, Object... args);

    /**
     * Executes a {@link ComputeJob} represented by the given class on one node from the nodes set.
     *
     * @param nodes    candidate nodes; the job will be executed on one of them
     * @param jobClassName name of the job class to execute
     * @param args     arguments of the job
     * @param <R>      job result type
     * @return future job result
     */
    <R> CompletableFuture<R> execute(Set<ClusterNode> nodes, String jobClassName, Object... args);

    /**
     * Executes a job represented by the given class on one node where the given key is located. The node is the leader
     * of the corresponding Raft group.
     *
     * @param tableName name of the table which key is used to determine the node on which the job will be executed
     * @param key key which will be used to determine the node on which the job will be executed
     * @param jobClass class of the job to execute
     * @param args arguments of the job
     * @param <R> job result type
     * @return future job result
     */
    <R> CompletableFuture<R> executeColocated(String tableName, Tuple key, Class<? extends ComputeJob<R>> jobClass, Object... args);

    /**
     * Executes a job represented by the given class on one node where the given key is located. The node is the leader
     * of the corresponding Raft group.
     *
     * @param tableName name of the table which key is used to determine the node on which the job will be executed
     * @param key key which will be used to determine the node on which the job will be executed
     * @param keyMapper mapper that will be used to map the key to a binary representation
     * @param jobClass class of the job to execute
     * @param args arguments of the job
     * @param <R> job result type
     * @return future job result
     */
    <K, R> CompletableFuture<R> executeColocated(String tableName, K key, Mapper<K> keyMapper,
                                                 Class<? extends ComputeJob<R>> jobClass, Object... args);

    /**
     * Executes a job represented by the given class on one node where the given key is located. The node is the leader
     * of the corresponding Raft group.
     *
     * @param tableName name of the table which key is used to determine the node on which the job will be executed
     * @param key key which will be used to determine the node on which the job will be executed
     * @param jobClassName name of the job class to execute
     * @param args arguments of the job
     * @param <R> job result type
     * @return future job result
     */
    <R> CompletableFuture<R> executeColocated(String tableName, Tuple key, String jobClassName, Object... args);

    /**
     * Executes a job represented by the given class on one node where the given key is located. The node is the leader
     * of the corresponding Raft group.
     *
     * @param tableName name of the table which key is used to determine the node on which the job will be executed
     * @param key key which will be used to determine the node on which the job will be executed
     * @param keyMapper mapper that will be used to map the key to a binary representation
     * @param jobClassName name of the job class to execute
     * @param args arguments of the job
     * @param <R> job result type
     * @return future job result
     */
    <K, R> CompletableFuture<R> executeColocated(String tableName, K key, Mapper<K> keyMapper, String jobClassName, Object... args);

    /**
     * Executes a {@link ComputeJob} represented by the given class on all nodes from the given nodes set.
     *
     * @param nodes nodes on each of which the job will be executed
     * @param jobClass class of the job to execute
     * @param args     arguments of the job
     * @param <R>      job result type
     * @return future job results
     */
    <R> Map<ClusterNode, CompletableFuture<R>> broadcast(Set<ClusterNode> nodes, Class<? extends ComputeJob<R>> jobClass, Object... args);

    /**
     * Executes a {@link ComputeJob} represented by the given class on all nodes from the given nodes set.
     *
     * @param nodes nodes on each of which the job will be executed
     * @param jobClassName name of the job class to execute
     * @param args     arguments of the job
     * @param <R>      job result type
     * @return future job results
     */
    <R> Map<ClusterNode, CompletableFuture<R>> broadcast(Set<ClusterNode> nodes, String jobClassName, Object... args);
}
