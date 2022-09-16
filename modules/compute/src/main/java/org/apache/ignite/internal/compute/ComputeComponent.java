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

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.network.ClusterNode;

/**
 * Compute functionality.
 */
public interface ComputeComponent extends IgniteComponent {
    /**
     * Executes a job of the given class on the current node.
     *
     * @param jobClass job class
     * @param args     job args
     * @param <R>      result type
     * @return future execution result
     */
    <R> CompletableFuture<R> executeLocally(Class<? extends ComputeJob<R>> jobClass, Object... args);

    /**
     * Executes a job of the given class on the current node.
     *
     * @param jobClassName name of the job class
     * @param args     job args
     * @param <R>      result type
     * @return future execution result
     */
    <R> CompletableFuture<R> executeLocally(String jobClassName, Object... args);

    /**
     * Executes a job of the given class on a remote node.
     *
     * @param remoteNode name of the job class
     * @param jobClass job class
     * @param args     job args
     * @param <R>      result type
     * @return future execution result
     */
    <R> CompletableFuture<R> executeRemotely(ClusterNode remoteNode, Class<? extends ComputeJob<R>> jobClass, Object... args);

    /**
     * Executes a job of the given class on a remote node.
     *
     * @param remoteNode name of the job class
     * @param jobClassName name of the job class
     * @param args     job args
     * @param <R>      result type
     * @return future execution result
     */
    <R> CompletableFuture<R> executeRemotely(ClusterNode remoteNode, String jobClassName, Object... args);
}
