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

package org.apache.ignite.internal.compute;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.compute.ComputeException;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.network.ClusterNode;

/**
 * {@link ComputeTask}'s execution.
 *
 * @param <T> Intermediate result's type.
 */
public class Execution<T> {
    /** Map of the compute task's results. */
    private final ConcurrentMap<String, ExecutionResult<T>> results = new ConcurrentHashMap<>();

    /** Set of the nodes that are required to send back the results. */
    private final Set<String> requiredNodes;

    /** Future that is resolved when all of the results are acquired or exception has been caught during task's execution. */
    private final CompletableFuture<Collection<ExecutionResult<T>>> future = new CompletableFuture<>();

    public Execution(Collection<String> requiredNodes) {
        this.requiredNodes = ConcurrentHashMap.newKeySet(requiredNodes.size());
        this.requiredNodes.addAll(requiredNodes);
    }

    /**
     * Waits and returns the result of the execution of the compute task on every required node.
     *
     * @return Collection of execution results.
     * @throws ComputeException If execution failed on any node or if the thread was interrupted.
     */
    Collection<ExecutionResult<T>> getResult() throws ComputeException {
        try {
            return future.get();
        } catch (InterruptedException e) {
            throw new ComputeException("Failed to wait for compute result: " + e.getMessage(), e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();

            assert cause instanceof ComputeException;

            throw (ComputeException) cause;
        }
    }

    /**
     * Handles receiving of the result from a node.
     *
     * @param nodeId Which node sent the result.
     * @param result Result ({@code null} if task failed on that node.
     * @param exception Task error ({@code null} if task finished successfully).
     */
    void onResultReceived(String nodeId, T result, Throwable exception) {
        if (exception != null) {
            // We shouldn't always fail the future, different fail-over modes should be introduced
            assert exception instanceof ComputeException;

            future.completeExceptionally(exception);
            return;
        }

        requiredNodes.remove(nodeId);
        results.put(nodeId, new ExecutionResult<>(nodeId, result, exception));

        if (requiredNodes.isEmpty()) {
            future.complete(results.values());
        }
    }

    /**
     * Notifies execution that a node left the cluster.
     *
     * @param member Node that is gone from the cluster.
     */
    void notifyNodeDisappeared(ClusterNode member) {
        requiredNodes.remove(member.id());

        if (requiredNodes.isEmpty()) {
            future.complete(results.values());
        }
    }
}
