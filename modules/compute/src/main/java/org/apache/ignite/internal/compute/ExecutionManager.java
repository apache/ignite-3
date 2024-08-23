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

import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.util.CompletableFutures.allOfToList;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.Compute.RESULT_NOT_FOUND_ERR;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.compute.ComputeException;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.internal.compute.configuration.ComputeConfiguration;
import org.apache.ignite.internal.compute.messaging.RemoteJobExecution;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.marshalling.Marshaller;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Manages job executions. Stores them in the TTL-based cache.
 */
public class ExecutionManager {
    private final ComputeConfiguration computeConfiguration;

    private final TopologyService topologyService;

    private final Cleaner<JobExecution<?>> cleaner = new Cleaner<>();

    private final Map<UUID, JobExecution<?>> executions = new ConcurrentHashMap<>();

    ExecutionManager(ComputeConfiguration computeConfiguration, TopologyService topologyService) {
        this.computeConfiguration = computeConfiguration;
        this.topologyService = topologyService;
    }

    /**
     * Puts an execution to the cache. When the job completes, it will be evicted from the cache after some time.
     *
     * @param jobId Job id.
     * @param execution Job execution.
     */
    void addExecution(UUID jobId, JobExecution<?> execution) {
        executions.put(jobId, execution);
        execution.resultAsync().whenComplete((r, throwable) -> cleaner.scheduleRemove(jobId));
    }

    /**
     * Starts execution manager.
     */
    void start() {
        long ttlMillis = computeConfiguration.statesLifetimeMillis().value();
        String nodeName = topologyService.localMember().name();
        cleaner.start(executions::remove, ttlMillis, nodeName);
    }

    /**
     * Stops execution manager.
     */
    void stop() {
        cleaner.stop();
    }

    /**
     * Returns job's execution result.
     *
     * @param jobId Job id.
     * @return Job's execution result future.
     */
    public CompletableFuture<?> resultAsync(UUID jobId) {
        JobExecution<?> execution = executions.get(jobId);
        if (execution != null) {
            if (execution instanceof MarshallerProvider) {
                Marshaller<Object, byte[]> marshaller = ((MarshallerProvider) execution).resultMarshaller();
                if (marshaller != null) {
                    return execution.resultAsync().thenApply(marshaller::marshal);
                }
            }
            return execution.resultAsync();
        }
        return failedFuture(new ComputeException(RESULT_NOT_FOUND_ERR, "Job result not found for the job with ID: " + jobId));
    }

    /**
     * Retrieves the current state of all jobs in the local node.
     *
     * @return The set of all job states.
     */
    public CompletableFuture<List<JobState>> localStatesAsync() {
        CompletableFuture<JobState>[] statesFutures = executions.values().stream()
                .filter(it -> !(it instanceof RemoteJobExecution) && !(it instanceof FailSafeJobExecution))
                .map(JobExecution::stateAsync)
                .toArray(CompletableFuture[]::new);

        return allOfToList(statesFutures)
                .thenApply(states -> states.stream()
                        .filter(Objects::nonNull)
                        .collect(toList()));
    }

    /**
     * Retrieves the current state of the job.
     *
     * @param jobId Job id.
     * @return The current state of the job, or {@code null} if there's no job with the specified id.
     */
    public CompletableFuture<@Nullable JobState> stateAsync(UUID jobId) {
        JobExecution<?> execution = executions.get(jobId);
        if (execution != null) {
            return execution.stateAsync();
        }
        return nullCompletedFuture();
    }

    /**
     * Cancels the running job.
     *
     * @param jobId Job id.
     * @return The future which will be completed with {@code true} when the job is cancelled, {@code false} when the job couldn't be
     *         cancelled (either it's not yet started, or it's already completed), or {@code null} if there's no job with the specified id.
     */
    public CompletableFuture<@Nullable Boolean> cancelAsync(UUID jobId) {
        JobExecution<?> execution = executions.get(jobId);
        if (execution != null) {
            return execution.cancelAsync();
        }
        return nullCompletedFuture();
    }

    /**
     * Changes job priority.
     *
     * @param jobId Job id.
     * @return The future which will be completed with {@code true} when the priority is changed, {@code false} when the priority couldn't
     *         be changed (it's already executing or completed), or {@code null} iif there's no job with the specified id.
     */
    public CompletableFuture<@Nullable Boolean> changePriorityAsync(UUID jobId, int newPriority) {
        JobExecution<?> execution = executions.get(jobId);
        if (execution != null) {
            return execution.changePriorityAsync(newPriority);
        }
        return nullCompletedFuture();
    }

    @TestOnly
    Set<UUID> executions() {
        return executions.keySet();
    }
}
