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
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.Compute.RESULT_NOT_FOUND_ERR;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.compute.ComputeException;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.internal.compute.configuration.ComputeConfiguration;
import org.apache.ignite.network.TopologyService;
import org.jetbrains.annotations.Nullable;

/**
 * Manages job executions. Stores them in the TTL-based cache.
 */
public class ExecutionManager {
    private final Cleaner<JobExecution<?>> cleaner;

    private final Map<UUID, JobExecution<?>> executions = new ConcurrentHashMap<>();

    ExecutionManager(ComputeConfiguration computeConfiguration, TopologyService topologyService) {
        long ttl = computeConfiguration.statesLifetimeMillis().value();
        String nodeName = topologyService.localMember().name();
        cleaner = new Cleaner<>(ttl, nodeName);
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
        cleaner.start(executions::remove);
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
            return execution.resultAsync();
        }
        return failedFuture(new ComputeException(RESULT_NOT_FOUND_ERR, "Job result not found for the job with ID: " + jobId));
    }

    /**
     * Retrieves the current status of the job.
     *
     * @param jobId Job id.
     * @return The current status of the job, or {@code null} if there's no job with the specified id.
     */
    public CompletableFuture<@Nullable JobStatus> statusAsync(UUID jobId) {
        JobExecution<?> execution = executions.get(jobId);
        if (execution != null) {
            return execution.statusAsync();
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
}
