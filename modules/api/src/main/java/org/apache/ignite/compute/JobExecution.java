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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Job control object, provides information about the job execution process and result, allows cancelling the job.
 *
 * @param <R> Job result type.
 */
public interface JobExecution<R> {
    /**
     * Returns job's execution result.
     *
     * @return Job's execution result future.
     */
    CompletableFuture<R> resultAsync();

    /**
     * Returns the current status of the job. The job status may be deleted and thus return {@code null} if the time for retaining job
     * status has been exceeded.
     *
     * @return The current status of the job, or {@code null} if the job status no longer exists due to exceeding the retention time limit.
     */
    CompletableFuture<JobStatus> statusAsync();

    /**
     * Returns the id of the job. The job status may be deleted and thus return {@code null} if the time for retaining job status has been
     * exceeded.
     *
     * @return The id of the job, or {@code null} if the job status no longer exists due to exceeding the retention time limit.
     */
    default CompletableFuture<UUID> idAsync() {
        return statusAsync().thenApply(status -> status != null ? status.id() : null);
    }

    /**
     * Cancels the job.
     *
     * @return The future which will be completed when cancel request is processed.
     */
    CompletableFuture<Void> cancelAsync();

    /**
     * Changes job priority. After priority change job will be the last in the queue of jobs with the same priority.
     *
     * @param newPriority new priority.
     * @return The future which will be completed when change priority request is processed.
     */
    CompletableFuture<Void> changePriority(int newPriority);
}
