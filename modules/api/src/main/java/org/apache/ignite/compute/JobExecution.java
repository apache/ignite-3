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
import org.jetbrains.annotations.Nullable;

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
     * Returns the current state of the job. The job state may be deleted and thus return {@code null} if the time for retaining job state
     * has been exceeded.
     *
     * @return The current state of the job, or {@code null} if the job state no longer exists due to exceeding the retention time limit.
     */
    CompletableFuture<@Nullable JobState> stateAsync();

    /**
     * Returns the id of the job. The job state may be deleted and thus return {@code null} if the time for retaining job state has been
     * exceeded.
     *
     * @return The id of the job, or {@code null} if the job state no longer exists due to exceeding the retention time limit.
     */
    default CompletableFuture<@Nullable UUID> idAsync() {
        return stateAsync().thenApply(state -> state != null ? state.id() : null);
    }

    /**
     * Cancels the job.
     *
     * @return The future which will be completed with {@code true} when the job is cancelled, {@code false} when the job couldn't be
     *         cancelled (if it's already completed or in the process of cancelling), or {@code null} if the job no longer exists due to
     *         exceeding the retention time limit.
     */
    CompletableFuture<@Nullable Boolean> cancelAsync();

    /**
     * Changes job priority. After priority change job will be the last in the queue of jobs with the same priority.
     *
     * @param newPriority new priority.
     * @return The future which will be completed with {@code true} when the priority is changed, {@code false} when the priority couldn't
     *         be changed (if the job is already executing or completed), or {@code null} if the job no longer exists due to exceeding the
     *         retention time limit.
     */
    CompletableFuture<@Nullable Boolean> changePriorityAsync(int newPriority);
}
