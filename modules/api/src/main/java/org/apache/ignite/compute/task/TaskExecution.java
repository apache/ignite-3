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

package org.apache.ignite.compute.task;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.compute.TaskState;
import org.jetbrains.annotations.Nullable;

/**
 * Compute task control object. Methods inherited from the {@link JobExecution} allows control of the task coordination job.
 *
 * @param <R> Task result type.
 */
public interface TaskExecution<R> {
    /**
     * Returns a collection of states of the jobs which are executing under this task. The resulting future is completed only after the
     * jobs are submitted for execution. The list could contain {@code null} values if the time for retaining job state has been exceeded.
     *
     * @return A list of current states of the jobs.
     */
    CompletableFuture<List<@Nullable JobState>> statesAsync();

    /**
     * Returns a collection of ids of the jobs which are executing under this task. The resulting future is completed only after the
     * jobs are submitted for execution. The list could contain {@code null} values if the time for retaining job state has been exceeded.
     *
     * @return A list of ids of the jobs.
     */
    default CompletableFuture<List<@Nullable UUID>> idsAsync() {
        return statesAsync().thenApply(states -> states.stream()
                .map(state -> state != null ? state.id() : null)
                .collect(Collectors.toList()));
    }

    /**
     * Returns task's execution result.
     *
     * @return Task's execution result future.
     */
    CompletableFuture<R> resultAsync();

    /**
     * Returns the current state of the task. The task state may be deleted and thus return {@code null} if the time for retaining task
     * state has been exceeded.
     *
     * @return The current state of the task, or {@code null} if the task state no longer exists due to exceeding the retention time limit.
     */
    CompletableFuture<@Nullable TaskState> stateAsync();

    /**
     * Returns the id of the task. The task state may be deleted and thus return {@code null} if the time for retaining task state has been
     * exceeded.
     *
     * @return The id of the task, or {@code null} if the task state no longer exists due to exceeding the retention time limit.
     */
    default CompletableFuture<@Nullable UUID> idAsync() {
        return stateAsync().thenApply(state -> state != null ? state.id() : null);
    }

    /**
     * Cancels the task.
     *
     * @return The future which will be completed with {@code true} when the task is cancelled, {@code false} when the task couldn't be
     *         cancelled (if it's already completed or in the process of cancelling), or {@code null} if the task no longer exists due to
     *         exceeding the retention time limit.
     */
    CompletableFuture<@Nullable Boolean> cancelAsync();

    /**
     * Changes task priority. After priority change task will be the last in the queue of tasks with the same priority.
     *
     * @param newPriority new priority.
     * @return The future which will be completed with {@code true} when the priority is changed, {@code false} when the priority couldn't
     *         be changed (if the task is already executing or completed), or {@code null} if the task no longer exists due to exceeding the
     *         retention time limit.
     */
    CompletableFuture<@Nullable Boolean> changePriorityAsync(int newPriority);
}
