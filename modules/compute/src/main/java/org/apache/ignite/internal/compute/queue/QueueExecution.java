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

package org.apache.ignite.internal.compute.queue;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.JobState;
import org.jetbrains.annotations.Nullable;

/**
 * Provides information about the task executing on the {@link PriorityQueueExecutor}, allows cancelling the task,
 * changing the job priority.
 *
 * @param <R> Job result type.
 */
public interface QueueExecution<R> {
    /**
     * Returns job's execution result.
     *
     * @return Job's execution result future.
     */
    CompletableFuture<R> resultAsync();

    /**
     * Returns the current state of the job. The job state may be deleted and thus return {@code null} if the time for retaining job
     * state has been exceeded.
     *
     * @return The current state of the job, or {@code null} if the job state no longer exists due to exceeding the retention time limit.
     */
    @Nullable
    JobState state();

    /**
     * Cancels the job.
     *
     * @return {@code true} if job was successfully cancelled.
     */
    boolean cancel();

    /**
     * Change job priority. Priority can be changed only if task still in executor's queue.
     * After priority change task will be removed from the execution queue and run once again.
     * Queue entry will be executed last in the queue of entries with the same priority (FIFO).
     *
     * @param newPriority new priority.
     * @return {@code true} if job priority was successfully changed.
     */
    boolean changePriority(int newPriority);
}
