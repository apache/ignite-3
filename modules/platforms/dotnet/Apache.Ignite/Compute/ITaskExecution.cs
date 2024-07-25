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

namespace Apache.Ignite.Compute;

using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;

/// <summary>
/// Compute task control object, provides information about the task execution process and result, allows cancelling the task.
/// </summary>
/// <typeparam name="T">Job result type.</typeparam>
public interface ITaskExecution<T>
{
    /// <summary>
    /// Gets the task ID.
    /// </summary>
    Guid Id { get; }

    /// <summary>
    /// Gets the task execution result.
    /// </summary>
    /// <returns>Task execution result.</returns>
    Task<T> GetResultAsync();

    /// <summary>
    /// Gets the task execution state. Can be <c>null</c> if the task status no longer exists due to exceeding the retention time limit.
    /// </summary>
    /// <returns>
    /// Task execution state. Can be <c>null</c> if the task status no longer exists due to exceeding the retention time limit.
    /// </returns>
    Task<TaskState?> GetStateAsync();

    /// <summary>
    /// Cancels the task execution.
    /// </summary>
    /// <returns>
    /// Returns <c>true</c> if the task was successfully cancelled, <c>false</c> if the task has already finished,
    /// <c>null</c> if the task was not found (no longer exists due to exceeding the retention time limit).
    /// </returns>
    Task<bool?> CancelAsync();

    /// <summary>
    /// Changes the task priority. After priority change the task will be the last in the queue of tasks with the same priority.
    /// </summary>
    /// <param name="priority">New priority.</param>
    /// <returns>
    /// Returns <c>true</c> if the priority was successfully changed,
    /// <c>false</c> when the priority couldn't be changed (task is already executing or completed),
    /// <c>null</c> if the task was not found (no longer exists due to exceeding the retention time limit).
    /// </returns>
    Task<bool?> ChangePriorityAsync(int priority);

    /// <summary>
    /// Gets the job states for all jobs that are part of this task.
    /// </summary>
    /// <returns>A list of job states. Can contain nulls when the time for retaining job state has been exceeded.</returns>
    Task<IList<JobState?>> GetJobStatesAsync();
}
