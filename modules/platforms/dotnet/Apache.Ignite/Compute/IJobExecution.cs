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
using System.Threading.Tasks;

/// <summary>
/// Job control object, provides information about the job execution process and result, allows cancelling the job.
/// </summary>
/// <typeparam name="T">Job result type.</typeparam>
public interface IJobExecution<T>
{
    /// <summary>
    /// Gets the job ID.
    /// </summary>
    Guid Id { get; }

    /// <summary>
    /// Gets the job execution result.
    /// </summary>
    /// <returns>Job execution result.</returns>
    Task<T> GetResultAsync();

    /// <summary>
    /// Gets the job execution status. Can be <c>null</c> if the job status no longer exists due to exceeding the retention time limit.
    /// </summary>
    /// <returns>
    /// Job execution status. Can be <c>null</c> if the job status no longer exists due to exceeding the retention time limit.
    /// </returns>
    Task<JobState?> GetStatusAsync();

    /// <summary>
    /// Cancels the job execution.
    /// </summary>
    /// <returns>
    /// Returns <c>true</c> if the job was successfully cancelled, <c>false</c> if the job has already finished,
    /// <c>null</c> if the job was not found (no longer exists due to exceeding the retention time limit).
    /// </returns>
    Task<bool?> CancelAsync();

    /// <summary>
    /// Changes the job priority. After priority change the job will be the last in the queue of jobs with the same priority.
    /// </summary>
    /// <param name="priority">New priority.</param>
    /// <returns>
    /// Returns <c>true</c> if the priority was successfully changed,
    /// <c>false</c> when the priority couldn't be changed (job is already executing or completed),
    /// <c>null</c> if the job was not found (no longer exists due to exceeding the retention time limit).
    /// </returns>
    Task<bool?> ChangePriorityAsync(int priority);
}
