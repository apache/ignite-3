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

namespace Apache.Ignite.Internal.Compute;

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Ignite.Compute;
using TaskStatus = Ignite.Compute.TaskStatus;

/// <summary>
/// Job execution.
/// </summary>
/// <typeparam name="T">Job result type.</typeparam>
internal sealed record TaskExecution<T> : ITaskExecution<T>
{
    private readonly Task<(T Result, TaskState Status)> _resultTask;

    private readonly Compute _compute;

    private volatile TaskState? _finalState;

    /// <summary>
    /// Initializes a new instance of the <see cref="TaskExecution{T}"/> class.
    /// </summary>
    /// <param name="id">Job id.</param>
    /// <param name="jobIds">Job ids.</param>
    /// <param name="resultTask">Result task.</param>
    /// <param name="compute">Compute.</param>
    public TaskExecution(Guid id, IReadOnlyList<Guid> jobIds, Task<(T Result, TaskState Status)> resultTask, Compute compute)
    {
        Id = id;
        JobIds = jobIds;

        _resultTask = resultTask;
        _compute = compute;

        // Wait for completion in background and cache the status.
        _ = CacheStatusOnCompletion();
    }

    /// <inheritdoc/>
    public Guid Id { get; }

    /// <inheritdoc/>
    public IReadOnlyList<Guid> JobIds { get; }

    /// <inheritdoc/>
    public async Task<T> GetResultAsync()
    {
        var (result, _) = await _resultTask.ConfigureAwait(false);
        return result;
    }

    /// <inheritdoc/>
    public async Task<TaskState?> GetStateAsync()
    {
        var finalStatus = _finalState;
        if (finalStatus != null)
        {
            return finalStatus;
        }

        var status = await _compute.GetTaskStateAsync(Id).ConfigureAwait(false);
        if (status is { Status: TaskStatus.Completed or TaskStatus.Failed or TaskStatus.Canceled })
        {
            // Can't be transitioned to another state, cache it.
            _finalState = status;
        }

        return status;
    }

    /// <inheritdoc/>
    public async Task<bool?> CancelAsync() =>
        await _compute.CancelJobAsync(Id).ConfigureAwait(false);

    /// <inheritdoc/>
    public async Task<bool?> ChangePriorityAsync(int priority) =>
        await _compute.ChangeJobPriorityAsync(Id, priority).ConfigureAwait(false);

    /// <inheritdoc/>
    public async Task<IList<JobState?>> GetJobStatesAsync()
    {
        var res = new List<JobState?>(JobIds.Count);

        foreach (var jobId in JobIds)
        {
            var state = await _compute.GetJobStateAsync(jobId).ConfigureAwait(false);
            res.Add(state);
        }

        return res;
    }

    private async Task CacheStatusOnCompletion()
    {
        var (_, status) = await _resultTask.ConfigureAwait(false);

        _finalState = status;
    }
}
