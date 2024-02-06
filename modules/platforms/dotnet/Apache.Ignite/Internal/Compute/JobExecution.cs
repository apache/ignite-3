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
using System.Threading.Tasks;
using Ignite.Compute;

/// <summary>
/// Job execution.
/// </summary>
/// <typeparam name="T">Job result type.</typeparam>
internal sealed record JobExecution<T> : IJobExecution<T>
{
    private readonly Task<(T Result, JobStatus Status)> _resultTask;

    private readonly Compute _compute;

    /// <summary>
    /// Initializes a new instance of the <see cref="JobExecution{T}"/> class.
    /// </summary>
    /// <param name="id">Job id.</param>
    /// <param name="resultTask">Result task.</param>
    /// <param name="compute">Compute.</param>
    public JobExecution(Guid id, Task<(T Result, JobStatus Status)> resultTask, Compute compute)
    {
        Id = id;
        _resultTask = resultTask;
        _compute = compute;
    }

    /// <inheritdoc/>
    public Guid Id { get; }

    /// <inheritdoc/>
    public async Task<T> GetResultAsync()
    {
        var (result, _) = await _resultTask.ConfigureAwait(false);
        return result;
    }

    /// <inheritdoc/>
    public async Task<JobStatus?> GetStatusAsync()
    {
        if (_resultTask.IsCompletedSuccessfully)
        {
            // TODO: What if the task failed, how do we cache the failed result?
            var (_, status) = await _resultTask.ConfigureAwait(false);
            return status;
        }

        // TODO: Cache the result when it is final (i.e. not running or waiting).
        return await _compute.GetJobStatusAsync(Id).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public Task<bool?> CancelAsync()
    {
        throw new NotImplementedException();
    }
}
