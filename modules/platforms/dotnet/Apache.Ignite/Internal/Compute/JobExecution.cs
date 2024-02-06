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
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using Ignite.Compute;

/// <summary>
/// Job execution.
/// </summary>
/// <param name="Id">Job ID.</param>
/// <param name="ResultTask">Result task.</param>
/// <typeparam name="T"></typeparam>
internal sealed record JobExecution<T>(Guid Id, Task<(T Result, JobStatus Status)> ResultTask) : IJobExecution<T>
{
    /// <inheritdoc/>
    public async Task<T> GetResultAsync()
    {
        var (result, _) = await ResultTask.ConfigureAwait(false);
        return result;
    }

    /// <inheritdoc/>
    public async Task<JobStatus?> GetStatusAsync()
    {
        if (ResultTask.IsCompletedSuccessfully)
        {
            // TODO: What if the task failed, how do we cache the failed result?
            var (_, status) = await ResultTask.ConfigureAwait(false);
            return status;
        }

        // TODO
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public Task<bool?> CancelAsync()
    {
        throw new NotImplementedException();
    }
}
