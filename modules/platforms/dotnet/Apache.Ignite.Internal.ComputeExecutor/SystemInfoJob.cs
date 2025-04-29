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

namespace Apache.Ignite.Internal.ComputeExecutor;

using System.Diagnostics;
using Ignite.Compute;

/// <summary>
/// A built-in compute job that provides system information about the executor.
/// Useful for .NET executor troubleshooting.
/// </summary>
public sealed class SystemInfoJob : IComputeJob<object?, string>
{
    /// <inheritdoc/>
    public ValueTask<string> ExecuteAsync(IJobExecutionContext context, object? arg, CancellationToken cancellationToken)
    {
        var uptime = DateTime.Now - Process.GetCurrentProcess().StartTime;

        var response = $"{nameof(SystemInfoJob)} [CLR version={Environment.Version}, " +
                       $"MemoryUsed={Environment.WorkingSet / 1024 / 1024}MB, " +
                       $"OS={Environment.OSVersion}, " +
                       $"Uptime={uptime}, " +
                       $"JobArg={arg}]";

        return ValueTask.FromResult(response);
    }
}
