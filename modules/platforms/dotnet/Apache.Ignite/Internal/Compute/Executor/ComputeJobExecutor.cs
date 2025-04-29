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

namespace Apache.Ignite.Internal.Compute.Executor;

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Buffers;
using Proto.MsgPack;

/// <summary>
/// Compute executor utilities.
/// </summary>
internal static class ComputeJobExecutor
{
    /// <summary>
    /// Compute executor id.
    /// </summary>
    internal static readonly string? IgniteComputeExecutorId = Environment.GetEnvironmentVariable("IGNITE_COMPUTE_EXECUTOR_ID");

    /// <summary>
    /// Executes compute job.
    /// </summary>
    /// <param name="request">Request.</param>
    /// <param name="response">Response.</param>
    /// <returns>Task.</returns>
    internal static async Task ExecuteJobAsync(
        PooledBuffer request,
        PooledArrayBuffer response)
    {
        var jobReq = Read(request.GetReader());
        var jobRes = await ExecuteJobAsync(jobReq).ConfigureAwait(false);

        Write(response.MessageWriter, jobRes);

        static JobExecuteRequest Read(MsgPackReader r)
        {
            long jobId = r.ReadInt64();
            string jobClassName = r.ReadString();

            int cnt = r.ReadInt32();
            List<string> deploymentUnitPaths = new List<string>(cnt);
            for (int i = 0; i < cnt; i++)
            {
                deploymentUnitPaths.Add(r.ReadString());
            }

            bool retainDeploymentUnits = r.ReadBoolean();

            if (retainDeploymentUnits)
            {
                // TODO IGNITE-25257 Cache dedployment units and JobLoadContext.
                throw new NotSupportedException("Caching deployment units is not supported yet.");
            }

            object arg = ComputePacker.UnpackArgOrResult<object>(ref r, null);

            return new JobExecuteRequest(jobId, deploymentUnitPaths, jobClassName, arg);
        }

        static void Write(MsgPackWriter w, object? res)
        {
            w.Write(0); // Flags: success.
            ComputePacker.PackArgOrResult(ref w, res, null);
        }
    }

    private static ValueTask<object?> ExecuteJobAsync(JobExecuteRequest req)
    {
        // TODO IGNITE-25115 Implement platform job executor.
        if (req.JobClassName == "TEST_ONLY_DOTNET_JOB:ECHO")
        {
            return ValueTask.FromResult(req.Arg)!;
        }

        throw new NotImplementedException("Platform jobs are not supported yet.");
    }

    private record JobExecuteRequest(long JobId, IList<string> DeploymentUnitPaths, string JobClassName, object Arg);
}
