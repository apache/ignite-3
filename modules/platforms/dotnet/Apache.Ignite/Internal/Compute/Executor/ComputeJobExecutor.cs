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
using System.Threading;
using System.Threading.Tasks;
using Buffers;

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
        var jobReq = Read(request);
        await ExecuteJobAsync(jobReq, request, response).ConfigureAwait(false);

        static JobExecuteRequest Read(PooledBuffer request)
        {
            var r = request.GetReader();
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
                // TODO IGNITE-25257 Cache deployment units and JobLoadContext.
                throw new NotSupportedException("Caching deployment units is not supported yet.");
            }

            request.Position += r.Consumed;

            return new JobExecuteRequest(jobId, new(deploymentUnitPaths), jobClassName);
        }
    }

    private static async ValueTask ExecuteJobAsync(
        JobExecuteRequest req,
        PooledBuffer argBuf,
        PooledArrayBuffer resBuf)
    {
        // Unload assemblies after job execution.
        // TODO IGNITE-25257 Cache deployment units and JobLoadContext.
        using JobLoadContext jobLoadCtx = DeploymentUnitLoader.GetJobLoadContext(req.DeploymentUnitPaths);
        IComputeJobWrapper jobWrapper = jobLoadCtx.CreateJobWrapper(req.JobClassName);

        resBuf.MessageWriter.Write(0); // Response flags: success.

        // TODO IGNITE-25116: IJobExecutionContext.
        // TODO IGNITE-25153: Cancellation.
        await jobWrapper.ExecuteAsync(null!, argBuf, resBuf, CancellationToken.None).ConfigureAwait(false);
    }

    private record JobExecuteRequest(long JobId, DeploymentUnitPaths DeploymentUnitPaths, string JobClassName);
}
