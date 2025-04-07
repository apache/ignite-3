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
using Proto;
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
    /// <param name="jobId">Request id.</param>
    /// <param name="request">Request.</param>
    /// <param name="socket">Socket.</param>
    /// <returns>Task.</returns>
    internal static async Task ExecuteJobAsync(long jobId, PooledBuffer request, ClientSocket socket)
    {
        using (request)
        {
            var jobReq = Read(request.GetReader());

            using var responseBuf = new PooledArrayBuffer();
            Write(responseBuf.MessageWriter, "Hello from .NET: " + jobReq.Arg);

            using var ignored = await socket.DoOutInOpAsync(ClientOp.ComputeExecutorReturnJobResult, responseBuf).ConfigureAwait(false);
        }

        JobExecuteRequest Read(MsgPackReader r)
        {
            int cnt = r.ReadInt32();
            List<string> deploymentUnitPaths = new List<string>(cnt);
            for (int i = 0; i < cnt; i++)
            {
                deploymentUnitPaths.Add(r.ReadString());
            }

            // TODO: Get marshaller from the job class.
            // TODO: Load libraries into AssemblyLoadContext.
            string jobClassName = r.ReadString();
            object arg = ComputePacker.UnpackArgOrResult<object>(ref r, null);

            return new JobExecuteRequest(deploymentUnitPaths, jobClassName, arg);
        }

        void Write(MsgPackWriter w, object res)
        {
            w.Write(jobId);
            ComputePacker.PackArgOrResult(ref w, res, null);
        }
    }

    private record JobExecuteRequest(IList<string> DeploymentUnitPaths, string JobClassName, object Arg);
}
