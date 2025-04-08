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
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Buffers;
using Ignite.Compute;
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

    private static readonly MethodInfo ExecMethod =
        typeof(ComputeJobExecutor).GetMethod(nameof(ExecuteGenericJobAsync), BindingFlags.NonPublic | BindingFlags.Static)!;

    /// <summary>
    /// Executes compute job.
    /// </summary>
    /// <param name="requestId">Request id.</param>
    /// <param name="request">Request.</param>
    /// <param name="socket">Socket.</param>
    /// <returns>Task.</returns>
    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Thread root.")]
    internal static async Task ExecuteJobAsync(long requestId, PooledBuffer request, ClientSocket socket)
    {
        using (request)
        {
            try
            {
                var jobReq = Read(request.GetReader());
                var jobRes = await ExecuteJobAsync(jobReq).ConfigureAwait(false);

                using var responseBuf = ProtoCommon.GetMessageWriter();
                Write(responseBuf.MessageWriter, jobRes);

                await socket.SendServerOpResponseAsync(requestId, responseBuf).ConfigureAwait(false);
            }
            catch (Exception jobEx)
            {
                // TODO: Log error.
                Console.WriteLine(jobEx);

                try
                {
                    // TODO: Send error to the server.
                }
                catch (Exception resultSendEx)
                {
                    // TODO: Log failed to reply with error.
                    Console.WriteLine(resultSendEx.Message);
                }
            }
        }

        static JobExecuteRequest Read(MsgPackReader r)
        {
            // TODO: Get marshaller from the job class.
            // TODO: Load libraries into AssemblyLoadContext.
            string jobClassName = r.ReadString();

            int cnt = r.ReadInt32();
            List<string> deploymentUnitPaths = new List<string>(cnt);
            for (int i = 0; i < cnt; i++)
            {
                deploymentUnitPaths.Add(r.ReadString());
            }

            object arg = ComputePacker.UnpackArgOrResult<object>(ref r, null);

            return new JobExecuteRequest(deploymentUnitPaths, jobClassName, arg);
        }

        void Write(MsgPackWriter w, object? res)
        {
            w.Write(0); // Flags: success.
            ComputePacker.PackArgOrResult(ref w, res, null);
        }
    }

    private static ValueTask<object?> ExecuteJobAsync(JobExecuteRequest req)
    {
        // TODO: AssemblyLoadContext and assembly resolution with additional paths.
        var type = Type.GetType(req.JobClassName);
        if (type == null)
        {
            throw new InvalidOperationException($"Failed to load job class: {req.JobClassName}");
        }

        var jobInterface = type
            .GetInterfaces()
            .FirstOrDefault(x => x.IsGenericType && x.GetGenericTypeDefinition() == typeof(IComputeJob<,>));

        if (jobInterface == null)
        {
            throw new InvalidOperationException($"Failed to find job interface: {req.JobClassName}");
        }

        var job = Activator.CreateInstance(type)!;

        var method = ExecMethod.MakeGenericMethod(jobInterface.GenericTypeArguments[0], jobInterface.GenericTypeArguments[1]);

        return (ValueTask<object?>)method.Invoke(null, [job, req.Arg])!;
    }

    private static async ValueTask<object?> ExecuteGenericJobAsync<TArg, TResult>(IComputeJob<TArg, TResult> job, object? arg)
    {
        // TODO: Use marshallers.
        TResult res = await job.ExecuteAsync(null!, (TArg)arg!).ConfigureAwait(false);

        return res;
    }

    private record JobExecuteRequest(IList<string> DeploymentUnitPaths, string JobClassName, object Arg);
}
