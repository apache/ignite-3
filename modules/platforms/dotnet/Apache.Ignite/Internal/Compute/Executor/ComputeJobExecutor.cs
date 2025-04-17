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
using System.Threading.Tasks;
using Buffers;
using Microsoft.Extensions.Logging;
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
    /// <param name="requestId">Request id.</param>
    /// <param name="request">Request.</param>
    /// <param name="socket">Socket.</param>
    /// <param name="logger">Logger.</param>
    /// <returns>Task.</returns>
    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Thread root.")]
    internal static async Task ExecuteJobAsync(long requestId, PooledBuffer request, ClientSocket socket, ILogger logger)
    {
        JobExecuteRequest? jobReq = null;

        try
        {
            jobReq = Read(request.GetReader());
            var jobRes = await ExecuteJobAsync(jobReq).ConfigureAwait(false);

            using var responseBuf = ProtoCommon.GetMessageWriter();
            Write(responseBuf.MessageWriter, jobRes);

            await socket.SendServerOpResponseAsync(requestId, responseBuf).ConfigureAwait(false);
        }
        catch (Exception jobEx)
        {
            try
            {
                using var errResponseBuf = ProtoCommon.GetMessageWriter();
                WriteError(errResponseBuf.MessageWriter, jobEx);

                await socket.SendServerOpResponseAsync(requestId, errResponseBuf).ConfigureAwait(false);
            }
            catch (Exception resultSendEx)
            {
                var aggregateEx = new AggregateException(jobEx, resultSendEx);
                logger.LogComputeJobResponseError(aggregateEx, requestId, jobEx.Message, jobReq?.JobClassName);
            }
        }

        static JobExecuteRequest Read(MsgPackReader r)
        {
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

        static void Write(MsgPackWriter w, object? res)
        {
            w.Write(0); // Flags: success.
            ComputePacker.PackArgOrResult(ref w, res, null);
        }

        static void WriteError(MsgPackWriter w, Exception e)
        {
            var igniteEx = e as IgniteException;

            Guid traceId = igniteEx?.TraceId ?? Guid.NewGuid();
            int code = igniteEx?.ErrorCode ?? ErrorGroups.Compute.ComputeJobFailed;
            string className = e.GetType().ToString();
            string message = e.Message;
            string? stackTrace = e.StackTrace;

            w.Write((int)ServerOpResponseFlags.Error);
            w.Write(traceId);
            w.Write(code);
            w.Write(className);
            w.Write(message);
            w.Write(stackTrace);
            w.Write(0); // Extensions count.
        }
    }

    private static ValueTask<object?> ExecuteJobAsync(JobExecuteRequest req)
    {
        if (req.JobClassName == "TEST_ONLY_DOTNET_JOB:ECHO")
        {
            return ValueTask.FromResult(req.Arg)!;
        }

        throw new NotImplementedException("Platform jobs are not supported yet.");
    }

    private record JobExecuteRequest(IList<string> DeploymentUnitPaths, string JobClassName, object Arg);
}
