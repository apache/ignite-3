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

namespace Apache.Ignite.Internal;

using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Buffers;
using Compute.Executor;
using Proto;
using Proto.MsgPack;

/// <summary>
/// Server -> client request handling logic.
/// </summary>
internal sealed partial class ClientSocket
{
    private static async Task HandleServerOpInnerAsync(ServerOp op, PooledBuffer request, PooledArrayBuffer response)
    {
        switch (op)
        {
            case ServerOp.Ping:
                // No-op.
                break;

            case ServerOp.ComputeJobExec:
                await ComputeJobExecutor.ExecuteJobAsync(request, response).ConfigureAwait(false);
                break;

            case ServerOp.ComputeJobCancel:
                // TODO IGNITE-25153: Add cancellation support for platform jobs.
                response.MessageWriter.Write(false);
                break;

            case ServerOp.DeploymentUnitsUndeploy:
                response.MessageWriter.Write(false);
                break;

            default:
                throw new InvalidOperationException("Unsupported ServerOp code: " + op);
        }
    }

    private bool QueueServerOp(long requestId, ServerOp op, PooledBuffer request)
    {
        ThreadPool.QueueUserWorkItem<(ClientSocket Socket, PooledBuffer Buf, long RequestId, ServerOp Op)>(
            callBack: static state =>
            {
                // Ignore the returned task.
                _ = state.Socket.HandleServerOpAsync(state.Buf, state.RequestId, state.Op);
            },
            state: (this, request, requestId, op),
            preferLocal: true);

        return true;
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Thread root.")]
    private async Task HandleServerOpAsync(PooledBuffer buf, long requestId, ServerOp op)
    {
        _logger.LogServerOpTrace(requestId, (int)op, op, ConnectionContext.ClusterNode.Address);

        using var request = buf;
        using var response = ProtoCommon.GetMessageWriter();

        try
        {
            await HandleServerOpInnerAsync(op, request, response).ConfigureAwait(false);

            await SendServerOpResponseAsync(requestId, response).ConfigureAwait(false);
        }
        catch (Exception serverOpEx)
        {
            try
            {
                using var errResponse = ProtoCommon.GetMessageWriter();
                WriteError(errResponse.MessageWriter, serverOpEx);

                await SendServerOpResponseAsync(requestId, errResponse).ConfigureAwait(false);
            }
            catch (Exception resultSendEx)
            {
                var aggregateEx = new AggregateException(serverOpEx, resultSendEx);

                _logger.LogServerOpResponseError(aggregateEx, requestId, serverOpEx.Message);
            }
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

    private async Task SendServerOpResponseAsync(
        long requestId,
        PooledArrayBuffer? response) =>
        await SendRequestAsync(response, ClientOp.ServerOpResponse, requestId).ConfigureAwait(false);
}
