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
using System.Threading;
using System.Threading.Tasks;
using Buffers;
using Ignite.Compute;

/// <summary>
/// Wraps a generic compute job to be called from a non-generic context.
/// </summary>
/// <typeparam name="TJob">Type of the job.</typeparam>
/// <typeparam name="TArg">Type of the job argument.</typeparam>
/// <typeparam name="TResult">Type of the job result.</typeparam>
internal sealed class ComputeJobWrapper<TJob, TArg, TResult> : IComputeJobWrapper
    where TJob : IComputeJob<TArg, TResult>, new()
{
    /// <inheritdoc />
    public async ValueTask ExecuteAsync(
        IJobExecutionContext context,
        PooledBuffer argBuf,
        PooledArrayBuffer responseBuf,
        CancellationToken cancellationToken)
    {
        TJob job = new TJob();
        TArg arg = ReadArg();

        try
        {
            TResult res = await job.ExecuteAsync(context, arg, cancellationToken).ConfigureAwait(false);

            WriteRes(res);
        }
        finally
        {
            if (job is IAsyncDisposable asyncDisposable)
            {
                await asyncDisposable.DisposeAsync().ConfigureAwait(false);
            }
            else if (job is IDisposable disposable)
            {
                disposable.Dispose();
            }
        }

        TArg ReadArg()
        {
            var reader = argBuf.GetReader();
            return ComputePacker.UnpackArgOrResult(ref reader, job.InputMarshaller);
        }

        void WriteRes(TResult res)
        {
            var writer = responseBuf.MessageWriter;
            ComputePacker.PackArgOrResult(ref writer, res, job.ResultMarshaller);
        }
    }
}
