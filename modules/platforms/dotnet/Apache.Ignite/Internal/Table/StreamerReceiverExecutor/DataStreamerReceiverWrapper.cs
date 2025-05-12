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

namespace Apache.Ignite.Internal.Table.StreamerReceiverExecutor;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Buffers;
using Compute;
using Ignite.Table;

/// <summary>
/// Wraps a generic receiver to be called from a non-generic context.
/// </summary>
/// <typeparam name="TReceiver">Receiver type.</typeparam>
/// <typeparam name="TItem">Receiver item type.</typeparam>
/// <typeparam name="TArg">Arg type.</typeparam>
/// <typeparam name="TResult">Result type.</typeparam>
internal sealed class DataStreamerReceiverWrapper<TReceiver, TItem, TArg, TResult> : IDataStreamerReceiverWrapper
    where TReceiver : IDataStreamerReceiver<TItem, TArg, TResult>, new()
{
    /// <inheritdoc/>
    public async ValueTask ExecuteAsync(
        IDataStreamerReceiverContext context,
        PooledBuffer argBuf,
        PooledArrayBuffer responseBuf,
        CancellationToken cancellationToken)
    {
        TReceiver receiver = new TReceiver();
        List<TItem> page = ReadPage();

        TArg arg = ReadArg();

        try
        {
            ICollection<TResult>? res = await receiver.ReceiveAsync(page, context, arg, cancellationToken).ConfigureAwait(false);

            WriteRes(res);
        }
        finally
        {
            if (receiver is IAsyncDisposable asyncDisposable)
            {
                await asyncDisposable.DisposeAsync().ConfigureAwait(false);
            }
            else if (receiver is IDisposable disposable)
            {
                disposable.Dispose();
            }
        }

        List<TItem> ReadPage()
        {
            throw new NotImplementedException();
        }

        TArg ReadArg()
        {
            var reader = argBuf.GetReader();
            return ComputePacker.UnpackArgOrResult<TArg>(ref reader, null);
        }

        void WriteRes(ICollection<TResult> res)
        {
            var writer = responseBuf.MessageWriter;
            ComputePacker.PackArgOrResult(ref writer, res, null);
        }
    }
}
