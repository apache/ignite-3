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
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Buffers;
using Compute;
using Ignite.Sql;
using Ignite.Table;
using Proto.BinaryTuple;
using Serialization;

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
        var (page, arg) = ReadPageAndArg(argBuf);
        TReceiver receiver = new TReceiver();

        try
        {
            IList<TResult>? res = await receiver.ReceiveAsync(page, context, arg, cancellationToken).ConfigureAwait(false);

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


        void WriteRes(IList<TResult>? res)
        {
            var writer = responseBuf.MessageWriter;

            if (res == null)
            {
                writer.WriteNil();
                return;
            }

            int resTupleElementCount = res.Count + 2;
            var builder = new BinaryTupleBuilder(resTupleElementCount);
            builder.AppendObjectCollectionWithType(res);

            responseBuf.MessageWriter.Write(builder.Build().Span);
        }
    }

    private static (List<TItem> Page, TArg Arg) ReadPageAndArg(PooledBuffer argBuf)
    {
        BinaryTupleReader receiverInfo = StreamerReceiverJob.GetReceiverInfoReaderFast(argBuf);

        object? argObj = ReadArg(ref receiverInfo, 1);
        List<TItem> items = ReadPage(ref receiverInfo);

        return (items, (TArg)argObj!);
    }

    [SuppressMessage("Design", "CA1002:Do not expose generic lists", Justification = "Private method.")]
    private static List<TItem> ReadPage(ref BinaryTupleReader receiverInfo)
    {
        int itemType = receiverInfo.GetInt(4);
        int itemCount = receiverInfo.GetInt(5);

        List<TItem> items = new List<TItem>(itemCount);

        if (itemType == TupleWithSchemaMarshalling.TypeIdTuple)
        {
            for (int i = 0; i < itemCount; i++)
            {
                IgniteTuple tuple = TupleWithSchemaMarshalling.Unpack(receiverInfo.GetBytesSpan(i + 6));
                items.Add((TItem)(object)tuple);
            }
        }
        else
        {
            ColumnType colType = (ColumnType)itemType;
            for (int i = 0; i < itemCount; i++)
            {
                object? item = receiverInfo.GetObject(i + 6, colType);
                items.Add((TItem)item!);
            }
        }

        return items;
    }

    private static object? ReadArg(ref BinaryTupleReader reader, int index)
    {
        if (reader.IsNull(index))
        {
            return null;
        }

        if (reader.GetInt(index) == TupleWithSchemaMarshalling.TypeIdTuple)
        {
            return TupleWithSchemaMarshalling.Unpack(reader.GetBytesSpan(index + 2));
        }

        return reader.GetObject(index);
    }
}
