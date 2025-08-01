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

namespace Apache.Ignite.Tests.Table;

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ignite.Marshalling;
using Ignite.Table;

/// <summary>
/// .NET streamer receivers.
/// </summary>
[SuppressMessage("Design", "CA1034:Nested types should not be visible", Justification = "Tests.")]
public static class DotNetReceivers
{
    public static readonly ReceiverDescriptor<object, object, object> Echo = ReceiverDescriptor.Of(new EchoReceiver());

    public static readonly ReceiverDescriptor<object, object, object> EchoArgs = ReceiverDescriptor.Of(new EchoArgsReceiver());

    public static readonly ReceiverDescriptor<object, object, object> Error = ReceiverDescriptor.Of(new ErrorReceiver());

    public static readonly ReceiverDescriptor<int, string, IIgniteTuple> CreateTableAndUpsert =
        ReceiverDescriptor.Of(new CreateTableAndUpsertReceiver());

    public static readonly ReceiverDescriptor<IIgniteTuple, object?, IIgniteTuple> UpdateTuple = ReceiverDescriptor.Of(new UpdateTupleReceiver());

    public static readonly ReceiverDescriptor<ReceiverItem<string>, ReceiverArg, ReceiverResult<string>> Marshaller =
        ReceiverDescriptor.Of(new MarshallerReceiver());

    public class EchoReceiver : IDataStreamerReceiver<object, object, object>
    {
        public ValueTask<IList<object>?> ReceiveAsync(
            IList<object> page,
            object arg,
            IDataStreamerReceiverContext context,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(page)!;
    }

    public class EchoArgsReceiver : IDataStreamerReceiver<object, object, object>
    {
        public ValueTask<IList<object>?> ReceiveAsync(
            IList<object> page,
            object arg,
            IDataStreamerReceiverContext context,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult<IList<object>?>([arg!]);
    }

    public class ErrorReceiver : IDataStreamerReceiver<object, object, object>
    {
        public async ValueTask<IList<object>?> ReceiveAsync(
            IList<object> page,
            object arg,
            IDataStreamerReceiverContext context,
            CancellationToken cancellationToken)
        {
            await Task.Delay(1, cancellationToken);

            throw new IgniteException(Guid.NewGuid(), ErrorGroups.Catalog.Validation, $"Error in receiver: {arg}");
        }
    }

    public class CreateTableAndUpsertReceiver : IDataStreamerReceiver<int, string, IIgniteTuple>
    {
        public async ValueTask<IList<IIgniteTuple>?> ReceiveAsync(
            IList<int> page,
            string arg,
            IDataStreamerReceiverContext context,
            CancellationToken cancellationToken)
        {
            var ignite = context.Ignite;

            var sql = $"CREATE TABLE IF NOT EXISTS {arg} (key INT PRIMARY KEY, val VARCHAR)";
            await using var queryRes = await ignite.Sql.ExecuteAsync(null, sql);

            ITable table = await ignite.Tables.GetTableAsync(arg) ?? throw new InvalidOperationException();
            IRecordView<IIgniteTuple> view = table.RecordBinaryView;

            var res = new List<IIgniteTuple>();

            await using var tx = await ignite.Transactions.BeginAsync();

            foreach (var id in page)
            {
                var rec = new IgniteTuple { ["key"] = id, ["val"] = $"val-{id}" };
                await view.UpsertAsync(tx, rec);
                res.Add(rec);
            }

            await tx.CommitAsync();

            return res;
        }
    }

    public class UpdateTupleReceiver : IDataStreamerReceiver<IIgniteTuple, object?, IIgniteTuple>
    {
        public ValueTask<IList<IIgniteTuple>?> ReceiveAsync(
            IList<IIgniteTuple> page,
            object? arg,
            IDataStreamerReceiverContext context,
            CancellationToken cancellationToken)
        {
            foreach (var rec in page)
            {
                rec["val2"] = "dotnet-test";
            }

            return ValueTask.FromResult<IList<IIgniteTuple>?>(page);
        }
    }

    public class MarshallerReceiver : IDataStreamerReceiver<ReceiverItem<string>, ReceiverArg, ReceiverResult<string>>
    {
        public IMarshaller<ReceiverItem<string>> PayloadMarshaller => new TestJsonMarshaller<ReceiverItem<string>>(new());

        public IMarshaller<ReceiverArg> ArgumentMarshaller => new TestJsonMarshaller<ReceiverArg>(new());

        public IMarshaller<ReceiverResult<string>> ResultMarshaller => new TestJsonMarshaller<ReceiverResult<string>>(new());

        public async ValueTask<IList<ReceiverResult<string>>?> ReceiveAsync(
            IList<ReceiverItem<string>> page,
            ReceiverArg arg,
            IDataStreamerReceiverContext context,
            CancellationToken cancellationToken)
        {
            await Task.Yield();

            return page.Select(x => new ReceiverResult<string>(x, arg)).ToList();
        }
    }

    public record ReceiverItem<T>(Guid Id, T Value);

    public record ReceiverArg(int A, string B);

    public record ReceiverResult<T>(ReceiverItem<T> Item, ReceiverArg Arg);
}
