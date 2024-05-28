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
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Ignite.Compute;
using Ignite.Table;
using Internal.Proto;
using Microsoft.Extensions.Logging;
using NUnit.Framework;

/// <summary>
/// Tests for <see cref="IDataStreamerTarget{T}"/>.
/// <para />
/// See DataStreamer partition awareness tests in <see cref="PartitionAwarenessTests"/>.
/// </summary>
public class DataStreamerTests : IgniteTestsBase
{
    private const string TestReceiverClassName = "org.apache.ignite.internal.runner.app.PlatformTestNodeRunner$TestReceiver";

    private const int Count = 100;

    private const int UpdatedKey = Count / 2;

    private const int DeletedKey = Count + 1;

    private static int _unknownKey = 333000;

    [SetUp]
    public async Task PrepareData()
    {
        await TupleView.UpsertAsync(null, GetTuple(UpdatedKey, "update me"));
        await TupleView.UpsertAsync(null, GetTuple(DeletedKey, "delete me"));
    }

    [TearDown]
    public async Task DeleteAll() => await Client.Sql.ExecuteAsync(null, $"DELETE FROM {TableName}");

    [Test]
    public async Task TestBasicStreamingRecordBinaryView()
    {
        await TupleView.StreamDataAsync(GetData(), DataStreamerOptions.Default with { PageSize = 10 });
        await CheckData();

        static async IAsyncEnumerable<DataStreamerItem<IIgniteTuple>> GetData()
        {
            for (int i = 0; i < Count; i++)
            {
                yield return DataStreamerItem.Create(GetTuple(i, "t" + i));
            }

            await Task.Yield();
            yield return DataStreamerItem.Create(GetTuple(DeletedKey), DataStreamerOperationType.Remove);
        }
    }

    [Test]
    public async Task TestBasicStreamingRecordView()
    {
        var options = DataStreamerOptions.Default with { PageSize = 5 };
        var data = Enumerable.Range(0, Count)
            .Select(x => DataStreamerItem.Create(GetPoco(x, "t" + x)))
            .Concat(new[] { DataStreamerItem.Create(GetPoco(DeletedKey), DataStreamerOperationType.Remove) })
            .ToList();

        await Table.GetRecordView<Poco>().StreamDataAsync(data.ToAsyncEnumerable(), options);
        await CheckData();
    }

    [Test]
    public async Task TestBasicStreamingKeyValueBinaryView()
    {
        var options = DataStreamerOptions.Default with { PageSize = 10_000 };
        var data = Enumerable.Range(0, Count)
            .Select(x => DataStreamerItem.Create(KeyValuePair.Create(GetTuple(x), GetTuple("t" + x))))
            .Concat(new[] { DataStreamerItem.Create(KeyValuePair.Create(GetTuple(DeletedKey), default(IIgniteTuple)!), DataStreamerOperationType.Remove) })
            .ToList();

        await Table.KeyValueBinaryView.StreamDataAsync(data.ToAsyncEnumerable(), options);
        await CheckData();
    }

    [Test]
    public async Task TestBasicStreamingKeyValueView()
    {
        var options = DataStreamerOptions.Default with { PageSize = 1 };
        var data = Enumerable.Range(0, Count)
            .Select(x => DataStreamerItem.Create(KeyValuePair.Create((long)x, GetPoco(x, "t" + x))))
            .Concat(new[] { DataStreamerItem.Create(KeyValuePair.Create((long)DeletedKey, default(Poco)!), DataStreamerOperationType.Remove) })
            .ToList();

        await Table.GetKeyValueView<long, Poco>().StreamDataAsync(data.ToAsyncEnumerable(), options);
        await CheckData();
    }

    [Test]
    public async Task TestAutoFlushFrequency([Values(true, false)] bool enabled)
    {
        using var cts = new CancellationTokenSource();

        _ = TupleView.StreamDataAsync(
            GetTuplesWithDelay(cts.Token),
            new()
            {
                AutoFlushFrequency = enabled
                    ? TimeSpan.FromMilliseconds(50)
                    : TimeSpan.MaxValue
            });

        if (enabled)
        {
            TestUtils.WaitForCondition(() => TupleView.ContainsKeyAsync(null, GetTuple(0)).GetAwaiter().GetResult(), 3000);
        }
        else
        {
            await Task.Delay(300);
            Assert.IsFalse(await TupleView.ContainsKeyAsync(null, GetTuple(0)));
        }

        Assert.IsFalse(await TupleView.ContainsKeyAsync(null, GetTuple(1)));

        cts.Cancel();
    }

    [Test]
    public async Task TestCancellation()
    {
        using var cts = new CancellationTokenSource();
        var streamTask = TupleView.StreamDataAsync(GetTuplesWithDelay(), cancellationToken: cts.Token);

        cts.Cancel();
        Assert.CatchAsync<OperationCanceledException>(async () => await streamTask);

        Assert.IsFalse(
            await TupleView.ContainsKeyAsync(null, GetTuple(0)),
            "No data was streamed - cancelled before any batches were full.");
    }

    [Test]
    public void TestOptionsValidation()
    {
        AssertException(DataStreamerOptions.Default with { PageSize = -10 }, "PageSize should be positive.");
        AssertException(DataStreamerOptions.Default with { RetryLimit = -1 }, "RetryLimit should be non-negative.");
        AssertException(
            DataStreamerOptions.Default with { AutoFlushFrequency = TimeSpan.FromDays(-1) },
            "AutoFlushFrequency should be positive.");

        void AssertException(DataStreamerOptions options, string message)
        {
            var ex = Assert.ThrowsAsync<ArgumentException>(
                async () => await Table.RecordBinaryView.StreamDataAsync(Array.Empty<IIgniteTuple>().ToAsyncEnumerable(), options));

            StringAssert.Contains(message, ex?.Message);
        }
    }

    [Test]
    public async Task TestRetryLimitExhausted()
    {
        using var server = new FakeServer(
            shouldDropConnection: ctx => ctx is { OpCode: ClientOp.StreamerBatchSend, RequestCount: > 7 });

        using var client = await server.ConnectClientAsync();
        var table = await client.Tables.GetTableAsync(FakeServer.ExistingTableName);

        var ex = Assert.ThrowsAsync<IgniteClientConnectionException>(
            async () => await table!.RecordBinaryView.StreamDataAsync(GetFakeServerData(10_000)));

        StringAssert.StartsWith("Operation StreamerBatchSend failed after 16 retries", ex!.Message);
    }

    [Test]
    public async Task TestRetryLimitExhaustedWithReceiver()
    {
        using var server = new FakeServer(
            shouldDropConnection: ctx => ctx is { OpCode: ClientOp.StreamerWithReceiverBatchSend, RequestCount: > 7 });

        using var client = await server.ConnectClientAsync();
        var table = await client.Tables.GetTableAsync(FakeServer.ExistingTableName);

        var ex = Assert.ThrowsAsync<IgniteClientConnectionException>(
            async () => await table!.RecordBinaryView.StreamDataAsync<IIgniteTuple, string>(
                GetFakeServerData(10_000),
                DataStreamerOptions.Default,
                keySelector: t => t,
                payloadSelector: t => t[0]!.ToString()!,
                Array.Empty<DeploymentUnit>(),
                TestReceiverClassName,
                null,
                CancellationToken.None));

        StringAssert.StartsWith("Operation StreamerWithReceiverBatchSend failed after 16 retries", ex!.Message);
    }

    [Test]
    public async Task TestManyItemsWithDisconnectAndRetry([Values(true, false)] bool withReceiver)
    {
        const int count = 100_000;
        int upsertIdx = 0;

        using var server = new FakeServer(
            shouldDropConnection: ctx => ctx.OpCode is ClientOp.StreamerBatchSend or ClientOp.StreamerWithReceiverBatchSend
                                         && Interlocked.Increment(ref upsertIdx) % 2 == 1);

        // Streamer has its own retry policy, so we can disable retries on the client.
        using var client = await server.ConnectClientAsync(new IgniteClientConfiguration
        {
            RetryPolicy = new RetryNonePolicy(),
            LoggerFactory = new ConsoleLogger(LogLevel.Trace)
        });

        var table = await client.Tables.GetTableAsync(FakeServer.ExistingTableName);

        if (withReceiver)
        {
            await table!.RecordBinaryView.StreamDataAsync<IIgniteTuple, string>(
                GetFakeServerData(count),
                DataStreamerOptions.Default,
                keySelector: t => t,
                payloadSelector: t => t[0]!.ToString()!,
                Array.Empty<DeploymentUnit>(),
                TestReceiverClassName,
                null,
                CancellationToken.None);
        }
        else
        {
            await table!.RecordBinaryView.StreamDataAsync(GetFakeServerData(count));
        }

        Assert.AreEqual(count, server.StreamerRowCount);
        Assert.That(server.DroppedConnectionCount, Is.GreaterThanOrEqualTo(count / DataStreamerOptions.Default.PageSize));
    }

    [Test]
    public async Task TestAddUpdateRemoveMixed(
        [Values(1, 2, 100)] int pageSize,
        [Values(true, false)] bool existingMinKey)
    {
        var minKey = existingMinKey ? UpdatedKey : Interlocked.Add(ref _unknownKey, 10);
        await Table.GetRecordView<Poco>().StreamDataAsync(
            GetData(),
            DataStreamerOptions.Default with { PageSize = pageSize });

        IList<Option<Poco>> res = await PocoView.GetAllAsync(null, Enumerable.Range(minKey, 4).Select(x => GetPoco(x)));
        Assert.AreEqual(4, res.Count);

        Assert.IsFalse(res[0].HasValue, "Deleted key should not exist: " + res[0]);

        Assert.IsTrue(res[1].HasValue);
        Assert.AreEqual("created2", res[1].Value.Val);

        Assert.IsTrue(res[2].HasValue);
        Assert.AreEqual("updated", res[2].Value.Val);

        Assert.IsTrue(res[3].HasValue);
        Assert.AreEqual("created", res[3].Value.Val);

        async IAsyncEnumerable<DataStreamerItem<Poco>> GetData()
        {
            await Task.Yield();
            yield return DataStreamerItem.Create(GetPoco(minKey, "created"));
            yield return DataStreamerItem.Create(GetPoco(minKey, "updated"));
            yield return DataStreamerItem.Create(GetPoco(minKey, "deleted"), DataStreamerOperationType.Remove);

            yield return DataStreamerItem.Create(GetPoco(minKey + 1, "created"));
            yield return DataStreamerItem.Create(GetPoco(minKey + 1, "updated"));
            yield return DataStreamerItem.Create(GetPoco(minKey + 1, "deleted"), DataStreamerOperationType.Remove);
            yield return DataStreamerItem.Create(GetPoco(minKey + 1, "created2"));

            yield return DataStreamerItem.Create(GetPoco(minKey + 2, "created"));
            yield return DataStreamerItem.Create(GetPoco(minKey + 2, "updated"));

            yield return DataStreamerItem.Create(GetPoco(minKey + 3, "created"));
        }
    }

    [Test]
    public async Task TestWithReceiverRecordBinaryView()
    {
        await TupleView.StreamDataAsync<int, string>(
            Enumerable.Range(0, Count).ToList().ToAsyncEnumerable(),
            DataStreamerOptions.Default,
            keySelector: x => GetTuple(x),
            payloadSelector: x => $"{x}-value{x * 10}",
            units: Array.Empty<DeploymentUnit>(),
            receiverClassName: TestReceiverClassName,
            receiverArgs: new object[] { Table.Name, "arg1", 22 });

        for (int i = 0; i < Count; i++)
        {
            var res = await TupleView.GetAsync(null, GetTuple(i));

            Assert.IsTrue(res.HasValue);
            Assert.AreEqual($"value{i * 10}_arg1_22", res.Value[ValCol]);
        }
    }

    [Test]
    public async Task TestWithReceiverRecordView()
    {
        await PocoView.StreamDataAsync<int, string>(
            Enumerable.Range(0, Count).ToList().ToAsyncEnumerable(),
            DataStreamerOptions.Default,
            keySelector: x => GetPoco(x),
            payloadSelector: x => $"{x}-value{x * 10}",
            units: Array.Empty<DeploymentUnit>(),
            receiverClassName: TestReceiverClassName,
            receiverArgs: new object[] { Table.Name, "arg1", 22 });

        for (int i = 0; i < Count; i++)
        {
            var res = await TupleView.GetAsync(null, GetTuple(i));

            Assert.IsTrue(res.HasValue);
            Assert.AreEqual($"value{i * 10}_arg1_22", res.Value[ValCol]);
        }
    }

    [Test]
    public async Task TestWithReceiverKeyValueBinaryView()
    {
        await Table.KeyValueBinaryView.StreamDataAsync<int, string>(
            Enumerable.Range(0, Count).ToList().ToAsyncEnumerable(),
            DataStreamerOptions.Default,
            keySelector: x => new KeyValuePair<IIgniteTuple, IIgniteTuple>(GetTuple(x), new IgniteTuple()),
            payloadSelector: x => $"{x}-value{x * 10}",
            units: Array.Empty<DeploymentUnit>(),
            receiverClassName: TestReceiverClassName,
            receiverArgs: new object[] { Table.Name, "arg1", 22 });

        for (int i = 0; i < Count; i++)
        {
            var res = await TupleView.GetAsync(null, GetTuple(i));

            Assert.IsTrue(res.HasValue);
            Assert.AreEqual($"value{i * 10}_arg1_22", res.Value[ValCol]);
        }
    }

    [Test]
    public async Task TestWithReceiverKeyValueView()
    {
        await Table.GetKeyValueView<long, Poco>().StreamDataAsync<int, string>(
            Enumerable.Range(0, Count).ToList().ToAsyncEnumerable(),
            DataStreamerOptions.Default,
            keySelector: x => new KeyValuePair<long, Poco>(x, null!),
            payloadSelector: x => $"{x}-value{x * 10}",
            units: Array.Empty<DeploymentUnit>(),
            receiverClassName: TestReceiverClassName,
            receiverArgs: new object[] { Table.Name, "arg11", 55});

        for (int i = 0; i < Count; i++)
        {
            var res = await TupleView.GetAsync(null, GetTuple(i));

            Assert.IsTrue(res.HasValue);
            Assert.AreEqual($"value{i * 10}_arg11_55", res.Value[ValCol]);
        }
    }

    [Test]
    public async Task TestUnknownReceiverClass()
    {
        // TODO
        await Task.Yield();
    }

    [Test]
    public async Task TestReceiverException()
    {
        // TODO
        await Task.Yield();
    }

    [Test]
    public async Task TestWithReceiverAllDataTypes()
    {
        // TODO
        await Task.Yield();
    }

    private static async IAsyncEnumerable<IIgniteTuple> GetFakeServerData(int count)
    {
        for (var i = 0; i < count; i++)
        {
            yield return new IgniteTuple { ["ID"] = i };
            await Task.Yield();
        }
    }

    private static async IAsyncEnumerable<IIgniteTuple> GetTuplesWithDelay([EnumeratorCancellation] CancellationToken ct = default)
    {
        for (var i = 0; i < 3; i++)
        {
            yield return GetTuple(i, "t" + i);
            await Task.Delay(15000, ct);
        }
    }

    private async Task CheckData()
    {
        var data = Enumerable.Range(0, Count).Select(x => GetTuple(x));
        var res = await TupleView.GetAllAsync(null, data);

        Assert.AreEqual(Count, res.Count);

        foreach (var (_, hasVal) in res)
        {
            Assert.IsTrue(hasVal);
        }

        var deletedExists = await TupleView.ContainsKeyAsync(null, GetTuple(DeletedKey));
        Assert.IsFalse(deletedExists);
    }
}
