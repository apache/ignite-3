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
using Ignite.Table;
using Internal.Proto;
using NUnit.Framework;

/// <summary>
/// Tests for <see cref="IDataStreamerTarget{T}.StreamDataAsync"/>.
/// <para />
/// See DataStreamer partition awareness tests in <see cref="PartitionAwarenessTests"/>.
/// </summary>
public class DataStreamerTests : IgniteTestsBase
{
    private const int Count = 100;

    [SetUp]
    public async Task DeleteAll() =>
        await TupleView.DeleteAllAsync(null, Enumerable.Range(0, Count).Select(x => GetTuple(x)));

    [Test]
    public async Task TestBasicStreamingRecordBinaryView()
    {
        var options = DataStreamerOptions.Default with { BatchSize = 10 };
        var data = Enumerable.Range(0, Count).Select(x => GetTuple(x, "t" + x)).ToList();

        await TupleView.StreamDataAsync(data.ToAsyncEnumerable(), options);
        await CheckData();
    }

    [Test]
    public async Task TestBasicStreamingRecordView()
    {
        var options = DataStreamerOptions.Default with { BatchSize = 5 };
        var data = Enumerable.Range(0, Count).Select(x => GetPoco(x, "t" + x)).ToList();

        await Table.GetRecordView<Poco>().StreamDataAsync(data.ToAsyncEnumerable(), options);
        await CheckData();
    }

    [Test]
    public async Task TestBasicStreamingKeyValueBinaryView()
    {
        var options = DataStreamerOptions.Default with { BatchSize = 10_000 };
        var data = Enumerable.Range(0, Count)
            .Select(x => new KeyValuePair<IIgniteTuple, IIgniteTuple>(GetTuple(x), GetTuple("t" + x)))
            .ToList();

        await Table.KeyValueBinaryView.StreamDataAsync(data.ToAsyncEnumerable(), options);
        await CheckData();
    }

    [Test]
    public async Task TestBasicStreamingKeyValueView()
    {
        var options = DataStreamerOptions.Default with { BatchSize = 1 };
        var data = Enumerable.Range(0, Count)
            .Select(x => new KeyValuePair<long, Poco>(x, GetPoco(x, "t" + x)))
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
        AssertException(DataStreamerOptions.Default with { BatchSize = -10 }, "BatchSize should be positive.");
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
            shouldDropConnection: ctx => ctx is { OpCode: ClientOp.TupleUpsertAll, RequestCount: > 7 });

        using var client = await server.ConnectClientAsync();
        var table = await client.Tables.GetTableAsync(FakeServer.ExistingTableName);

        var ex = Assert.ThrowsAsync<IgniteClientConnectionException>(
            async () => await table!.RecordBinaryView.StreamDataAsync(GetFakeServerData(10_000)));

        StringAssert.StartsWith("Operation TupleUpsertAll failed after 16 retries", ex!.Message);
    }

    [Test]
    public async Task TestManyItemsWithDisconnectAndRetry()
    {
        const int count = 100_000;
        int upsertIdx = 0;

        using var server = new FakeServer(
            shouldDropConnection: ctx => ctx.OpCode == ClientOp.TupleUpsertAll && Interlocked.Increment(ref upsertIdx) % 2 == 1);

        // Streamer has it's own retry policy, so we can disable retries on the client.
        using var client = await server.ConnectClientAsync(new IgniteClientConfiguration
        {
            RetryPolicy = new RetryNonePolicy()
        });

        var table = await client.Tables.GetTableAsync(FakeServer.ExistingTableName);
        await table!.RecordBinaryView.StreamDataAsync(GetFakeServerData(count));

        Assert.AreEqual(count, server.UpsertAllRowCount);
        Assert.That(server.DroppedConnectionCount, Is.GreaterThanOrEqualTo(count / DataStreamerOptions.Default.BatchSize));
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
    }
}
