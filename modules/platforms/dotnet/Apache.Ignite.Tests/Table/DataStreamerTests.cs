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
    public async Task SetUp() =>
        await TupleView.DeleteAllAsync(null, Enumerable.Range(0, Count).Select(x => GetTuple(x)));

    [Test]
    public async Task TestBasicStreamingRecordBinaryView()
    {
        var options = DataStreamerOptions.Default with { BatchSize = 10 };
        var data = Enumerable.Range(1, Count).Select(x => GetTuple(x, "t" + x)).ToList();

        await TupleView.StreamDataAsync(data.ToAsyncEnumerable(), options);
        var res = await TupleView.GetAllAsync(null, data);

        Assert.AreEqual(res.Count, data.Count);

        foreach (var (_, hasVal) in res)
        {
            Assert.IsTrue(hasVal);
        }
    }

    [Test]
    public async Task TestBasicStreamingKeyValueBinaryView()
    {
        var options = DataStreamerOptions.Default with { BatchSize = 10 };
        var data = Enumerable.Range(1, Count)
            .Select(x => new KeyValuePair<IIgniteTuple, IIgniteTuple>(GetTuple(x), GetTuple(x, "t" + x)))
            .ToList();

        await Table.KeyValueBinaryView.StreamDataAsync(data.ToAsyncEnumerable(), options);
        var res = await TupleView.GetAllAsync(null, data.Select(x => x.Key));

        Assert.AreEqual(res.Count, data.Count);

        foreach (var (_, hasVal) in res)
        {
            Assert.IsTrue(hasVal);
        }
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

        await Task.Delay(100);

        Assert.AreEqual(enabled, await TupleView.ContainsKeyAsync(null, GetTuple(0)));
        Assert.IsFalse(await TupleView.ContainsKeyAsync(null, GetTuple(1)));

        cts.Cancel();

        async IAsyncEnumerable<IIgniteTuple> GetTuplesWithDelay([EnumeratorCancellation] CancellationToken ct = default)
        {
            for (var i = 0; i < 3; i++)
            {
                yield return GetTuple(i, "t" + i);
                await Task.Delay(15000, ct);
            }
        }
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
        await Task.Delay(1);
        Assert.Fail("TODO");
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
        await table!.RecordBinaryView.StreamDataAsync(GetData());

        Assert.AreEqual(count, server.UpsertAllRowCount);
        Assert.AreEqual(count / DataStreamerOptions.Default.BatchSize, server.DroppedConnectionCount);

        async IAsyncEnumerable<IIgniteTuple> GetData()
        {
            for (var i = 0; i < count; i++)
            {
                yield return new IgniteTuple { ["ID"] = i };
                await Task.Yield();
            }
        }
    }
}
