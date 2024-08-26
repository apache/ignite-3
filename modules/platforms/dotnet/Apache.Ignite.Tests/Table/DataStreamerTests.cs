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
using System.Data;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Compute;
using Ignite.Table;
using Internal.Proto;
using Microsoft.Extensions.Logging;
using NodaTime;
using NUnit.Framework;

/// <summary>
/// Tests for <see cref="IDataStreamerTarget{T}"/>.
/// <para />
/// See DataStreamer partition awareness tests in <see cref="PartitionAwarenessTests"/>.
/// </summary>
public class DataStreamerTests : IgniteTestsBase
{
    private const string TestReceiverClassName = ComputeTests.PlatformTestNodeRunner + "$TestReceiver";

    private const string EchoArgsReceiverClassName = ComputeTests.PlatformTestNodeRunner + "$EchoArgsReceiver";

    private const string UpsertElementTypeNameReceiverClassName = ComputeTests.PlatformTestNodeRunner + "$UpsertElementTypeNameReceiver";

    private const int Count = 100;

    private const int UpdatedKey = Count / 2;

    private const int DeletedKey = Count + 1;

    private static readonly ReceiverDescriptor<string?, string> TestReceiver = new(TestReceiverClassName);

    private static readonly ReceiverDescriptor<object?> TestReceiverNoResults = new(TestReceiverClassName);

    private static readonly ReceiverDescriptor<object, object> EchoArgsReceiver = new(EchoArgsReceiverClassName);

    private static readonly object[] AllSupportedTypes =
    {
        true,
        sbyte.MaxValue,
        short.MinValue,
        int.MaxValue,
        long.MinValue,
        float.MaxValue,
        double.MinValue,
        decimal.One,
        new LocalDate(1234, 5, 6),
        new LocalTime(12, 3, 4, 567),
        new LocalDateTime(1234, 5, 6, 7, 8, 9),
        Instant.FromUnixTimeSeconds(123456),
        Guid.Empty,
        "str123",
        new byte[] { 1, 2, 3 },
        Period.FromDays(999),
        Duration.FromSeconds(12345),
    };

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
    public async Task TestAutoFlushInterval(
        [Values(true, false)] bool enabled,
        [Values(true, false)] bool withReceiver)
    {
        using var cts = new CancellationTokenSource();

        var options = new DataStreamerOptions
        {
            AutoFlushInterval = enabled
                ? TimeSpan.FromMilliseconds(50)
                : TimeSpan.MaxValue
        };

        if (withReceiver)
        {
            _ = TupleView.StreamDataAsync(
                GetTuplesWithDelay(cts.Token),
                x => GetTuple((long)x[0]!),
                x => $"{x[0]}-value",
                TestReceiverNoResults,
                receiverArg: GetReceiverArg(Table.Name, "arg1", 22),
                options: options);
        }
        else
        {
            _ = TupleView.StreamDataAsync(GetTuplesWithDelay(cts.Token), options);
        }

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
    public void TestOptionsValidation([Values(true, false, null)] bool? withReceiverResults)
    {
        AssertException(DataStreamerOptions.Default with { PageSize = -10 }, "PageSize should be positive.");
        AssertException(DataStreamerOptions.Default with { RetryLimit = -1 }, "RetryLimit should be non-negative.");
        AssertException(
            DataStreamerOptions.Default with { AutoFlushInterval = TimeSpan.FromDays(-1) },
            "AutoFlushInterval should be positive.");

        void AssertException(DataStreamerOptions options, string message)
        {
            var ex = Assert.ThrowsAsync<ArgumentException>(
                async () =>
                {
                    switch (withReceiverResults)
                    {
                        // No receiver.
                        case null:
                            await Table.RecordBinaryView.StreamDataAsync(Array.Empty<IIgniteTuple>().ToAsyncEnumerable(), options);
                            break;

                        // Receiver without results.
                        case false:
                            await Table.RecordBinaryView.StreamDataAsync(
                                Array.Empty<IIgniteTuple>().ToAsyncEnumerable(),
                                t => t,
                                t => t.ToString()!,
                                TestReceiverNoResults,
                                null,
                                options);

                            break;

                        // Receiver with results.
                        case true:
                            await Table.RecordBinaryView.StreamDataAsync(
                                Array.Empty<IIgniteTuple>().ToAsyncEnumerable(),
                                t => t,
                                t => t.ToString()!,
                                TestReceiver,
                                null,
                                options).ToListAsync();

                            break;
                    }
                });

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
            async () => await table!.RecordBinaryView.StreamDataAsync(
                GetFakeServerData(10_000),
                keySelector: t => t,
                payloadSelector: t => t[0]!.ToString()!,
                TestReceiverNoResults,
                null));

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
            await table!.RecordBinaryView.StreamDataAsync(
                GetFakeServerData(count),
                keySelector: t => t,
                payloadSelector: t => t[0]!.ToString()!,
                TestReceiverNoResults,
                null);
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
        await TupleView.StreamDataAsync(
            Enumerable.Range(0, Count).ToAsyncEnumerable(),
            keySelector: x => GetTuple(x),
            payloadSelector: x => $"{x}-value{x * 10}",
            receiver: TestReceiverNoResults,
            receiverArg: GetReceiverArg(Table.Name, "arg1", 22),
            options: DataStreamerOptions.Default);

        for (int i = 0; i < Count; i++)
        {
            var res = await TupleView.GetAsync(null, GetTuple(i));

            Assert.IsTrue(res.HasValue);
            Assert.AreEqual($"value{i * 10}_arg1_22", res.Value[ValCol]);
        }
    }

    [Test]
    public async Task TestWithReceiverWithResultsRecordBinaryView()
    {
        IAsyncEnumerable<string> results = TupleView.StreamDataAsync(
            Enumerable.Range(0, Count).ToAsyncEnumerable(),
            keySelector: x => GetTuple(x),
            payloadSelector: x => $"{x}-value{x * 10}",
            TestReceiver,
            receiverArg: GetReceiverArg(Table.Name, "arg1", 22),
            options: DataStreamerOptions.Default);

        var resultSet = await results.ToHashSetAsync();

        for (int i = 0; i < Count; i++)
        {
            var res = await TupleView.GetAsync(null, GetTuple(i));

            var expectedVal = $"value{i * 10}_arg1_22";

            Assert.IsTrue(res.HasValue);
            Assert.AreEqual(expectedVal, res.Value[ValCol]);

            CollectionAssert.Contains(resultSet, expectedVal);
        }

        Assert.AreEqual(Count, resultSet.Count);
    }

    [Test]
    public async Task TestWithReceiverRecordView()
    {
        await PocoView.StreamDataAsync(
            Enumerable.Range(0, Count).ToAsyncEnumerable(),
            keySelector: x => GetPoco(x),
            payloadSelector: x => $"{x}-value{x * 10}",
            receiver: TestReceiverNoResults,
            receiverArg: GetReceiverArg(Table.Name, "arg1", 22),
            options: DataStreamerOptions.Default);

        for (int i = 0; i < Count; i++)
        {
            var res = await TupleView.GetAsync(null, GetTuple(i));

            Assert.IsTrue(res.HasValue);
            Assert.AreEqual($"value{i * 10}_arg1_22", res.Value[ValCol]);
        }
    }

    [Test]
    public async Task TestWithReceiverResultsRecordView()
    {
        IAsyncEnumerable<string> results = PocoView.StreamDataAsync(
            Enumerable.Range(0, Count).ToAsyncEnumerable(),
            keySelector: x => GetPoco(x),
            payloadSelector: x => $"{x}-value{x * 10}",
            TestReceiver,
            receiverArg: GetReceiverArg(Table.Name, "arg1", 22),
            options: DataStreamerOptions.Default);

        var resultSet = await results.ToHashSetAsync();

        for (int i = 0; i < Count; i++)
        {
            var res = await TupleView.GetAsync(null, GetTuple(i));

            var expectedVal = $"value{i * 10}_arg1_22";

            Assert.IsTrue(res.HasValue);
            Assert.AreEqual(expectedVal, res.Value[ValCol]);

            CollectionAssert.Contains(resultSet, expectedVal);
        }

        Assert.AreEqual(Count, resultSet.Count);
    }

    [Test]
    public async Task TestWithReceiverKeyValueBinaryView()
    {
        await Table.KeyValueBinaryView.StreamDataAsync(
            Enumerable.Range(0, Count).ToAsyncEnumerable(),
            keySelector: x => new KeyValuePair<IIgniteTuple, IIgniteTuple>(GetTuple(x), new IgniteTuple()),
            payloadSelector: x => $"{x}-value{x * 10}",
            receiver: TestReceiverNoResults,
            receiverArg: GetReceiverArg(Table.Name, "arg1", 22));

        for (int i = 0; i < Count; i++)
        {
            var res = await TupleView.GetAsync(null, GetTuple(i));

            Assert.IsTrue(res.HasValue);
            Assert.AreEqual($"value{i * 10}_arg1_22", res.Value[ValCol]);
        }
    }

    [Test]
    public async Task TestWithReceiverResultsKeyValueBinaryView()
    {
        IAsyncEnumerable<string> results = Table.KeyValueBinaryView.StreamDataAsync(
            Enumerable.Range(0, Count).ToAsyncEnumerable(),
            keySelector: x => new KeyValuePair<IIgniteTuple, IIgniteTuple>(GetTuple(x), new IgniteTuple()),
            payloadSelector: x => $"{x}-value{x * 10}",
            TestReceiver,
            receiverArg: GetReceiverArg(Table.Name, "arg1", 22));

        var resultSet = await results.ToHashSetAsync();

        for (int i = 0; i < Count; i++)
        {
            var res = await TupleView.GetAsync(null, GetTuple(i));

            var expectedVal = $"value{i * 10}_arg1_22";

            Assert.IsTrue(res.HasValue);
            Assert.AreEqual(expectedVal, res.Value[ValCol]);

            CollectionAssert.Contains(resultSet, expectedVal);
        }

        Assert.AreEqual(Count, resultSet.Count);
    }

    [Test]
    public async Task TestWithReceiverKeyValueView()
    {
        await Table.GetKeyValueView<long, Poco>().StreamDataAsync(
            Enumerable.Range(0, Count).ToAsyncEnumerable(),
            keySelector: x => new KeyValuePair<long, Poco>(x, null!),
            payloadSelector: x => $"{x}-value{x * 10}",
            receiver: TestReceiverNoResults,
            receiverArg: GetReceiverArg(Table.Name, "arg11", 55));

        for (int i = 0; i < Count; i++)
        {
            var res = await TupleView.GetAsync(null, GetTuple(i));

            Assert.IsTrue(res.HasValue);
            Assert.AreEqual($"value{i * 10}_arg11_55", res.Value[ValCol]);
        }
    }

    [Test]
    public async Task TestWithReceiverResultsKeyValueView()
    {
        IAsyncEnumerable<string> results = Table.GetKeyValueView<long, Poco>().StreamDataAsync(
            Enumerable.Range(0, Count).ToAsyncEnumerable(),
            keySelector: x => new KeyValuePair<long, Poco>(x, null!),
            payloadSelector: x => $"{x}-value{x * 10}",
            TestReceiver,
            receiverArg: GetReceiverArg(Table.Name, "arg11", 55));

        var resultSet = await results.ToHashSetAsync();

        for (int i = 0; i < Count; i++)
        {
            var res = await TupleView.GetAsync(null, GetTuple(i));

            var expectedVal = $"value{i * 10}_arg11_55";

            Assert.IsTrue(res.HasValue);
            Assert.AreEqual(expectedVal, res.Value[ValCol]);

            CollectionAssert.Contains(resultSet, expectedVal);
        }

        Assert.AreEqual(Count, resultSet.Count);
    }

    [Test]
    public void TestUnknownReceiverClass()
    {
        var ex = Assert.ThrowsAsync<IgniteException>(async () =>
            await TupleView.StreamDataAsync(
                Enumerable.Range(0, 1).ToAsyncEnumerable(),
                keySelector: x => GetTuple(x),
                payloadSelector: _ => string.Empty,
                new ReceiverDescriptor<object?>("_unknown_"),
                null));

        Assert.AreEqual("Streamer receiver failed: Cannot load receiver class by name '_unknown_'", ex.Message);
    }

    [Test]
    public void TestReceiverException()
    {
        var ex = Assert.ThrowsAsync<IgniteException>(async () =>
            await PocoView.StreamDataAsync(
                Enumerable.Range(0, 1).ToAsyncEnumerable(),
                keySelector: x => GetPoco(x),
                payloadSelector: _ => string.Empty,
                receiver: TestReceiverNoResults,
                receiverArg: GetReceiverArg("throw", "throw", 1)));

        Assert.AreEqual("Streamer receiver failed: Job execution failed: java.lang.ArithmeticException: Test exception: 1", ex.Message);
    }

    [Test]
    public void TestReceiverWithResultsException()
    {
        var ex = Assert.ThrowsAsync<IgniteException>(async () =>
            await PocoView.StreamDataAsync(
                Enumerable.Range(0, 1).ToAsyncEnumerable(),
                keySelector: x => GetPoco(x),
                payloadSelector: _ => string.Empty,
                TestReceiver,
                receiverArg: GetReceiverArg("throw", "throw", 1)).ToListAsync());

        Assert.AreEqual("Streamer receiver failed: Job execution failed: java.lang.ArithmeticException: Test exception: 1", ex.Message);
    }

    [Test]
    public void TestReceiverSelectorException([Values(true, false)] bool keySelector)
    {
        var ex = Assert.ThrowsAsync<DataException>(async () =>
            await PocoView.StreamDataAsync<int, object, object?>(
                Enumerable.Range(0, 1).ToAsyncEnumerable(),
                keySelector: x => keySelector ? throw new DataException("key") : GetPoco(x),
                payloadSelector: _ => throw new DataException("payload"),
                receiver: TestReceiverNoResults,
                receiverArg: GetReceiverArg("throw", "throw", 1)));

        Assert.AreEqual(keySelector ? "key" : "payload", ex.Message);
    }

    [Test]
    public async Task TestWithReceiverAllDataTypes()
    {
        // Invoke receiver with all supported element types and check resulting Java class and string representation.
        await CheckReceiverValue(true, "java.lang.Boolean", "true");
        await CheckReceiverValue((sbyte)-3, "java.lang.Byte", "-3");
        await CheckReceiverValue(short.MinValue, "java.lang.Short", "-32768");
        await CheckReceiverValue(int.MinValue, "java.lang.Integer", "-2147483648");
        await CheckReceiverValue(long.MinValue, "java.lang.Long", "-9223372036854775808");
        await CheckReceiverValue(float.MinValue, "java.lang.Float", "-3.4028235E38");
        await CheckReceiverValue(double.MinValue, "java.lang.Double", "-1.7976931348623157E308");

        await CheckReceiverValue(decimal.One, "java.math.BigDecimal", "1");
        await CheckReceiverValue(decimal.MinValue, "java.math.BigDecimal", "-79228162514264337593543950335");

        await CheckReceiverValue(new LocalDate(1234, 5, 6), "java.time.LocalDate", "1234-05-06");
        await CheckReceiverValue(new LocalTime(12, 3, 4, 567), "java.time.LocalTime", "12:03:04.567");
        await CheckReceiverValue(new LocalDateTime(1234, 5, 6, 7, 8, 9), "java.time.LocalDateTime", "1234-05-06T07:08:09");
        await CheckReceiverValue(Instant.MinValue, "java.time.Instant", "-9998-01-01T00:00:00Z");

        await CheckReceiverValue("str1", "java.lang.String", "str1");
        await CheckReceiverValue(Guid.Empty, "java.util.UUID", "00000000-0000-0000-0000-000000000000");
        await CheckReceiverValue(new byte[] { 1, 2, 3 }, "[B", "[1, 2, 3]");

        await CheckReceiverValue(Period.FromDays(999), "java.time.Period", "P999D");
        await CheckReceiverValue(Duration.FromSeconds(12345), "java.time.Duration", "PT3H25M45S");
    }

    [Test]
    public void TestWithReceiverUnsupportedDataTypeThrows()
    {
        var ex = Assert.ThrowsAsync<IgniteClientException>(
            async () => await CheckReceiverValue(GetPoco(1), "java.lang.Boolean", "true"));

        Assert.AreEqual("Unsupported type: Apache.Ignite.Tests.Table.Poco", ex.Message);
    }

    [Test]
    public void TestWithReceiverDifferentDataTypesThrows()
    {
        var ex = Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await PocoView.StreamDataAsync(
                new object[] { 1, "2" }.ToAsyncEnumerable(),
                keySelector: _ => new Poco(),
                payloadSelector: x => x,
                receiver: TestReceiverNoResults,
                receiverArg: null));

        Assert.AreEqual(
            "All streamer items returned by payloadSelector must be of the same type. Expected: System.Int32, actual: System.String.",
            ex.Message);
    }

    [Test]
    public void TestWithReceiverNullItemThrows()
    {
        var ex = Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await PocoView.StreamDataAsync(
                new object[] { "2", null! }.ToAsyncEnumerable(),
                keySelector: _ => new Poco(),
                payloadSelector: x => x,
                receiver: TestReceiverNoResults,
                receiverArg: null));

        Assert.AreEqual(
            "Value cannot be null. (Parameter 'payload')",
            ex.Message);
    }

    [TestCaseSource(nameof(AllSupportedTypes))]
    public async Task TestEchoReceiverAllDataTypes(object arg)
    {
        var res = await PocoView.StreamDataAsync<object, object, object, object>(
            new object[] { 1 }.ToAsyncEnumerable(),
            keySelector: _ => new Poco(),
            payloadSelector: x => x.ToString()!,
            EchoArgsReceiver,
            receiverArg: arg).SingleAsync();

        Assert.AreEqual(arg, res);
    }

    [Test]
    public async Task TestResultConsumerEarlyExit()
    {
        IAsyncEnumerable<string> results = PocoView.StreamDataAsync(
            Enumerable.Range(0, Count).ToAsyncEnumerable(),
            keySelector: x => GetPoco(x),
            payloadSelector: x => $"{x}-value{x * 10}",
            TestReceiver,
            receiverArg: GetReceiverArg(Table.Name, "arg1", 22),
            options: DataStreamerOptions.Default with { PageSize = 1 });

        // Read only part of the results.
        var resultSet = await results.Take(3).ToListAsync();
        Assert.AreEqual(3, resultSet.Count);

        for (int i = 0; i < Count; i++)
        {
            var res = await TupleView.GetAsync(null, GetTuple(i));

            var expectedVal = $"value{i * 10}_arg1_22";

            Assert.IsTrue(res.HasValue, $"Key {i} not found");
            Assert.AreEqual(expectedVal, res.Value[ValCol]);
        }
    }

    [Test]
    public async Task TestResultConsumerCancellation()
    {
        IAsyncEnumerable<string> results = PocoView.StreamDataAsync<int, string, string?, string>(
            Enumerable.Range(0, Count).ToAsyncEnumerable(),
            keySelector: x => GetPoco(x),
            payloadSelector: x => $"{x}-value{x * 10}",
            TestReceiver,
            receiverArg: GetReceiverArg(Table.Name, "arg1", 22),
            options: DataStreamerOptions.Default with { PageSize = 1 });

        var cts = new CancellationTokenSource();

        await using var enumerator = results.GetAsyncEnumerator(cts.Token);
        Assert.IsTrue(await enumerator.MoveNextAsync());

        // Cancel the resulting enumerator before it's fully consumed. This stops the streamer.
        cts.Cancel();
        Assert.ThrowsAsync<TaskCanceledException>(async () => await enumerator.MoveNextAsync());

        // Only part of the data was streamed.
        var streamedData = await TupleView.GetAllAsync(null, Enumerable.Range(0, Count).Select(x => GetTuple(x)));
        Assert.Less(streamedData.Count(x => x.HasValue), Count / 2);
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

    private static string GetReceiverArg(string tableName, string arg1, int arg2) =>
        $"{tableName}:{arg1}:{arg2}";

    private async Task CheckReceiverValue(object value, string expectedClassName, string expectedValue)
    {
        var key1 = 1L;
        var key2 = 2L;

        await PocoView.StreamDataAsync(
            Enumerable.Range(0, 1).ToAsyncEnumerable(),
            keySelector: x => GetPoco(x),
            payloadSelector: _ => value,
            receiver: new ReceiverDescriptor<string>(UpsertElementTypeNameReceiverClassName),
            receiverArg: $"{TableName}:{key1}:{key2}");

        var className = (await TupleView.GetAsync(null, GetTuple(key1))).Value[1];
        var valueStr = (await TupleView.GetAsync(null, GetTuple(key2))).Value[1];
        Assert.AreEqual(expectedClassName, className);
        Assert.AreEqual(expectedValue, valueStr);
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
