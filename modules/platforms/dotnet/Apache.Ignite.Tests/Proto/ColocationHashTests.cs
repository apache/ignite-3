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

namespace Apache.Ignite.Tests.Proto;

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Threading.Tasks;
using Ignite.Table;
using Internal.Proto;
using Internal.Proto.BinaryTuple;
using Internal.Table;
using Internal.Table.Serialization;
using NodaTime;
using NUnit.Framework;

/// <summary>
/// Tests that colocation hash calculation is consistent with server logic.
/// </summary>
public class ColocationHashTests : IgniteTestsBase
{
    private const string ColocationHashJob = "org.apache.ignite.internal.runner.app.PlatformTestNodeRunner$ColocationHashJob";

    private static readonly object[] TestCases =
    {
        sbyte.MinValue,
        (sbyte)1,
        (sbyte)-1,
        sbyte.MaxValue,
        short.MinValue,
        (short)1,
        (short)-1,
        short.MaxValue,
        int.MinValue,
        1,
        0,
        -1,
        int.MaxValue,
        long.MinValue,
        1L,
        -1L,
        long.MaxValue,
        float.MinValue,
        -1.1f,
        1.1f,
        float.Epsilon,
        float.MaxValue,
        double.MinValue,
        -1.1d,
        1.1d,
        double.Epsilon,
        double.MaxValue,
        decimal.MinValue,
        -1.1m,
        1.1m,
        123.45678m,
        decimal.MaxValue,
        string.Empty,
        "abc αβγ 🔥",
        Guid.Empty,
        Guid.NewGuid(),
        BigInteger.One,
        BigInteger.Zero,
        BigInteger.MinusOne,
        (BigInteger)int.MaxValue,
        (BigInteger)int.MinValue,
        (BigInteger)ulong.MaxValue,
        BigInteger.Pow(123, 100),
        new BitArray(1, false),
        new BitArray(new byte[] {0, 5, 0}),
        new BitArray(17, true),
        new LocalDate(9876, 7, 30),
        new LocalDate(2, 1, 1),
        new LocalDate(1, 1, 1),
        default(LocalDate),
        new LocalTime(9, 8, 7),
        LocalTime.Midnight,
        LocalTime.Noon,
        LocalDateTime.FromDateTime(DateTime.UtcNow).TimeOfDay,
        default(LocalTime),
        new LocalDateTime(year: 1, month: 1, day: 1, hour: 1, minute: 1, second: 1, millisecond: 1),
        new LocalDateTime(year: 2022, month: 10, day: 22, hour: 10, minute: 30, second: 55, millisecond: 123),
        LocalDateTime.FromDateTime(DateTime.UtcNow),
        default(LocalDateTime),
        Instant.FromUnixTimeSeconds(0),
        default(Instant)
    };

    [Test]
    [TestCaseSource(nameof(TestCases))]
    public async Task TestSingleKeyColocationHashIsSameOnServerAndClient(object key) =>
        await AssertClientAndServerHashesAreEqual(keys: key);

    [Test]
    public async Task TestLocalTimeColocationHashIsSameOnServerAndClient([Values(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)] int timePrecision) =>
        await AssertClientAndServerHashesAreEqual(timePrecision, keys: new LocalTime(1, 2, 3, 999));

    [Test]
    public async Task TestLocalDateTimeColocationHashIsSameOnServerAndClient([Values(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)] int timePrecision) =>
        await AssertClientAndServerHashesAreEqual(timePrecision, keys: new LocalDateTime(2022, 01, 27, 1, 2, 3, 999));

    [Test]
    public async Task TestTimestampColocationHashIsSameOnServerAndClient(
        [Values(0, 1, 2, 3, 4, 5, 6)] int timestampPrecision) =>
        await AssertClientAndServerHashesAreEqual(timestampPrecision: timestampPrecision, keys: Instant.FromDateTimeUtc(DateTime.UtcNow));

    [Test]
    public async Task TestMultiKeyColocationHashIsSameOnServerAndClient()
    {
        for (var i = 0; i < TestCases.Length; i++)
        {
            await AssertClientAndServerHashesAreEqual(keys: TestCases.Take(i + 1).ToArray());
            await AssertClientAndServerHashesAreEqual(keys: TestCases.Skip(i).ToArray());
        }
    }

    [Test]
    public async Task TestMultiKeyColocationHashIsSameOnServerAndClientCustomTimePrecision(
        [Values(0, 1, 4, 9)] int timePrecision,
        [Values(0, 1, 3, 6)] int timestampPrecision)
    {
        for (var i = 0; i < TestCases.Length; i++)
        {
            await AssertClientAndServerHashesAreEqual(timePrecision, timestampPrecision, TestCases.Take(i + 1).ToArray());
            await AssertClientAndServerHashesAreEqual(timePrecision, timestampPrecision, TestCases.Skip(i).ToArray());
        }
    }

    private static (byte[] Bytes, int Hash) WriteAsBinaryTuple(IReadOnlyCollection<object> arr, int timePrecision, int timestampPrecision)
    {
        using var builder = new BinaryTupleBuilder(arr.Count * 3, hashedColumnsPredicate: new TestIndexProvider(x => x % 3 == 2));

        foreach (var obj in arr)
        {
            builder.AppendObjectWithType(obj, timePrecision, timestampPrecision);
        }

        return (builder.Build().ToArray(), builder.Hash);
    }

    private static int WriteAsIgniteTuple(IReadOnlyCollection<object> arr, int timePrecision, int timestampPrecision)
    {
        var igniteTuple = new IgniteTuple();
        int i = 0;

        foreach (var obj in arr)
        {
            igniteTuple["c-" + i++] = obj;
        }

        var builder = new BinaryTupleBuilder(arr.Count, hashedColumnsPredicate: new TestIndexProvider(_ => true));

        try
        {
            var schema = GetSchema(arr, timePrecision, timestampPrecision);
            var noValueSet = new byte[arr.Count].AsSpan();

            TupleSerializerHandler.Instance.Write(ref builder, igniteTuple, schema, arr.Count, noValueSet);
            return builder.Hash;
        }
        finally
        {
            builder.Dispose();
        }
    }

    private static int WriteAsPoco(IReadOnlyCollection<object> arr, int timePrecision, int timestampPrecision)
    {
        // TODO
        return 0;
    }

    private static Schema GetSchema(IReadOnlyCollection<object> arr, int timePrecision, int timestampPrecision)
    {
        var columns = arr.Select((obj, ci) => GetColumn(obj, ci, timePrecision, timestampPrecision)).ToArray();

        return new Schema(Version: 0, arr.Count, columns);
    }

    private static Column GetColumn(object value, int schemaIndex, int timePrecision, int timestampPrecision)
    {
        var colType = value switch
        {
            sbyte => ClientDataType.Int8,
            short => ClientDataType.Int16,
            int => ClientDataType.Int32,
            long => ClientDataType.Int64,
            float => ClientDataType.Float,
            double => ClientDataType.Double,
            decimal => ClientDataType.Decimal,
            Guid => ClientDataType.Uuid,
            byte[] => ClientDataType.Bytes,
            string => ClientDataType.String,
            BigInteger => ClientDataType.Number,
            BitArray => ClientDataType.BitMask,
            LocalTime => ClientDataType.Time,
            LocalDate => ClientDataType.Date,
            LocalDateTime => ClientDataType.DateTime,
            Instant => ClientDataType.Timestamp,
            _ => throw new Exception("Unknown type: " + value.GetType())
        };

        var precision = colType switch
        {
            ClientDataType.Time => timePrecision,
            ClientDataType.DateTime => timePrecision,
            ClientDataType.Timestamp => timestampPrecision,
            _ => 0
        };

        // TODO: Which decimal scale do we need?
        var scale = value is decimal d ? BitConverter.GetBytes(decimal.GetBits(d)[3])[2] : 0;

        return new Column("c-" + schemaIndex, colType, false, true, true, schemaIndex, Scale: scale, precision);
    }

    private async Task AssertClientAndServerHashesAreEqual(int timePrecision = 9, int timestampPrecision = 6, params object[] keys)
    {
        // TODO: Test POCO and IgniteTuple serialization here as well.
        var (bytes, clientHash) = WriteAsBinaryTuple(keys, timePrecision, timestampPrecision);
        var clientHash2 = WriteAsIgniteTuple(keys, timePrecision, timestampPrecision);
        var clientHash3 = WriteAsPoco(keys, timePrecision, timestampPrecision);

        var serverHash = await GetServerHash(bytes, keys.Length, timePrecision, timestampPrecision);

        var msg = $"Time precision: {timePrecision}, timestamp precision: {timestampPrecision}, keys: {string.Join(", ", keys)}";

        Assert.AreEqual(serverHash, clientHash, $"Server hash mismatch. {msg}");
        Assert.AreEqual(clientHash, clientHash2, $"IgniteTuple hash mismatch. {msg}");
    }

    private async Task<int> GetServerHash(byte[] bytes, int count, int timePrecision, int timestampPrecision)
    {
        var nodes = await Client.GetClusterNodesAsync();

        return await Client.Compute.ExecuteAsync<int>(nodes, ColocationHashJob, count, bytes, timePrecision, timestampPrecision);
    }

    private record TestIndexProvider(Func<int, bool> Delegate) : IHashedColumnIndexProvider
    {
        public bool IsHashedColumnIndex(int index) => Delegate(index);
    }
}
