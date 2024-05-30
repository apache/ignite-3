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
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Numerics;
using System.Reflection;
using System.Threading.Tasks;
using Compute;
using Ignite.Compute;
using Ignite.Sql;
using Ignite.Table;
using Internal.Buffers;
using Internal.Common;
using Internal.Proto;
using Internal.Proto.BinaryTuple;
using Internal.Proto.MsgPack;
using Internal.Table;
using Internal.Table.Serialization;
using NodaTime;
using NUnit.Framework;

/// <summary>
/// Tests that colocation hash calculation is consistent with server logic.
/// </summary>
public class ColocationHashTests : IgniteTestsBase
{
    private const string PlatformTestNodeRunner = "org.apache.ignite.internal.runner.app.PlatformTestNodeRunner";

    private const string ColocationHashJob = PlatformTestNodeRunner + "$ColocationHashJob";

    private const string TableRowColocationHashJob = PlatformTestNodeRunner + "$TableRowColocationHashJob";

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
        ((char)BinaryTupleCommon.VarlenEmptyByte).ToString(),
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
        new LocalTime(9, 8, 7, 6),
        LocalTime.FromHourMinuteSecondNanosecond(hour: 1, minute: 2, second: 3, nanosecondWithinSecond: 456789),
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
    public async Task TestSingleKeyColocationHashIsSameOnServerAndClientCustomTimePrecision(
        [Values(0, 1, 3, 4, 5, 6, 7, 8, 9)] int timePrecision,
        [Values(0, 1, 3, 6)] int timestampPrecision)
    {
        foreach (var t in TestCases)
        {
            await AssertClientAndServerHashesAreEqual(timePrecision, timestampPrecision, t);
        }
    }

    [Test]
    public async Task TestLocalTimeColocationHashIsSameOnServerAndClient([Values(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)] int timePrecision) =>
        await AssertClientAndServerHashesAreEqual(timePrecision, keys: LocalTime.FromHourMinuteSecondNanosecond(11, 33, 44, 123_456));

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
        [Values(0, 1, 4, 5, 6, 7, 8, 9)] int timePrecision,
        [Values(0, 1, 3, 6)] int timestampPrecision)
    {
        for (var i = 0; i < TestCases.Length; i++)
        {
            await AssertClientAndServerHashesAreEqual(timePrecision, timestampPrecision, TestCases.Take(i + 1).ToArray());
            await AssertClientAndServerHashesAreEqual(timePrecision, timestampPrecision, TestCases.Skip(i).ToArray());
        }
    }

    [Test]
    public async Task TestCustomColocationColumnOrder([Values(true, false)] bool reverseColocationOrder)
    {
        var tableName = $"{nameof(TestCustomColocationColumnOrder)}_{reverseColocationOrder}";
        var sql = $"create table if not exists {tableName} " +
                  $"(id integer, id0 bigint, id1 varchar, v INTEGER, primary key(id, id0, id1)) " +
                  $"colocate by {(reverseColocationOrder ? "(id1, id0)" : "(id0, id1)")}";

        await Client.Sql.ExecuteAsync(null, sql);

        // Perform get to populate schema.
        var table = await Client.Tables.GetTableAsync(tableName);
        var view = table!.RecordBinaryView;
        await view.GetAsync(null, new IgniteTuple{["id"] = 1, ["id0"] = 2L, ["id1"] = "3"});

        var ser = view.GetFieldValue<RecordSerializer<IIgniteTuple>>("_ser");
        var schemas = table.GetFieldValue<IDictionary<int, Task<Schema>>>("_schemas");
        var schema = schemas[1].GetAwaiter().GetResult();
        var clusterNodes = await Client.GetClusterNodesAsync();

        for (int i = 0; i < 100; i++)
        {
            var key = new IgniteTuple { ["id"] = 1 + i, ["id0"] = 2L + i, ["id1"] = "3" + i };

            using var writer = ProtoCommon.GetMessageWriter();
            var (clientColocationHash, _) = ser.Write(writer, null, schema, key);

            var serverColocationHashExec = await Client.Compute.SubmitAsync<int>(
                clusterNodes,
                Array.Empty<DeploymentUnit>(),
                TableRowColocationHashJob,
                tableName,
                i);

            var serverColocationHash = await serverColocationHashExec.GetResultAsync();

            Assert.AreEqual(serverColocationHash, clientColocationHash, key.ToString());
        }
    }

    private static (byte[] Bytes, int Hash) WriteAsBinaryTuple(IReadOnlyCollection<object> arr, int timePrecision, int timestampPrecision)
    {
        using var builder = new BinaryTupleBuilder(
            numElements: arr.Count * 2,
            hashedColumnsPredicate: new TestIndexProvider(x => x % 2 == 1 ? x / 2 : -1, arr.Count));

        foreach (var obj in arr)
        {
            builder.AppendObjectWithType(obj, timePrecision, timestampPrecision);
        }

        return (builder.Build().ToArray(), Hash: builder.GetHash());
    }

    private static int WriteAsIgniteTuple(IReadOnlyCollection<object> arr, int timePrecision, int timestampPrecision)
    {
        var igniteTuple = new IgniteTuple();
        int i = 1;

        foreach (var obj in arr)
        {
            igniteTuple["m_Item" + i++] = obj;
        }

        var builder = new BinaryTupleBuilder(arr.Count, hashedColumnsPredicate: new TestIndexProvider(idx => idx, arr.Count));

        try
        {
            var schema = GetSchema(arr, timePrecision, timestampPrecision);
            var noValueSet = new byte[arr.Count].AsSpan();

            TupleSerializerHandler.Instance.Write(ref builder, igniteTuple, schema, keyOnly: false, noValueSet);
            return builder.GetHash();
        }
        finally
        {
            builder.Dispose();
        }
    }

    [SuppressMessage("ReSharper", "UnusedMember.Local", Justification = "Used by reflection.")]
    private static int WriteAsPoco<T>(T obj, int timePrecision, int timestampPrecision)
    {
        var poco = Tuple.Create(obj);
        IRecordSerializerHandler<Tuple<T>> handler = new ObjectSerializerHandler<Tuple<T>>();
        var schema = GetSchema(new object[] { obj! }, timePrecision, timestampPrecision);

        using var buf = new PooledArrayBuffer();
        var writer = new MsgPackWriter(buf);

        return handler.Write(ref writer, schema, poco, computeHash: true);
    }

    private static Schema GetSchema(IReadOnlyCollection<object> arr, int timePrecision, int timestampPrecision)
    {
        var columns = arr.Select((obj, ci) => GetColumn(obj, ci, timePrecision, timestampPrecision)).ToArray();

        return Schema.CreateInstance(version: 0, tableId: 0, columns);
    }

    private static Column GetColumn(object value, int schemaIndex, int timePrecision, int timestampPrecision)
    {
        var colType = value switch
        {
            sbyte => ColumnType.Int8,
            short => ColumnType.Int16,
            int => ColumnType.Int32,
            long => ColumnType.Int64,
            float => ColumnType.Float,
            double => ColumnType.Double,
            decimal => ColumnType.Decimal,
            Guid => ColumnType.Uuid,
            byte[] => ColumnType.ByteArray,
            string => ColumnType.String,
            BigInteger => ColumnType.Number,
            BitArray => ColumnType.Bitmask,
            LocalTime => ColumnType.Time,
            LocalDate => ColumnType.Date,
            LocalDateTime => ColumnType.Datetime,
            Instant => ColumnType.Timestamp,
            _ => throw new Exception("Unknown type: " + value.GetType())
        };

        var precision = colType switch
        {
            ColumnType.Time => timePrecision,
            ColumnType.Datetime => timePrecision,
            ColumnType.Timestamp => timestampPrecision,
            _ => 0
        };

        var scale = value is decimal d ? BitConverter.GetBytes(decimal.GetBits(d)[3])[2] : 0;

        return new Column(
            Name: "m_Item" + (schemaIndex + 1),
            Type: colType,
            IsNullable: false,
            KeyIndex: schemaIndex,
            ColocationIndex: schemaIndex,
            SchemaIndex: schemaIndex,
            Scale: scale,
            Precision: precision);
    }

    private async Task AssertClientAndServerHashesAreEqual(int timePrecision = 9, int timestampPrecision = 6, params object[] keys)
    {
        var (bytes, clientHash) = WriteAsBinaryTuple(keys, timePrecision, timestampPrecision);
        var clientHash2 = WriteAsIgniteTuple(keys, timePrecision, timestampPrecision);

        var serverHash = await GetServerHash(bytes, keys.Length, timePrecision, timestampPrecision);

        var msg = $"Time precision: {timePrecision}, timestamp precision: {timestampPrecision}, keys: {keys.StringJoin()}";

        Assert.AreEqual(serverHash, clientHash, $"Server hash mismatch. {msg}");
        Assert.AreEqual(clientHash, clientHash2, $"IgniteTuple hash mismatch. {msg}");

        if (keys.Length == 1)
        {
            var obj = keys[0];
            var method = GetType().GetMethod("WriteAsPoco", BindingFlags.Static | BindingFlags.NonPublic)!.MakeGenericMethod(obj.GetType());
            var clientHash3 = (int)method.Invoke(null, new[] { obj, timePrecision, timestampPrecision })!;

            Assert.AreEqual(clientHash, clientHash3, $"Poco hash mismatch. {msg}");
        }
    }

    private async Task<int> GetServerHash(byte[] bytes, int count, int timePrecision, int timestampPrecision)
    {
        var nodes = await Client.GetClusterNodesAsync();

        IJobExecution<int> jobExecution = await Client.Compute.SubmitAsync<int>(
            nodes,
            Array.Empty<DeploymentUnit>(),
            ColocationHashJob,
            count,
            bytes,
            timePrecision,
            timestampPrecision);

        return await jobExecution.GetResultAsync();
    }

    private record TestIndexProvider(Func<int, int> ColumnOrderDelegate, int HashedColumnCount) : IHashedColumnIndexProvider
    {
        public int HashedColumnOrder(int index) => ColumnOrderDelegate(index);
    }
}
