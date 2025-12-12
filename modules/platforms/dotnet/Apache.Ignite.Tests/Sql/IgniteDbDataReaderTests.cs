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

namespace Apache.Ignite.Tests.Sql;

using System;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using Common.Table;
using Ignite.Sql;
using Internal.Proto.BinaryTuple;
using Internal.Sql;
using NodaTime;
using NUnit.Framework;

/// <summary>
/// Tests for <see cref="ISql.ExecuteReaderAsync"/>.
/// </summary>
public class IgniteDbDataReaderTests : IgniteTestsBase
{
    private const int ColumnCount = 17;

    private const string AllColumnsQuery = "select \"KEY\", \"STR\", \"INT8\", \"INT16\", \"INT32\", \"INT64\", \"FLOAT\", " +
                                           "\"DOUBLE\", \"DATE\", \"TIME\", \"DATETIME\", \"TIMESTAMP\", \"BLOB\", \"DECIMAL\", \"UUID\", " +
                                           "\"BOOLEAN\", NULL from TBL_ALL_COLUMNS_SQL ORDER BY KEY";

    private static readonly LocalDate LocalDate = new(2023, 01, 18);
    private static readonly LocalTime LocalTime = new(09, 28);
    private static readonly LocalDateTime LocalDateTime = new(2023, 01, 18, 09, 29);
    private static readonly Instant Instant = Instant.FromUnixTimeSeconds(123);
    private static readonly byte[] Bytes = { 1, 2 };
    private static readonly Guid Guid = Guid.NewGuid();

    [OneTimeSetUp]
    public async Task InsertTestData()
    {
        await Client.Sql.ExecuteAsync(null, "delete from TBL_ALL_COLUMNS_SQL");

        var pocoAllColumns1 = new PocoAllColumnsSqlNullable(
            Key: 1,
            Str: "v-1",
            Int8: 2,
            Int16: 3,
            Int32: 4,
            Int64: 5,
            Float: 6.5F,
            Double: 7.5D,
            Date: LocalDate,
            Time: LocalTime,
            DateTime: LocalDateTime,
            Timestamp: Instant,
            Blob: Bytes,
            Decimal: 8.7M,
            Uuid: Guid,
            Boolean: true);

        var pocoAllColumns2 = new PocoAllColumnsSqlNullable(
            Key: 2,
            Str: "v-2",
            Int8: sbyte.MinValue,
            Int16: short.MinValue,
            Int32: int.MinValue,
            Int64: long.MinValue,
            Float: float.MinValue,
            Double: double.MinValue,
            Blob: new[] { BinaryTupleCommon.VarlenEmptyByte });

        var pocoAllColumns3 = new PocoAllColumnsSqlNullable(Key: 3);

        await PocoAllColumnsSqlNullableView.UpsertAllAsync(null, new[] { pocoAllColumns1, pocoAllColumns2, pocoAllColumns3 });

        for (int i = 3; i < 10; i++)
        {
            await PocoAllColumnsSqlNullableView.UpsertAsync(null, new(i));
        }
    }

    [Test]
    [SuppressMessage("ReSharper", "ReturnValueOfPureMethodIsNotUsed", Justification = "Reviewed.")]
    public async Task TestBasicUsage()
    {
        await using IgniteDbDataReader reader = await Client.Sql.ExecuteReaderAsync(
            null,
            "select KEY, INT8 from TBL_ALL_COLUMNS_SQL ORDER BY KEY");

        Assert.AreEqual(2, reader.FieldCount);
        Assert.IsTrue(reader.HasRows);
        Assert.AreEqual(0, reader.Depth);
        Assert.AreEqual(-1, reader.RecordsAffected);

        Assert.AreEqual("KEY", reader.Metadata.Columns[0].Name);
        Assert.AreEqual("INT8", reader.Metadata.Columns[1].Name);

        StringAssert.StartsWith(
            "IgniteDbDataReader { FieldCount = 2, RecordsAffected = -1, HasRows = True, IsClosed = False, " +
            "Metadata = ResultSetMetadata { Columns = [",
            reader.ToString());

        Assert.Throws<InvalidOperationException>(() => reader.GetByte(1));

        await reader.ReadAsync();
        Assert.AreEqual(2, reader.GetByte(1));
    }

    [Test]
    public async Task TestAllColumnTypes()
    {
        await using var reader = await ExecuteReader();

        Assert.AreEqual(ColumnCount, reader.FieldCount);

        Assert.AreEqual(1, reader.GetInt64("KEY"));
        Assert.AreEqual("v-1", reader.GetString("STR"));
        Assert.AreEqual(2, reader.GetByte("INT8"));
        Assert.AreEqual(3, reader.GetInt16("INT16"));
        Assert.AreEqual(4, reader.GetInt32("INT32"));
        Assert.AreEqual(5, reader.GetInt64("INT64"));
        Assert.AreEqual(6.5f, reader.GetFloat("FLOAT"));
        Assert.AreEqual(7.5d, reader.GetDouble("DOUBLE"));
        Assert.AreEqual(new DateTime(2023, 01, 18), reader.GetDateTime("DATE"));
        Assert.AreEqual(LocalTime, reader.GetFieldValue<LocalTime>("TIME"));
        Assert.AreEqual(new DateTime(2023, 01, 18, 09, 29, 0), reader.GetDateTime("DATETIME"));
        Assert.AreEqual(Instant.ToDateTimeUtc(), reader.GetDateTime("TIMESTAMP"));
        Assert.AreEqual(8.7m, reader.GetDecimal("DECIMAL"));
        Assert.AreEqual(new BigDecimal( 8.7m), reader.GetValue("DECIMAL"));
        Assert.AreEqual(2, reader.GetBytes("BLOB", 0, null!, 0, 0));
        Assert.AreEqual(Guid, reader.GetGuid("UUID"));
        Assert.IsTrue(reader.GetBoolean("BOOLEAN"));
        Assert.IsTrue(reader.IsDBNull("NULL"));
    }

    [Test]
    public async Task TestAllColumnTypesGetFieldValue()
    {
        await using var reader = await ExecuteReader();

        Assert.AreEqual(1, reader.GetFieldValue<long>("KEY"));
        Assert.AreEqual(1, reader.GetFieldValue<int>("KEY"));
        Assert.AreEqual(1, reader.GetFieldValue<byte>("KEY"));
        Assert.AreEqual(1, reader.GetFieldValue<short>("KEY"));

        Assert.AreEqual("v-1", reader.GetFieldValue<string>("STR"));

        Assert.AreEqual(2, reader.GetFieldValue<byte>("INT8"));
        Assert.AreEqual(2, reader.GetFieldValue<short>("INT8"));
        Assert.AreEqual(2, reader.GetFieldValue<int>("INT8"));
        Assert.AreEqual(2, reader.GetFieldValue<long>("INT8"));

        Assert.AreEqual(3, reader.GetFieldValue<short>("INT16"));
        Assert.AreEqual(3, reader.GetFieldValue<byte>("INT16"));
        Assert.AreEqual(3, reader.GetFieldValue<int>("INT16"));
        Assert.AreEqual(3, reader.GetFieldValue<long>("INT16"));

        Assert.AreEqual(4, reader.GetFieldValue<int>("INT32"));
        Assert.AreEqual(4, reader.GetFieldValue<byte>("INT32"));
        Assert.AreEqual(4, reader.GetFieldValue<short>("INT32"));
        Assert.AreEqual(4, reader.GetFieldValue<long>("INT32"));

        Assert.AreEqual(5, reader.GetFieldValue<long>("INT64"));
        Assert.AreEqual(5, reader.GetFieldValue<byte>("INT64"));
        Assert.AreEqual(5, reader.GetFieldValue<short>("INT64"));
        Assert.AreEqual(5, reader.GetFieldValue<int>("INT64"));

        Assert.AreEqual(6.5f, reader.GetFieldValue<float>("FLOAT"));
        Assert.AreEqual(6.5f, reader.GetFieldValue<double>("FLOAT"));

        Assert.AreEqual(7.5d, reader.GetFieldValue<double>("DOUBLE"));
        Assert.AreEqual(7.5d, reader.GetFieldValue<float>("DOUBLE"));

        Assert.AreEqual(LocalDate, reader.GetFieldValue<LocalDate>("DATE"));
        Assert.AreEqual(LocalDate.ToDateTimeUnspecified(), reader.GetFieldValue<DateTime>("DATE"));

        Assert.AreEqual(LocalTime, reader.GetFieldValue<LocalTime>("TIME"));
        Assert.AreEqual(LocalDateTime, reader.GetFieldValue<LocalDateTime>("DATETIME"));
        Assert.AreEqual(LocalDateTime.ToDateTimeUnspecified(), reader.GetFieldValue<DateTime>("DATETIME"));

        Assert.AreEqual(Instant, reader.GetFieldValue<Instant>("TIMESTAMP"));
        Assert.AreEqual(8.7m, reader.GetFieldValue<decimal>("DECIMAL"));
        Assert.AreEqual(Bytes, reader.GetFieldValue<byte[]>("BLOB"));

        Assert.IsNull(reader.GetFieldValue<object>("NULL"));

        Assert.Throws<InvalidCastException>(() => reader.GetFieldValue<Array>("TIME"));
    }

    [Test]
    public async Task TestAllColumnTypesGetFieldValueAsync()
    {
        await using var reader = await ExecuteReader();

        Assert.AreEqual(1, await reader.GetFieldValueAsync<long>("KEY"));
        Assert.AreEqual(1, await reader.GetFieldValueAsync<int>("KEY"));
        Assert.AreEqual(1, await reader.GetFieldValueAsync<byte>("KEY"));
        Assert.AreEqual(1, await reader.GetFieldValueAsync<short>("KEY"));

        Assert.AreEqual("v-1", await reader.GetFieldValueAsync<string>("STR"));

        Assert.AreEqual(2, await reader.GetFieldValueAsync<byte>("INT8"));
        Assert.AreEqual(2, await reader.GetFieldValueAsync<short>("INT8"));
        Assert.AreEqual(2, await reader.GetFieldValueAsync<int>("INT8"));
        Assert.AreEqual(2, await reader.GetFieldValueAsync<long>("INT8"));

        Assert.AreEqual(3, await reader.GetFieldValueAsync<short>("INT16"));
        Assert.AreEqual(3, await reader.GetFieldValueAsync<byte>("INT16"));
        Assert.AreEqual(3, await reader.GetFieldValueAsync<int>("INT16"));
        Assert.AreEqual(3, await reader.GetFieldValueAsync<long>("INT16"));

        Assert.AreEqual(4, await reader.GetFieldValueAsync<int>("INT32"));
        Assert.AreEqual(4, await reader.GetFieldValueAsync<byte>("INT32"));
        Assert.AreEqual(4, await reader.GetFieldValueAsync<short>("INT32"));
        Assert.AreEqual(4, await reader.GetFieldValueAsync<long>("INT32"));

        Assert.AreEqual(5, await reader.GetFieldValueAsync<long>("INT64"));
        Assert.AreEqual(5, await reader.GetFieldValueAsync<byte>("INT64"));
        Assert.AreEqual(5, await reader.GetFieldValueAsync<short>("INT64"));
        Assert.AreEqual(5, await reader.GetFieldValueAsync<int>("INT64"));

        Assert.AreEqual(6.5f, await reader.GetFieldValueAsync<float>("FLOAT"));
        Assert.AreEqual(6.5f, await reader.GetFieldValueAsync<double>("FLOAT"));

        Assert.AreEqual(7.5d, await reader.GetFieldValueAsync<double>("DOUBLE"));
        Assert.AreEqual(7.5d, await reader.GetFieldValueAsync<float>("DOUBLE"));

        Assert.AreEqual(LocalDate, await reader.GetFieldValueAsync<LocalDate>("DATE"));
        Assert.AreEqual(LocalDate.ToDateTimeUnspecified(), await reader.GetFieldValueAsync<DateTime>("DATE"));

        Assert.AreEqual(LocalTime, await reader.GetFieldValueAsync<LocalTime>("TIME"));
        Assert.AreEqual(LocalDateTime, await reader.GetFieldValueAsync<LocalDateTime>("DATETIME"));
        Assert.AreEqual(LocalDateTime.ToDateTimeUnspecified(), await reader.GetFieldValueAsync<DateTime>("DATETIME"));

        Assert.AreEqual(Instant, await reader.GetFieldValueAsync<Instant>("TIMESTAMP"));
        Assert.AreEqual(8.7m, await reader.GetFieldValueAsync<decimal>("DECIMAL"));
        Assert.AreEqual(Bytes, await reader.GetFieldValueAsync<byte[]>("BLOB"));
    }

    [Test]
    public async Task TestAllColumnTypesAsCompatibleTypes()
    {
        await using var reader = await ExecuteReader();

        Assert.AreEqual(2, reader.GetByte("INT8"));
        Assert.AreEqual(2, reader.GetInt16("INT8"));
        Assert.AreEqual(2, reader.GetInt32("INT8"));
        Assert.AreEqual(2, reader.GetInt64("INT8"));

        Assert.AreEqual(3, reader.GetByte("INT16"));
        Assert.AreEqual(3, reader.GetInt16("INT16"));
        Assert.AreEqual(3, reader.GetInt32("INT16"));
        Assert.AreEqual(3, reader.GetInt64("INT16"));

        Assert.AreEqual(4, reader.GetByte("INT32"));
        Assert.AreEqual(4, reader.GetInt16("INT32"));
        Assert.AreEqual(4, reader.GetInt32("INT32"));
        Assert.AreEqual(4, reader.GetInt64("INT32"));

        Assert.AreEqual(5, reader.GetByte("INT64"));
        Assert.AreEqual(5, reader.GetInt16("INT64"));
        Assert.AreEqual(5, reader.GetInt32("INT64"));
        Assert.AreEqual(5, reader.GetInt64("INT64"));

        Assert.AreEqual(6.5f, reader.GetFloat("FLOAT"));
        Assert.AreEqual(6.5f, reader.GetDouble("FLOAT"));

        Assert.AreEqual(7.5d, reader.GetFloat("DOUBLE"));
        Assert.AreEqual(7.5d, reader.GetDouble("DOUBLE"));

        await reader.ReadAsync();

        Assert.AreEqual(unchecked((byte)sbyte.MinValue), reader.GetByte("INT8"));
        Assert.AreEqual(sbyte.MinValue, reader.GetInt16("INT8"));
        Assert.AreEqual(sbyte.MinValue, reader.GetInt32("INT8"));
        Assert.AreEqual(sbyte.MinValue, reader.GetInt64("INT8"));

        Assert.AreEqual(short.MinValue, reader.GetInt16("INT16"));
        Assert.AreEqual(short.MinValue, reader.GetInt32("INT16"));
        Assert.AreEqual(short.MinValue, reader.GetInt64("INT16"));

        Assert.AreEqual(int.MinValue, reader.GetInt32("INT32"));
        Assert.AreEqual(int.MinValue, reader.GetInt64("INT32"));

        Assert.AreEqual(long.MinValue, reader.GetInt64("INT64"));

        Assert.AreEqual(float.MinValue, reader.GetFloat("FLOAT"));
        Assert.AreEqual(float.MinValue, reader.GetDouble("FLOAT"));

        Assert.AreEqual(double.MinValue, reader.GetDouble("DOUBLE"));
    }

    [Test]
    [SuppressMessage("ReSharper", "AccessToDisposedClosure", Justification = "Reviewed.")]
    public async Task TestIntFloatColumnsValueOutOfRangeThrows()
    {
        await using IgniteDbDataReader reader = await Client.Sql.ExecuteReaderAsync(null, AllColumnsQuery);
        await reader.ReadAsync();
        await reader.ReadAsync();

        Test(() => reader.GetByte("INT16"), 1, 2);
        Test(() => reader.GetByte("INT32"), 1, 4);
        Test(() => reader.GetByte("INT64"), 1, 8);

        Test(() => reader.GetInt16("INT32"), 2, 4);
        Test(() => reader.GetInt16("INT64"), 2, 8);

        Test(() => reader.GetInt32("INT64"), 4, 8);

        Test(() => reader.GetFloat("DOUBLE"), 4, 8);

        static void Test(TestDelegate testDelegate, int expected, int actual)
        {
            var ex = Assert.Throws<InvalidOperationException>(testDelegate);
            StringAssert.StartsWith("Binary tuple element with index", ex!.Message);
            StringAssert.Contains($"has invalid length (expected {expected}, actual {actual}).", ex.Message);
        }
    }

    [Test]
    [SuppressMessage("ReSharper", "AccessToDisposedClosure", Justification = "Reviewed.")]
    public async Task TestAllColumnTypesAsIncompatibleTypeThrows()
    {
        await using var reader = await ExecuteReader();

        Test(() => reader.GetBoolean("STR"), "STR", ColumnType.String, typeof(bool), typeof(string));
        Test(() => reader.GetString("INT8"), "INT8", ColumnType.Int8, typeof(string), typeof(sbyte));
        Test(() => reader.GetGuid("INT16"), "INT16", ColumnType.Int16, typeof(Guid), typeof(short));
        Test(() => reader.GetDateTime("INT32"), "INT32", ColumnType.Int32, typeof(DateTime), typeof(int));
        Test(() => reader.GetFloat("INT64"), "INT64", ColumnType.Int64, typeof(float), typeof(long));
        Test(() => reader.GetDouble("INT64"), "INT64", ColumnType.Int64, typeof(double), typeof(long));
        Test(() => reader.GetString("INT64"), "INT64", ColumnType.Int64, typeof(string), typeof(long));
        Test(() => reader.GetByte("STR"), "STR", ColumnType.String, typeof(byte), typeof(string));
        Test(() => reader.GetBytes("STR", 0, null!, 0, 0), "STR", ColumnType.String, typeof(byte[]), typeof(string));
        Test(() => reader.GetDecimal("STR"), "STR", ColumnType.String, typeof(decimal), typeof(string));
        Test(() => reader.GetInt16("STR"), "STR", ColumnType.String, typeof(short), typeof(string));
        Test(() => reader.GetInt32("STR"), "STR", ColumnType.String, typeof(int), typeof(string));
        Test(() => reader.GetInt64("STR"), "STR", ColumnType.String, typeof(long), typeof(string));

        static void Test(TestDelegate testDelegate, string columnName, ColumnType columnType, Type expectedType, Type actualType)
        {
            var ex = Assert.Throws<InvalidCastException>(testDelegate);

            Assert.AreEqual(
                $"Column {columnName} of type {columnType.ToSqlTypeName()} ({actualType}) can not be cast to {expectedType}.",
                ex!.Message);
        }
    }

    [Test]
    [SuppressMessage("Performance", "CA1849:Call async methods when in an async method", Justification = "Testing sync method.")]
    public async Task TestMultiplePages([Values(true, false)] bool async)
    {
        var statement = new SqlStatement(AllColumnsQuery, pageSize: 2);
        await using var reader = await Client.Sql.ExecuteReaderAsync(null, statement);

        var count = 0;

        while (async ? await reader.ReadAsync() : reader.Read())
        {
            count++;

            Assert.AreEqual(count, reader.GetInt32(0));
        }

        Assert.IsFalse(reader.Read());
        Assert.IsFalse(await reader.ReadAsync());

        Assert.AreEqual(9, count);
    }

    [Test]
    public async Task TestGetColumnSchema([Values(true, false)] bool async)
    {
        await using var reader = await ExecuteReader();

        ReadOnlyCollection<DbColumn> schema = async ? await reader.GetColumnSchemaAsync() : reader.GetColumnSchema();

        Assert.AreEqual(ColumnCount, schema.Count);

        Assert.AreEqual("KEY", schema[0].ColumnName);
        Assert.AreEqual(0, schema[0].ColumnOrdinal);
        Assert.IsNull(schema[0].ColumnSize);
        Assert.AreEqual(typeof(long), schema[0].DataType);
        Assert.AreEqual("bigint", schema[0].DataTypeName);
        Assert.IsFalse(schema[0].AllowDBNull);
        Assert.AreEqual(19, schema[0].NumericPrecision);
        Assert.AreEqual(0, schema[0].NumericScale);
        Assert.IsNotNull((schema[0] as IgniteDbColumn)?.ColumnMetadata);

        Assert.AreEqual("STR", schema[1].ColumnName);
        Assert.AreEqual(1, schema[1].ColumnOrdinal);
        Assert.IsNull(schema[1].ColumnSize);
        Assert.AreEqual(typeof(string), schema[1].DataType);
        Assert.AreEqual("varchar", schema[1].DataTypeName);
        Assert.IsTrue(schema[1].AllowDBNull);
        Assert.AreEqual(1000, schema[1].NumericPrecision);
        Assert.IsNull(schema[1].NumericScale);
        Assert.IsNotNull((schema[1] as IgniteDbColumn)?.ColumnMetadata);
    }

    [Test]
    public async Task TestGetName()
    {
        // ReSharper disable once UseAwaitUsing (test sync variant)
        using var reader = await ExecuteReader();

        Assert.AreEqual("KEY", reader.GetName(0));
        Assert.AreEqual("STR", reader.GetName(1));
        Assert.AreEqual("DECIMAL", reader.GetName(13));
    }

    [Test]
    public async Task TestGetValue()
    {
        await using var reader = await ExecuteReader();

        Assert.AreEqual(1, reader.GetValue("KEY"));
        Assert.AreEqual("v-1", reader.GetValue("STR"));
        Assert.AreEqual(2, reader.GetValue("INT8"));
        Assert.AreEqual(3, reader.GetValue("INT16"));
        Assert.AreEqual(4, reader.GetValue("INT32"));
        Assert.AreEqual(5, reader.GetValue("INT64"));
        Assert.AreEqual(6.5f, reader.GetValue("FLOAT"));
        Assert.AreEqual(7.5d, reader.GetValue("DOUBLE"));
        Assert.AreEqual(LocalDate, reader.GetValue("DATE"));
        Assert.AreEqual(LocalTime, reader.GetValue("TIME"));
        Assert.AreEqual(LocalDateTime, reader.GetValue("DATETIME"));
        Assert.AreEqual(Instant, reader.GetValue("TIMESTAMP"));
        Assert.AreEqual(new BigDecimal(8.7m), reader.GetValue("DECIMAL"));
        Assert.AreEqual(Bytes, reader.GetValue("BLOB"));
        Assert.IsNull(reader.GetValue("NULL"));
    }

    [Test]
    public async Task TestGetValues()
    {
        await using var reader = await ExecuteReader();

        var values = new object[reader.FieldCount];
        var count = reader.GetValues(values);

        var expected = new object?[]
        {
            1, "v-1", 2, 3, 4, 5, 6.5f, 7.5d, LocalDate, LocalTime, LocalDateTime, Instant, Bytes, new BigDecimal(8.7m), Guid, true, null
        };

        CollectionAssert.AreEqual(expected, values);
        Assert.AreEqual(reader.FieldCount, count);
    }

    [Test]
    public async Task TestGetChar()
    {
        await using var reader = await ExecuteReader();

        Assert.Throws<NotSupportedException>(() => reader.GetChar("KEY"));
    }

    [Test]
    public async Task TestGetBytes()
    {
        await using var reader = await ExecuteReader();

        var bytesLen = reader.GetBytes(name: "BLOB", dataOffset: 0, buffer: null!, bufferOffset: 0, length: 0);
        var bytes = new byte[bytesLen];

        Assert.AreEqual(2, bytesLen);
        Assert.AreEqual(2, reader.GetBytes(name: "BLOB", dataOffset: 0L, buffer: bytes, bufferOffset: 0, length: (int)bytesLen));
        Assert.AreEqual(Bytes, bytes);

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            reader.GetBytes(ordinal: 0, dataOffset: -1, buffer: null, bufferOffset: 0, length: 0));

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            reader.GetBytes(ordinal: 0, dataOffset: 0, buffer: bytes, bufferOffset: 10, length: 0));

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            reader.GetBytes(ordinal: 0, dataOffset: 0, buffer: bytes, bufferOffset: 0, length: 10));
    }

    [Test]
    public async Task TestGetBytesWithVarlenEmptyByte()
    {
        await using var reader = await ExecuteReader();
        await reader.ReadAsync();

        var bytesLen = reader.GetBytes(name: "BLOB", dataOffset: 0, buffer: null!, bufferOffset: 0, length: 0);
        var bytes = new byte[bytesLen];
        reader.GetBytes(name: "BLOB", dataOffset: 0L, buffer: bytes, bufferOffset: 0, length: (int)bytesLen);

        Assert.AreEqual(new[] { BinaryTupleCommon.VarlenEmptyByte }, bytes);
    }

    [Test]
    public async Task TestGetChars()
    {
        await using var reader = await ExecuteReader();

        var len = reader.GetChars(name: "STR", dataOffset: 0, buffer: null!, bufferOffset: 0, length: 0);
        var chars = new char[len];
        var count = reader.GetChars(name: "STR", dataOffset: 1, buffer: chars, bufferOffset: 1, length: 2);

        Assert.AreEqual(3, len);
        Assert.AreEqual(2, count);
        Assert.AreEqual(new[] { (char)0, '-', '1' }, chars);

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            reader.GetChars(ordinal: 0, dataOffset: -1, buffer: null, bufferOffset: 0, length: 0));

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            reader.GetChars(ordinal: 0, dataOffset: 0, buffer: chars, bufferOffset: 10, length: 0));

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            reader.GetChars(ordinal: 0, dataOffset: 0, buffer: chars, bufferOffset: 0, length: 10));
    }

    [Test]
    public async Task TestIndexers()
    {
        await using var reader = await ExecuteReader();

        Assert.AreEqual(1, reader[0]);
        Assert.AreEqual(1, reader["KEY"]);

        Assert.AreEqual("v-1", reader[1]);
        Assert.AreEqual("v-1", reader["STR"]);

        Assert.Throws<ArgumentOutOfRangeException>(() => _ = reader[100]);
        Assert.Throws<InvalidOperationException>(() => _ = reader["ABC"]);
    }

    [Test]
    [SuppressMessage("Performance", "CA1849:Call async methods when in an async method", Justification = "Testing sync method.")]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload", Justification = "Testing sync method.")]
    public async Task TestClose([Values(true, false)] bool async)
    {
        await using var reader = await ExecuteReader();
        Assert.IsFalse(reader.IsClosed);

        if (async)
        {
            await reader.CloseAsync();
        }
        else
        {
            reader.Close();
        }

        Assert.IsTrue(reader.IsClosed);
    }

    [Test]
    public async Task TestReadAllRowsClosesReader()
    {
        await using var reader = await ExecuteReader();
        Assert.IsFalse(reader.IsClosed);

        while (await reader.ReadAsync())
        {
            // No-op.
        }

        Assert.IsTrue(reader.IsClosed);
    }

    [Test]
    public async Task TestIsDbNull()
    {
        await using var reader = await ExecuteReader();
        await reader.ReadAsync();
        await reader.ReadAsync();

        /* ReSharper disable MethodHasAsyncOverload */
        Assert.IsFalse(reader.IsDBNull("KEY"));
        Assert.IsTrue(reader.IsDBNull("BLOB"));
        /* ReSharper restore MethodHasAsyncOverload */
    }

    [Test]
    [SuppressMessage("Performance", "CA1849:Call async methods when in an async method", Justification = "Testing sync method.")]
    public async Task TestNextResult()
    {
        await using var reader = await ExecuteReader();

        Assert.IsFalse(reader.NextResult());
        Assert.IsFalse(await reader.NextResultAsync());
    }

    [Test]
    public async Task TestGetEnumerator()
    {
        await using var reader = await Client.Sql.ExecuteReaderAsync(null, AllColumnsQuery);

        foreach (DbDataRecord row in reader)
        {
            // DbDataRecord delegates to methods in DbDataReader, no need to test everything here.
            Assert.AreEqual(ColumnCount, row.FieldCount);
            Assert.AreEqual("KEY", row.GetName(0));
        }
    }

    [Test]
    public async Task TestGetFieldType()
    {
        await using var reader = await ExecuteReader();

        Assert.AreEqual(typeof(long), reader.GetFieldType(0));
        Assert.AreEqual(typeof(string), reader.GetFieldType(1));
        Assert.AreEqual(typeof(sbyte), reader.GetFieldType(2));
        Assert.AreEqual(typeof(short), reader.GetFieldType(3));
        Assert.AreEqual(typeof(int), reader.GetFieldType(4));
        Assert.AreEqual(typeof(long), reader.GetFieldType(5));
        Assert.AreEqual(typeof(float), reader.GetFieldType(6));
        Assert.AreEqual(typeof(double), reader.GetFieldType(7));
        Assert.AreEqual(typeof(LocalDate), reader.GetFieldType(8));
        Assert.AreEqual(typeof(LocalTime), reader.GetFieldType(9));
        Assert.AreEqual(typeof(LocalDateTime), reader.GetFieldType(10));
        Assert.AreEqual(typeof(Instant), reader.GetFieldType(11));
        Assert.AreEqual(typeof(byte[]), reader.GetFieldType(12));
        Assert.AreEqual(typeof(BigDecimal), reader.GetFieldType(13));
    }

    [Test]
    public async Task TestGetDataTypeName()
    {
        await using var reader = await ExecuteReader();

        Assert.AreEqual("bigint", reader.GetDataTypeName(0));
        Assert.AreEqual("varchar", reader.GetDataTypeName(1));
        Assert.AreEqual("tinyint", reader.GetDataTypeName(2));
        Assert.AreEqual("smallint", reader.GetDataTypeName(3));
        Assert.AreEqual("int", reader.GetDataTypeName(4));
        Assert.AreEqual("bigint", reader.GetDataTypeName(5));
        Assert.AreEqual("real", reader.GetDataTypeName(6));
        Assert.AreEqual("double", reader.GetDataTypeName(7));
        Assert.AreEqual("date", reader.GetDataTypeName(8));
        Assert.AreEqual("time", reader.GetDataTypeName(9));
        Assert.AreEqual("timestamp", reader.GetDataTypeName(10));
        Assert.AreEqual("timestamp_tz", reader.GetDataTypeName(11));
        Assert.AreEqual("varbinary", reader.GetDataTypeName(12));
        Assert.AreEqual("decimal", reader.GetDataTypeName(13));
    }

    [Test]
    public async Task TestDataTableLoad()
    {
        await using var reader = await Client.Sql.ExecuteReaderAsync(null, AllColumnsQuery);

        // This calls GetSchemaTable underneath.
        var dt = new DataTable();
        dt.Load(reader);

        Assert.AreEqual(ColumnCount, dt.Columns.Count);
        Assert.AreEqual(9, dt.Rows.Count);
    }

    [Test]
    public async Task TestDataTableLoadEmptyResultSet()
    {
        await using var reader = await Client.Sql.ExecuteReaderAsync(null, "SELECT * FROM TBL_ALL_COLUMNS_SQL WHERE KEY > 100");

        var dt = new DataTable();
        dt.Load(reader);

        Assert.AreEqual(19, dt.Columns.Count);
        Assert.AreEqual(0, dt.Rows.Count);
    }

    [Test]
    public void TestExecuteReaderThrowsOnDmlQuery()
    {
        var ex = Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await Client.Sql.ExecuteReaderAsync(null, "UPDATE TBL_ALL_COLUMNS_SQL SET STR='s' WHERE KEY > 100"));

        Assert.AreEqual("ExecuteReaderAsync does not support queries without row set (DDL, DML).", ex!.Message);
    }

    [Test]
    public async Task TestEmptyResultSet()
    {
        await using var reader = await Client.Sql.ExecuteReaderAsync(null, "SELECT * FROM TBL_ALL_COLUMNS_SQL WHERE KEY > 100");
        bool readRes = await reader.ReadAsync();

        Assert.IsFalse(readRes);
        Assert.AreEqual(19, reader.FieldCount);
        Assert.IsFalse(reader.HasRows);
    }

    private async Task<IgniteDbDataReader> ExecuteReader()
    {
        var reader = await Client.Sql.ExecuteReaderAsync(null, AllColumnsQuery);
        await reader.ReadAsync();

        return reader;
    }
}
