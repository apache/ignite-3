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
using Ignite.Sql;
using Internal.Sql;
using NodaTime;
using NUnit.Framework;
using Table;

/// <summary>
/// Tests for SQL API: <see cref="ISql"/>.
/// </summary>
public partial class SqlTests
{
    private const string AllColumnsQuery = "select \"KEY\", \"STR\", \"INT8\", \"INT16\", \"INT32\", \"INT64\", \"FLOAT\", " +
                                           "\"DOUBLE\", \"DATE\", \"TIME\", \"DATETIME\", \"TIMESTAMP\", \"BLOB\", \"DECIMAL\" " +
                                           "from TBL_ALL_COLUMNS_SQL ORDER BY KEY";

    private static readonly LocalDate LocalDate = new(2023, 01, 18);

    private static readonly LocalTime LocalTime = new(09, 28);

    private static readonly LocalDateTime LocalDateTime = new(2023, 01, 18, 09, 29);

    [OneTimeSetUp]
    public async Task InsertTestData()
    {
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
            Timestamp: Instant.FromUnixTimeSeconds(123),
            Blob: new byte[] { 1, 2 },
            Decimal: 8.7M);

        var pocoAllColumns2 = new PocoAllColumnsSqlNullable(
            Key: 2,
            Str: "v-2",
            Int8: sbyte.MinValue,
            Int16: short.MinValue,
            Int32: int.MinValue,
            Int64: long.MinValue,
            Float: float.MinValue,
            Double: double.MinValue);

        await PocoAllColumnsSqlNullableView.UpsertAllAsync(null, new[] { pocoAllColumns1, pocoAllColumns2 });
    }

    [Test]
    [SuppressMessage("ReSharper", "ReturnValueOfPureMethodIsNotUsed", Justification = "Reviewed.")]
    public async Task TestIgniteDbDataReader()
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

        Assert.Throws<InvalidOperationException>(() => reader.GetByte(1));

        await reader.ReadAsync();
        Assert.AreEqual(2, reader.GetByte(1));
    }

    [Test]
    public async Task TestIgniteDbDataReaderAllColumnTypes()
    {
        await using IgniteDbDataReader reader = await Client.Sql.ExecuteReaderAsync(null, AllColumnsQuery);
        await reader.ReadAsync();

        Assert.AreEqual(14, reader.FieldCount);

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
        Assert.AreEqual(Instant.FromUnixTimeSeconds(123).ToDateTimeUtc(), reader.GetDateTime("TIMESTAMP"));
        Assert.AreEqual(8.7m, reader.GetDecimal("DECIMAL"));

        var bytesLen = reader.GetBytes("BLOB", 0, null!, 0, 0);
        var byteArr = new byte[bytesLen];

        Assert.AreEqual(2, bytesLen);
        Assert.AreEqual(2, reader.GetBytes("BLOB", 0L, byteArr, 0, (int)bytesLen));
        Assert.AreEqual(new byte[] { 1, 2 }, byteArr);
    }

    [Test]
    public async Task TestIgniteDbDataReaderAllColumnTypesGetFieldValue()
    {
        await using IgniteDbDataReader reader = await Client.Sql.ExecuteReaderAsync(null, AllColumnsQuery);
        await reader.ReadAsync();

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

        Assert.AreEqual(Instant.FromUnixTimeSeconds(123), reader.GetFieldValue<Instant>("TIMESTAMP"));
        Assert.AreEqual(8.7m, reader.GetFieldValue<decimal>("DECIMAL"));
        Assert.AreEqual(new byte[] { 1, 2 }, reader.GetFieldValue<byte[]>("BLOB"));
    }

    [Test]
    public async Task TestIgniteDbDataReaderAllColumnTypesAsCompatibleTypes()
    {
        await using IgniteDbDataReader reader = await Client.Sql.ExecuteReaderAsync(null, AllColumnsQuery);
        await reader.ReadAsync();

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
    public async Task TestIgniteDbDataReaderIntFloatColumnsValueOutOfRangeThrows()
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
    public async Task TestIgniteDbDataReaderAllColumnTypesAsIncompatibleTypeThrows()
    {
        await using IgniteDbDataReader reader = await Client.Sql.ExecuteReaderAsync(null, AllColumnsQuery);
        await reader.ReadAsync();

        Test(() => reader.GetBoolean("STR"), "STR", SqlColumnType.String, typeof(bool), typeof(string));
        Test(() => reader.GetString("INT8"), "INT8", SqlColumnType.Int8, typeof(string), typeof(sbyte));
        Test(() => reader.GetGuid("INT16"), "INT16", SqlColumnType.Int16, typeof(Guid), typeof(short));
        Test(() => reader.GetDateTime("INT32"), "INT32", SqlColumnType.Int32, typeof(DateTime), typeof(int));
        Test(() => reader.GetFloat("INT64"), "INT64", SqlColumnType.Int64, typeof(float), typeof(long));
        Test(() => reader.GetDouble("INT64"), "INT64", SqlColumnType.Int64, typeof(double), typeof(long));
        Test(() => reader.GetString("INT64"), "INT64", SqlColumnType.Int64, typeof(string), typeof(long));
        Test(() => reader.GetByte("STR"), "STR", SqlColumnType.String, typeof(byte), typeof(string));
        Test(() => reader.GetBytes("STR", 0, null!, 0, 0), "STR", SqlColumnType.String, typeof(byte[]), typeof(string));
        Test(() => reader.GetDecimal("STR"), "STR", SqlColumnType.String, typeof(decimal), typeof(string));
        Test(() => reader.GetInt16("STR"), "STR", SqlColumnType.String, typeof(short), typeof(string));
        Test(() => reader.GetInt32("STR"), "STR", SqlColumnType.String, typeof(int), typeof(string));
        Test(() => reader.GetInt64("STR"), "STR", SqlColumnType.String, typeof(long), typeof(string));

        static void Test(TestDelegate testDelegate, string columnName, SqlColumnType columnType, Type expectedType, Type actualType)
        {
            var ex = Assert.Throws<InvalidCastException>(testDelegate);

            Assert.AreEqual(
                $"Column {columnName} of type {columnType.ToSqlTypeName()} ({actualType}) can not be cast to {expectedType}.",
                ex!.Message);
        }
    }

    [Test]
    [SuppressMessage("Performance", "CA1849:Call async methods when in an async method", Justification = "Testing sync method.")]
    public async Task TestIgniteDbDataReaderMultiplePages([Values(true, false)] bool async)
    {
        var statement = new SqlStatement("select ID, VAL FROM TEST ORDER BY ID", pageSize: 2);
        await using IgniteDbDataReader reader = await Client.Sql.ExecuteReaderAsync(null, statement);

        var count = 0;

        while (async ? await reader.ReadAsync() : reader.Read())
        {
            var id = reader.GetInt32(0);
            var val = reader.GetString(1);

            Assert.AreEqual(count, id);
            Assert.AreEqual($"s-{count}", val);

            count++;
        }

        Assert.IsFalse(reader.Read());
        Assert.IsFalse(await reader.ReadAsync());

        Assert.AreEqual(10, count);
    }

    [Test]
    public async Task TestIgniteDbDataReaderGetColumnSchema([Values(true, false)] bool async)
    {
        var statement = new SqlStatement("select ID, VAL FROM TEST ORDER BY ID", pageSize: 2);
        await using IgniteDbDataReader reader = await Client.Sql.ExecuteReaderAsync(null, statement);

        ReadOnlyCollection<DbColumn> schema = async ? await reader.GetColumnSchemaAsync() : reader.GetColumnSchema();

        Assert.AreEqual(2, schema.Count);

        Assert.AreEqual("ID", schema[0].ColumnName);
        Assert.AreEqual(0, schema[0].ColumnOrdinal);
        Assert.IsNull(schema[0].ColumnSize);
        Assert.AreEqual(typeof(int), schema[0].DataType);
        Assert.AreEqual("int", schema[0].DataTypeName);
        Assert.IsFalse(schema[0].AllowDBNull);
        Assert.AreEqual(10, schema[0].NumericPrecision);
        Assert.AreEqual(0, schema[0].NumericScale);
        Assert.IsNotNull((schema[0] as IgniteDbColumn)?.ColumnMetadata);

        Assert.AreEqual("VAL", schema[1].ColumnName);
        Assert.AreEqual(1, schema[1].ColumnOrdinal);
        Assert.IsNull(schema[1].ColumnSize);
        Assert.AreEqual(typeof(string), schema[1].DataType);
        Assert.AreEqual("varchar", schema[1].DataTypeName);
        Assert.IsTrue(schema[1].AllowDBNull);
        Assert.AreEqual(65536, schema[1].NumericPrecision);
        Assert.IsNull(schema[1].NumericScale);
        Assert.IsNotNull((schema[1] as IgniteDbColumn)?.ColumnMetadata);
    }

    [Test]
    public void TestGetStream()
    {
        Assert.Fail("TODO");
    }

    [Test]
    public void TestGetTextReader()
    {
        Assert.Fail("TODO");
    }
}
