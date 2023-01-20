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
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using Ignite.Sql;
using NodaTime;
using NUnit.Framework;

/// <summary>
/// Tests for SQL API: <see cref="ISql"/>.
/// </summary>
public partial class SqlTests
{
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
        Assert.AreEqual(new LocalTime(09, 28), reader.GetFieldValue<LocalTime>("TIME"));
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

        // TODO: All types
        Test(() => reader.GetBoolean("STR"), "STR", SqlColumnType.String, typeof(bool), typeof(string));
        Test(() => reader.GetString("INT8"), "INT8", SqlColumnType.Int8, typeof(string), typeof(sbyte));
        Test(() => reader.GetGuid("INT16"), "INT16", SqlColumnType.Int16, typeof(Guid), typeof(short));
        Test(() => reader.GetDateTime("INT32"), "INT32", SqlColumnType.Int32, typeof(DateTime), typeof(int));
        Test(() => reader.GetFloat("INT64"), "INT64", SqlColumnType.Int64, typeof(float), typeof(long));

        // Assert.AreEqual(2, reader.GetString("INT8"));
        // Assert.AreEqual(3, reader.GetGuid("INT16"));
        // Assert.AreEqual(4, reader.GetGuid("INT32"));
        // Assert.AreEqual(5, reader.GetGuid("INT64"));
        // Assert.AreEqual(6.5f, reader.GetGuid("FLOAT"));
        // Assert.AreEqual(7.5d, reader.GetGuid("DOUBLE"));
        // Assert.AreEqual(new DateTime(2023, 01, 18), reader.GetGuid("DATE"));
        // Assert.AreEqual(new LocalTime(09, 28), reader.GetGuid("TIME"));
        // Assert.AreEqual(new DateTime(2023, 01, 18, 09, 29, 0), reader.GetGuid("DATETIME"));
        // Assert.AreEqual(Instant.FromUnixTimeSeconds(123).ToDateTimeUtc(), reader.GetGuid("TIMESTAMP"));
        // Assert.AreEqual(8.7m, reader.GetGuid("DECIMAL"));
        // reader.GetFloat("BLOB", 0, null!, 0, 0);
        static void Test(TestDelegate testDelegate, string columnName, SqlColumnType columnType, Type expectedType, Type actualType)
        {
            var ex = Assert.Throws<InvalidCastException>(testDelegate);
            Assert.AreEqual($"Column {columnName} of type {columnType} ({actualType}) can not be cast to {expectedType}.", ex!.Message);
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
    public async Task TestIgniteDbDataReaderGetColumnSchema()
    {
        await Task.Yield();
        Assert.Fail("TODO");
    }
}
