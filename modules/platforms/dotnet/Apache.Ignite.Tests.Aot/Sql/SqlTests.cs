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

namespace Apache.Ignite.Tests.Aot.Sql;

using Common.Table;
using Ignite.Sql;
using Ignite.Table;
using JetBrains.Annotations;
using NodaTime;
using Transactions;

public class SqlTests(IIgniteClient client)
{
    [UsedImplicitly]
    public async Task TestSimpleQuery()
    {
        await using IResultSet<IIgniteTuple> resultSet = await client.Sql.ExecuteAsync(null, "select 1 as num, 'hello' as str");
        var rows = await resultSet.ToListAsync();

        Assert.AreEqual(-1L, resultSet.AffectedRows);
        Assert.AreEqual(false, resultSet.WasApplied);
        Assert.AreEqual(true, resultSet.HasRowSet);

        Assert.AreEqual("NUM", resultSet.Metadata!.Columns[0].Name);
        Assert.AreEqual("STR", resultSet.Metadata!.Columns[1].Name);

        Assert.AreEqual(0, resultSet.Metadata!.IndexOf("NUM"));
        Assert.AreEqual(1, resultSet.Metadata!.IndexOf("STR"));

        Assert.AreEqual(
            "ResultSetMetadata { Columns = [ " +
            "ColumnMetadata { Name = NUM, Type = Int32, Precision = 10, Scale = 0, Nullable = False, Origin =  }, " +
            "ColumnMetadata { Name = STR, Type = String, Precision = 5, Scale = -2147483648, Nullable = False, Origin =  } ] }",
            resultSet.Metadata.ToString());

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("IgniteTuple { NUM = 1, STR = hello }", rows[0].ToString());
    }

    [UsedImplicitly]
    public async Task TestAllColumnTypes()
    {
        var table = await client.Tables.GetTableAsync(TestTables.TableAllColumnsSqlName);
        var view = table!.GetRecordView(new PocoAllColumnsSqlMapper());

        var poco = new PocoAllColumnsSql(
            Key: 1234,
            Str: "str!",
            Int8: 88,
            Int16: 166,
            Int32: 322,
            Int64: 644,
            Float: 32.32f,
            Double: 64.64,
            Date: new LocalDate(2025, 12, 11),
            Time: new LocalTime(10, 20, 30, 123),
            DateTime: new LocalDateTime(2025, 12, 11, 10, 20, 30, 123),
            Timestamp: Instant.FromUtc(2025, 12, 11, 10, 20, 30),
            Blob: [1, 2, 3, 4, 5],
            Decimal: 123.456m,
            Uuid: Guid.Parse("123e4567-e89b-12d3-a456-426614174000"),
            Boolean: true);

        await view.UpsertAsync(null, poco);

        await using IResultSet<IIgniteTuple> resultSet = await client.Sql.ExecuteAsync(
            transaction: null, $"select * from {table.Name} where KEY = ?", poco.Key);

        List<IIgniteTuple> rows = await resultSet.ToListAsync();

        Assert.AreEqual(1, rows.Count);
        var row = rows[0];

        Assert.AreEqual(poco.Key, row["KEY"]);
        Assert.AreEqual(poco.Str, row["STR"]);
        Assert.AreEqual(poco.Int8, row["INT8"]);
        Assert.AreEqual(poco.Int16, row["INT16"]);
        Assert.AreEqual(poco.Int32, row["INT32"]);
        Assert.AreEqual(poco.Int64, row["INT64"]);
        Assert.AreEqual(poco.Float, row["FLOAT"]);
        Assert.AreEqual(poco.Double, row["DOUBLE"]);
        Assert.AreEqual(poco.Uuid, row["UUID"]);
        Assert.AreEqual(new BigDecimal(poco.Decimal), row["DECIMAL"]);
    }
}
