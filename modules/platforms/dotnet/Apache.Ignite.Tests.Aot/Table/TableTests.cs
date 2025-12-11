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

namespace Apache.Ignite.Tests.Aot.Table;

using Common.Table;
using Ignite.Table;
using JetBrains.Annotations;
using NodaTime;
using static Common.Table.TestTables;

public class TableTests(IIgniteClient client)
{
    [UsedImplicitly]
    public async Task TestAllColumns()
    {
        var table = await client.Tables.GetTableAsync(TableAllColumnsName);
        var tupleView = table!.RecordBinaryView;

        var keyTuple = new IgniteTuple { ["Key"] = 123L };
        var dt = LocalDateTime.FromDateTime(DateTime.UtcNow);
        var tuple = new IgniteTuple
        {
            ["Key"] = 123L,
            ["Str"] = "str",
            ["Int8"] = (sbyte)8,
            ["Int16"] = (short)16,
            ["Int32"] = 32,
            ["Int64"] = 64L,
            ["Float"] = 32.32f,
            ["Double"] = 64.64,
            ["Uuid"] = Guid.NewGuid(),
            ["Date"] = dt.Date,
            ["Time"] = dt.TimeOfDay,
            ["DateTime"] = dt,
            ["Timestamp"] = Instant.FromDateTimeUtc(DateTime.UtcNow),
            ["Blob"] = new byte[] { 1, 2, 3 },
            ["Decimal"] = new BigDecimal(123.456m),
            ["Boolean"] = true
        };

        await tupleView.UpsertAsync(null, tuple);

        var res = (await tupleView.GetAsync(null, keyTuple)).Value;

        Assert.AreEqual(tuple["Blob"], res["Blob"]);
        Assert.AreEqual(tuple["Date"], res["Date"]);
        Assert.AreEqual(tuple["Decimal"], res["Decimal"]);
        Assert.AreEqual(tuple["Double"], res["Double"]);
        Assert.AreEqual(tuple["Float"], res["Float"]);
        Assert.AreEqual(tuple["Int8"], res["Int8"]);
        Assert.AreEqual(tuple["Int16"], res["Int16"]);
        Assert.AreEqual(tuple["Int32"], res["Int32"]);
        Assert.AreEqual(tuple["Int64"], res["Int64"]);
        Assert.AreEqual(tuple["Str"], res["Str"]);
        Assert.AreEqual(tuple["Uuid"], res["Uuid"]);
        Assert.AreEqual(tuple["Timestamp"], res["Timestamp"]);
        Assert.AreEqual(tuple["Time"], res["Time"]);
        Assert.AreEqual(tuple["DateTime"], res["DateTime"]);
        Assert.AreEqual(tuple["Boolean"], res["Boolean"]);
    }

    [UsedImplicitly]
    public async Task TestAllColumnsPoco()
    {
        var table = await client.Tables.GetTableAsync(TableAllColumnsNotNullName);
        var pocoView = table!.GetRecordView(new PocoAllColumnsMapper());

        var poco = new PocoAllColumns(
            Key: 123,
            Str: "str",
            Int8: 8,
            Int16: 16,
            Int32: 32,
            Int64: 64,
            Float: 32.32f,
            Double: 64.64,
            Uuid: Guid.NewGuid(),
            Decimal: 123.456m);

        await pocoView.UpsertAsync(null, poco);

        var res = (await pocoView.GetAsync(null, poco)).Value;

        Assert.AreEqual(poco.Decimal, res.Decimal);
        Assert.AreEqual(poco.Double, res.Double);
        Assert.AreEqual(poco.Float, res.Float);
        Assert.AreEqual(poco.Int8, res.Int8);
        Assert.AreEqual(poco.Int16, res.Int16);
        Assert.AreEqual(poco.Int32, res.Int32);
        Assert.AreEqual(poco.Int64, res.Int64);
        Assert.AreEqual(poco.Str, res.Str);
        Assert.AreEqual(poco.Uuid, res.Uuid);
    }

    [UsedImplicitly]
    public async Task TestDataStreamer()
    {
        var table = await client.Tables.GetTableAsync(TableAllColumnsSqlName);
        var view = table!.GetRecordView(new PocoAllColumnsSqlMapper());

        // Delete any existing data first
        await client.Sql.ExecuteAsync(null, $"DELETE FROM {TableAllColumnsSqlName} WHERE KEY >= 9000 AND KEY < 9010");

        // Create test data
        var pocos = Enumerable.Range(0, 10).Select(i => new PocoAllColumnsSql(
            Key: 9000 + i,
            Str: $"streamer-test-{i}",
            Int8: (sbyte)(10 + i),
            Int16: (short)(100 + i),
            Int32: 1000 + i,
            Int64: 10000 + i,
            Float: 1.1f * i,
            Double: 2.2 * i,
            Date: new LocalDate(2025, 12, 11),
            Time: new LocalTime(12, 30, 45),
            DateTime: new LocalDateTime(2025, 12, 11, 12, 30, 45),
            Timestamp: Instant.FromUtc(2025, 12, 11, 12, 30, 45),
            Blob: [(byte)i, (byte)(i + 1), (byte)(i + 2)],
            Decimal: 100m + i,
            Uuid: Guid.NewGuid(),
            Boolean: i % 2 == 0)).ToList();

        // Stream data
        var options = DataStreamerOptions.Default with { PageSize = 3 };
        await view.StreamDataAsync(GetData(), options);

        // Verify data was inserted
        await using var rs = await client.Sql.ExecuteAsync(
            transaction: null, $"SELECT * FROM {TableAllColumnsSqlName} WHERE KEY >= 9000 AND KEY < 9010 ORDER BY KEY");

        int index = 0;
        await foreach (var row in rs)
        {
            var expected = pocos[index];
            Assert.AreEqual(expected.Key, row["KEY"]);
            Assert.AreEqual(expected.Str, row["STR"]);
            Assert.AreEqual(expected.Int8, row["INT8"]);
            Assert.AreEqual(expected.Int16, row["INT16"]);
            Assert.AreEqual(expected.Int32, row["INT32"]);
            Assert.AreEqual(expected.Int64, row["INT64"]);
            Assert.AreEqual(expected.Float, row["FLOAT"]);
            Assert.AreEqual(expected.Double, row["DOUBLE"]);
            Assert.AreEqual(expected.Date, row["DATE"]);
            Assert.AreEqual(expected.Time, row["TIME"]);
            Assert.AreEqual(expected.DateTime, row["DATETIME"]);
            Assert.AreEqual(expected.Timestamp, row["TIMESTAMP"]);
            Assert.AreEqual(expected.Blob, row["BLOB"]!);
            Assert.AreEqual(expected.Decimal, row["DECIMAL"]);
            Assert.AreEqual(expected.Uuid, row["UUID"]);
            Assert.AreEqual(expected.Boolean, row["BOOLEAN"]);
        }

        async IAsyncEnumerable<DataStreamerItem<PocoAllColumnsSql>> GetData()
        {
            await Task.Yield();

            foreach (var item in pocos)
            {
                yield return DataStreamerItem.Create(item);
            }
        }
    }
}
