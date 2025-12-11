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
        var voew2 = table.GetRecordView<PocoAllColumns>();

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
}
