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
using System.Linq;
using System.Threading.Tasks;
using NodaTime;
using NUnit.Framework;
using Table;

/// <summary>
/// Tests result set object mapping with <see cref="ISql.ExecuteAsync{T}"/>.
/// </summary>
public class SqlResultSetObjectMappingTests : IgniteTestsBase
{
    private const int Count = 5;

    [OneTimeSetUp]
    public async Task InsertData()
    {
        await Client.Sql.ExecuteAsync(null, "delete from " + TableAllColumnsSqlName);

        for (int i = 0; i < Count; i++)
        {
            var poco = new PocoAllColumnsSqlNullable(
                i,
                "v-" + i,
                (sbyte)(i + 1),
                (short)(i + 2),
                i + 3,
                i + 4,
                i + 5.5f,
                i + 6.5,
                new LocalDate(2022, 12, i + 1),
                new LocalTime(11, 38, i + 1),
                new LocalDateTime(2022, 12, 19, 11, i + 1),
                Instant.FromUnixTimeSeconds(i + 1),
                new byte[] { 1, 2 },
                i + 7.7m);

            await PocoAllColumnsSqlNullableView.UpsertAsync(null, poco);
        }

        await PocoAllColumnsSqlNullableView.UpsertAsync(null, new PocoAllColumnsSqlNullable(100));

        await PocoView.UpsertAsync(null, new Poco { Key = 1, Val = "v1" });
    }

    [Test]
    public async Task TestSelectOneColumnAsPrimitiveType()
    {
        var resultSet = await Client.Sql.ExecuteAsync<int>(null, "select INT32 from TBL_ALL_COLUMNS_SQL where INT32 is not null order by 1");
        var rows = await resultSet.ToListAsync();

        Assert.AreEqual(Count, rows.Count);
        Assert.AreEqual(3, rows.First());
        Assert.AreEqual(7, rows.Last());
    }

    [Test]
    public async Task TestSelectAllColumns()
    {
        var resultSet = await Client.Sql.ExecuteAsync<PocoAllColumnsSqlNullable>(null, "select * from TBL_ALL_COLUMNS_SQL order by 1");
        var rows = await resultSet.ToListAsync();

        Assert.AreEqual(Count + 1, rows.Count);

        Assert.AreEqual(1, rows[1].Key);
        Assert.AreEqual("v-1", rows[1].Str);
        Assert.AreEqual(2, rows[1].Int8);
        Assert.AreEqual(3, rows[1].Int16);
        Assert.AreEqual(4, rows[1].Int32);
        Assert.AreEqual(5, rows[1].Int64);
        Assert.AreEqual(6.5f, rows[1].Float);
        Assert.AreEqual(7.5d, rows[1].Double);
        Assert.AreEqual(new LocalDate(2022, 12, 2), rows[1].Date);
        Assert.AreEqual(new LocalTime(11, 38, 2), rows[1].Time);
        Assert.AreEqual(new LocalDateTime(2022, 12, 19, 11, 2), rows[1].DateTime);
        Assert.AreEqual(Instant.FromUnixTimeSeconds(2), rows[1].Timestamp);
        Assert.AreEqual(new byte[] { 1, 2 }, rows[1].Blob);
        Assert.AreEqual(8.7m, rows[1].Decimal);

        Assert.AreEqual(new PocoAllColumnsSqlNullable(100), rows[Count]);
    }

    [Test]
    public async Task TestSelectNullIntoNonNullablePrimitiveTypeThrows()
    {
        var resultSet = await Client.Sql.ExecuteAsync<int>(null, "select INT32 from TBL_ALL_COLUMNS_SQL");

        var ex = Assert.ThrowsAsync<InvalidOperationException>(async () => await resultSet.ToListAsync());

        Assert.AreEqual(
            "Can not read NULL from column 'INT32' of type 'System.Nullable`1[System.Int32]' into type 'System.Int32'.",
            ex!.Message);
    }

    [Test]
    public async Task TestSelectNullIntoNonNullablePrimitiveTypeFieldThrows()
    {
        var resultSet = await Client.Sql.ExecuteAsync<IntRec>(null, "select INT32 from TBL_ALL_COLUMNS_SQL");

        var ex = Assert.ThrowsAsync<InvalidOperationException>(async () => await resultSet.ToListAsync());

        Assert.AreEqual(
            "Can not read NULL from column 'INT32' of type 'System.Nullable`1[System.Int32]' into type 'System.Int32'.",
            ex!.Message);
    }

    [Test]
    public void TestSelectNullIntoIncompatiblePrimitiveTypeThrows()
    {
        var ex = Assert.ThrowsAsync<NotSupportedException>(async () =>
            await Client.Sql.ExecuteAsync<Guid>(null, "select INT32 from TBL_ALL_COLUMNS_SQL where INT32 is not null"));

        Assert.AreEqual("Conversion from System.Int32 to System.Guid is not supported (column 'INT32').", ex!.Message);
    }

    [Test]
    public void TestNoMatchingFieldThrows()
    {
        var ex = Assert.ThrowsAsync<IgniteClientException>(async () =>
            await Client.Sql.ExecuteAsync<EmptyRec>(null, "select INT32, STR from TBL_ALL_COLUMNS_SQL"));

        Assert.AreEqual($"Can't map '{typeof(EmptyRec)}' to columns 'Int32 INT32, String STR'. Matching fields not found.", ex!.Message);
    }

    [Test]
    public async Task TestCustomColumnNameMapping()
    {
        var resultSet = await Client.Sql.ExecuteAsync<PocoCustomNames>(null, "select * from TBL1 where key = 1");
        var row = await resultSet.SingleAsync();

        Assert.AreEqual(1, row.Id);
        Assert.AreEqual("v1", row.Name);
    }

    private record EmptyRec;

    private record IntRec(int Int32);
}
