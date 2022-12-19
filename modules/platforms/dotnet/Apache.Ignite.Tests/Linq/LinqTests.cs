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

namespace Apache.Ignite.Tests.Linq;

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading.Tasks;
using Ignite.Sql;
using Ignite.Table;
using NodaTime;
using NUnit.Framework;
using Table;

/// <summary>
/// Basic LINQ provider tests.
/// </summary>
[SuppressMessage("ReSharper", "PossibleLossOfFraction", Justification = "Tests")]
[SuppressMessage("ReSharper", "NotAccessedPositionalProperty.Local", Justification = "Tests")]
public partial class LinqTests : IgniteTestsBase
{
    private const int Count = 10;

    private IRecordView<PocoByte> PocoByteView { get; set; } = null!;

    private IRecordView<PocoShort> PocoShortView { get; set; } = null!;

    private IRecordView<PocoInt> PocoIntView { get; set; } = null!;

    private IRecordView<PocoLong> PocoLongView { get; set; } = null!;

    private IRecordView<PocoFloat> PocoFloatView { get; set; } = null!;

    private IRecordView<PocoDouble> PocoDoubleView { get; set; } = null!;

    private IRecordView<PocoDecimal> PocoDecimalView { get; set; } = null!;

    private IRecordView<PocoString> PocoStringView { get; set; } = null!;

    [OneTimeSetUp]
    public async Task InsertData()
    {
        PocoByteView = (await Client.Tables.GetTableAsync(TableInt8Name))!.GetRecordView<PocoByte>();
        PocoShortView = (await Client.Tables.GetTableAsync(TableInt16Name))!.GetRecordView<PocoShort>();
        PocoIntView = (await Client.Tables.GetTableAsync(TableInt32Name))!.GetRecordView<PocoInt>();
        PocoLongView = (await Client.Tables.GetTableAsync(TableInt64Name))!.GetRecordView<PocoLong>();
        PocoFloatView = (await Client.Tables.GetTableAsync(TableFloatName))!.GetRecordView<PocoFloat>();
        PocoDoubleView = (await Client.Tables.GetTableAsync(TableDoubleName))!.GetRecordView<PocoDouble>();
        PocoDecimalView = (await Client.Tables.GetTableAsync(TableDecimalName))!.GetRecordView<PocoDecimal>();
        PocoStringView = (await Client.Tables.GetTableAsync(TableStringName))!.GetRecordView<PocoString>();

        for (int i = 0; i < Count; i++)
        {
            await PocoView.UpsertAsync(null, new Poco { Key = i, Val = "v-" + i });

            await PocoByteView.UpsertAsync(null, new PocoByte((sbyte)i, (sbyte)(i / 3)));
            await PocoShortView.UpsertAsync(null, new PocoShort((short)(i * 2), (short)(i * 2)));
            await PocoIntView.UpsertAsync(null, new PocoInt(i, i * 100));
            await PocoLongView.UpsertAsync(null, new PocoLong(i, i * 2));

            await PocoFloatView.UpsertAsync(null, new(i, i));
            await PocoDoubleView.UpsertAsync(null, new(i, i));
            await PocoDecimalView.UpsertAsync(null, new(i, i));

            await PocoStringView.UpsertAsync(null, new("k-" + i, "v-" + i * 2));

            var pocoAllColumns = new PocoAllColumnsSqlNullable(
                i,
                "v -" + i,
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

            await PocoAllColumnsSqlNullableView.UpsertAsync(null, pocoAllColumns);
        }

        await PocoAllColumnsSqlNullableView.UpsertAsync(null, new PocoAllColumnsSqlNullable(100));
    }

    [OneTimeTearDown]
    public async Task CleanTables()
    {
        await TupleView.DeleteAllAsync(null, Enumerable.Range(0, Count).Select(x => GetTuple(x)));
        await PocoIntView.DeleteAllAsync(null, Enumerable.Range(0, Count).Select(x => new PocoInt(x, 0)));
    }

    [Test]
    public void TestSelectOneColumn()
    {
        var query = PocoView.AsQueryable()
            .Where(x => x.Key == 3)
            .Select(x => x.Val);

        string?[] res = query.ToArray();

        CollectionAssert.AreEqual(new[] { "v-3" }, res);
    }

    [Test]
    public void TestSelectOneColumnSingle()
    {
        var res = PocoView.AsQueryable()
            .Where(x => x.Key == 3)
            .Select(x => x.Val)
            .Single();

        Assert.AreEqual("v-3", res);
    }

    [Test]
    public void TestSelectOneColumnSingleWithMultipleRowsThrows()
    {
        // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
        var ex = Assert.Throws<InvalidOperationException>(
            () => PocoView.AsQueryable()
            .Select(x => x.Val)
            .Single());

        const string expected = "ResultSet is expected to have one row, but has more: " +
                                "select _T0.VAL from PUBLIC.TBL1 as _T0 limit 2";

        Assert.AreEqual(expected, ex!.Message);
    }

    [Test]
    public void TestSelectOneColumnFirst()
    {
        var res = PocoView.AsQueryable()
            .OrderBy(x => x.Key)
            .Select(x => x.Val)
            .First();

        Assert.AreEqual("v-0", res);
    }

    [Test]
    public async Task TestSelectOneColumnAsResultSet()
    {
        var query = PocoView.AsQueryable()
            .Where(x => x.Key == 3)
            .Select(x => x.Val);

        await using IResultSet<string?> resultSet = await query.ToResultSetAsync();
        List<string?> rows = await resultSet.ToListAsync();

        CollectionAssert.AreEqual(new[] { "v-3" }, rows);
        Assert.IsTrue(resultSet.HasRowSet);
        Assert.IsNotNull(resultSet.Metadata);
        Assert.AreEqual("VAL", resultSet.Metadata!.Columns.Single().Name);
    }

    [Test]
    public void TestSelectEntireObject()
    {
        Poco[] res = PocoView.AsQueryable()
            .Where(x => x.Key == 3)
            .ToArray();

        Assert.AreEqual(1, res.Length);
        Assert.AreEqual(3, res[0].Key);
        Assert.AreEqual("v-3", res[0].Val);
    }

    [Test]
    public void TestSelectEntireRecordObject()
    {
        PocoInt res = PocoIntView.AsQueryable().Single(x => x.Key == 3);

        Assert.AreEqual(3, res.Key);
        Assert.AreEqual(300, res.Val);
    }

    [Test]
    public void TestSelectTwoColumns()
    {
        var res = PocoView.AsQueryable()
            .Where(x => x.Key == 2)
            .Select(x => new { x.Key, x.Val })
            .ToArray();

        Assert.AreEqual(1, res.Length);
        Assert.AreEqual(2, res[0].Key);
        Assert.AreEqual("v-2", res[0].Val);
    }

    [Test]
    public void TestSelectComputedColumnIntoAnonymousType()
    {
        var res = PocoView.AsQueryable()
            .Where(x => x.Key == 7)
            .Select(x => new { x.Key, x.Val, Key2 = x.Key + 1 })
            .ToArray();

        Assert.AreEqual(1, res.Length);
        Assert.AreEqual(7, res[0].Key);
        Assert.AreEqual(8, res[0].Key2);
        Assert.AreEqual("v-7", res[0].Val);
    }

    [Test]
    [Ignore("IGNITE-18120 Allow arbitrary MemberInit projections in LINQ")]
    public void TestSelectComputedColumnIntoPoco()
    {
        var res = PocoView.AsQueryable()
            .Where(x => x.Key == 3)
            .Select(x => new Poco { Val = x.Val, Key = x.Key - 1 })
            .ToArray();

        Assert.AreEqual(1, res.Length);
        Assert.AreEqual(2, res[0].Key);
        Assert.AreEqual("v-3", res[0].Val);
    }

    [Test]
    public void TestSkip()
    {
        var query = PocoView.AsQueryable()
            .OrderBy(x => x.Key)
            .Select(x => x.Key)
            .Skip(7);

        List<long> res = query.ToList();

        Assert.AreEqual(new long[] { 7, 8, 9 }, res);

        StringAssert.Contains(
            "select _T0.KEY from PUBLIC.TBL1 as _T0 " +
            "order by (_T0.KEY) asc " +
            "offset ?",
            query.ToString());
    }

    [Test]
    public void TestTake()
    {
        var query = PocoView.AsQueryable()
            .OrderBy(x => x.Key)
            .Take(2);

        List<Poco> res = query.ToList();

        Assert.AreEqual(new long[] { 0, 1 }, res.Select(x => x.Key));

        StringAssert.Contains(
            "select _T0.KEY, _T0.VAL from PUBLIC.TBL1 as _T0 " +
            "order by (_T0.KEY) asc " +
            "limit ?",
            query.ToString());
    }

    [Test]
    public void TestOrderBySkipTake()
    {
        var query = PocoView.AsQueryable()
            .OrderByDescending(x => x.Key)
            .Select(x => x.Key)
            .Skip(1)
            .Take(2);

        List<long> res = query.ToList();

        Assert.AreEqual(new long[] { 8, 7 }, res);

        StringAssert.Contains(
            "select _T0.KEY from PUBLIC.TBL1 as _T0 " +
            "order by (_T0.KEY) desc " +
            "limit ? offset ?",
            query.ToString());
    }

    [Test]
    public void TestSkipTakeFirst()
    {
        var query = PocoView.AsQueryable()
            .OrderByDescending(x => x.Key)
            .Select(x => x.Key)
            .Skip(1)
            .Take(2);

        long res = query.First();

        Assert.AreEqual(8, res);

        StringAssert.Contains(
            "select _T0.KEY from PUBLIC.TBL1 as _T0 " +
            "order by (_T0.KEY) desc " +
            "limit ? offset ?",
            query.ToString());
    }

    [Test]
    [Ignore("IGNITE-18311")]
    public void TestOrderBySkipTakeBeforeSelect()
    {
        var query = PocoView.AsQueryable()
            .OrderByDescending(x => x.Key)
            .Skip(1)
            .Take(2)
            .Select(x => x.Key);

        List<long> res = query.ToList();

        Assert.AreEqual(new long[] { 8, 7 }, res);

        StringAssert.Contains(
            "select _T0.KEY from (" +
            "select _T1.KEY, _T1.VAL " +
            "from PUBLIC.TBL1 as _T1 " +
            "order by (_T1.KEY) desc " +
            "limit ? offset ?) as _T0",
            query.ToString());
    }

    [Test]
    public void TestContains()
    {
        var keys = new long[] { 4, 2 };

        var query = PocoView.AsQueryable()
            .Where(x => keys.Contains(x.Key))
            .Select(x => x.Val);

        List<string?> res = query.ToList();

        CollectionAssert.AreEquivalent(new[] { "v-2", "v-4" }, res);

        StringAssert.Contains(
            "select _T0.VAL from PUBLIC.TBL1 as _T0 where (_T0.KEY IN (?, ?)), Parameters=4, 2",
            query.ToString());
    }

    [Test]
    public void TestDistinctOneField()
    {
        var query = PocoByteView.AsQueryable()
            .Select(x => x.Val)
            .Distinct();

        List<sbyte> res = query.ToList();

        CollectionAssert.AreEquivalent(new[] { 0, 1, 2, 3 }, res);

        StringAssert.Contains("select distinct _T0.VAL from PUBLIC.TBL_INT8 as _T0", query.ToString());
    }

    [Test]
    public void TestDistinctEntireObject()
    {
        var query = PocoByteView.AsQueryable()
            .Distinct();

        List<PocoByte> res = query.ToList();

        Assert.AreEqual(10, res.Count);

        StringAssert.Contains("select distinct _T0.KEY, _T0.VAL from PUBLIC.TBL_INT8 as _T0", query.ToString());
    }

    [Test]
    public void TestDistinctProjection()
    {
        var query = PocoByteView.AsQueryable()
            .Select(x => new { Id = x.Val + 10, V = x.Val })
            .Distinct();

        var res = query.ToList();

        Assert.AreEqual(4, res.Count);

        StringAssert.Contains(
            "select distinct (cast(_T0.VAL as int) + ?) as ID, _T0.VAL " +
            "from PUBLIC.TBL_INT8 as _T0",
            query.ToString());
    }

    [Test]
    public void TestDistinctAfterOrderBy()
    {
        var query = PocoByteView.AsQueryable()
            .Select(x => new { Id = x.Val + 10, V = x.Val })
            .OrderByDescending(x => x.V)
            .Distinct();

        var res = query.ToList();

        Assert.AreEqual(4, res.Count);
        Assert.AreEqual(13, res[0].Id);

        StringAssert.Contains(
            "select distinct (cast(_T0.VAL as int) + ?) as ID, _T0.VAL " +
            "from PUBLIC.TBL_INT8 as _T0 " +
            "order by (_T0.VAL) desc",
            query.ToString());
    }

    [Test]
    public void TestDistinctBeforeOrderBy()
    {
        var query = PocoByteView.AsQueryable()
            .Select(x => new { Id = x.Val + 10, V = x.Val })
            .Distinct()
            .OrderByDescending(x => x.Id);

        var res = query.ToList();

        Assert.AreEqual(4, res.Count);
        Assert.AreEqual(13, res[0].Id);

        StringAssert.Contains(
            "select * from " +
            "(select distinct (cast(_T0.VAL as int) + ?) as ID, _T0.VAL from PUBLIC.TBL_INT8 as _T0) as _T1 " +
            "order by (_T1.ID) desc",
            query.ToString());
    }

    [Test]
    public void TestCustomColumnNameMapping()
    {
        var res = Table.GetRecordView<PocoCustomNames>().AsQueryable()
            .Select(x => new { Key = x.Id, Val = x.Name })
            .Where(x => x.Key == 3)
            .ToArray();

        Assert.AreEqual(1, res.Length);
        Assert.AreEqual(3, res[0].Key);
        Assert.AreEqual("v-3", res[0].Val);
    }

    [Test]
    public async Task TestTransactionIsPropagatedToServer()
    {
        using var server = new FakeServer();
        using var client = await server.ConnectClientAsync();

        var tx = await client.Transactions.BeginAsync();
        var tbl = await client.Tables.GetTableAsync(FakeServer.ExistingTableName);
        var pocoView = tbl!.GetRecordView<PocoInt>();

        _ = pocoView.AsQueryable(tx).Select(x => x.Key).ToArray();

        Assert.AreEqual(0, server.LastSqlTxId);
    }

    [Test]
    public void TestEnumeration()
    {
        var query = PocoView.AsQueryable(options: new QueryableOptions(PageSize: 2)).OrderBy(x => x.Key);
        int count = 0;

        foreach (var poco in query)
        {
            Assert.AreEqual(count++, poco.Key);
        }

        Assert.AreEqual(10, count);
    }

    [Test]
    public void TestQueryToString()
    {
        var query = PocoView.AsQueryable()
            .Where(x => x.Key == 3 && x.Val != "v-2")
            .Select(x => new { x.Val, x.Key });

        const string expected =
            "IgniteQueryable`1 [Query=" +
            "select _T0.VAL, _T0.KEY " +
            "from PUBLIC.TBL1 as _T0 " +
            "where ((_T0.KEY IS NOT DISTINCT FROM ?) and (_T0.VAL IS DISTINCT FROM ?))" +
            ", Parameters=3, v-2]";

        Assert.AreEqual(expected, query.ToString());
    }

    [Test]
    public void TestSelectDecimalIntoAnonymousTypeUsesCorrectScale()
    {
        var query = PocoDecimalView.AsQueryable()
            .OrderByDescending(x => x.Val)
            .Select(x => new
            {
                Id = x.Key
            });

        var res = query.ToList();
        Assert.AreEqual(9.0m, res[0].Id);
    }

    [Test]
    public void TestSelectDecimalIntoUserDefinedTypeUsesCorrectScale()
    {
        var query = PocoDecimalView.AsQueryable()
            .OrderByDescending(x => x.Val)
            .Select(x => new PocoDecimal(x.Val, x.Key));

        var res = query.ToList();
        Assert.AreEqual(9.0m, res[0].Val);
    }

    [Test]
    public void TestSelectAllColumnTypes()
    {
        var res = PocoAllColumnsSqlView.AsQueryable()
            .OrderBy(x => x.Key)
            .Take(3)
            .ToList();

        Assert.AreEqual(0, res[0].Key);
        Assert.AreEqual(1, res[0].Int8);
        Assert.AreEqual(2, res[0].Int16);
        Assert.AreEqual(3, res[0].Int32);
        Assert.AreEqual(4, res[0].Int64);
        Assert.AreEqual(5.5f, res[0].Float);
        Assert.AreEqual(6.5, res[0].Double);
        Assert.AreEqual(7.7, res[0].Decimal);
        Assert.AreEqual(new LocalDate(2022, 12, 1), res[0].Date);
        Assert.AreEqual(new LocalTime(11, 38, 1), res[0].Time);
        Assert.AreEqual(new LocalDateTime(2022, 12, 19, 11, 1), res[0].DateTime);
        Assert.AreEqual(Instant.FromUnixTimeSeconds(1), res[0].Timestamp);
        Assert.AreEqual(new byte[] { 1, 2 }, res[0].Blob);
    }

    [Test]
    public void TestSelectAllColumnTypesNullable()
    {
        var res = PocoAllColumnsSqlNullableView.AsQueryable()
            .OrderBy(x => x.Key)
            .ToList();

        Assert.AreEqual(0, res[0].Key);
        Assert.AreEqual(1, res[0].Int8);
        Assert.AreEqual(2, res[0].Int16);
        Assert.AreEqual(3, res[0].Int32);
        Assert.AreEqual(4, res[0].Int64);
        Assert.AreEqual(5.5f, res[0].Float);
        Assert.AreEqual(6.5, res[0].Double);
        Assert.AreEqual(7.7, res[0].Decimal);
        Assert.AreEqual(new LocalDate(2022, 12, 1), res[0].Date);
        Assert.AreEqual(new LocalTime(11, 38, 1), res[0].Time);
        Assert.AreEqual(new LocalDateTime(2022, 12, 19, 11, 1), res[0].DateTime);
        Assert.AreEqual(Instant.FromUnixTimeSeconds(1), res[0].Timestamp);
        Assert.AreEqual(new byte[] { 1, 2 }, res[0].Blob);
    }

    [Test]
    public void TestSelectAllColumnTypesNullableNull()
    {
        var res = PocoAllColumnsSqlNullableView.AsQueryable()
            .OrderByDescending(x => x.Key)
            .ToList();

        Assert.AreEqual(100, res[0].Key);
        Assert.IsNull(res[0].Str);
        Assert.IsNull(res[0].Int8);
        Assert.IsNull(res[0].Int16);
        Assert.IsNull(res[0].Int32);
        Assert.IsNull(res[0].Int64);
        Assert.IsNull(res[0].Float);
        Assert.IsNull(res[0].Double);
        Assert.IsNull(res[0].Decimal);
        Assert.IsNull(res[0].Date);
        Assert.IsNull(res[0].Time);
        Assert.IsNull(res[0].DateTime);
        Assert.IsNull(res[0].Timestamp);
        Assert.IsNull(res[0].Blob);
    }

    [Test]
    public void TestWhereNull()
    {
        var res = PocoAllColumnsSqlNullableView.AsQueryable()
            .Where(x => x.Int8 == null)
            .ToList();

        Assert.AreEqual(100, res[0].Key);
        Assert.AreEqual(1, res.Count);
    }

    [Test]
    public void TestWhereNotNull()
    {
        var res = PocoAllColumnsSqlNullableView.AsQueryable()
            .Where(x => x.Int8 != null)
            .OrderBy(x => x.Key)
            .ToList();

        Assert.AreEqual(0, res[0].Key);
    }

    private record PocoByte(sbyte Key, sbyte Val);

    private record PocoShort(short Key, short Val);

    private record PocoInt(int Key, int Val);

    private record PocoLong(long Key, long Val);

    private record PocoFloat(float Key, float Val);

    private record PocoDouble(double Key, double Val);

    private record PocoDecimal(decimal Key, decimal Val);

    private record PocoString(string Key, string Val);
}
