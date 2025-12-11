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
using Common;
using Common.Table;
using Ignite.Sql;
using Ignite.Table;
using Microsoft.Extensions.Logging;
using NodaTime;
using NUnit.Framework;
using Table;

/// <summary>
/// Basic LINQ provider tests.
/// </summary>
[SuppressMessage("ReSharper", "PossibleLossOfFraction", Justification = "Tests")]
[SuppressMessage("ReSharper", "NotAccessedPositionalProperty.Local", Justification = "Tests")]
[SuppressMessage("Naming", "CA1711:Identifiers should not have incorrect suffix", Justification = "Tests")]
[SuppressMessage("StyleCop.CSharp.OrderingRules", "SA1201:Elements should appear in the correct order", Justification = "Tests")]
[SuppressMessage("ReSharper", "UnusedMember.Local", Justification = "Tests")]
public partial class LinqTests : IgniteTestsBase
{
    private const int Count = 10;

    private IRecordView<PocoByte> PocoByteView { get; set; } = null!;

    private IRecordView<PocoShort> PocoShortView { get; set; } = null!;

    private IRecordView<PocoInt> PocoIntView { get; set; } = null!;

    private IRecordView<PocoIntEnum> PocoIntEnumView { get; set; } = null!;

    private IRecordView<PocoLong> PocoLongView { get; set; } = null!;

    private IRecordView<PocoFloat> PocoFloatView { get; set; } = null!;

    private IRecordView<PocoDouble> PocoDoubleView { get; set; } = null!;

    private IRecordView<PocoDecimal> PocoDecimalView { get; set; } = null!;

    private IRecordView<PocoBigDecimal> PocoBigDecimalView { get; set; } = null!;

    private IRecordView<PocoString> PocoStringView { get; set; } = null!;

    [OneTimeSetUp]
    public async Task InsertData()
    {
        var tableNames = new[]
        {
            TableName, TableDateTimeName, TableDoubleName, TableFloatName, TableDecimalName, TableInt8Name,
            TableInt16Name, TableInt32Name, TableInt64Name
        };

        foreach (var tableName in tableNames)
        {
            await Client.Sql.ExecuteAsync(null, "delete from " + tableName);
        }

        PocoByteView = (await Client.Tables.GetTableAsync(TableInt8Name))!.GetRecordView<PocoByte>();
        PocoShortView = (await Client.Tables.GetTableAsync(TableInt16Name))!.GetRecordView<PocoShort>();
        PocoIntView = (await Client.Tables.GetTableAsync(TableInt32Name))!.GetRecordView<PocoInt>();
        PocoIntEnumView = (await Client.Tables.GetTableAsync(TableInt32Name))!.GetRecordView<PocoIntEnum>();
        PocoLongView = (await Client.Tables.GetTableAsync(TableInt64Name))!.GetRecordView<PocoLong>();
        PocoFloatView = (await Client.Tables.GetTableAsync(TableFloatName))!.GetRecordView<PocoFloat>();
        PocoDoubleView = (await Client.Tables.GetTableAsync(TableDoubleName))!.GetRecordView<PocoDouble>();
        PocoStringView = (await Client.Tables.GetTableAsync(TableStringName))!.GetRecordView<PocoString>();

        var tableDecimal = await Client.Tables.GetTableAsync(TableDecimalName);
        PocoDecimalView = tableDecimal!.GetRecordView<PocoDecimal>();
        PocoBigDecimalView = tableDecimal.GetRecordView<PocoBigDecimal>();

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
                i + 7.7m,
                new Guid(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, (byte)(i + 1)),
                i % 2 == 0);

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
    public async Task TestSelectOneColumnAsAsyncEnumerable()
    {
        var query = PocoView.AsQueryable()
            .Where(x => x.Key == 3)
            .Select(x => x.Val);

        var count = 0;

        await foreach (var row in query.AsAsyncEnumerable())
        {
            Assert.AreEqual("v-3", row);
            count++;
        }

        Assert.AreEqual(1, count);
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
            "order by _T0.KEY asc " +
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
            "order by _T0.KEY asc " +
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
            "order by _T0.KEY desc " +
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
            "order by _T0.KEY desc " +
            "limit ? offset ?",
            query.ToString());
    }

    [Test]
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
            "select * from PUBLIC.TBL1 as _T1 " +
            "order by _T1.KEY desc " +
            "limit ? offset ?) as _T0",
            query.ToString());
    }

    [Test]
    public void TestOrderByMultiple()
    {
        var query = PocoAllColumnsSqlView.AsQueryable()
            .Where(x => x.Key < 10)
            .OrderBy(x => x.Key)
            .ThenByDescending(x => x.Int8)
            .ThenBy(x => x.Int16)
            .Select(x => new { x.Decimal, x.Double });

        var res = query.ToList();

        Assert.AreEqual(6.5d, res[0].Double);
        Assert.AreEqual(7.7m, res[0].Decimal);

        StringAssert.Contains(
            "select _T0.\"DECIMAL\", _T0.\"DOUBLE\" " +
            "from PUBLIC.TBL_ALL_COLUMNS_SQL as _T0 " +
            "where (_T0.KEY < ?) " +
            "order by _T0.KEY asc, _T0.INT8 desc, _T0.INT16 asc",
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
            "select _T0.VAL from PUBLIC.TBL1 as _T0 where (_T0.KEY IN (?, ?)), Parameters = [ 4, 2 ]",
            query.ToString());
    }

    [Test]
    public void TestDistinctOneField()
    {
        var query = PocoByteView.AsQueryable()
            .Select(x => x.Val)
            .Distinct();

        List<sbyte?> res = query.ToList();

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
            "order by _T0.VAL desc",
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
            "order by _T1.ID desc",
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
            .Select(x => new PocoDecimal(x.Key, x.Val));

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
        Assert.AreEqual(new Guid(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1), res[0].Uuid);
        Assert.IsTrue(res[0].Boolean);
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
        Assert.AreEqual(new Guid(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1), res[0].Uuid);
        Assert.IsTrue(res[0].Boolean);
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
        Assert.IsNull(res[0].Uuid);
        Assert.IsNull(res[0].Boolean);
    }

    [Test]
    public void TestWhereNull()
    {
        var query = PocoAllColumnsSqlNullableView.AsQueryable()
            .Where(x => x.Int8 == null);

        StringAssert.Contains("where (_T0.INT8 IS NOT DISTINCT FROM ?)", query.ToString());

        var res = query.ToList();

        Assert.AreEqual(100, res[0].Key);
        Assert.AreEqual(1, res.Count);
    }

    [Test]
    public void TestWhereNotNull()
    {
        var query = PocoAllColumnsSqlNullableView.AsQueryable()
            .Where(x => x.Int8 != null)
            .OrderBy(x => x.Key);

        StringAssert.Contains("where (_T0.INT8 IS DISTINCT FROM ?)", query.ToString());

        var res = query.ToList();

        Assert.AreEqual(0, res[0].Key);
        Assert.AreEqual(10, res.Count);
    }

    [Test]
    public void TestFilterAndSelectEnumColumn()
    {
        var query = PocoIntEnumView.AsQueryable()
            .Where(x => x.Val == TestEnum.B)
            .Select(x => new { x.Key, Res = x.Val });

        StringAssert.Contains(
            "select _T0.KEY, _T0.VAL from PUBLIC.TBL_INT32 as _T0 where (cast(_T0.VAL as int) IS NOT DISTINCT FROM ?), Parameters = [ 300 ]",
            query.ToString());

        var res = query.ToList();
        var resEnum = res[0].Res;

        Assert.AreEqual(3, res[0].Key);
        Assert.AreEqual(TestEnum.B, resEnum);
        Assert.AreEqual(1, res.Count);
    }

    [Test]
    [SuppressMessage("Globalization", "CA1311:Specify a culture or use an invariant version", Justification = "SQL")]
    [SuppressMessage("Performance", "CA1862:Use the \'StringComparison\' method overloads to perform case-insensitive string comparisons", Justification = "SQL")]
    public async Task TestGeneratedSqlIsLoggedWithDebugLevel()
    {
        var config = GetConfig();
        var logger = new ListLoggerFactory(new[] { LogLevel.Debug });
        config.LoggerFactory = logger;

        using var client = await IgniteClient.StartAsync(config);
        var table = (await client.Tables.GetTableAsync(TableName))!;
        var view = table.GetRecordView<Poco>();

        _ = await view.AsQueryable(null, new QueryableOptions { Timeout = TimeSpan.FromSeconds(15), PageSize = 123 })
            .Where(x => x.Key > 5 && x.Val!.ToUpper() == "abc")
            .SumAsync(x => x.Key);

        var logEntry = logger.Entries.Single(x => x.Category == "Apache.Ignite.Internal.Linq.IgniteQueryExecutor");

        var expectedMessage =
            "Executing SQL statement generated by LINQ provider [statement=SqlStatement { " +
            "Query = select sum(_T0.KEY) from PUBLIC.TBL1 as _T0 where ((_T0.KEY > ?) and (upper(_T0.VAL) IS NOT DISTINCT FROM ?)), " +
            "Timeout = 00:00:15, " +
            "Schema = PUBLIC, " +
            "PageSize = 123, " +
            "Properties = { } }, " +
            "parameters=5, abc]";

        Assert.AreEqual(expectedMessage, logEntry.Message);
        Assert.AreEqual(LogLevel.Debug, logEntry.Level);
        Assert.IsNull(logEntry.Exception);
    }

    private enum TestEnum
    {
        None = 0,
        A = 100,
        B = 300
    }

    private record PocoByte(sbyte Key, sbyte? Val);

    private record PocoShort(short Key, short? Val);

    private record PocoInt(int Key, int? Val);

    private record PocoLong(long Key, long? Val);

    private record PocoFloat(float Key, float? Val);

    private record PocoDouble(double Key, double? Val);

    private record PocoDecimal(decimal Key, decimal? Val);

    private record PocoBigDecimal(BigDecimal Key, BigDecimal? Val);

    private record PocoString(string Key, string? Val);

    private record PocoDate(LocalDate Key, LocalDate? Val);

    private record PocoTime(LocalTime Key, LocalTime? Val);

    private record PocoDateTime(LocalDateTime Key, LocalDateTime? Val);

    private record PocoIntEnum(int Key, TestEnum? Val);
}
