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
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Ignite.Sql;
using Ignite.Table;
using NUnit.Framework;
using Table;

/// <summary>
/// Tests LINQ to SQL conversion.
/// <para />
/// Uses <see cref="FakeServer"/> to get the actual SQL sent from the client.
/// </summary>
public partial class LinqSqlGenerationTests
{
    private IIgniteClient _client = null!;
    private FakeServer _server = null!;
    private ITable _table = null!;

    [Test]
    public void TestSelectOneColumn() =>
        AssertSql("select _T0.KEY from PUBLIC.tbl1 as _T0", q => q.Select(x => x.Key).ToList());

    [Test]
    public void TestSelectAllColumns() =>
        AssertSql("select _T0.KEY, _T0.VAL from PUBLIC.tbl1 as _T0", q => q.ToList());

    [Test]
    public void TestSelectAllColumnsOneColumnPoco() =>
        AssertSql(
            "select _T0.KEY from PUBLIC.tbl1 as _T0",
            tbl => tbl.GetRecordView<OneColumnPoco>().AsQueryable().ToList());

    [Test]
    public void TestSelectAllColumnsCustomNames() =>
        AssertSql(
            "select _T0.\"KEY\", _T0.\"VAL\" from PUBLIC.tbl1 as _T0",
            tbl => tbl.GetRecordView<PocoCustomNames>().AsQueryable().ToList());

    [Test]
    public void TestSum() =>
        AssertSql("select sum(_T0.KEY) from PUBLIC.tbl1 as _T0", q => q.Sum(x => x.Key));

    [Test]
    public void TestSumAsync() =>
        AssertSql("select sum(_T0.KEY) from PUBLIC.tbl1 as _T0", q => q.SumAsync(x => x.Key).Result);

    [Test]
    public void TestAvg() =>
        AssertSql("select avg(_T0.KEY) from PUBLIC.tbl1 as _T0", q => q.Average(x => x.Key));

    [Test]
    public void TestAvgAsync() =>
        AssertSql("select avg(_T0.KEY) from PUBLIC.tbl1 as _T0", q => q.AverageAsync(x => x.Key).Result);

    [Test]
    public void TestMin() =>
        AssertSql("select min(_T0.KEY) from PUBLIC.tbl1 as _T0", q => q.Min(x => x.Key));

    [Test]
    public void TestMinAsync() =>
        AssertSql("select min(_T0.KEY) from PUBLIC.tbl1 as _T0", q => q.MinAsync(x => x.Key).Result);

    [Test]
    public void TestMax() =>
        AssertSql("select max(_T0.KEY) from PUBLIC.tbl1 as _T0", q => q.Max(x => x.Key));

    [Test]
    public void TestMaxAsync() =>
        AssertSql("select max(_T0.KEY) from PUBLIC.tbl1 as _T0", q => q.MaxAsync(x => x.Key).Result);

    [Test]
    public void TestCount() =>
        AssertSql("select count(*) from PUBLIC.tbl1 as _T0", q => q.Count());

    [Test]
    public void TestCountAsync() =>
        AssertSql("select count(*) from PUBLIC.tbl1 as _T0", q => q.CountAsync().Result);

    [Test]
    public void TestLongCount() =>
        AssertSql("select count(*) from PUBLIC.tbl1 as _T0", q => q.LongCount());

    [Test]
    public void TestLongCountAsync() =>
        AssertSql("select count(*) from PUBLIC.tbl1 as _T0", q => q.LongCountAsync().Result);

    [Test]
    public void TestDistinct() =>
        AssertSql("select distinct _T0.VAL from PUBLIC.tbl1 as _T0", q => q.Select(x => x.Val).Distinct().ToArray());

    [Test]
    public void TestAll() =>
        AssertSql(
            "select not exists (select 1 from PUBLIC.tbl1 as _T0 where not (_T0.KEY > ?))",
            q => q.All(x => x.Key > 10));

    [Test]
    public void TestAllAsync() =>
        AssertSql(
            "select not exists (select 1 from PUBLIC.tbl1 as _T0 where not (_T0.KEY > ?))",
            q => q.AllAsync(x => x.Key > 10).Result);

    [Test]
    public void TestAllWithWhere() =>
        AssertSql(
            "select not exists (select 1 from PUBLIC.tbl1 as _T0 where (_T0.VAL IS DISTINCT FROM ?) and not (_T0.KEY > ?))",
            q => q.Where(x => x.Val != "1").All(x => x.Key > 10));

    [Test]
    public void TestAny() =>
        AssertSql("select exists (select 1 from PUBLIC.tbl1 as _T0)", q => q.Any());

    [Test]
    public void TestAnyAsync() =>
        AssertSql("select exists (select 1 from PUBLIC.tbl1 as _T0)", q => q.AnyAsync().Result);

    [Test]
    public void TestAnyWithPredicate() =>
        AssertSql("select exists (select 1 from PUBLIC.tbl1 as _T0 where (_T0.KEY > ?))", q => q.Any(x => x.Key > 10));

    [Test]
    public void TestAnyAsyncWithPredicate() =>
        AssertSql("select exists (select 1 from PUBLIC.tbl1 as _T0 where (_T0.KEY > ?))", q => q.AnyAsync(x => x.Key > 10).Result);

    [Test]
    public void TestSelectOrderByOffsetLimit() =>
        AssertSql(
            "select _T0.KEY, _T0.VAL, (_T0.KEY + ?) as KEY2 " +
            "from PUBLIC.tbl1 as _T0 " +
            "order by (_T0.KEY + ?) asc, _T0.VAL desc " +
            "limit ? offset ?",
            q => q.Select(x => new { x.Key, x.Val, Key2 = x.Key + 1})
                .OrderBy(x => x.Key2)
                .ThenByDescending(x => x.Val)
                .Skip(2)
                .Take(3)
                .ToList());

    [Test]
    public void TestFirst() =>
        AssertSql("select _T0.KEY, _T0.VAL from PUBLIC.tbl1 as _T0 limit 1", q => q.First());

    [Test]
    public void TestFirstAsync() =>
        AssertSql("select _T0.KEY, _T0.VAL from PUBLIC.tbl1 as _T0 limit 1", q => q.FirstAsync().Result);

    [Test]
    public void TestFirstOrDefault() =>
        AssertSql("select _T0.KEY, _T0.VAL from PUBLIC.tbl1 as _T0 limit 1", q => q.FirstOrDefault());

    [Test]
    public void TestFirstOrDefaultAsync() =>
        AssertSql("select _T0.KEY, _T0.VAL from PUBLIC.tbl1 as _T0 limit 1", q => q.FirstOrDefaultAsync().Result);

    [Test]
    public void TestSingle() =>
        AssertSql("select _T0.KEY, _T0.VAL from PUBLIC.tbl1 as _T0 limit 2", q => q.Single());

    [Test]
    public void TestSingleAsync() =>
        AssertSql("select _T0.KEY, _T0.VAL from PUBLIC.tbl1 as _T0 limit 2", q => q.SingleAsync().Result);

    [Test]
    public void TestSingleOrDefault() =>
        AssertSql("select _T0.KEY, _T0.VAL from PUBLIC.tbl1 as _T0 limit 2", q => q.SingleOrDefault());

    [Test]
    public void TestSingleOrDefaultAsync() =>
        AssertSql("select _T0.KEY, _T0.VAL from PUBLIC.tbl1 as _T0 limit 2", q => q.SingleOrDefaultAsync().Result);

    [Test]
    public void TestOffsetLimitFirst() =>
        AssertSql(
            "select _T0.KEY, _T0.VAL " +
            "from PUBLIC.tbl1 as _T0 " +
            "limit 1 offset ?",
            q => q.Skip(2).Take(3).First());

    [Test]
    public void TestOffsetLimitSingle() =>
        AssertSql(
            "select _T0.KEY, _T0.VAL " +
            "from PUBLIC.tbl1 as _T0 " +
            "limit 2 offset ?",
            q => q.Skip(2).Take(3).Single());

    [Test]
    public void TestOffsetMultipleNotSupported()
    {
        // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
        var ex = Assert.Throws<NotSupportedException>(
            () => _table.GetRecordView<Poco>().AsQueryable().Skip(1).Skip(2).ToList());

        Assert.AreEqual("Multiple Skip operators on the same subquery are not supported.", ex!.Message);
    }

    [Test]
    public void TestLimitMultipleNotSupported()
    {
        // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
        var ex = Assert.Throws<NotSupportedException>(
            () => _table.GetRecordView<Poco>().AsQueryable().Take(1).Take(2).ToList());

        Assert.AreEqual("Multiple Take operators on the same subquery are not supported.", ex!.Message);
    }

    [Test]
    public void TestSelectOrderDistinct() =>
        AssertSql(
            "select * from (select distinct _T0.KEY, (_T0.KEY + ?) as KEY2 from PUBLIC.tbl1 as _T0) as _T1 order by _T1.KEY2 asc",
            q => q.Select(x => new { x.Key, Key2 = x.Key + 1})
                .Distinct()
                .OrderBy(x => x.Key2)
                .ToList());

    [Test]
    public void TestDefaultQueryableOptions()
    {
        _server.LastSqlTimeoutMs = null;
        _server.LastSqlPageSize = null;

        _ = _table.GetRecordView<Poco>().AsQueryable().Select(x => (int)x.Key).ToArray();

        Assert.AreEqual(SqlStatement.DefaultTimeout.TotalMilliseconds, _server.LastSqlTimeoutMs);
        Assert.AreEqual(SqlStatement.DefaultPageSize, _server.LastSqlPageSize);
    }

    [Test]
    public void TestCustomQueryableOptions()
    {
        _server.LastSqlTimeoutMs = null;
        _server.LastSqlPageSize = null;

        _ = _table.GetRecordView<Poco>().AsQueryable(options: new(TimeSpan.FromSeconds(25), 128)).Select(x => (int)x.Key).ToArray();

        Assert.AreEqual(25000, _server.LastSqlTimeoutMs);
        Assert.AreEqual(128, _server.LastSqlPageSize);
    }

    [Test]
    public void TestGroupBySubQuery()
    {
        AssertSql(
            "select (_T0.KEY + ?) as _G0, count(*) as CNT from PUBLIC.tbl1 as _T0 group by _G0",
            q => q.Select(x => new { x.Key, Key2 = x.Key + 1 })
                .GroupBy(x => x.Key2)
                .Select(g => new { g.Key, Cnt = g.Count() })
                .ToList());
    }

    [Test]
    public void TestPrimitiveTypeMappingNotSupported()
    {
        // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
        var ex = Assert.Throws<NotSupportedException>(
            () => _table.GetRecordView<int>().AsQueryable().Where(x => x > 0).ToList());

        Assert.AreEqual(
            "Primitive types are not supported in LINQ queries: System.Int32. " +
            "Use a custom type (class, record, struct) with a single field instead.",
            ex!.Message);
    }

    [Test]
    public void TestEmptyTypeMappingNotSupported()
    {
        // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
        var ex = Assert.Throws<NotSupportedException>(() => _table.GetRecordView<EmptyPoco>().AsQueryable().ToList());

        Assert.AreEqual(
            "Type 'Apache.Ignite.Tests.Linq.LinqSqlGenerationTests+EmptyPoco' can not be mapped to SQL columns: " +
            "it has no fields, or all fields are [NotMapped].",
            ex!.Message);
    }

    [Test]
    public void TestAllNotMappedTypeMappingNotSupported()
    {
        // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
        var ex = Assert.Throws<NotSupportedException>(() => _table.GetRecordView<UnmappedPoco>().AsQueryable().ToList());

        Assert.AreEqual(
            "Type 'Apache.Ignite.Tests.Linq.LinqSqlGenerationTests+UnmappedPoco' can not be mapped to SQL columns: " +
            "it has no fields, or all fields are [NotMapped].",
            ex!.Message);
    }

    [Test]
    public void TestRecordViewKeyValuePairNotSupported()
    {
        // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
        var ex = Assert.Throws<NotSupportedException>(() => _table.GetRecordView<KeyValuePair<int, int>>().AsQueryable().ToList());

        Assert.AreEqual(
            "Can't use System.Collections.Generic.KeyValuePair`2[TKey,TValue] for LINQ queries: " +
            "it is reserved for Apache.Ignite.Table.IKeyValueView`2[TK,TV].AsQueryable. Use a custom type instead.",
            ex!.Message);
    }

    [Test]
    public void TestUnion() =>
        AssertSql(
            "select (_T0.KEY + ?) as KEY, _T0.VAL from PUBLIC.tbl1 as _T0 " +
            "union (select (_T1.KEY + ?) as KEY, _T1.VAL from PUBLIC.tbl1 as _T1)",
            q => q.Select(x => new { Key = x.Key + 1, x.Val })
                .Union(q.Select(x => new { Key = x.Key + 100, x.Val }))
                .ToList());

    [Test]
    public void TestIntersect() =>
        AssertSql(
            "select (_T0.KEY + ?) as KEY, concat(_T0.VAL, ?) as VAL from PUBLIC.tbl1 as _T0 " +
            "intersect (select (_T1.KEY + ?) as KEY, concat(_T1.VAL, ?) as VAL from PUBLIC.tbl1 as _T1)",
            q => q.Select(x => new { Key = x.Key + 1, Val = x.Val + "_" })
                .Intersect(q.Select(x => new { Key = x.Key + 100, Val = x.Val + "!" }))
                .ToList());

    [Test]
    public void TestExcept() =>
        AssertSql(
            "select (_T0.KEY + ?) from PUBLIC.tbl1 as _T0 except (select (_T1.KEY + ?) from PUBLIC.tbl1 as _T1)",
            q => q.Select(x => x.Key + 1)
                .Except(q.Select(x => x.Key + 5))
                .ToList());

    [Test]
    public void TestQueryToString()
    {
        var query = _table.GetRecordView<Poco>().AsQueryable()
            .Where(x => x.Key == 3 && x.Val != "v-2")
            .Select(x => new { x.Val, x.Key });

        const string expectedQueryText =
            "select _T0.VAL, _T0.KEY " +
            "from PUBLIC.tbl1 as _T0 " +
            "where ((_T0.KEY IS NOT DISTINCT FROM ?) and (_T0.VAL IS DISTINCT FROM ?))";

        const string expectedToString =
            "IgniteQueryable`1[<>f__AnonymousType5`2[String, Int64]] { Query = " +
            expectedQueryText +
            ", Parameters = [ 3, v-2 ] }";

        Assert.AreEqual(expectedQueryText, query.ToQueryString());
        Assert.AreEqual(expectedToString, query.ToString());
    }

    [Test]
    public void TestExecuteDelete() =>
        AssertSql("delete from PUBLIC.tbl1 as _T0", q => q.ExecuteDeleteAsync().Result);

    [Test]
    public void TestExecuteDeleteWithCondition() =>
        AssertSql(
            "delete from PUBLIC.tbl1 as _T0 where ((_T0.KEY IS NOT DISTINCT FROM ?) and (_T0.VAL IS DISTINCT FROM ?))",
            q => q.Where(x => x.Key == 3 && x.Val != "v-2").ExecuteDeleteAsync().Result);

    [Test]
    public void TestExecuteDeleteWithInlineCondition() =>
        AssertSql(
            "delete from PUBLIC.tbl1 as _T0 where ((_T0.KEY IS NOT DISTINCT FROM ?) and (_T0.VAL IS DISTINCT FROM ?))",
            q => q.ExecuteDeleteAsync(x => x.Key == 3 && x.Val != "v-2").Result);

    [Test]
    public void TestExecuteUpdateWithConstantValue() =>
        AssertSql(
            "update PUBLIC.tbl1 as _T0 set VAL = ? where (_T0.KEY IS NOT DISTINCT FROM ?)",
            q => q.Where(x => x.Key == 3).ExecuteUpdateAsync(row => row.SetProperty(x => x.Val, "1")).Result);

    [Test]
    public void TestExecuteUpdateWithComputedValue() =>
        AssertSql(
            "update PUBLIC.tbl1 as _T0 set VAL = concat(concat(_T0.VAL, ?), cast(_T0.KEY as varchar)) where (_T0.KEY > ?)",
            q => q.Where(x => x.Key > 3).ExecuteUpdateAsync(row => row.SetProperty(x => x.Val, x => x.Val + "_" + x.Key)).Result);

    [Test]
    public void TestExecuteUpdateWithComputedValueFromSubquery()
    {
        IQueryable<Poco> q2 = _table.GetRecordView<Poco>().AsQueryable();

        AssertSql(
            "update PUBLIC.tbl1 as _T0 " +
            "set VAL = (select concat(?, cast(_T1.KEY as varchar)) from PUBLIC.tbl1 as _T1 where (_T1.KEY IS NOT DISTINCT FROM (_T0.KEY + ?)) limit 1) " +
            "where (_T0.KEY > ?)",
            q => q.Where(x => x.Key > 3).ExecuteUpdateAsync(
                row => row.SetProperty(
                    x => x.Val,
                    x => q2.Where(y => y.Key == x.Key + 1).Select(y => "_" + y.Key).First())).Result);
    }

    [Test]
    public void TestExecuteUpdateWithMultipleSetters() =>
        AssertSql(
            "update PUBLIC.tbl1 as _T0 set VAL = ?, KEY = (_T0.KEY + ?) where (_T0.KEY > ?)",
            q => q.Where(x => x.Key > 3).ExecuteUpdateAsync(
                row => row
                    .SetProperty(x => x.Key, x => x.Key + 1)
                    .SetProperty(x => x.Val, "!"))
                .Result);

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _server = new FakeServer();
        _client = await _server.ConnectClientAsync();
        _table = (await _client.Tables.GetTableAsync(FakeServer.ExistingTableName))!;
    }

    [OneTimeTearDown]
    public void OneTimeTearDown()
    {
        _client.Dispose();
        _server.Dispose();

        TestUtils.CheckByteArrayPoolLeak();
    }

    private void AssertSql(string expectedSql, Func<IQueryable<Poco>, object?> query) =>
        AssertSql(expectedSql, t => query(t.GetRecordView<Poco>().AsQueryable()));

    private void AssertSqlKv(string expectedSql, Func<IQueryable<KeyValuePair<OneColumnPoco, Poco>>, object?> query) =>
        AssertSql(expectedSql, t => query(t.GetKeyValueView<OneColumnPoco, Poco>().AsQueryable()));

    private void AssertSql(string expectedSql, Func<ITable, object?> query)
    {
        _server.LastSql = string.Empty;
        Exception? ex = null;

        try
        {
            query(_table);
        }
        catch (Exception e)
        {
            // Ignore.
            // Result deserialization may fail because FakeServer returns one column always.
            // We are only interested in the generated SQL.
            ex = e;
        }

        Assert.AreEqual(expectedSql, _server.LastSql, string.IsNullOrEmpty(_server.LastSql) ? ex?.ToString() : _server.LastSql);
    }

    // ReSharper disable NotAccessedPositionalProperty.Local, ClassNeverInstantiated.Local
    private record OneColumnPoco(long Key);

    private record EmptyPoco;

    private record UnmappedPoco([property: NotMapped] long Key, [field: NotMapped] string Val);
}
