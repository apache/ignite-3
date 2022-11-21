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
using System.Diagnostics.CodeAnalysis;
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
public class LinqSqlGenerationTests
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
        AssertSql("select sum (_T0.KEY) from PUBLIC.tbl1 as _T0", q => q.Sum(x => x.Key));

    [Test]
    public void TestAvg() =>
        AssertSql("select avg (_T0.KEY) from PUBLIC.tbl1 as _T0", q => q.Average(x => x.Key));

    [Test]
    public void TestMin() =>
        AssertSql("select min (_T0.KEY) from PUBLIC.tbl1 as _T0", q => q.Min(x => x.Key));

    [Test]
    public void TestMax() =>
        AssertSql("select max (_T0.KEY) from PUBLIC.tbl1 as _T0", q => q.Max(x => x.Key));

    [Test]
    public void TestCount() =>
        AssertSql("select count (*) from PUBLIC.tbl1 as _T0", q => q.Count());

    [Test]
    public void TestDistinct() =>
        AssertSql("select distinct _T0.VAL from PUBLIC.tbl1 as _T0", q => q.Select(x => x.Val).Distinct().ToArray());

    [Test]
    public void TestSelectOrderByOffsetLimit() =>
        AssertSql(
            "select _T0.KEY, _T0.VAL, (_T0.KEY + ?) " +
            "from PUBLIC.tbl1 as _T0 " +
            "order by ((_T0.KEY + ?)) asc, (_T0.VAL) desc " +
            "limit ? offset ?",
            q => q.Select(x => new { x.Key, x.Val, Key2 = x.Key + 1})
                .OrderBy(x => x.Key2)
                .ThenByDescending(x => x.Val)
                .Skip(2)
                .Take(3)
                .ToList());

    [Test]
    [Ignore("IGNITE-18131 Distinct support")]
    public void TestSelectOrderDistinct() =>
        AssertSql(
            "select distinct _T0.KEY, (_T0.KEY + ?) from PUBLIC.tbl1 as _T0 order by ((_T0.KEY + ?)) asc",
            q => q.Select(x => new { x.Key, Key2 = x.Key + 1})
                .Distinct()
                .OrderBy(x => x.Key2)
                .ToList());

    [Test]
    public void TestDefaultQueryableOptions()
    {
        _server.LastSqlTimeoutMs = null;
        _server.LastSqlPageSize = null;

        _ = _table.GetRecordView<Poco>().AsQueryable().Select(x => x.Key).ToArray();

        Assert.AreEqual(SqlStatement.DefaultTimeout.TotalMilliseconds, _server.LastSqlTimeoutMs);
        Assert.AreEqual(SqlStatement.DefaultPageSize, _server.LastSqlPageSize);
    }

    [Test]
    public void TestCustomQueryableOptions()
    {
        _server.LastSqlTimeoutMs = null;
        _server.LastSqlPageSize = null;

        _ = _table.GetRecordView<Poco>().AsQueryable(options: new(TimeSpan.FromSeconds(25), 128)).Select(x => x.Key).ToArray();

        Assert.AreEqual(25000, _server.LastSqlTimeoutMs);
        Assert.AreEqual(128, _server.LastSqlPageSize);
    }

    [Test]
    [Ignore("IGNITE-18215 Group by calculated value")]
    public void TestGroupBySubQuery()
    {
        AssertSql(
            "select (_T0.KEY + ?) as _G0, count (*)  from PUBLIC.tbl1 as _T0 group by G0",
            q => q.Select(x => new { x.Key, Key2 = x.Key + 1 })
                .GroupBy(x => x.Key2)
                .Select(g => new { g.Key, Count = g.Count() })
                .ToList());
    }

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
    }

    private void AssertSql(string expectedSql, Func<IQueryable<Poco>, object?> query) =>
        AssertSql(expectedSql, t => query(t.GetRecordView<Poco>().AsQueryable()));

    private void AssertSql(string expectedSql, Func<ITable, object?> query)
    {
        _server.LastSql = string.Empty;

        try
        {
            query(_table);
        }
        catch (Exception)
        {
            // Ignore.
            // Result deserialization may fail because FakeServer returns one column always.
            // We are only interested in the generated SQL.
        }

        Assert.AreEqual(expectedSql, _server.LastSql);
    }

    // ReSharper disable once NotAccessedPositionalProperty.Local
    [SuppressMessage("Microsoft.Performance", "CA1812:AvoidUninstantiatedInternalClasses", Justification = "Query tests.")]
    private record OneColumnPoco(long Key);
}
