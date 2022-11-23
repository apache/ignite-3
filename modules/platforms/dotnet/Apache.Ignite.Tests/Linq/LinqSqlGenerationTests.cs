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
    public void TestAll() =>
        AssertSql(
            "select not exists (select 1 from PUBLIC.tbl1 as _T0 where not (_T0.KEY > ?))",
            q => q.All(x => x.Key > 10));

    [Test]
    public void TestAllWithWhere() =>
        AssertSql(
            "select not exists (select 1 from PUBLIC.tbl1 as _T0 where (_T0.VAL IS DISTINCT FROM ?) and not (_T0.KEY > ?))",
            q => q.Where(x => x.Val != "1").All(x => x.Key > 10));

    [Test]
    public void TestAny() =>
        AssertSql("select exists (select 1 from PUBLIC.tbl1 as _T0 where (_T0.KEY > ?))", q => q.Any(x => x.Key > 10));

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

        Assert.AreEqual(expectedSql, _server.LastSql, string.IsNullOrEmpty(_server.LastSql) ? ex?.ToString() : null);
    }

    // ReSharper disable once NotAccessedPositionalProperty.Local
    [SuppressMessage("Microsoft.Performance", "CA1812:AvoidUninstantiatedInternalClasses", Justification = "Query tests.")]
    private record OneColumnPoco(long Key);

    [SuppressMessage("Microsoft.Performance", "CA1812:AvoidUninstantiatedInternalClasses", Justification = "Query tests.")]
    private record EmptyPoco;

    [SuppressMessage("Microsoft.Performance", "CA1812:AvoidUninstantiatedInternalClasses", Justification = "Query tests.")]
    private record UnmappedPoco([property: NotMapped] long Key, [field: NotMapped] string Val);
}
