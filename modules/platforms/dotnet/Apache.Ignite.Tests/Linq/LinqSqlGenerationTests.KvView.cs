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
using System.Linq;
using Common.Table;
using Ignite.Sql;
using Ignite.Table;
using NUnit.Framework;
using Table;

/// <summary>
/// Tests LINQ to SQL conversion for <see cref="IKeyValueView{TK,TV}"/>.
/// <para />
/// Uses <see cref="FakeServer"/> to get the actual SQL sent from the client.
/// </summary>
public partial class LinqSqlGenerationTests
{
    [Test]
    public void TestSelectPrimitiveKeyColumnKv() =>
        AssertSqlKv("select _T0.VAL from PUBLIC.TBL1 as _T0", q => q.Select(x => x.Value.Val).ToList());

    [Test]
    public void TestSelectPocoValColumnKv() =>
        AssertSqlKv("select _T0.KEY, _T0.VAL from PUBLIC.TBL1 as _T0", q => q.Select(x => x.Value).ToList());

    [Test]
    public void TestSelectTwoColumnsKv() =>
        AssertSqlKv(
            "select (_T0.KEY + ?) as KEY, _T0.VAL from PUBLIC.TBL1 as _T0",
            q => q.Select(x => new { Key = x.Key.Key + 1, x.Value.Val }).ToList());

    [Test]
    public void TestSelectAllColumnsCustomNamesKv() =>
        AssertSql(
            "select _T0.\"KEY\", _T0.\"VAL\" from PUBLIC.TBL1 as _T0",
            tbl => tbl.GetKeyValueView<PocoCustomNames, PocoCustomNames>().AsQueryable().ToList());

    [Test]
    public void TestSelectSameColumnFromPairKeyAndValKv()
    {
        // We avoid selecting same column twice if it is included in both Key and Value parts,
        // but if the user requests it explicitly, we keep it.
        AssertSqlKv(
            "select _T0.KEY, _T0.KEY from PUBLIC.TBL1 as _T0",
            q => q.Select(x => new { Key1 = x.Key.Key, Key2 = x.Value.Key }).ToList());
    }

    [Test]
    public void TestSelectEntirePairKv() =>
        AssertSqlKv("select _T0.KEY, _T0.VAL from PUBLIC.TBL1 as _T0 where (_T0.KEY > ?)", q => q.Where(x => x.Key.Key > 1).ToList());

    [Test]
    public void TestSelectPairKeyKv() =>
        AssertSqlKv("select _T0.KEY from PUBLIC.TBL1 as _T0", q => q.Select(x => x.Key).ToList());

    [Test]
    public void TestSelectPairValKv() =>
        AssertSqlKv("select _T0.KEY, _T0.VAL from PUBLIC.TBL1 as _T0", q => q.Select(x => x.Value).ToList());

    [Test]
    public void TestPrimitiveTypeMappingNotSupportedKv()
    {
        // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
        var ex = Assert.Throws<NotSupportedException>(
            () => _table.GetKeyValueView<long, string>().AsQueryable().Select(x => x.Key).ToList());

        Assert.AreEqual(
            "Primitive types are not supported in LINQ queries: System.Int64. " +
            "Use a custom type (class, record, struct) with a single field instead.",
            ex!.Message);
    }

    [Test]
    public void TestDefaultQueryableOptionsKv()
    {
        _server.LastSqlTimeoutMs = null;
        _server.LastSqlPageSize = null;

        _ = _table.GetKeyValueView<Poco, Poco>().AsQueryable()
            .Select(x => (int)x.Key.Key).ToArray();

        Assert.AreEqual(SqlStatement.DefaultTimeout.TotalMilliseconds, _server.LastSqlTimeoutMs);
        Assert.AreEqual(SqlStatement.DefaultPageSize, _server.LastSqlPageSize);
    }

    [Test]
    public void TestCustomQueryableOptionsKv()
    {
        _server.LastSqlTimeoutMs = null;
        _server.LastSqlPageSize = null;

        _ = _table.GetKeyValueView<Poco, Poco>().AsQueryable(options: new(TimeSpan.FromSeconds(25), 128))
            .Select(x => (int)x.Key.Key).ToArray();

        Assert.AreEqual(25000, _server.LastSqlTimeoutMs);
        Assert.AreEqual(128, _server.LastSqlPageSize);
    }
}
