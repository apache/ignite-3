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
using System.Linq;
using System.Threading.Tasks;
using Ignite.Sql;
using NUnit.Framework;
using Table;

/// <summary>
/// Basic LINQ provider tests.
/// </summary>
public class LinqTests : IgniteTestsBase
{
    [OneTimeSetUp]
    public async Task InsertData()
    {
        for (int i = 0; i < 10; i++)
        {
            await PocoView.UpsertAsync(null, new() { Key = i, Val = "v-" + i });
        }
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

        IResultSet<string?> resultSet = await query.ToResultSetAsync();
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
    public void TestCount()
    {
        int res = PocoView
            .AsQueryable()
            .Count(x => x.Key < 3);

        Assert.AreEqual(3, res);
    }

    [Test]
    public void TestLongCount()
    {
        long res = PocoView
            .AsQueryable()
            .LongCount(x => x.Key < 3);

        Assert.AreEqual(3, res);
    }

    [Test]
    public void TestSum()
    {
        long res = PocoView.AsQueryable()
            .Where(x => x.Key < 3)
            .Select(x => x.Key)
            .Sum();

        Assert.AreEqual(3, res);
    }

    [Test]
    public void TestOrderBySkipTake()
    {
        List<long> res = PocoView.AsQueryable()
            .OrderByDescending(x => x.Key)
            .Select(x => x.Key)
            .Skip(1)
            .Take(2)
            .ToList();

        Assert.AreEqual(new long[] { 8, 7 }, res);
    }

    [Test]
    [Ignore("IGNITE-18123 LINQ: Skip and Take (offset / limit) support")]
    public void TestOrderBySkipTakeBeforeSelect()
    {
        List<long> res = PocoView.AsQueryable()
            .OrderByDescending(x => x.Key)
            .Skip(1)
            .Take(2)
            .Select(x => x.Key)
            .ToList();

        Assert.AreEqual(new long[] { 8, 7 }, res);
    }

    [Test]
    public void TestCustomColumnNameMapping()
    {
        Assert.Fail("TODO: Test that ColumnAttribute is honored.");
    }

    [Test]
    public async Task TestTransaction()
    {
        await using var tx = await Client.Transactions.BeginAsync();
        await PocoView.UpsertAsync(tx, new Poco { Key = 5, Val = "new-val" });

        var txRes = PocoView.AsQueryable(tx).Where(x => x.Key == 5).Select(x => x.Val).AsEnumerable().Single();
        var noTxRes = PocoView.AsQueryable().Where(x => x.Key == 5).Select(x => x.Val).AsEnumerable().Single();

        Assert.AreEqual("new-val", txRes);
        Assert.AreEqual("v-5", noTxRes);
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
}
