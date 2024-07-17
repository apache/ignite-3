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
using System.Linq.Expressions;
using System.Threading.Tasks;
using Ignite.Sql;
using NUnit.Framework;
using Table;

/// <summary>
/// Linq DML tests.
/// </summary>
public partial class LinqTests
{
    private const int DmlMinKey = 1000;

    [SetUp]
    public async Task DmlSetUp()
    {
        if (IsDmlTest())
        {
            await PocoAllColumnsSqlNullableView.UpsertAllAsync(
                null,
                Enumerable.Range(1, 10).Select(x => new PocoAllColumnsSqlNullable(DmlMinKey + x)));
        }
    }

    [TearDown]
    public async Task DmlTearDown()
    {
        if (IsDmlTest())
        {
            await PocoAllColumnsSqlNullableView.DeleteAllAsync(
                null,
                Enumerable.Range(1, 10).Select(x => new PocoAllColumnsSqlNullable(DmlMinKey + x)));
        }
    }

    [Test]
    public async Task TestExecuteDelete([Values(true, false)] bool inlineCondition)
    {
        var view = PocoAllColumnsSqlNullableView;
        var query = view.AsQueryable();

        var tableSizeBefore = await query.CountAsync();

        Expression<Func<PocoAllColumnsSqlNullable, bool>> condition = x => x.Key >= DmlMinKey;

        if (!inlineCondition)
        {
            // Condition can be in a separate "Where" clause, or directly in the "ExecuteDeleteAsync".
            query = query.Where(condition);
        }

        var countBefore = await query.CountAsync(condition);

        var deleteRes = inlineCondition
            ? await query.ExecuteDeleteAsync(condition)
            : await query.ExecuteDeleteAsync();

        var countAfter = await query.CountAsync(condition);
        var tableSizeAfter = await view.AsQueryable().CountAsync();

        Assert.AreEqual(10, countBefore);
        Assert.AreEqual(10, deleteRes);
        Assert.AreEqual(0, countAfter);
        Assert.AreEqual(tableSizeBefore, tableSizeAfter + 10);
    }

    [Test]
    public async Task TestExecuteDeleteWithTx()
    {
        await using (var tx = await Client.Transactions.BeginAsync())
        {
            var rowCount = await PocoView.AsQueryable(tx).ExecuteDeleteAsync();
            Assert.Greater(rowCount, 0);

            CollectionAssert.IsEmpty(await PocoView.AsQueryable(tx).ToListAsync());
            CollectionAssert.IsNotEmpty(await PocoView.AsQueryable().ToListAsync());
        }

        CollectionAssert.IsNotEmpty(await PocoView.AsQueryable().ToListAsync());
    }

    [Test]
    public async Task TestExecuteUpdateWithConstantValue()
    {
        var query = PocoAllColumnsSqlNullableView.AsQueryable().Where(x => x.Key >= DmlMinKey);
        await query.ExecuteUpdateAsync(row => row.SetProperty(x => x.Str, "updated"));

        var res = await query.Select(x => x.Str).Distinct().ToListAsync();
        CollectionAssert.AreEqual(new[] { "updated" }, res);
    }

    [Test]
    public async Task TestExecuteUpdateWithComputedValue()
    {
        var query = PocoAllColumnsSqlNullableView.AsQueryable().Where(x => x.Key >= DmlMinKey);
        await query.ExecuteUpdateAsync(row => row.SetProperty(x => x.Str, x => "updated_" + x.Key + "_"));

        var res = await query.OrderBy(x => x.Key).Select(x => x.Str).ToListAsync();

        Assert.AreEqual("updated_1001_", res[0]);
        Assert.AreEqual("updated_1002_", res[1]);

        Assert.AreEqual(10, res.Count);
    }

    [Test]
    public async Task TestExecuteUpdateWithComputedValueFromSubquery()
    {
        var query = PocoAllColumnsSqlNullableView.AsQueryable().Where(x => x.Key >= DmlMinKey);
        var query2 = PocoView.AsQueryable();

        await query.ExecuteUpdateAsync(
            row => row.SetProperty(
                tbl1 => tbl1.Str,
                tbl1 => query2
                    .Where(tbl2 => tbl2.Key < tbl1.Key)
                    .OrderBy(tbl2 => tbl2.Key)
                    .Select(tbl2 => tbl2.Val)
                    .First()));

        var res = await query.OrderBy(x => x.Key).Select(x => x.Str).ToListAsync();

        Assert.AreEqual("v-0", res[0]);
        Assert.AreEqual("v-0", res[1]);

        Assert.AreEqual(10, res.Count);
    }

    [Test]
    public async Task TestExecuteUpdateWithMultipleSetters()
    {
        var query = PocoAllColumnsSqlNullableView.AsQueryable().Where(x => x.Key >= DmlMinKey);
        await query.ExecuteUpdateAsync(row => row
            .SetProperty(x => x.Int16, (short)16)
            .SetProperty(x => x.Int32, x => 32)
            .SetProperty(x => x.Str, x => string.Empty + x.Key + "!"));

        var res = await query.OrderBy(x => x.Key).ToListAsync();

        Assert.AreEqual(16, res[0].Int16);
        Assert.AreEqual(32, res[0].Int32);
        Assert.AreEqual("1001!", res[0].Str);

        Assert.AreEqual(16, res[1].Int16);
        Assert.AreEqual(32, res[1].Int32);
        Assert.AreEqual("1002!", res[1].Str);

        Assert.AreEqual(10, res.Count);
    }

    [Test]
    public async Task TestExecuteUpdateWithTx()
    {
        await using (var tx = await Client.Transactions.BeginAsync())
        {
            await PocoAllColumnsSqlNullableView.AsQueryable(tx)
                .Where(x => x.Key >= DmlMinKey)
                .ExecuteUpdateAsync(row => row.SetProperty(x => x.Str, x => "updated_" + x.Key + "_"));

            // Check updated result with SQL and Record APIs.
            Assert.AreEqual("updated_1001_", PocoAllColumnsSqlNullableView.AsQueryable(tx).Single(x => x.Key == 1001).Str);
            Assert.AreEqual("updated_1001_", (await PocoAllColumnsSqlNullableView.GetAsync(tx, new(1001))).Value.Str);
        }

        Assert.IsNull(PocoAllColumnsSqlNullableView.AsQueryable().Single(x => x.Key == 1001).Str);
    }

    [Test]
    public void TestExecuteDeleteWithResultOperatorsIsNotSupported()
    {
        var ex = Assert.ThrowsAsync<NotSupportedException>(() => PocoView.AsQueryable().Skip(1).Take(2).ExecuteDeleteAsync());
        Assert.AreEqual("ExecuteDeleteAsync can not be combined with result operators: Skip(1), Take(2)", ex!.Message);
    }

    [Test]
    public void TestExecuteUpdateWithResultOperatorsIsNotSupported()
    {
        var ex = Assert.ThrowsAsync<NotSupportedException>(
            () => PocoView.AsQueryable().DefaultIfEmpty().ExecuteUpdateAsync(x => x.SetProperty(p => p.Key, 2)));

        Assert.AreEqual("ExecuteUpdateAsync can not be combined with result operators: DefaultIfEmpty()", ex!.Message);
    }

    private static bool IsDmlTest() => new[] { "ExecuteDelete", "ExecuteUpdate" }
        .Any(x => TestContext.CurrentContext.Test.Name.Contains(x, StringComparison.OrdinalIgnoreCase));
}
