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
    [SetUp]
    public async Task DmlSetUp()
    {
        if (IsDmlTest())
        {
            await PocoAllColumnsSqlNullableView.UpsertAllAsync(
                null,
                Enumerable.Range(1, 10).Select(x => new PocoAllColumnsSqlNullable(1000 + x)));
        }
    }

    [TearDown]
    public async Task DmlTearDown()
    {
        if (IsDmlTest())
        {
            await PocoAllColumnsSqlNullableView.DeleteAllAsync(
                null,
                Enumerable.Range(1, 10).Select(x => new PocoAllColumnsSqlNullable(1000 + x)));
        }
    }

    [Test]
    public async Task TestRemoveAll([Values(true, false)] bool inlineCondition)
    {
        var view = PocoAllColumnsSqlNullableView;
        var query = view.AsQueryable();

        var tableSizeBefore = await view.AsQueryable().CountAsync();

        Expression<Func<PocoAllColumnsSqlNullable, bool>> condition = x => x.Key >= 1000;

        if (!inlineCondition)
        {
            // Condition can be in a separate "Where" clause, or directly in the "RemoveAllAsync".
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
    public async Task TestRemoveAllWithTx()
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
    public async Task TestUpdateAllConstantValue()
    {
        var query = PocoAllColumnsSqlNullableView.AsQueryable().Where(x => x.Key >= 1000);
        await query.ExecuteUpdateAsync(row => row.SetProperty(x => x.Str, "updated"));

        var res = await query.Select(x => x.Str).Distinct().ToListAsync();
        CollectionAssert.AreEqual(new[] { "updated" }, res);
    }

    [Test]
    public async Task TestUpdateAllComputedValue()
    {
        var query = PocoAllColumnsSqlNullableView.AsQueryable().Where(x => x.Key >= 1000);
        await query.ExecuteUpdateAsync(row => row.SetProperty(x => x.Str, x => "updated_" + x.Key + "_"));

        var res = await query.OrderBy(x => x.Key).Select(x => x.Str).ToListAsync();

        Assert.AreEqual("updated_1001_", res[0]);
        Assert.AreEqual("updated_1002_", res[1]);

        Assert.AreEqual(10, res.Count);
    }

    [Test]
    public async Task TestUpdateAllMultipleSetters()
    {
        var query = PocoAllColumnsSqlNullableView.AsQueryable().Where(x => x.Key >= 1000);
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
    public async Task TestUpdateAllWithTx()
    {
        await using (var tx = await Client.Transactions.BeginAsync())
        {
            await PocoAllColumnsSqlNullableView.AsQueryable(tx)
                .Where(x => x.Key >= 1000)
                .ExecuteUpdateAsync(row => row.SetProperty(x => x.Str, x => "updated_" + x.Key + "_"));

            Assert.AreEqual("updated_1001_", PocoAllColumnsSqlNullableView.AsQueryable(tx).Single(x => x.Key == 1001).Str);
        }

        Assert.IsNull(PocoAllColumnsSqlNullableView.AsQueryable().Single(x => x.Key == 1001).Str);
    }

    [Test]
    public void TestRemoveAllWithResultOperatorsIsNotSupported()
    {
        var ex = Assert.ThrowsAsync<NotSupportedException>(() => PocoView.AsQueryable().Skip(1).Take(2).ExecuteDeleteAsync());
        Assert.AreEqual("ExecuteDeleteAsync can not be combined with result operators: Skip(1), Take(2)", ex!.Message);
    }

    [Test]
    public void TestUpdateAllWithResultOperatorsIsNotSupported()
    {
        var ex = Assert.ThrowsAsync<NotSupportedException>(
            () => PocoView.AsQueryable().DefaultIfEmpty().ExecuteUpdateAsync(x => x.SetProperty(p => p.Key, 2)));

        Assert.AreEqual("ExecuteUpdateAsync can not be combined with result operators: DefaultIfEmpty()", ex!.Message);
    }

    private static bool IsDmlTest()
    {
        var testName = TestContext.CurrentContext.Test.Name;

        return testName.Contains("UpdateAll", StringComparison.Ordinal) || testName.Contains("RemoveAll", StringComparison.Ordinal);
    }
}
