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
using Common.Table;
using Ignite.Sql;
using NUnit.Framework;

/// <summary>
/// Linq async materialization tests (retrieving results in async manner, such as CountAsync or ToListAsync).
/// </summary>
public partial class LinqTests
{
    [Test]
    public async Task TestToListAsync()
    {
        List<Poco> res = await PocoView.AsQueryable()
            .OrderBy(x => x.Key)
            .Where(x => x.Key > 7)
            .ToListAsync();

        Assert.AreEqual(2, res.Count);
        Assert.AreEqual(8, res[0].Key);
    }

    [Test]
    public async Task TestToDictionaryAsync()
    {
        var res = await PocoView.AsQueryable()
            .OrderBy(x => x.Key)
            .Where(x => x.Key > 7)
            .ToDictionaryAsync(x => x.Key, x => x);

        Assert.AreEqual(2, res.Count);
        Assert.AreEqual("v-8", res[8].Val);
    }

    [Test]
    public async Task TestToDictionaryAsyncCustomComparer()
    {
        var res = await PocoView.AsQueryable()
            .OrderBy(x => x.Key)
            .Where(x => x.Key > 7)
            .ToDictionaryAsync(x => x.Val!, x => x, StringComparer.OrdinalIgnoreCase);

        Assert.AreEqual(2, res.Count);
        Assert.AreEqual(8, res["v-8"].Key);
        Assert.AreEqual(8, res["V-8"].Key);
    }

    [Test]
    public async Task TestAnyAsync()
    {
        Assert.IsTrue(await PocoView.AsQueryable().AnyAsync());
        Assert.IsTrue(await PocoView.AsQueryable().AnyAsync(x => x.Key > 0));

        Assert.IsFalse(await PocoView.AsQueryable().Where(x => x.Key > 1000).AnyAsync());
        Assert.IsFalse(await PocoView.AsQueryable().AnyAsync(x => x.Key > 1000));
    }

    [Test]
    public async Task TestAllAsync()
    {
        Assert.IsTrue(await PocoView.AsQueryable().AllAsync(x => x.Key < 1000));
        Assert.IsFalse(await PocoView.AsQueryable().AllAsync(x => x.Key > 1000));
    }

    [Test]
    public async Task TestCountAsync()
    {
        Assert.AreEqual(10, await PocoView.AsQueryable().CountAsync());
        Assert.AreEqual(4, await PocoView.AsQueryable().CountAsync(x => x.Key > 5));
    }

    [Test]
    public async Task TestLongCountAsync()
    {
        Assert.AreEqual(10L, await PocoView.AsQueryable().LongCountAsync());
        Assert.AreEqual(4L, await PocoView.AsQueryable().LongCountAsync(x => x.Key > 5));
    }

    [Test]
    public async Task TestFirstAsync()
    {
        var query = PocoView.AsQueryable().OrderBy(x => x.Key);

        Assert.AreEqual(0L, (await query.FirstAsync()).Key);
        Assert.AreEqual(6L, (await query.FirstAsync(x => x.Key > 5)).Key);

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => query.FirstAsync(x => x.Key > 1000));
        StringAssert.StartsWith("ResultSet is empty: ", ex!.Message);
    }

    [Test]
    public async Task TestFirstOrDefaultAsync()
    {
        var query = PocoView.AsQueryable().OrderBy(x => x.Key);

        Assert.AreEqual(0L, (await query.FirstOrDefaultAsync())!.Key);
        Assert.AreEqual(6L, (await query.FirstOrDefaultAsync(x => x.Key > 5))!.Key);
        Assert.IsNull(await query.FirstOrDefaultAsync(x => x.Key > 1000));
    }

    [Test]
    public async Task TestSingleAsync()
    {
        var query = PocoView.AsQueryable().OrderBy(x => x.Key);

        Assert.AreEqual(3L, (await query.Where(x => x.Key == 3).SingleAsync()).Key);
        Assert.AreEqual(6L, (await query.SingleAsync(x => x.Key == 6)).Key);

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => query.SingleAsync());
        StringAssert.StartsWith("ResultSet is expected to have one row, but has more: ", ex!.Message);

        var ex2 = Assert.ThrowsAsync<InvalidOperationException>(() => query.SingleAsync(x => x.Key > 1000));
        StringAssert.StartsWith("ResultSet is empty: ", ex2!.Message);
    }

    [Test]
    public async Task TestSingleOrDefaultAsync()
    {
        var query = PocoView.AsQueryable().OrderBy(x => x.Key);

        Assert.AreEqual(3L, (await query.Where(x => x.Key == 3).SingleOrDefaultAsync())!.Key);
        Assert.AreEqual(6L, (await query.SingleOrDefaultAsync(x => x.Key == 6))!.Key);
        Assert.IsNull(await query.SingleOrDefaultAsync(x => x.Key > 1000));

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => query.SingleOrDefaultAsync());
        StringAssert.StartsWith("ResultSet is expected to have one row, but has more: ", ex!.Message);
    }

    [Test]
    public async Task TestMinAsync()
    {
        var query = PocoView.AsQueryable();

        Assert.AreEqual(0L, await query.Select(x => x.Key).MinAsync());
        Assert.AreEqual(-9L, await query.MinAsync(x => -x.Key));
    }

    [Test]
    public void TestMinAsyncWithEmptySubqueryThrowsNoElements()
    {
        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => PocoIntView.AsQueryable().Where(x => x.Key > 1000).MinAsync(x => x.Val));

        var ex2 = Assert.ThrowsAsync<InvalidOperationException>(
            () => PocoIntView.AsQueryable().Where(x => x.Key > 1000).Select(x => x.Key).MinAsync());

        Assert.AreEqual("Sequence contains no elements", ex!.Message);
        Assert.AreEqual("Sequence contains no elements", ex2!.Message);
    }

    [Test]
    public async Task TestMaxAsync()
    {
        var query = PocoView.AsQueryable();

        Assert.AreEqual(9L, await query.Select(x => x.Key).MaxAsync());
        Assert.AreEqual(19L, await query.MaxAsync(x => x.Key + 10));
    }

    [Test]
    public void TestMaxAsyncWithEmptySubqueryThrowsNoElements()
    {
        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => PocoIntView.AsQueryable().Where(x => x.Key > 1000).MaxAsync(x => x.Val));

        var ex2 = Assert.ThrowsAsync<InvalidOperationException>(
            () => PocoIntView.AsQueryable().Where(x => x.Key > 1000).Select(x => x.Key).MaxAsync());

        Assert.AreEqual("Sequence contains no elements", ex!.Message);
        Assert.AreEqual("Sequence contains no elements", ex2!.Message);
    }

    [Test]
    public async Task TestSumAsync()
    {
        Assert.AreEqual(45, await PocoIntView.AsQueryable().Select(x => x.Key).SumAsync());
        Assert.AreEqual(145, await PocoIntView.AsQueryable().SumAsync(x => x.Key + 10));

        Assert.AreEqual(45, await PocoLongView.AsQueryable().Select(x => x.Key).SumAsync());
        Assert.AreEqual(145, await PocoLongView.AsQueryable().SumAsync(x => x.Key + 10));

        Assert.AreEqual(45, await PocoFloatView.AsQueryable().Select(x => x.Key).SumAsync());
        Assert.AreEqual(145, await PocoFloatView.AsQueryable().SumAsync(x => x.Key + 10));

        Assert.AreEqual(45, await PocoDoubleView.AsQueryable().Select(x => x.Key).SumAsync());
        Assert.AreEqual(145, await PocoDoubleView.AsQueryable().SumAsync(x => x.Key + 10));

        Assert.AreEqual(45, await PocoDecimalView.AsQueryable().Select(x => x.Key).SumAsync());
        Assert.AreEqual(45, await PocoDecimalView.AsQueryable().SumAsync(x => x.Key));
    }

    [Test]
    public async Task TestSumAsyncNullable()
    {
        var query = PocoAllColumnsSqlNullableView.AsQueryable();

        Assert.AreEqual(75, await query.Select(x => x.Int32).SumAsync());
        Assert.AreEqual(175, await query.SumAsync(x => x.Int32 + 10));
        Assert.AreEqual(0, await query.Where(x => x.Key < -100).SumAsync(x => x.Int32));
        Assert.AreEqual(0, await query.Where(x => x.Key < -100).Select(x => x.Int32).SumAsync());

        Assert.AreEqual(85L, await query.Select(x => x.Int64).SumAsync());
        Assert.AreEqual(185L, await query.SumAsync(x => x.Int64 + 10));
        Assert.AreEqual(0, await query.Where(x => x.Key < -100).SumAsync(x => x.Int64));
        Assert.AreEqual(0, await query.Where(x => x.Key < -100).Select(x => x.Int64).SumAsync());

        Assert.AreEqual(100f, await query.Select(x => x.Float).SumAsync());
        Assert.AreEqual(200f, await query.SumAsync(x => x.Float + 10));
        Assert.AreEqual(0, await query.Where(x => x.Key < -100).SumAsync(x => x.Float));
        Assert.AreEqual(0, await query.Where(x => x.Key < -100).Select(x => x.Float).SumAsync());

        Assert.AreEqual(110d, await query.Select(x => x.Double).SumAsync());
        Assert.AreEqual(210d, await query.SumAsync(x => x.Double + 10));
        Assert.AreEqual(0, await query.Where(x => x.Key < -100).SumAsync(x => x.Double));
        Assert.AreEqual(0, await query.Where(x => x.Key < -100).Select(x => x.Double).SumAsync());

        Assert.AreEqual(122m, await query.Select(x => x.Decimal).SumAsync());
        Assert.AreEqual(122m, await query.SumAsync(x => x.Decimal));
        Assert.AreEqual(0, await query.Where(x => x.Key < -100).SumAsync(x => x.Decimal));
        Assert.AreEqual(0, await query.Where(x => x.Key < -100).Select(x => x.Decimal).SumAsync());
    }

    [Test]
    public async Task TestSumAsyncWithEmptySubqueryReturnsZero()
    {
        Assert.AreEqual(0, await PocoIntView.AsQueryable().Where(x => x.Key < -100).SumAsync(x => x.Val));
        Assert.AreEqual(0, await PocoIntView.AsQueryable().Where(x => x.Key < -100).Select(x => x.Val).SumAsync());
    }

    [Test]
    public async Task TestAverageAsync()
    {
        Assert.AreEqual(4.5d, await PocoIntView.AsQueryable().Select(x => x.Key).AverageAsync());
        Assert.AreEqual(14.5d, await PocoIntView.AsQueryable().AverageAsync(x => x.Key + 10));

        Assert.AreEqual(4.5d, await PocoLongView.AsQueryable().Select(x => x.Key).AverageAsync());
        Assert.AreEqual(14.5d, await PocoLongView.AsQueryable().AverageAsync(x => x.Key + 10));

        Assert.AreEqual(4.5d, await PocoDoubleView.AsQueryable().Select(x => x.Key).AverageAsync());
        Assert.AreEqual(14.5d, await PocoDoubleView.AsQueryable().AverageAsync(x => x.Key + 10));

        Assert.AreEqual(4.5f, await PocoFloatView.AsQueryable().Select(x => x.Key).AverageAsync());
        Assert.AreEqual(14.5f, await PocoFloatView.AsQueryable().AverageAsync(x => x.Key + 10));

        Assert.AreEqual(4.5m, await PocoDecimalView.AsQueryable().Select(x => x.Key).AverageAsync());
        Assert.AreEqual(4.5m, await PocoDecimalView.AsQueryable().AverageAsync(x => x.Key));
    }

    [Test]
    public async Task TestAverageAsyncNullable()
    {
        var query = PocoAllColumnsSqlNullableView.AsQueryable();

        Assert.AreEqual(7.5d, await query.Select(x => x.Int32).AverageAsync());
        Assert.AreEqual(17.5d, await query.AverageAsync(x => x.Int32 + 10));

        Assert.AreEqual(8.5d, await query.Select(x => x.Int64).AverageAsync());
        Assert.AreEqual(18.5d, await query.AverageAsync(x => x.Int64 + 10));

        Assert.AreEqual(11.0d, await query.Select(x => x.Double).AverageAsync());
        Assert.AreEqual(21.0d, await query.AverageAsync(x => x.Double + 10));

        Assert.AreEqual(10.0f, await query.Select(x => x.Float).AverageAsync());
        Assert.AreEqual(20.0f, await query.AverageAsync(x => x.Float + 10));

        Assert.AreEqual(12.2m, await query.Select(x => x.Decimal).AverageAsync());
        Assert.AreEqual(12.2m, await query.AverageAsync(x => x.Decimal));
    }

    [Test]
    public void TestAverageAsyncWithEmptySubqueryThrowsNoElements()
    {
        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => PocoIntView.AsQueryable().Where(x => x.Key > 1000).AverageAsync(x => x.Val));

        Assert.AreEqual("Sequence contains no elements", ex!.Message);
    }

    [Test]
    public void TestDecimalMaterialization()
    {
        var query = PocoBigDecimalView.AsQueryable();

        var key = new BigDecimal(6);

        BigDecimal? primitive = query
            .Where(x => x.Key == key)
            .Select(x => x.Val)
            .Single();

        PocoBigDecimal poco = query.Single(x => x.Key == key);

        Assert.AreEqual(key, primitive);
        Assert.AreEqual(key, poco.Key);
        Assert.AreEqual(key, poco.Val);
    }
}
