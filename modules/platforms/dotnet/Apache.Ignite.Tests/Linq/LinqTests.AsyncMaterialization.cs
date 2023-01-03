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
        // TODO: All supported types.
        var query = PocoIntView.AsQueryable();

        Assert.AreEqual(45, await query.Select(x => x.Key).SumAsync());
        Assert.AreEqual(145, await query.SumAsync(x => x.Key + 10));
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
        // TODO: All supported types.
        var query = PocoIntView.AsQueryable();

        Assert.AreEqual(4.0d, await query.Select(x => x.Key).AverageAsync());
        Assert.AreEqual(14.0d, await query.AverageAsync(x => x.Key + 10));
    }

    [Test]
    public void TestAverageAsyncWithEmptySubqueryThrowsNoElements()
    {
        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => PocoIntView.AsQueryable().Where(x => x.Key > 1000).AverageAsync(x => x.Val));

        Assert.AreEqual("Sequence contains no elements", ex!.Message);
    }
}
