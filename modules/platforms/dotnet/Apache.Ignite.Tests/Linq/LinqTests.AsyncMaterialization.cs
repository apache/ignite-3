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
using System.Threading.Tasks;
using Ignite.Sql;
using NUnit.Framework;

/// <summary>
/// Linq async materialization tests (retrieving results in async manner, such as CountAsync or ToListAsync).
/// </summary>
public partial class LinqTests
{
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
}
