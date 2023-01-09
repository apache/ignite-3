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

using System.Linq;
using System.Threading.Tasks;
using Ignite.Sql;
using NUnit.Framework;
using Table;

/// <summary>
/// Linq type cast tests.
/// </summary>
public partial class LinqTests
{
    [Test]
    public async Task TestRemoveAll()
    {
        var view = PocoAllColumnsSqlNullableView;
        var tableSizeBefore = await view.AsQueryable().CountAsync();

        for (int i = 0; i < 10; i++)
        {
            await view.UpsertAsync(null, new PocoAllColumnsSqlNullable(1000 + i));
        }

        var query = view.AsQueryable().Where(x => x.Key >= 1000);

        var countBefore = await query.CountAsync();
        var deleteRes = await query.RemoveAllAsync();
        var countAfter = await query.CountAsync();
        var tableSizeAfter = await view.AsQueryable().CountAsync();

        Assert.AreEqual(10, countBefore);
        Assert.AreEqual(10, deleteRes);
        Assert.AreEqual(0, countAfter);
        Assert.AreEqual(tableSizeBefore, tableSizeAfter);
    }

    [Test]
    public async Task TestRemoveAllWithTx()
    {
        await using (var tx = await Client.Transactions.BeginAsync())
        {
            var rowCount = await PocoView.AsQueryable(tx).RemoveAllAsync();
            Assert.Greater(rowCount, 0);

            CollectionAssert.IsEmpty(await PocoView.AsQueryable(tx).ToListAsync());
            CollectionAssert.IsNotEmpty(await PocoView.AsQueryable().ToListAsync());
        }

        CollectionAssert.IsNotEmpty(await PocoView.AsQueryable().ToListAsync());
    }
}
