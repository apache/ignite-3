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

namespace Apache.Extensions.Cache.Ignite.Tests;

using Apache.Ignite;
using Apache.Ignite.Tests;
using Caching.Ignite;
using Microsoft.Extensions.Caching.Distributed;

/// <summary>
/// Tests for <see cref="IgniteDistributedCache"/>.
/// </summary>
public class IgniteDistributedCacheTests : IgniteTestsBase
{
    private IgniteClientGroup _clientGroup = null!;

    [OneTimeSetUp]
    public void InitClientGroup() =>
        _clientGroup = new IgniteClientGroup(new IgniteClientGroupConfiguration { ClientConfiguration = GetConfig() });

    [OneTimeTearDown]
    public void StopClientGroup() =>
        _clientGroup.Dispose();

    [Test]
    public async Task TestSetGetRemove()
    {
        const string key = nameof(TestSetGetRemove);
        byte[] value = [1, 2, 3];

        IDistributedCache cache = GetCache();

        // No value.
        Assert.IsNull(await cache.GetAsync(key));

        // Set and get.
        await cache.SetAsync(key, value, new());
        CollectionAssert.AreEqual(value, await cache.GetAsync(key));

        // Remove and get.
        await cache.RemoveAsync(key);
        Assert.IsNull(await cache.GetAsync(key));
    }

    [Test]
    public void TestCaseSensitiveKeys()
    {
        var key1 = nameof(TestCaseSensitiveKeys);
        var key2 = key1.ToUpperInvariant();

        byte[] val1 = [1];
        byte[] val2 = [2];

        IDistributedCache cache = GetCache();

        cache.Set(key1, val1);
        cache.Set(key2, val2);

        Assert.AreEqual(val1, cache.Get(key1));
    }

    [Test]
    public void TestSetOverwritesExistingValue()
    {
        const string key = nameof(TestSetOverwritesExistingValue);
        byte[] val1 = [1, 2, 3];
        byte[] val2 = [4, 5, 6];

        IDistributedCache cache = GetCache();

        cache.Set(key, val1);
        Assert.AreEqual(val1, cache.Get(key));

        cache.Set(key, val2);
        Assert.AreEqual(val2, cache.Get(key));
    }

    [Test]
    public void TestNullKeyOrValueNotAllowed()
    {
        IDistributedCache cache = GetCache();

        Assert.Throws<ArgumentNullException>(() => cache.Get(null!));
        Assert.Throws<ArgumentNullException>(() => cache.Set(null!, [1]));
        Assert.Throws<ArgumentNullException>(() => cache.Set("k", null!));

        Assert.ThrowsAsync<ArgumentNullException>(async () => await cache.GetAsync(null!));
        Assert.ThrowsAsync<ArgumentNullException>(async () => await cache.SetAsync(null!, [1]));
        Assert.ThrowsAsync<ArgumentNullException>(async () => await cache.SetAsync("k", null!));
    }

    [Test]
    public async Task TestEmptyKey()
    {
        await Task.Delay(1);
    }

    [Test]
    public async Task TestEmptyValue()
    {
        await Task.Delay(1);
    }

    [Test]
    public async Task TestRemoveTableBreaksCaching()
    {
        var cacheOptions = new IgniteDistributedCacheOptions();
        IDistributedCache cache = GetCache(cacheOptions);

        await cache.SetAsync("x", [1], new(), CancellationToken.None);

        await Client.Sql.ExecuteAsync(null, $"DROP TABLE {cacheOptions.TableName}");

        TableNotFoundException? ex = Assert.ThrowsAsync<TableNotFoundException>(
            async () => await cache.GetAsync("x", CancellationToken.None));

        StringAssert.StartsWith("Table does not exist or was dropped concurrently", ex.Message);
    }

    [Test]
    public async Task TestExistingTable()
    {
        await Task.Delay(1);
    }

    [Test]
    public async Task TestNonExistingTable()
    {
        await Task.Delay(1);
    }

    [Test]
    public async Task TestCustomTableAndColumnNames()
    {
        var cacheOptions = new IgniteDistributedCacheOptions
        {
            TableName = nameof(TestCustomTableAndColumnNames),
            KeyColumnName = "_K",
            ValueColumnName = "_V"
        };

        IDistributedCache cache = GetCache(cacheOptions);

        await cache.SetAsync("x", [1]);
        CollectionAssert.AreEqual(new[] { 1 }, await cache.GetAsync("x"));

        await using var resultSet = await Client.Sql.ExecuteAsync(null, $"SELECT * FROM {cacheOptions.TableName}");
        var rows = await resultSet.ToListAsync();
        var row = rows.Single();

        Assert.AreEqual(2, row.FieldCount);
        Assert.AreEqual("_K", row.GetName(0));
        Assert.AreEqual("_V", row.GetName(1));

        Assert.AreEqual("x", row[0]);
        Assert.AreEqual(new[] { 1 }, (byte[]?)row[1]);
    }

    [Test]
    public async Task TestKeyPrefix()
    {
        await Task.Delay(1);
    }

    private IDistributedCache GetCache(IgniteDistributedCacheOptions? options = null) =>
        new IgniteDistributedCache(options ?? new IgniteDistributedCacheOptions(), _clientGroup);
}
