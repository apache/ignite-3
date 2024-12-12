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
using Apache.Ignite.Table;
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
    public void TestEmptyKey()
    {
        IDistributedCache cache = GetCache();

        cache.Set(string.Empty, [1]);

        Assert.AreEqual(new[] { 1 }, cache.Get(string.Empty));
    }

    [Test]
    public void TestEmptyValue()
    {
        IDistributedCache cache = GetCache();

        cache.Set("k", []);

        Assert.AreEqual(Array.Empty<byte>(), cache.Get("k"));
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
        const string tableName = nameof(TestExistingTable);

        await Client.Sql.ExecuteAsync(null, $"DROP TABLE IF EXISTS {tableName}");
        await Client.Sql.ExecuteAsync(null, $"CREATE TABLE {tableName} (K VARCHAR PRIMARY KEY, V VARBINARY)");

        await Client.Sql.ExecuteAsync(null, $"INSERT INTO {tableName} (K, V) VALUES ('x', x'010203')");

        var options = new IgniteDistributedCacheOptions
        {
            TableName = tableName,
            KeyColumnName = "K",
            ValueColumnName = "V"
        };

        IDistributedCache cache = GetCache(options);

        Assert.AreEqual(new[] { 1, 2, 3 }, await cache.GetAsync("x"));
    }

    [Test]
    public async Task TestNonExistingTable()
    {
        const string tableName = nameof(TestNonExistingTable);

        await Client.Sql.ExecuteAsync(null, $"DROP TABLE IF EXISTS {tableName}");

        IDistributedCache cache = GetCache(new() { TableName = tableName });

        await cache.SetAsync("x", [1]);
        Assert.AreEqual(new[] { 1 }, await cache.GetAsync("x"));
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

        Assert.AreEqual(4, row.FieldCount);
        Assert.AreEqual("_K", row.GetName(0));
        Assert.AreEqual("_V", row.GetName(1));

        Assert.AreEqual("x", row[0]);
        Assert.AreEqual(new[] { 1 }, (byte[]?)row[1]);
    }

    [Test]
    public async Task TestKeyPrefix()
    {
        var options = new IgniteDistributedCacheOptions { CacheKeyPrefix = "prefix_" };
        IDistributedCache cache = GetCache(options);

        await cache.SetAsync("x", [255]);
        Assert.AreEqual(new byte[] { 255 }, await cache.GetAsync("x"));

        var table = await Client.Tables.GetTableAsync(options.TableName);
        var tuple = await table!.RecordBinaryView.GetAsync(null, new IgniteTuple { ["KEY"] = "prefix_x" });

        Assert.AreEqual(new byte[] { 255 }, tuple.Value["VAL"]);
    }

    [Test]
    public void TestExpirationNotSupported()
    {
        var cache = GetCache();

        Test(new() { AbsoluteExpiration = DateTimeOffset.Now });
        Test(new() { SlidingExpiration = TimeSpan.FromMinutes(1) });
        Test(new() { AbsoluteExpirationRelativeToNow = TimeSpan.FromHours(1) });

        void Test(DistributedCacheEntryOptions options)
        {
            var ex = Assert.Throws<ArgumentException>(() => cache.Set("x", [1], options));
            Assert.AreEqual("Expiration is not supported. (Parameter 'options')", ex.Message);
        }
    }

    private IDistributedCache GetCache(IgniteDistributedCacheOptions? options = null) =>
        new IgniteDistributedCache(options ?? new IgniteDistributedCacheOptions(), _clientGroup);
}
