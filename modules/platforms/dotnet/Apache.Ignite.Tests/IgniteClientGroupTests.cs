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

namespace Apache.Ignite.Tests;

using System;
using System.Threading.Tasks;
using NUnit.Framework;

/// <summary>
/// Tests for <see cref="IgniteClientGroup"/>.
/// </summary>
public class IgniteClientGroupTests
{
    private FakeServer _server;

    [SetUp]
    public void StartServer() => _server = new FakeServer();

    [TearDown]
    public void StopServer() => _server.Dispose();

    [Test]
    public async Task TestGetClient()
    {
        using IgniteClientGroup group = CreatePool();
        IIgnite client = await group.GetIgniteAsync();
        IIgnite client2 = await group.GetIgniteAsync();

        Assert.IsNotNull(client);
        Assert.AreSame(client, client2);

        await client.Tables.GetTablesAsync();
    }

    [Test]
    public async Task TestRoundRobin()
    {
        using IgniteClientGroup group = CreatePool(size: 3);

        var client1 = await group.GetIgniteAsync();
        var client2 = await group.GetIgniteAsync();
        var client3 = await group.GetIgniteAsync();

        Assert.AreNotSame(client1, client2);
        Assert.AreNotSame(client2, client3);

        Assert.AreSame(client1, await group.GetIgniteAsync());
        Assert.AreSame(client2, await group.GetIgniteAsync());
        Assert.AreSame(client3, await group.GetIgniteAsync());

        Assert.AreSame(client1, await group.GetIgniteAsync());
        Assert.AreSame(client2, await group.GetIgniteAsync());
        Assert.AreSame(client3, await group.GetIgniteAsync());
    }

    [Test]
    public async Task TestPoolReconnectsDisposedClient()
    {
        using IgniteClientGroup group = CreatePool();
        IIgnite client = await group.GetIgniteAsync();

        await client.Tables.GetTablesAsync();
        ((IDisposable)client).Dispose();

        IIgnite client2 = await group.GetIgniteAsync();
        await client2.Tables.GetTablesAsync();

        Assert.AreNotSame(client, client2);
    }

    [Test]
    public void TestConstructorValidatesArgs()
    {
        // ReSharper disable once ObjectCreationAsStatement
        Assert.Throws<ArgumentNullException>(() => new IgniteClientGroup(null!));
    }

    [Test]
    public async Task TestUseAfterDispose()
    {
        IgniteClientGroup group = CreatePool(size: 2);

        var client1 = await group.GetIgniteAsync();
        var client2 = await group.GetIgniteAsync();

        Assert.AreNotSame(client1, client2);

        group.Dispose();

        // Pool and clients are disposed, all operations should throw.
        Assert.ThrowsAsync<ObjectDisposedException>(async () => await group.GetIgniteAsync());
        Assert.ThrowsAsync<ObjectDisposedException>(async () => await client1.Tables.GetTablesAsync());
        Assert.ThrowsAsync<ObjectDisposedException>(async () => await client2.Tables.GetTablesAsync());
    }

    [Test]
    public async Task TestToString()
    {
        var pool = CreatePool(5);

        await pool.GetIgniteAsync();
        await pool.GetIgniteAsync();

        Assert.AreEqual("IgniteClientPool { Connected = 2, Size = 5 }", pool.ToString());
    }

    private IgniteClientGroup CreatePool(int size = 1) =>
        new IgniteClientGroup(
            new IgniteClientGroupConfiguration
            {
                Size = size,
                ClientConfiguration = new IgniteClientConfiguration(_server.Endpoint)
            });
}
