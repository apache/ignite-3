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
/// Tests for <see cref="IgniteClientPool"/>.
/// </summary>
public class IgniteClientPoolTests
{
    private FakeServer _server;

    [SetUp]
    public void StartServer() => _server = new FakeServer();

    [TearDown]
    public void StopServer() => _server.Dispose();

    [Test]
    public async Task TestGetClient()
    {
        using IgniteClientPool pool = CreatePool();
        IIgniteClient client = await pool.GetClientAsync();
        IIgniteClient client2 = await pool.GetClientAsync();

        Assert.IsNotNull(client);
        Assert.AreSame(client, client2);

        await client.Tables.GetTablesAsync();
    }

    [Test]
    public async Task TestRoundRobin()
    {
        using IgniteClientPool pool = CreatePool(size: 3);

        var client1 = await pool.GetClientAsync();
        var client2 = await pool.GetClientAsync();
        var client3 = await pool.GetClientAsync();

        Assert.AreNotSame(client1, client2);
        Assert.AreNotSame(client2, client3);

        Assert.AreSame(client1, await pool.GetClientAsync());
        Assert.AreSame(client2, await pool.GetClientAsync());
        Assert.AreSame(client3, await pool.GetClientAsync());

        Assert.AreSame(client1, await pool.GetClientAsync());
        Assert.AreSame(client2, await pool.GetClientAsync());
        Assert.AreSame(client3, await pool.GetClientAsync());
    }

    [Test]
    public async Task TestPoolReconnectsDisposedClient()
    {
        using IgniteClientPool pool = CreatePool();
        IIgniteClient client = await pool.GetClientAsync();

        await client.Tables.GetTablesAsync();
        client.Dispose();

        IIgniteClient client2 = await pool.GetClientAsync();
        await client2.Tables.GetTablesAsync();

        Assert.AreNotSame(client, client2);
    }

    [Test]
    public void TestConstructorValidatesArgs()
    {
        // ReSharper disable once ObjectCreationAsStatement
        Assert.Throws<ArgumentNullException>(() => new IgniteClientPool(null!));
    }

    [Test]
    public async Task TestUseAfterDispose()
    {
        IgniteClientPool pool = CreatePool(size: 2);

        var client1 = await pool.GetClientAsync();
        var client2 = await pool.GetClientAsync();

        Assert.AreNotSame(client1, client2);

        pool.Dispose();

        // Pool and clients are disposed, all operations should throw.
        Assert.ThrowsAsync<ObjectDisposedException>(async () => await pool.GetClientAsync());
        Assert.ThrowsAsync<ObjectDisposedException>(async () => await client1.Tables.GetTablesAsync());
        Assert.ThrowsAsync<ObjectDisposedException>(async () => await client2.Tables.GetTablesAsync());
    }

    [Test]
    public async Task TestToString()
    {
        var pool = CreatePool(5);

        await pool.GetClientAsync();
        await pool.GetClientAsync();

        Assert.AreEqual("IgniteClientPool { Connected = 2, Size = 5 }", pool.ToString());
    }

    private IgniteClientPool CreatePool(int size = 1) =>
        new IgniteClientPool(
            new IgniteClientPoolConfiguration
            {
                Size = size,
                ClientConfiguration = new IgniteClientConfiguration(_server.Endpoint)
            });
}
