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
using Internal;
using NUnit.Framework;

/// <summary>
/// Tests for <see cref="IgniteClientGroup"/>.
/// </summary>
public class IgniteClientGroupTests
{
    private FakeServer _server;

    [SetUp]
    public void StartServer() => _server = new FakeServer
    {
        AllowMultipleConnections = true
    };

    [TearDown]
    public void StopServer() => _server.Dispose();

    [Test]
    public async Task TestGetClient()
    {
        using var group = new IgniteClientGroup(
            new IgniteClientGroupConfiguration { ClientConfiguration = new(_server.Endpoint) });

        IIgnite client = await group.GetIgniteAsync();
        IIgnite client2 = await group.GetIgniteAsync();

        Assert.IsNotNull(client);
        Assert.AreSame(client, client2);

        await client.Tables.GetTablesAsync();
    }

    [Test]
    public async Task TestRoundRobin()
    {
        using IgniteClientGroup group = CreateGroup(size: 3);

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
    public async Task TestGroupReconnectsDisposedClient()
    {
        using IgniteClientGroup group = CreateGroup();
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
        IgniteClientGroup group = CreateGroup(size: 2);

        var client1 = await group.GetIgniteAsync();
        var client2 = await group.GetIgniteAsync();

        Assert.AreNotSame(client1, client2);

        group.Dispose();

        // Group and clients are disposed, all operations should throw.
        Assert.IsTrue(group.IsDisposed);

        Assert.ThrowsAsync<ObjectDisposedException>(async () => await group.GetIgniteAsync());
        Assert.ThrowsAsync<ObjectDisposedException>(async () => await client1.Tables.GetTablesAsync());
        Assert.ThrowsAsync<ObjectDisposedException>(async () => await client2.Tables.GetTablesAsync());
    }

    [Test]
    public async Task TestToString()
    {
        var group = CreateGroup(5);

        await group.GetIgniteAsync();
        await group.GetIgniteAsync();

        Assert.AreEqual("IgniteClientGroup { Connected = 2, Size = 5 }", group.ToString());
    }

    [Test]
    public void TestConfigurationCantBeChanged()
    {
        IgniteClientGroup group = CreateGroup(3);

        var configuration = group.Configuration;
        configuration.Size = 100;

        Assert.AreEqual(3, group.Configuration.Size);
        Assert.AreNotSame(configuration, group.Configuration);
    }

    [Test]
    public async Task TestClientsShareObservableTimestamp()
    {
        using IgniteClientGroup group = CreateGroup(size: 2);

        var client1 = (IgniteClientInternal)await group.GetIgniteAsync();
        var client2 = (IgniteClientInternal)await group.GetIgniteAsync();

        Assert.AreNotSame(client1, client2);
        Assert.AreSame(client1.Socket.Configuration.ObservableTimestamp, client2.Socket.Configuration.ObservableTimestamp);
    }

    private IgniteClientGroup CreateGroup(int size = 1) =>
        new IgniteClientGroup(
            new IgniteClientGroupConfiguration
            {
                Size = size,
                ClientConfiguration = new IgniteClientConfiguration(_server.Endpoint)
            });
}
