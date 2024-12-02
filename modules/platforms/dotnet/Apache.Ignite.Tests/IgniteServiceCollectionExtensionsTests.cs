// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

namespace Apache.Ignite.Tests;

using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;

/// <summary>
/// Tests for <see cref="IgniteServiceCollectionExtensionsTests"/>.
/// </summary>
public class IgniteServiceCollectionExtensionsTests
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
    public async Task TestRegisterConfigurationInstance()
    {
        var services = new ServiceCollection();
        services.AddIgniteClientGroup(CreateGroupConfig());

        var serviceProvider = services.BuildServiceProvider();

        var group = serviceProvider.GetService<IgniteClientGroup>();
        var group2 = serviceProvider.GetService<IgniteClientGroup>();

        Assert.That(group, Is.Not.Null);
        Assert.That(group2, Is.Not.Null);
        Assert.AreSame(group, group2);

        IIgnite client = await group.GetIgniteAsync();
        IIgnite client2 = await group.GetIgniteAsync();
        IIgnite client3 = await group2.GetIgniteAsync();

        Assert.That(client, Is.Not.Null);
        Assert.AreSame(client, client2);
        Assert.AreSame(client, client2);
        Assert.AreSame(client, client3);

        await client.Tables.GetTablesAsync();
    }

    [Test]
    public async Task TestRegisterConfigurationInstanceRoundRobin()
    {
        var services = new ServiceCollection();
        const int count = 3;
        services.AddIgniteClientGroup(CreateGroupConfig(size: count));

        var serviceProvider = services.BuildServiceProvider();

        var group = serviceProvider.GetService<IgniteClientGroup>();

        Assert.That(group, Is.Not.Null);

        await ValidateUniqueClientCount(group, count);

        await (await group.GetIgniteAsync()).Tables.GetTablesAsync();
    }

    [Test]
    public async Task TestRegisterConfigurationRoundRobinFunc()
    {
        var services = new ServiceCollection();
        const int count = 3;
        services.AddIgniteClientGroup(_ => CreateGroupConfig(size: count));

        var serviceProvider = services.BuildServiceProvider();

        var group = serviceProvider.GetService<IgniteClientGroup>();

        Assert.That(group, Is.Not.Null);

        await ValidateUniqueClientCount(group, count);

        await (await group.GetIgniteAsync()).Tables.GetTablesAsync();
    }

    [Test]
    public void TestRegisterScopesConfiguration([Values]ServiceLifetime lifetime)
    {
        var services = new ServiceCollection();

        services.AddIgniteClientGroup(CreateGroupConfig(), lifetime);

        var servicesDescriptors = services
            .Where(sd => sd.ServiceType == typeof(IgniteClientGroup))
            .ToList();

        Assert.AreEqual(1, servicesDescriptors.Count);
        Assert.AreEqual(lifetime, servicesDescriptors.First().Lifetime);
    }

    [Test]
    public void TestRegisterScopesConfigurationFunc([Values]ServiceLifetime lifetime)
    {
        var services = new ServiceCollection();

        services.AddIgniteClientGroup(_ => CreateGroupConfig(), lifetime);

        var servicesDescriptors = services
            .Where(sd => sd.ServiceType == typeof(IgniteClientGroup))
            .ToList();

        Assert.AreEqual(1, servicesDescriptors.Count);
        Assert.AreEqual(lifetime, servicesDescriptors.First().Lifetime);
    }

    [Test]
    public void TestRegisterScopesConfigurationKeyed([Values] ServiceLifetime lifetime)
    {
        ValidateKeyedRegistrationScope(
            lifetime,
            (s, key) => s.AddIgniteClientGroupKeyed(key, CreateGroupConfig(), lifetime));
    }

    [Test]
    public void TestRegisterScopesConfigurationFuncKeyed([Values] ServiceLifetime lifetime)
    {
        ValidateKeyedRegistrationScope(
            lifetime,
            (s, key) => s.AddIgniteClientGroupKeyed(key, _ => CreateGroupConfig(), lifetime));
    }

    [Test]
    public void TestRegisterScopesConfigurationFuncWithKeyKeyed([Values] ServiceLifetime lifetime)
    {
        ValidateKeyedRegistrationScope(
            lifetime,
            (s, key) => s.AddIgniteClientGroupKeyed(key, (_, _) => CreateGroupConfig(), lifetime));
    }

    private static void ValidateKeyedRegistrationScope(ServiceLifetime lifetime, Action<ServiceCollection, object> register)
    {
        var services = new ServiceCollection();
        var keys = new[] { "key1", "key2" };

        foreach (var key in keys)
        {
            register(services, key);
        }

        var servicesDescriptors = services
            .Where(sd => sd.ServiceType == typeof(IgniteClientGroup))
            .ToList();

        var actualKeys = servicesDescriptors.Select(s => s.ServiceKey).ToArray();

        Assert.AreEqual(keys.Length, servicesDescriptors.Count);
        foreach (var key in keys)
        {
            CollectionAssert.Contains(actualKeys, key);
        }

        Assert.AreEqual(1, servicesDescriptors.Select(sd => sd.Lifetime).Distinct().Count());
        Assert.AreEqual(lifetime, servicesDescriptors.First().Lifetime);
    }

    private static async Task<IIgnite[]> ValidateUniqueClientCount(IgniteClientGroup group, int count)
    {
        var clients = await Task.WhenAll(
            Enumerable.Range(0, count + 1).Select(async _ => await group.GetIgniteAsync()));

        var uniqueClients = clients.Distinct().ToArray();

        Assert.That(uniqueClients.Length, Is.EqualTo(count));

        return uniqueClients;
    }

    private IgniteClientGroupConfiguration CreateGroupConfig(int size = 1) =>
        new()
        {
            Size = size,
            ClientConfiguration = new IgniteClientConfiguration(_server.Endpoint)
        };
}
