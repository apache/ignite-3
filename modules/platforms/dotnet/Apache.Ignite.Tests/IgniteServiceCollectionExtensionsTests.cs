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
using Microsoft.Extensions.Logging;
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
    public async Task TestRegisterSingleClient()
    {
        await ValidateRegisterSingleClient(
            services => services.AddIgniteClientGroup(CreateGroupConfig()),
            services => services.GetService<IgniteClientGroup>());

        await ValidateRegisterSingleClient(
            services => services.AddIgniteClientGroup(_ => CreateGroupConfig()),
            services => services.GetService<IgniteClientGroup>());

        const string serviceKey = "key";

        await ValidateRegisterSingleClient(
            services => services.AddIgniteClientGroupKeyed(serviceKey, CreateGroupConfig()),
            services => services.GetKeyedService<IgniteClientGroup>(serviceKey));

        await ValidateRegisterSingleClient(
            services => services.AddIgniteClientGroupKeyed(serviceKey, _ => CreateGroupConfig()),
            services => services.GetKeyedService<IgniteClientGroup>(serviceKey));

        await ValidateRegisterSingleClient(
            services => services.AddIgniteClientGroupKeyed(serviceKey, (_, _) => CreateGroupConfig()),
            services => services.GetKeyedService<IgniteClientGroup>(serviceKey));
    }

    [Test]
    public async Task TestRegisterConfigurationInstanceRoundRobin()
    {
        const int count = 3;

        await ValidateRegisterRoundRobin(
            count,
            services => services.AddIgniteClientGroup(CreateGroupConfig(count)),
            services => services.GetService<IgniteClientGroup>());

        await ValidateRegisterRoundRobin(
            count,
            services => services.AddIgniteClientGroup(_ => CreateGroupConfig(count)),
            services => services.GetService<IgniteClientGroup>());

        const string? serviceKey = "key";

        await ValidateRegisterRoundRobin(
            count,
            services => services.AddIgniteClientGroupKeyed(serviceKey, CreateGroupConfig(count)),
            services => services.GetKeyedService<IgniteClientGroup>(serviceKey));

        await ValidateRegisterRoundRobin(
            count,
            services => services.AddIgniteClientGroupKeyed(serviceKey, _ => CreateGroupConfig(count)),
            services => services.GetKeyedService<IgniteClientGroup>(serviceKey));

        await ValidateRegisterRoundRobin(
            count,
            services => services.AddIgniteClientGroupKeyed(serviceKey, (_, _) => CreateGroupConfig(count)),
            services => services.GetKeyedService<IgniteClientGroup>(serviceKey));
    }

    [Test]
    public void TestRegisterScopesConfiguration([Values]ServiceLifetime lifetime)
    {
        var services = new ServiceCollection();

        var resServices = services.AddIgniteClientGroup(CreateGroupConfig(), lifetime);
        Assert.AreSame(services, resServices);

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

        var resServices = services.AddIgniteClientGroup(_ => CreateGroupConfig(), lifetime);
        Assert.AreSame(services, resServices);

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

    private static async Task ValidateRegisterSingleClient(Action<ServiceCollection> register, Func<IServiceProvider, IgniteClientGroup?> resolve)
    {
        var services = new ServiceCollection();

        register(services);

        await using var serviceProvider = services.BuildServiceProvider();

        using var group = resolve(serviceProvider);
        using var group2 = resolve(serviceProvider);

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

    private static async Task ValidateRegisterRoundRobin(
        int count,
        Action<ServiceCollection> register,
        Func<IServiceProvider, IgniteClientGroup?> resolve)
    {
        var services = new ServiceCollection();

        register(services);

        await using var serviceProvider = services.BuildServiceProvider();

        using var group = resolve(serviceProvider);

        Assert.That(group, Is.Not.Null);

        // ReSharper disable once AccessToDisposedClosure
        var clients = await Task.WhenAll(
            Enumerable.Range(0, count + 1).Select(async _ => await group.GetIgniteAsync()));

        var uniqueClients = clients.Distinct().ToArray();

        Assert.That(uniqueClients.Length, Is.EqualTo(count));

        await clients.First().Tables.GetTablesAsync();
    }

    private static void ValidateKeyedRegistrationScope(ServiceLifetime lifetime, Action<ServiceCollection, object> register)
    {
        var services = new ServiceCollection();

        services.AddIgniteClientGroup(new IgniteClientGroupConfiguration
        {
            ClientConfiguration = new IgniteClientConfiguration
            {
                Endpoints = { "localhost" }
            },
            Size = 2
        });

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

    private IgniteClientGroupConfiguration CreateGroupConfig(int size = 1) =>
        new()
        {
            Size = size,
            ClientConfiguration = new IgniteClientConfiguration(_server.Endpoint)
            {
                LoggerFactory = TestUtils.GetConsoleLoggerFactory(LogLevel.Trace)
            }
        };
}
