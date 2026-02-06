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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Common;
using Microsoft.Extensions.Logging;
using Network;
using NUnit.Framework;

/// <summary>
/// Tests DNS resolution behavior.
/// </summary>
public class DnsResolveTests
{
    private const string HostName = "fake-host";

    private const int Port = 10902;

    private ConsoleLogger _logger;

    private FakeServerGroup _servers;

    private ConcurrentDictionary<string, string[]> _dnsMap;

    [SetUp]
    public void SetUp()
    {
        _logger = new ConsoleLogger(LogLevel.Trace);

        _servers = FakeServerGroup.Create(
            count: 6,
            x => new FakeServer(nodeName: "fake-node-" + x, address: IPAddress.Parse("127.0.0.1" + x), port: Port));

        _dnsMap = new ConcurrentDictionary<string, string[]>
        {
            [HostName] = ["127.0.0.10", "127.0.0.11"]
        };
    }

    [TearDown]
    public void TearDown()
    {
        _servers.Dispose();
        _logger.Flush();
    }

    [Test]
    public async Task TestClientResolvesHostNamesToAllIps()
    {
        var cfg = new IgniteClientConfiguration($"{HostName}:{Port}")
        {
            LoggerFactory = _logger
        };

        using var client = await IgniteClient.StartInternalAsync(cfg, new TestDnsResolver(_dnsMap));
        client.WaitForConnections(2);

        var conns = client.GetConnections().OrderBy(x => x.Node.Name).ToList();

        Assert.AreEqual("127.0.0.10:10902", conns[0].Node.Address.ToString());
        Assert.AreEqual("fake-node-0", conns[0].Node.Name);

        Assert.AreEqual("127.0.0.11:10902", conns[1].Node.Address.ToString());
        Assert.AreEqual("fake-node-1", conns[1].Node.Name);
    }

    [Test]
    public async Task TestClientReResolvesHostNamesOnDisconnect()
    {
        var cfg = new IgniteClientConfiguration($"{HostName}:{Port}")
        {
            ReResolveAddressesInterval = Timeout.InfiniteTimeSpan,
            LoggerFactory = _logger
        };

        using var client = await IgniteClient.StartInternalAsync(cfg, new TestDnsResolver(_dnsMap));
        client.WaitForConnections(2, timeoutMs: 3000);

        _dnsMap[HostName] = ["127.0.0.12", "127.0.0.13", "127.0.0.14", "127.0.0.15"];

        // Close one of the existing connections to trigger re-resolve.
        _servers.Servers[0].Dispose();

        client.WaitForConnections(5, timeoutMs: 3000);
    }

    [Test]
    public async Task TestClientReResolvesHostNamesPeriodically()
    {
        var cfg = new IgniteClientConfiguration($"{HostName}:{Port}")
        {
            ReResolveAddressesInterval = TimeSpan.FromMilliseconds(300),
            LoggerFactory = _logger
        };

        using var client = await IgniteClient.StartInternalAsync(cfg, new TestDnsResolver(_dnsMap));
        client.WaitForConnections(2, timeoutMs: 3000);

        _dnsMap[HostName] = ["127.0.0.12", "127.0.0.13", "127.0.0.14", "127.0.0.15"];
        client.WaitForConnections(6, timeoutMs: 3000);
    }

    [Test]
    public async Task TestClientReResolvesHostNamesOnPrimaryReplicaAssignmentChange()
    {
        var cfg = new IgniteClientConfiguration($"{HostName}:{Port}")
        {
            ReResolveAddressesInterval = Timeout.InfiniteTimeSpan,
            LoggerFactory = _logger,
            HeartbeatInterval = TimeSpan.FromMilliseconds(100)
        };

        using var client = await IgniteClient.StartInternalAsync(cfg, new TestDnsResolver(_dnsMap));
        client.WaitForConnections(2, timeoutMs: 3000);

        _dnsMap[HostName] = ["127.0.0.12", "127.0.0.13", "127.0.0.14", "127.0.0.15"];

        // Heartbeat will trigger re-resolve on assignment change.
        _servers.Servers[0].PartitionAssignmentTimestamp = 42;

        client.WaitForConnections(6, timeoutMs: 3000);
    }

    [Test]
    public async Task TestClientRetainsExistingConnectionsOnEndpointRefresh()
    {
        var cfg = new IgniteClientConfiguration($"{HostName}:{Port}")
        {
            ReResolveAddressesInterval = TimeSpan.FromMilliseconds(300),
            LoggerFactory = _logger
        };

        using var client = await IgniteClient.StartInternalAsync(cfg, new TestDnsResolver(_dnsMap));
        client.WaitForConnections(2, timeoutMs: 3000);
        List<IClusterNode> initialConns = client.GetConnections().Select(x => x.Node).OrderBy(x => x.Name).ToList();

        _dnsMap[HostName] = ["127.0.0.12", "127.0.0.11", "127.0.0.10"]; // Same two + new one
        client.WaitForConnections(3, timeoutMs: 3000);
        List<IClusterNode> updatedConns = client.GetConnections().Select(x => x.Node).OrderBy(x => x.Name).ToList();

        Assert.AreSame(initialConns[0], updatedConns[0]);
        Assert.AreSame(initialConns[1], updatedConns[1]);
    }
}
