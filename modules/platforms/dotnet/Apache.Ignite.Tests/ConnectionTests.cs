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

using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using Internal.Network;
using Network;
using NUnit.Framework;

/// <summary>
/// Tests client connection behavior.
/// </summary>
public class ConnectionTests
{
    [Test]
    public async Task TestConnectToIp()
    {
        using var server = new FakeServer();

        var endpoint = $"127.0.0.33:{server.Port}";
        using var client = await IgniteClient.StartAsync(new IgniteClientConfiguration(endpoint));

        Assert.AreEqual(1, client.GetConnections().Count);
    }

    [Test]
    public async Task TestConnectToIpDefaultPort()
    {
        using var server = new FakeServer(port: IgniteClientConfiguration.DefaultPort);

        var endpoint = "127.0.0.34";
        using var client = await IgniteClient.StartAsync(new IgniteClientConfiguration(endpoint));

        Assert.AreEqual(1, client.GetConnections().Count);
    }

    [Test]
    public async Task TestConnectToHostName()
    {
        using var server = new FakeServer();

        var endpoint = $"localhost:{server.Port}";
        using var client = await IgniteClient.StartAsync(new IgniteClientConfiguration(endpoint));

        Assert.AreEqual(1, client.GetConnections().Count);
    }

    [Test]
    public async Task TestConnectToHostNameDefaultPort()
    {
        using var server = new FakeServer(port: IgniteClientConfiguration.DefaultPort);

        var endpoint = "localhost";
        using var client = await IgniteClient.StartAsync(new IgniteClientConfiguration(endpoint));

        Assert.AreEqual(1, client.GetConnections().Count);
    }

    [Test]
    public async Task TestConnectToIpAndHostName()
    {
        using var server = new FakeServer();
        using var server2 = new FakeServer();

        var endpoints = new[] { $"127.0.0.100:{server.Port}", $"localhost:{server2.Port}" };
        using var client = await IgniteClient.StartAsync(new IgniteClientConfiguration(endpoints));

        client.WaitForConnections(2);
    }

    [Test]
    public async Task TestGetClusterNodesWithHostname() =>
        await TestGetClusterNodes(new DnsEndPoint("foobar", 12345));

    [Test]
    public async Task TestGetClusterNodesWithIp(
        [Values("1.2.3.4:5678", "[2001:db8::1]:8080")] string ipString) =>
        await TestGetClusterNodes(IPEndPoint.Parse(ipString));

    private static async Task TestGetClusterNodes(EndPoint endpoint)
    {
        var clusterNode = new ClusterNode(
            id: "node-id",
            name: "node-name",
            endpoint: endpoint);

        using var server = new FakeServer
        {
            ClusterNodes = new List<IClusterNode> { clusterNode }
        };

        using var client = await server.ConnectClientAsync();

        var nodes = await client.GetClusterNodesAsync();

        Assert.AreEqual(clusterNode, nodes.Single());
    }
}
