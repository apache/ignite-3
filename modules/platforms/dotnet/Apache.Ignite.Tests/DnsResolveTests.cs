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
using System.Threading.Tasks;
using NUnit.Framework;

/// <summary>
/// Tests DNS resolution behavior.
/// </summary>
public class DnsResolveTests
{
    [Test]
    public async Task TestClientResolvesHostNamesToAllIps()
    {
        using var server1 = new FakeServer(nodeName: "fake-node-1", port: 10901, address: IPAddress.Parse("127.0.0.10"));
        using var server2 = new FakeServer(nodeName: "fake-node-2", port: 10901, address: IPAddress.Parse("127.0.0.11"));

        var dns = new TestDnsResolver(new Dictionary<string, string[]>
        {
            ["fake-host"] = ["127.0.0.10", "127.0.0.11"]
        });

        using var client = await IgniteClient.StartInternalAsync(new("fake-host:10901"), dns);
        client.WaitForConnections(2);

        var conns = client.GetConnections().OrderBy(x => x.Node.Name).ToList();

        Assert.AreEqual("127.0.0.10:10901", conns[0].Node.Address.ToString());
        Assert.AreEqual("fake-node-1", conns[0].Node.Name);

        Assert.AreEqual("127.0.0.11:10901", conns[1].Node.Address.ToString());
        Assert.AreEqual("fake-node-2", conns[1].Node.Name);
    }

    [Test]
    public async Task TestClientReResolvesHostNamesOnDisconnect()
    {
        await Task.Delay(1);
        Assert.Fail("TODO");
    }

    [Test]
    public async Task TestClientReResolvesHostNamesPeriodically()
    {
        using var servers = FakeServerGroup.Create(
            count: 6,
            x => new FakeServer(nodeName: "fake-node-" + x, address: IPAddress.Parse("127.0.0.1" + x), port: 10902));

        var dnsMap = new ConcurrentDictionary<string, string[]>
        {
            ["fake-host"] = ["127.0.0.10", "127.0.0.11"]
        };

        var dns = new TestDnsResolver(dnsMap);

        var cfg = new IgniteClientConfiguration("fake-host:10902")
        {
            ReResolveAddressesInterval = TimeSpan.FromMilliseconds(300),
            ReconnectInterval = TimeSpan.FromMilliseconds(500)
        };

        using var client = await IgniteClient.StartInternalAsync(cfg, dns);
        client.WaitForConnections(2, timeoutMs: 3000);

        dnsMap["fake-host"] = ["127.0.0.12", "127.0.0.13", "127.0.0.14", "127.0.0.15"];
        client.WaitForConnections(4, timeoutMs: 3000);
    }

    [Test]
    public async Task TestClientReResolvesHostNamesOnPrimaryReplicaAssignmentChange()
    {
        await Task.Delay(1);
        Assert.Fail("TODO");
    }
}
