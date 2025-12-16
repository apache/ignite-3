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
        await Task.Delay(1);

        using var server1 = new FakeServer(nodeName: "fake-node-1", port: 10901, address: IPAddress.Parse("127.0.0.10"));
        using var server2 = new FakeServer(nodeName: "fake-node-1", port: 10901, address: IPAddress.Parse("127.0.0.11"));

        var resolver = new TestDnsResolver(new Dictionary<string, string[]>
        {
            ["fake-host-1"] = ["127.0.0.10"],
            ["fake-host-2"] = ["127.0.0.11"]
        });

        var clientCfg = new IgniteClientConfiguration("fake-host-1:10901", "fake-host-2:10901");

        using var client = await IgniteClient.StartInternalAsync(clientCfg, resolver);

        client.WaitForConnections(2);
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
        await Task.Delay(1);
        Assert.Fail("TODO");
    }

    [Test]
    public async Task TestClientReResolvesHostNamesOnPrimaryReplicaAssignmentChange()
    {
        await Task.Delay(1);
        Assert.Fail("TODO");
    }
}
