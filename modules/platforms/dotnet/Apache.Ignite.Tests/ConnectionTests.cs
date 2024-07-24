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

using System.Linq;
using System.Threading.Tasks;
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

        var cfg = new IgniteClientConfiguration(server.Endpoint);
        using var client = await IgniteClient.StartAsync(cfg);

        Assert.AreEqual(server.Endpoint, client.GetConnections().Single().Node.Address.ToString());
    }

    [Test]
    public async Task TestConnectToIpDefaultPort()
    {
        await Task.Delay(1);
        Assert.Fail();
    }

    [Test]
    public async Task TestConnectToHostName()
    {
        await Task.Delay(1);
        Assert.Fail();
    }

    [Test]
    public async Task TestConnectToHostNameDefaultPort()
    {
        await Task.Delay(1);
        Assert.Fail();
    }
}
