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
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Common;
using Internal.Common;
using NUnit.Framework;

/// <summary>
/// Tests client behavior with multiple clusters.
/// </summary>
public class MultiClusterTest
{
    [TearDown]
    public void TearDown() => TestUtils.CheckByteArrayPoolLeak();

    [Test]
    public async Task TestClientDropsConnectionOnClusterIdMismatch()
    {
        using var server1 = new FakeServer(nodeName: "s1") { ClusterId = new Guid(1, 0, 0, new byte[8]) };
        using var server2 = new FakeServer(nodeName: "s2") { ClusterId = new Guid(2, 0, 0, new byte[8]) };

        var log = new ListLoggerFactory();
        var cfg = new IgniteClientConfiguration(server1.Endpoint, server2.Endpoint)
        {
            LoggerFactory = log
        };

        using var client = await IgniteClient.StartAsync(cfg);

        TestUtils.WaitForCondition(
            () => log.Entries.Any(e => e.Message.Contains("Cluster ID mismatch")),
            5000,
            () => log.Entries.StringJoin());

        Assert.AreEqual(1, client.GetConnections().Count);
    }

    [Test]
    public async Task TestReconnectToDifferentClusterFails()
    {
        using var server1 = new FakeServer(nodeName: "s1") { ClusterId = new Guid(1, 0, 0, new byte[8]) };
        using var server2 = new FakeServer(nodeName: "s2") { ClusterId = new Guid(2, 0, 0, new byte[8]) };

        using var client = await IgniteClient.StartAsync(new IgniteClientConfiguration(server1.Endpoint, server2.Endpoint));
        await client.Tables.GetTablesAsync();

        var addr = (IPEndPoint)client.GetConnections().Single().Node.Address;
        var primaryServer = addr.Port == server1.Port ? server1 : server2;
        var secondaryServer = primaryServer == server1 ? server2 : server1;

        primaryServer.Dispose();

        var ex = Assert.ThrowsAsync<IgniteClientConnectionException>(async () => await client.Tables.GetTablesAsync());

        var inner = EnumerateInnerExceptions(ex)
            .SingleOrDefault(e => e is IgniteClientConnectionException { Code: ErrorGroups.Client.ClusterIdMismatch });

        Assert.IsNotNull(inner, $"Unexpected exception, should be 'Cluster ID mismatch', but was {ex}");
        Assert.AreEqual($"Cluster ID mismatch: expected={primaryServer.ClusterId}, actual={secondaryServer.ClusterId}", inner!.Message);
    }

    private static IEnumerable<Exception> EnumerateInnerExceptions(Exception? e)
    {
        if (e == null)
        {
            yield break;
        }

        yield return e;

        if (e is AggregateException ae)
        {
            foreach (var inner in ae.InnerExceptions)
            {
                foreach (var inner2 in EnumerateInnerExceptions(inner))
                {
                    yield return inner2;
                }
            }
        }
        else if (e.InnerException is { } inner)
        {
            foreach (var inner2 in EnumerateInnerExceptions(inner))
            {
                yield return inner2;
            }
        }
    }
}
