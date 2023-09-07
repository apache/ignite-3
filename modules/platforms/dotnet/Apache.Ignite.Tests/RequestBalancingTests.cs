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

using System.Threading.Tasks;
using Internal.Proto;
using NUnit.Framework;

/// <summary>
/// Tests request balancing across active connections.
/// When client is connected to multiple nodes, requests will use all available connections in a round-robin fashion
/// (unless an operation is tied to a specific connection, like a transaction or a cursor).
/// </summary>
public class RequestBalancingTests
{
    [Test]
    public async Task TestRequestsAreRoundRobinBalanced()
    {
        using var server1 = new FakeServer(nodeName: "s1");
        using var server2 = new FakeServer(nodeName: "s2");
        using var server3 = new FakeServer(nodeName: "s3");

        var clientCfg = new IgniteClientConfiguration
        {
            Endpoints = { server1.Endpoint, server2.Endpoint, server3.Endpoint }
        };

        using var client = await IgniteClient.StartAsync(clientCfg);
        client.WaitForConnections(3);

        for (var i = 0; i < 10; i++)
        {
            await client.Tables.GetTablesAsync();
            await client.Tables.GetTableAsync(FakeServer.ExistingTableName);
        }

        // All servers get their share of requests.
        foreach (var server in new[] { server1, server2, server3 })
        {
            CollectionAssert.Contains(server.ClientOps, ClientOp.TableGet, server.Node.Name);
            CollectionAssert.Contains(server.ClientOps, ClientOp.TablesGet, server.Node.Name);
        }

        // Total number of requests across all servers.
        Assert.AreEqual(20, server1.ClientOps.Count + server2.ClientOps.Count + server3.ClientOps.Count);
    }
}
