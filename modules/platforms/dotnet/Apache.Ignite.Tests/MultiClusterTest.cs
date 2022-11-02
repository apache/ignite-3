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
using NUnit.Framework;

/// <summary>
/// Tests client behavior with multiple clusters.
/// </summary>
public class MultiClusterTest
{
    [Test]
    public async Task TestClientDropsConnectionOnClusterIdMismatch()
    {
        using var server1 = new FakeServer(nodeName: "s1") { ClusterId = new Guid(1, 0, 0, new byte[8]) };
        using var server2 = new FakeServer(nodeName: "s2") { ClusterId = new Guid(2, 0, 0, new byte[8]) };

        using var client = await IgniteClient.StartAsync(new IgniteClientConfiguration(server1.Endpoint, server2.Endpoint));
        await client.Tables.GetTablesAsync();

        server1.Dispose();
        await client.Tables.GetTablesAsync();
    }

    [Test]
    public void TestReconnectToDifferentClusterFails()
    {
        Assert.Fail("TODO");
    }
}
