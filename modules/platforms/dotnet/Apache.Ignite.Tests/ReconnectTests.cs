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
using Internal;
using NUnit.Framework;

/// <summary>
/// Automatic reconnect tests.
/// </summary>
public class ReconnectTests
{
    // TODO: Test reconnect to one node
    // TODO: Test reconnect to multiple nodes (all fail, some fail, etc)
    // TODO: Test connection to a node that was not initially available (add FakeServer flag that rejects connections)
    // TODO: Check that reconnect stops on dispose
    // TODO: What happens if all nodes are lost? Do we just keep trying?
    [Test]
    public void TestInvalidMagicFromAllServersThrowsException()
    {
    }

    [Test]
    public void TestFailedInitialConnectionToAllServersThrowsAggregateException()
    {
        using var servers = FakeServerGroup.Create(3, _ => new FakeServer { DropConnections = true });

        var ex = Assert.ThrowsAsync<AggregateException>(async () => await servers.ConnectClientAsync());
        Assert.AreEqual(3, ex!.InnerExceptions.Count);

        foreach (var innerEx in ex.InnerExceptions)
        {
            StringAssert.StartsWith("Failed to connect to endpoint: 127.0.0.1:", innerEx.Message);
            Assert.IsInstanceOf<IgniteClientConnectionException>(innerEx);
        }
    }

    [Test]
    public async Task TestFailedInitialConnectionToSomeServersAndSuccessfulConnectionToOneDoesNotThrow()
    {
        ClientFailoverSocket.ResetGlobalEndpointIndex();

        using var servers = FakeServerGroup.Create(10, idx => new FakeServer { DropConnections = idx < 9 });
        using var client = await servers.ConnectClientAsync();
    }
}
