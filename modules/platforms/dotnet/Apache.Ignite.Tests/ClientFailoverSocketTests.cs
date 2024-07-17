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
/// Tests for <see cref="ClientFailoverSocket"/>.
/// </summary>
public class ClientFailoverSocketTests
{
    [Test]
    public async Task TestBackgroundConnectionProcessDoesNotBlockOperations()
    {
        ClientFailoverSocket.ResetGlobalEndpointIndex();

        var operationTimeout = TimeSpan.FromSeconds(0.5);
        var handshakeDelay = TimeSpan.FromSeconds(2);

        using var server1 = new FakeServer { HandshakeDelay = handshakeDelay };
        using var server2 = new FakeServer();

        var clientCfg = new IgniteClientConfiguration
        {
            Endpoints = { server2.Endpoint, server1.Endpoint }
        };

        // First connection will go to server2, which does not have a delay.
        // Second connection will be established in background, and will take 2 seconds, but will not delay operations.
        using var client = await IgniteClient.StartAsync(clientCfg).WaitAsync(operationTimeout);
        var tables = await client.Tables.GetTablesAsync().WaitAsync(operationTimeout);

        Assert.AreEqual(0, tables.Count);
        Assert.AreEqual(1, client.GetConnections().Count);
    }
}
