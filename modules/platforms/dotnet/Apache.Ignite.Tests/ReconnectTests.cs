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
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using Internal;
using Microsoft.Extensions.Logging;
using NUnit.Framework;

/// <summary>
/// Automatic reconnect tests.
/// </summary>
[SuppressMessage("ReSharper", "AccessToDisposedClosure", Justification = "Tests.")]
public class ReconnectTests
{
    [Test]
    public void TestInvalidMagicThrowsException()
    {
        using var server = new FakeServer { SendInvalidMagic = true };

        var ex = Assert.ThrowsAsync<IgniteClientConnectionException>(async () => await server.ConnectClientAsync());

        StringAssert.StartsWith("Failed to connect to endpoint: 127.0.0.1:", ex!.Message);
        StringAssert.StartsWith("Invalid magic bytes returned from the server", ex.InnerException!.Message);
    }

    [Test]
    public void TestFailedInitialConnectionToAllServersThrowsAggregateException()
    {
        using var servers = FakeServerGroup.Create(3, _ => new FakeServer { DropNewConnections = true });

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

        using var servers = FakeServerGroup.Create(10, idx => new FakeServer { DropNewConnections = idx < 9 });
        using var client = await servers.ConnectClientAsync();

        Assert.DoesNotThrowAsync(async () => await client.Tables.GetTablesAsync());
    }

    [Test]
    public async Task TestDroppedConnectionIsRestoredOnDemand()
    {
        var cfg = new IgniteClientConfiguration
        {
            LoggerFactory = TestUtils.GetConsoleLoggerFactory(LogLevel.Debug)
        };

        using var server = new FakeServer();
        using var client = await server.ConnectClientAsync(cfg);

        Assert.DoesNotThrowAsync(async () => await client.Tables.GetTablesAsync());

        server.DropExistingConnection();

        Assert.DoesNotThrowAsync(async () => await client.Tables.GetTablesAsync());
    }

    [Test]
    public async Task TestDroppedConnectionsAreRestoredInBackground()
    {
        var cfg = new IgniteClientConfiguration
        {
            HeartbeatInterval = TimeSpan.FromMilliseconds(100),
            ReconnectInterval = TimeSpan.FromMilliseconds(300),
            LoggerFactory = TestUtils.GetConsoleLoggerFactory(LogLevel.Trace)
        };

        using var servers = FakeServerGroup.Create(10);
        using var client = await servers.ConnectClientAsync(cfg);

        client.WaitForConnections(10);
        servers.DropNewConnections = true;
        servers.DropExistingConnections();

        // Dropped connections are detected by heartbeat.
        client.WaitForConnections(0);

        // Connections are restored in background due to ReconnectInterval.
        servers.DropNewConnections = false;
        client.WaitForConnections(10);

        Assert.DoesNotThrowAsync(async () => await client.Tables.GetTablesAsync());
    }

    [Test]
    public async Task TestInitiallyUnavailableNodesAreConnectedInBackground()
    {
        var cfg = new IgniteClientConfiguration
        {
            ReconnectInterval = TimeSpan.FromMilliseconds(100)
        };

        using var servers = FakeServerGroup.Create(5, idx => new FakeServer { DropNewConnections = idx > 0 });
        using var client = await servers.ConnectClientAsync(cfg);

        Assert.AreEqual(1, client.GetConnections().Count);

        servers.DropNewConnections = false;

        // When all servers are back online, connections are established in background due to ReconnectInterval.
        client.WaitForConnections(5);

        Assert.DoesNotThrowAsync(async () => await client.Tables.GetTablesAsync());
    }

    [Test]
    [SuppressMessage("Performance", "CA1848:Use the LoggerMessage delegates", Justification = "Test")]
    public async Task TestReconnectAfterFullClusterRestart()
    {
        var loggerFactory = TestUtils.GetConsoleLoggerFactory(LogLevel.Trace);
        var logger = loggerFactory.CreateLogger("test");

        var cfg = new IgniteClientConfiguration
        {
            ReconnectInterval = TimeSpan.FromMilliseconds(100),
            SocketTimeout = TimeSpan.FromSeconds(2),
            LoggerFactory = loggerFactory
        };

        using var servers = FakeServerGroup.Create(10);
        using var client = await servers.ConnectClientAsync(cfg);

        Assert.DoesNotThrowAsync(async () => await client.Tables.GetTablesAsync());

        // Drop all connections and block new connections.
        logger.LogDebug("Dropping all connections and blocking new connections...");
        servers.DropNewConnections = true;
        servers.DropExistingConnections();
        logger.LogDebug("Dropped all connections and blocked new connections.");

        // Client fails to perform operations.
        Assert.ThrowsAsync<IgniteClientConnectionException>(async () => await client.Tables.GetTablesAsync());

        // Allow new connections.
        logger.LogDebug("Allowing new connections...");
        servers.DropNewConnections = false;
        logger.LogDebug("Allowed new connections.");

        // Client works again.
        Assert.DoesNotThrowAsync(async () => await client.Tables.GetTablesAsync());

        // All connections are restored.
        logger.LogDebug("Waiting for all connections to be restored...");
        client.WaitForConnections(10);
    }
}
