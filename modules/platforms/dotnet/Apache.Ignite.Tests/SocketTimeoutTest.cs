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
/// Tests <see cref="IgniteClientConfiguration.SocketTimeout"/> behavior.
/// </summary>
public class SocketTimeoutTest
{
    [Test]
    public void TestHandshakeTimeoutThrowsException()
    {
        using var server = new FakeServer
        {
            HandshakeDelay = TimeSpan.FromMilliseconds(100)
        };

        var cfg = new IgniteClientConfiguration
        {
            SocketTimeout = TimeSpan.FromMilliseconds(50)
        };

        Assert.ThrowsAsync<TimeoutException>(async () => await server.ConnectClientAsync(cfg));
    }

    [Test]
    public async Task TestHeartbeatTimeoutDisconnectsSocket()
    {
        using var server = new FakeServer
        {
            HeartbeatDelay = TimeSpan.FromMilliseconds(100)
        };

        var cfg = new IgniteClientConfiguration
        {
            SocketTimeout = TimeSpan.FromMilliseconds(50),
            HeartbeatInterval = TimeSpan.FromMilliseconds(100),
            RetryPolicy = new RetryNonePolicy()
        };

        using var client = await server.ConnectClientAsync(cfg);

        await Task.Delay(100);

        var ex = Assert.ThrowsAsync<TimeoutException>(async () => await client.Tables.GetTablesAsync());
        StringAssert.Contains("at Apache.Ignite.Internal.ClientSocket.SendHeartbeatAsync", ex!.ToString());
    }
}
