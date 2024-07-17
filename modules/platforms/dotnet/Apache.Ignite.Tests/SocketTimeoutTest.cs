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
using System.Linq;
using System.Threading;
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

        var ex = Assert.ThrowsAsync<IgniteClientConnectionException>(async () => await server.ConnectClientAsync(cfg));
        Assert.IsInstanceOf<TimeoutException>(ex?.InnerException);
    }

    [Test]
    public async Task TestHeartbeatTimeoutDisconnectsSocket()
    {
        using var server = new FakeServer
        {
            HeartbeatDelay = TimeSpan.FromMilliseconds(100)
        };

        var log = new ListLoggerFactory();

        var cfg = new IgniteClientConfiguration
        {
            SocketTimeout = TimeSpan.FromMilliseconds(50),
            HeartbeatInterval = TimeSpan.FromMilliseconds(100),
            RetryPolicy = new RetryNonePolicy(),
            LoggerFactory = log
        };

        using var client = await server.ConnectClientAsync(cfg);
        await Task.Delay(200);

        var expectedLog = "Heartbeat failed: The operation has timed out.";
        Assert.IsTrue(
            condition: log.Entries.Any(e => e.Message.Contains(expectedLog) && e.Exception is TimeoutException),
            message: string.Join(Environment.NewLine, log.Entries));
    }

    [Test]
    public void TestInfiniteTimeout()
    {
        using var server = new FakeServer
        {
            HandshakeDelay = TimeSpan.FromMilliseconds(300)
        };

        var cfg = new IgniteClientConfiguration
        {
            SocketTimeout = Timeout.InfiniteTimeSpan
        };

        Assert.DoesNotThrowAsync(async () =>
        {
            using var client = await server.ConnectClientAsync(cfg);
        });
    }
}
