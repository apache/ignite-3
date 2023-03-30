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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Metrics;
using System.Threading.Tasks;
using NUnit.Framework;

/// <summary>
/// Tests client metrics.
/// </summary>
public class MetricsTests
{
    private Listener _listener = null!;

    [SetUp]
    public void SetUp() => _listener = new Listener();

    [TearDown]
    public void TearDown() => _listener.Dispose();

    [Test]
    public async Task TestConnectionsMetrics()
    {
        using var server = new FakeServer();

        Assert.AreEqual(0, _listener.GetMetric("connections-established"));
        Assert.AreEqual(0, _listener.GetMetric("connections-active"));

        var client = await server.ConnectClientAsync();
        Assert.AreEqual(1, _listener.GetMetric("connections-established"));
        Assert.AreEqual(1, _listener.GetMetric("connections-active"));

        client.Dispose();
        Assert.AreEqual(0, _listener.GetMetric("connections-active"));

        (await server.ConnectClientAsync()).Dispose();
        Assert.AreEqual(2, _listener.GetMetric("connections-established"));
        Assert.AreEqual(0, _listener.GetMetric("connections-active"));
    }

    [Test]
    public async Task TestBytesSentReceived()
    {
        using var server = new FakeServer();

        Assert.AreEqual(0, _listener.GetMetric("bytes-sent"));
        Assert.AreEqual(0, _listener.GetMetric("bytes-received"));

        using var client = await server.ConnectClientAsync();

        Assert.AreEqual(11, _listener.GetMetric("bytes-sent"));
        Assert.AreEqual(63, _listener.GetMetric("bytes-received"));

        await client.Tables.GetTablesAsync();

        Assert.AreEqual(17, _listener.GetMetric("bytes-sent"));
        Assert.AreEqual(72, _listener.GetMetric("bytes-received"));
    }

    [Test]
    [SuppressMessage("ReSharper", "AccessToDisposedClosure", Justification = "Reviewed.")]
    public async Task TestConnectionsLost()
    {
        using var server = new FakeServer();
        using var client = await server.ConnectClientAsync();

        Assert.AreEqual(0, _listener.GetMetric("connections-lost"));
        Assert.AreEqual(0, _listener.GetMetric("connections-lost-timeout"));

        server.Dispose();

        TestUtils.WaitForCondition(
            () => _listener.GetMetric("connections-lost") == 1,
            1000,
            () => "connections-lost: " + _listener.GetMetric("connections-lost"));

        Assert.AreEqual(0, _listener.GetMetric("connections-lost-timeout"));
    }

    [Test]
    [SuppressMessage("ReSharper", "AccessToDisposedClosure", Justification = "Reviewed.")]
    public async Task TestConnectionsLostTimeout()
    {
        using var server = new FakeServer { HeartbeatDelay = TimeSpan.FromSeconds(3) };
        using var client = await server.ConnectClientAsync(GetConfigWithDelay());

        Assert.AreEqual(0, _listener.GetMetric("connections-lost-timeout"));

        TestUtils.WaitForCondition(
            () => _listener.GetMetric("connections-lost-timeout") == 1,
            10000,
            () => "connections-lost-timeout: " + _listener.GetMetric("connections-lost-timeout"));
    }

    [Test]
    public void TestHandshakesFailed()
    {
        using var server = new FakeServer { SendInvalidMagic = true };

        Assert.ThrowsAsync<IgniteClientConnectionException>(async () => await server.ConnectClientAsync());
        Assert.AreEqual(1, _listener.GetMetric("handshakes-failed"));
        Assert.AreEqual(0, _listener.GetMetric("handshakes-failed-timeout"));
    }

    [Test]
    public void TestHandshakesFailedTimeout()
    {
        using var server = new FakeServer { HandshakeDelay = TimeSpan.FromSeconds(1) };

        Assert.ThrowsAsync<IgniteClientConnectionException>(async () => await server.ConnectClientAsync(GetConfigWithDelay()));
        Assert.AreEqual(0, _listener.GetMetric("handshakes-failed"));
        Assert.AreEqual(1, _listener.GetMetric("handshakes-failed-timeout"));
    }

    private static IgniteClientConfiguration GetConfigWithDelay() =>
        new()
        {
            HeartbeatInterval = TimeSpan.FromMilliseconds(50),
            SocketTimeout = TimeSpan.FromMilliseconds(50)
        };

    private sealed class Listener : IDisposable
    {
        private readonly MeterListener _listener = new();

        private readonly ConcurrentDictionary<string, object> _metrics = new();

        public Listener()
        {
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == "Apache.Ignite")
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            };

            _listener.SetMeasurementEventCallback<int>(HandleInt);
            _listener.SetMeasurementEventCallback<long>(HandleLong);

            _listener.Start();
        }

        public int GetMetric(string name) => _metrics.TryGetValue(name, out var val) ? (int)val : 0;

        public void Dispose()
        {
            _listener.Dispose();
        }

        private void HandleInt(Instrument instrument, int measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state) =>
            _metrics.AddOrUpdate(instrument.Name, measurement, (_, val) => (int)val + measurement);

        private void HandleLong(Instrument instrument, long measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state) =>
            _metrics.AddOrUpdate(instrument.Name, (int)measurement, (_, val) => (int)val + (int)measurement);
    }
}
