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
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Ignite.Table;
using NUnit.Framework;

/// <summary>
/// Tests client metrics.
/// </summary>
public class MetricsTests
{
    private volatile Listener _listener = null!;

    [SetUp]
    public void SetUp() => _listener = new Listener();

    [TearDown]
    public void TearDown()
    {
        // ReSharper disable AccessToDisposedClosure
        TestUtils.WaitForCondition(
            () => _listener.GetMetric("requests-active") == 0,
            3000,
            () => "requests-active: " + _listener.GetMetric("requests-active"));

        TestUtils.WaitForCondition(
            () => _listener.GetMetric("connections-active") == 0,
            3000,
            () => "connections-active: " + _listener.GetMetric("connections-active"));

        // ReSharper restore AccessToDisposedClosure
        _listener.Dispose();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _listener.Dispose();

    [Test]
    public async Task TestConnectionsMetrics()
    {
        using var server = new FakeServer();

        Assert.AreEqual(0, _listener.GetMetric("connections-established"));
        Assert.AreEqual(0, _listener.GetMetric("connections-active"));

        using (await server.ConnectClientAsync())
        {
            Assert.AreEqual(1, _listener.GetMetric("connections-established"));
            Assert.AreEqual(1, _listener.GetMetric("connections-active"));
        }

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
        Assert.AreEqual(73, _listener.GetMetric("bytes-received"));
    }

    [Test]
    [SuppressMessage("ReSharper", "AccessToDisposedClosure", Justification = "Reviewed.")]
    public async Task TestConnectionsLost()
    {
        using var server = new FakeServer();
        using var client = await server.ConnectClientAsync(GetConfig());

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

        Assert.ThrowsAsync<IgniteClientConnectionException>(async () => await server.ConnectClientAsync(GetConfig()));
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

    [Test]
    public async Task TestRequestsSentCompletedFailed()
    {
        using var server = new FakeServer();
        using var client = await server.ConnectClientAsync();

        Assert.AreEqual(0, _listener.GetMetric("requests-sent"));
        Assert.AreEqual(0, _listener.GetMetric("requests-failed"));
        Assert.AreEqual(0, _listener.GetMetric("requests-completed"));

        await client.Tables.GetTablesAsync();

        Assert.AreEqual(1, _listener.GetMetric("requests-sent"));
        Assert.AreEqual(0, _listener.GetMetric("requests-failed"));
        Assert.AreEqual(1, _listener.GetMetric("requests-completed"));

        Assert.ThrowsAsync<IgniteException>(async () => await client.Tables.GetTableAsync("bad-table"));

        Assert.AreEqual(2, _listener.GetMetric("requests-sent"));
        Assert.AreEqual(1, _listener.GetMetric("requests-failed"));
        Assert.AreEqual(1, _listener.GetMetric("requests-completed"));
    }

    [Test]
    public async Task TestRequestsActive()
    {
        using var server = new FakeServer { OperationDelay = TimeSpan.FromSeconds(1) };
        using var client = await server.ConnectClientAsync();

        Assert.AreEqual(0, _listener.GetMetric("requests-active"));

        _ = client.Tables.GetTablesAsync();

        Assert.AreEqual(1, _listener.GetMetric("requests-active"));
        Assert.AreEqual(1, _listener.GetMetric("requests-sent"));
        Assert.AreEqual(0, _listener.GetMetric("requests-completed"));
        Assert.AreEqual(0, _listener.GetMetric("requests-failed"));
    }

    [Test]
    public async Task TestRequestsRetried()
    {
        using var server = new FakeServer(shouldDropConnection: ctx => ctx.RequestCount is > 1 and < 5);
        using var client = await server.ConnectClientAsync();

        await client.Tables.GetTablesAsync();
        Assert.AreEqual(0, _listener.GetMetric("requests-retried"));

        await client.Tables.GetTablesAsync();
        Assert.AreEqual(3, _listener.GetMetric("requests-retried"));
    }

    [Test]
    public async Task TestDataStreamerMetrics()
    {
        using var server = new FakeServer();
        using var client = await server.ConnectClientAsync();

        Assert.AreEqual(0, _listener.GetMetric("streamer-batches-sent"), "streamer-batches-sent");
        Assert.AreEqual(0, _listener.GetMetric("streamer-items-sent"), "streamer-items-sent");
        Assert.AreEqual(0, _listener.GetMetric("streamer-batches-active"), "streamer-batches-active");
        Assert.AreEqual(0, _listener.GetMetric("streamer-items-queued"), "streamer-items-queued");

        var table = await client.Tables.GetTableAsync(FakeServer.ExistingTableName);
        var view = table!.RecordBinaryView;

        await view.StreamDataAsync(GetTuples().ToAsyncEnumerable(), DataStreamerOptions.Default with { BatchSize = 2 });

        Assert.AreEqual(1, _listener.GetMetric("streamer-batches-sent"), "streamer-batches-sent");
        Assert.AreEqual(2, _listener.GetMetric("streamer-items-sent"), "streamer-items-sent");
        Assert.AreEqual(0, _listener.GetMetric("streamer-batches-active"), "streamer-batches-active");
        Assert.AreEqual(0, _listener.GetMetric("streamer-items-queued"), "streamer-items-queued");

        IEnumerable<IIgniteTuple> GetTuples()
        {
            Assert.AreEqual(0, _listener.GetMetric("streamer-batches-active"), "streamer-batches-active");
            Assert.AreEqual(0, _listener.GetMetric("streamer-items-queued"), "streamer-items-queued");

            yield return new IgniteTuple { ["ID"] = 1 };

            Assert.AreEqual(1, _listener.GetMetric("streamer-batches-active"), "streamer-batches-active");
            Assert.AreEqual(1, _listener.GetMetric("streamer-items-queued"), "streamer-items-queued");

            yield return new IgniteTuple { ["ID"] = 2 };

            Assert.AreEqual(2, _listener.GetMetric("streamer-batches-active"), "streamer-batches-active");
            Assert.AreEqual(2, _listener.GetMetric("streamer-items-queued"), "streamer-items-queued"); // TODO: Flaky (0 or 2)

            // TODO: Convert all assertions to WaitForMetric
            TestUtils.WaitForCondition(() => _listener.GetMetric("streamer-batches-sent") == 1);
            TestUtils.WaitForCondition(() => _listener.GetMetric("streamer-items-queued") == 0);
            TestUtils.WaitForCondition(() => _listener.GetMetric("streamer-batches-active") == 1);

            Assert.AreEqual(1, _listener.GetMetric("streamer-batches-active"), "streamer-batches-active");
            Assert.AreEqual(0, _listener.GetMetric("streamer-items-queued"), "streamer-items-queued");
            Assert.AreEqual(2, _listener.GetMetric("streamer-items-sent"), "streamer-items-sent");
        }
    }

    [Test]
    public async Task TestDataStreamerMetricsWithCancellation()
    {
        using var server = new FakeServer();
        using var client = await server.ConnectClientAsync();

        Assert.AreEqual(0, _listener.GetMetric("streamer-batches-sent"), "streamer-batches-sent");
        Assert.AreEqual(0, _listener.GetMetric("streamer-items-sent"), "streamer-items-sent");
        Assert.AreEqual(0, _listener.GetMetric("streamer-batches-active"), "streamer-batches-active");
        Assert.AreEqual(0, _listener.GetMetric("streamer-items-queued"), "streamer-items-queued");

        var table = await client.Tables.GetTableAsync(FakeServer.ExistingTableName);
        var view = table!.RecordBinaryView;
        var cts = new CancellationTokenSource();

        var task = view.StreamDataAsync(GetTuples(), DataStreamerOptions.Default with { BatchSize = 10 }, cts.Token);

        TestUtils.WaitForCondition(() => _listener.GetMetric("streamer-batches-sent") > 0);
        cts.Cancel();
        Assert.CatchAsync<OperationCanceledException>(async () => await task);

        Assert.GreaterOrEqual(_listener.GetMetric("streamer-batches-sent"), 1, "streamer-batches-sent");
        Assert.AreEqual(0, _listener.GetMetric("streamer-batches-active"), "streamer-batches-active");
        Assert.AreEqual(0, _listener.GetMetric("streamer-items-queued"), "streamer-items-queued");

        static async IAsyncEnumerable<IIgniteTuple> GetTuples([EnumeratorCancellation] CancellationToken ct = default)
        {
            for (int i = 0; i < 50; i++)
            {
                yield return new IgniteTuple { ["ID"] = i };

                if (i == 40)
                {
                    await Task.Delay(1000, ct);
                }
            }
        }
    }

    private static IgniteClientConfiguration GetConfig() =>
        new()
        {
            SocketTimeout = TimeSpan.FromMilliseconds(100),
            RetryPolicy = new RetryNonePolicy()
        };

    private static IgniteClientConfiguration GetConfigWithDelay() =>
        new()
        {
            HeartbeatInterval = TimeSpan.FromMilliseconds(50),
            SocketTimeout = TimeSpan.FromMilliseconds(50),
            RetryPolicy = new RetryNonePolicy()
        };

    private sealed class Listener : IDisposable
    {
        private readonly MeterListener _listener = new();

        private readonly ConcurrentDictionary<string, long> _metrics = new();

        public Listener()
        {
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == "Apache.Ignite")
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            };

            _listener.SetMeasurementEventCallback<long>(Handle);
            _listener.SetMeasurementEventCallback<int>(Handle);
            _listener.Start();
        }

        public int GetMetric(string name)
        {
            _listener.RecordObservableInstruments();
            return _metrics.TryGetValue(name, out var val) ? (int)val : 0;
        }

        public void Dispose()
        {
            _listener.Dispose();
        }

        private void Handle<T>(Instrument instrument, T measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
        {
            var newVal = Convert.ToInt64(measurement);

            if (instrument.IsObservable)
            {
                _metrics[instrument.Name] = newVal;
            }
            else
            {
                _metrics.AddOrUpdate(instrument.Name, newVal, (_, val) => val + newVal);
            }
        }
    }
}
