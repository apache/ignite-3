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
        AssertMetric("requests-active", 0);
        AssertMetric("connections-active", 0);

        _listener.Dispose();

        TestUtils.CheckByteArrayPoolLeak(5000);
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _listener.Dispose();

    [Test]
    public async Task TestConnectionsMetrics()
    {
        using var server = new FakeServer();

        AssertMetric("connections-established", 0);
        AssertMetric("connections-active", 0);

        using (await server.ConnectClientAsync())
        {
            AssertMetric("connections-established", 1);
            AssertMetric("connections-active", 1);
        }

        AssertMetric("connections-active", 0);

        (await server.ConnectClientAsync()).Dispose();
        AssertMetric("connections-established", 2);
        AssertMetric("connections-active", 0);
    }

    [Test]
    public async Task TestBytesSentReceived()
    {
        using var server = new FakeServer();

        AssertMetric("bytes-sent", 0);
        AssertMetric("bytes-received", 0);

        using var client = await server.ConnectClientAsync();

        AssertMetric("bytes-sent", 11);
        AssertMetric("bytes-received", 63);

        await client.Tables.GetTablesAsync();

        AssertMetric("bytes-sent", 17);
        AssertMetric("bytes-received", 72);
    }

    [Test]
    [SuppressMessage("ReSharper", "DisposeOnUsingVariable", Justification = "Test")]
    public async Task TestConnectionsLost()
    {
        using var server = new FakeServer();
        using var client = await server.ConnectClientAsync(GetConfig());

        AssertMetric("connections-lost", 0);
        AssertMetric("connections-lost-timeout", 0);

        server.Dispose();

        AssertMetric("connections-lost", 1);
        AssertMetric("connections-lost-timeout", 0);
    }

    [Test]
    public async Task TestConnectionsLostTimeout()
    {
        using var server = new FakeServer { HeartbeatDelay = TimeSpan.FromSeconds(3) };
        using var client = await server.ConnectClientAsync(GetConfigWithDelay());

        AssertMetric("connections-lost-timeout", 0);
        AssertMetric("connections-lost-timeout", 1, timeoutMs: 10_000);
    }

    [Test]
    public void TestHandshakesFailed()
    {
        using var server = new FakeServer { SendInvalidMagic = true };

        Assert.ThrowsAsync<IgniteClientConnectionException>(async () => await server.ConnectClientAsync(GetConfig()));
        AssertMetric("handshakes-failed", 1);
        AssertMetric("handshakes-failed-timeout", 0);
    }

    [Test]
    public void TestHandshakesFailedTimeout()
    {
        using var server = new FakeServer { HandshakeDelay = TimeSpan.FromSeconds(1) };

        Assert.ThrowsAsync<IgniteClientConnectionException>(async () => await server.ConnectClientAsync(GetConfigWithDelay()));
        AssertMetric("handshakes-failed", 0);
        AssertMetric("handshakes-failed-timeout", 1);
    }

    [Test]
    public async Task TestRequestsSentCompletedFailed()
    {
        using var server = new FakeServer();
        using var client = await server.ConnectClientAsync();

        AssertMetric("requests-sent", 0);
        AssertMetric("requests-failed", 0);
        AssertMetric("requests-completed", 0);

        await client.Tables.GetTablesAsync();

        AssertMetric("requests-sent", 1);
        AssertMetric("requests-failed", 0);
        AssertMetric("requests-completed", 1);

        Assert.ThrowsAsync<IgniteException>(async () => await client.Tables.GetTableAsync("bad-table"));

        AssertMetric("requests-sent", 2);
        AssertMetric("requests-failed", 1);
        AssertMetric("requests-completed", 1);
    }

    [Test]
    public async Task TestRequestsActive()
    {
        using var server = new FakeServer { OperationDelay = TimeSpan.FromSeconds(1) };
        using var client = await server.ConnectClientAsync();

        AssertMetric("requests-active", 0);

        _ = client.Tables.GetTablesAsync();

        AssertMetric("requests-active", 1);
        AssertMetric("requests-sent", 1);
        AssertMetric("requests-completed", 0);
        AssertMetric("requests-failed", 0);
    }

    [Test]
    public async Task TestRequestsRetried()
    {
        using var server = new FakeServer(shouldDropConnection: ctx => ctx.RequestCount is > 1 and < 5);
        using var client = await server.ConnectClientAsync();

        await client.Tables.GetTablesAsync();
        AssertMetric("requests-retried", 0);

        await client.Tables.GetTablesAsync();
        AssertMetric("requests-retried", 3);
    }

    [Test]
    public async Task TestDataStreamerMetrics()
    {
        using var server = new FakeServer();
        using var client = await server.ConnectClientAsync();

        AssertMetric("streamer-batches-sent", 0);
        AssertMetric("streamer-items-sent", 0);
        AssertMetric("streamer-batches-active", 0);
        AssertMetric("streamer-items-queued", 0);

        var table = await client.Tables.GetTableAsync(FakeServer.ExistingTableName);
        var view = table!.RecordBinaryView;

        await view.StreamDataAsync(GetTuples().ToAsyncEnumerable(), DataStreamerOptions.Default with { BatchSize = 2 });

        AssertMetric("streamer-batches-sent", 1);
        AssertMetric("streamer-items-sent", 2);
        AssertMetric("streamer-batches-active", 0);
        AssertMetric("streamer-items-queued", 0);

        IEnumerable<IIgniteTuple> GetTuples()
        {
            AssertMetric("streamer-batches-active", 0);
            AssertMetric("streamer-items-queued", 0);

            yield return new IgniteTuple { ["ID"] = 1 };

            AssertMetric("streamer-batches-active", 1);
            AssertMetric("streamer-items-queued", 1);

            yield return new IgniteTuple { ["ID"] = 2 };

            AssertMetric("streamer-batches-active", 2);
            AssertMetric("streamer-items-queued", 2);

            AssertMetric("streamer-batches-sent", 1);
            AssertMetric("streamer-batches-active", 1);
            AssertMetric("streamer-items-queued", 0);
            AssertMetric("streamer-items-sent", 2);
        }
    }

    [Test]
    public async Task TestDataStreamerMetricsWithCancellation()
    {
        using var server = new FakeServer();
        using var client = await server.ConnectClientAsync();

        AssertMetric("streamer-batches-sent", 0);
        AssertMetric("streamer-items-sent", 0);
        AssertMetric("streamer-batches-active", 0);
        AssertMetric("streamer-items-queued", 0);

        var table = await client.Tables.GetTableAsync(FakeServer.ExistingTableName);
        var view = table!.RecordBinaryView;
        var cts = new CancellationTokenSource();

        var task = view.StreamDataAsync(GetTuples(), DataStreamerOptions.Default with { BatchSize = 10 }, cts.Token);

        AssertMetricGreaterOrEqual("streamer-batches-sent", 1);
        cts.Cancel();
        Assert.CatchAsync<OperationCanceledException>(async () => await task);

        AssertMetricGreaterOrEqual("streamer-batches-sent", 2);
        AssertMetric("streamer-batches-active", 0);
        AssertMetric("streamer-items-queued", 0);

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

    private void AssertMetric(string name, int value, int timeoutMs = 1000) =>
        _listener.AssertMetric(name, value, timeoutMs);

    private void AssertMetricGreaterOrEqual(string name, int value, int timeoutMs = 1000) =>
        _listener.AssertMetricGreaterOrEqual(name, value, timeoutMs);

    internal sealed class Listener : IDisposable
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

        public void AssertMetric(string name, int value, int timeoutMs = 1000) =>
            TestUtils.WaitForCondition(
                condition: () => GetMetric(name) == value,
                timeoutMs: timeoutMs,
                messageFactory: () => $"{name}: expected '{value}', but was '{GetMetric(name)}'");

        public void AssertMetricGreaterOrEqual(string name, int value, int timeoutMs = 1000) =>
            TestUtils.WaitForCondition(
                condition: () => GetMetric(name) >= value,
                timeoutMs: timeoutMs,
                messageFactory: () => $"{name}: expected '>= {value}', but was '{GetMetric(name)}'");

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
