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
        AssertMetric(MetricNames.RequestsActive, 0);
        AssertMetric(MetricNames.ConnectionsActive, 0);

        _listener.Dispose();

        TestUtils.CheckByteArrayPoolLeak(5000);
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _listener.Dispose();

    [Test]
    public async Task TestConnectionsMetrics()
    {
        using var server = new FakeServer();

        AssertMetric(MetricNames.ConnectionsEstablished, 0);
        AssertMetric(MetricNames.ConnectionsActive, 0);

        using (await server.ConnectClientAsync())
        {
            AssertMetric(MetricNames.ConnectionsEstablished, 1);
            AssertMetric(MetricNames.ConnectionsActive, 1);
        }

        AssertMetric(MetricNames.ConnectionsActive, 0);

        (await server.ConnectClientAsync()).Dispose();
        AssertMetric(MetricNames.ConnectionsEstablished, 2);
        AssertMetric(MetricNames.ConnectionsActive, 0);
    }

    [Test]
    public async Task TestBytesSentReceived()
    {
        using var server = new FakeServer();

        AssertMetric(MetricNames.BytesSent, 0);
        AssertMetric(MetricNames.BytesReceived, 0);

        using var client = await server.ConnectClientAsync();

        AssertMetric(MetricNames.BytesSent, 15);
        AssertMetric(MetricNames.BytesReceived, 63);

        await client.Tables.GetTablesAsync();

        AssertMetric(MetricNames.BytesSent, 21);
        AssertMetric(MetricNames.BytesReceived, 72);

        AssertTaggedMetric(MetricNames.BytesSent, 21, client);
        AssertTaggedMetric(MetricNames.BytesReceived, 72, client);
    }

    [Test]
    [SuppressMessage("ReSharper", "DisposeOnUsingVariable", Justification = "Test")]
    public async Task TestConnectionsLost()
    {
        using var server = new FakeServer();
        using var client = await server.ConnectClientAsync(GetConfig());

        AssertMetric(MetricNames.ConnectionsLost, 0);
        AssertMetric(MetricNames.ConnectionsLostTimeout, 0);

        server.Dispose();

        AssertMetric(MetricNames.ConnectionsLost, 1);
        AssertMetric(MetricNames.ConnectionsLostTimeout, 0);
    }

    [Test]
    public async Task TestConnectionsLostTimeout()
    {
        using var server = new FakeServer { HeartbeatDelay = TimeSpan.FromSeconds(3) };
        using var client = await server.ConnectClientAsync(GetConfigWithDelay());

        AssertMetric(MetricNames.ConnectionsLostTimeout, 0);
        AssertMetric(MetricNames.ConnectionsLostTimeout, 1, timeoutMs: 10_000);
    }

    [Test]
    public void TestHandshakesFailed()
    {
        using var server = new FakeServer { SendInvalidMagic = true };

        Assert.ThrowsAsync<IgniteClientConnectionException>(async () => await server.ConnectClientAsync(GetConfig()));
        AssertMetric(MetricNames.HandshakesFailed, 1);
        AssertMetric(MetricNames.HandshakesFailedTimeout, 0);
        AssertMetric(MetricNames.ConnectionsActive, 0);
    }

    [Test]
    public void TestHandshakesFailedTimeout()
    {
        using var server = new FakeServer { HandshakeDelay = TimeSpan.FromSeconds(1) };

        Assert.ThrowsAsync<IgniteClientConnectionException>(async () => await server.ConnectClientAsync(GetConfigWithDelay()));
        AssertMetric(MetricNames.HandshakesFailed, 0);
        AssertMetric(MetricNames.HandshakesFailedTimeout, 1);
    }

    [Test]
    public async Task TestRequestsSentCompletedFailed()
    {
        using var server = new FakeServer();
        using var client = await server.ConnectClientAsync();

        AssertMetric(MetricNames.RequestsSent, 0);
        AssertMetric(MetricNames.RequestsFailed, 0);
        AssertMetric(MetricNames.RequestsCompleted, 0);

        await client.Tables.GetTablesAsync();

        AssertMetric(MetricNames.RequestsSent, 1);
        AssertMetric(MetricNames.RequestsFailed, 0);
        AssertMetric(MetricNames.RequestsCompleted, 1);

        Assert.ThrowsAsync<IgniteException>(async () => await client.Tables.GetTableAsync("bad-table"));

        AssertMetric(MetricNames.RequestsSent, 2);
        AssertMetric(MetricNames.RequestsFailed, 1);
        AssertMetric(MetricNames.RequestsCompleted, 1);
    }

    [Test]
    public async Task TestRequestsActive()
    {
        using var server = new FakeServer { OperationDelay = TimeSpan.FromSeconds(1) };
        using var client = await server.ConnectClientAsync();

        AssertMetric(MetricNames.RequestsActive, 0);

        _ = client.Tables.GetTablesAsync();

        AssertMetric(MetricNames.RequestsActive, 1);
        AssertMetric(MetricNames.RequestsSent, 1);
        AssertMetric(MetricNames.RequestsCompleted, 0);
        AssertMetric(MetricNames.RequestsFailed, 0);
    }

    [Test]
    public async Task TestRequestsRetried()
    {
        using var server = new FakeServer(shouldDropConnection: ctx => ctx.RequestCount is > 1 and < 5);
        using var client = await server.ConnectClientAsync();

        await client.Tables.GetTablesAsync();
        AssertMetric(MetricNames.RequestsRetried, 0);

        await client.Tables.GetTablesAsync();
        AssertMetric(MetricNames.RequestsRetried, 3);
    }

    [Test]
    public async Task TestDataStreamerMetrics()
    {
        using var server = new FakeServer();
        using var client = await server.ConnectClientAsync();

        AssertMetric(MetricNames.StreamerBatchesSent, 0);
        AssertMetric(MetricNames.StreamerItemsSent, 0);
        AssertMetric(MetricNames.StreamerBatchesActive, 0);
        AssertMetric(MetricNames.StreamerItemsQueued, 0);

        var table = await client.Tables.GetTableAsync(FakeServer.ExistingTableName);
        var view = table!.RecordBinaryView;

        await view.StreamDataAsync(GetTuples().ToAsyncEnumerable(), DataStreamerOptions.Default with { PageSize = 2 });

        AssertMetric(MetricNames.StreamerBatchesSent, 1);
        AssertMetric(MetricNames.StreamerItemsSent, 2);
        AssertMetric(MetricNames.StreamerBatchesActive, 0);
        AssertMetric(MetricNames.StreamerItemsQueued, 0);

        IEnumerable<IIgniteTuple> GetTuples()
        {
            AssertMetric(MetricNames.StreamerBatchesActive, 0);
            AssertMetric(MetricNames.StreamerItemsQueued, 0);

            yield return new IgniteTuple { ["ID"] = 1 };

            AssertMetric(MetricNames.StreamerBatchesActive, 1);
            AssertMetric(MetricNames.StreamerItemsQueued, 1);

            yield return new IgniteTuple { ["ID"] = 2 };

            AssertMetric(MetricNames.StreamerBatchesActive, 2);
            AssertMetric(MetricNames.StreamerItemsQueued, 2);

            AssertMetric(MetricNames.StreamerBatchesSent, 1);
            AssertMetric(MetricNames.StreamerBatchesActive, 1);
            AssertMetric(MetricNames.StreamerItemsQueued, 0);
            AssertMetric(MetricNames.StreamerItemsSent, 2);
        }
    }

    [Test]
    public async Task TestDataStreamerMetricsWithCancellation()
    {
        using var server = new FakeServer();
        using var client = await server.ConnectClientAsync();

        AssertMetric(MetricNames.StreamerBatchesSent, 0);
        AssertMetric(MetricNames.StreamerItemsSent, 0);
        AssertMetric(MetricNames.StreamerBatchesActive, 0);
        AssertMetric(MetricNames.StreamerItemsQueued, 0);

        var table = await client.Tables.GetTableAsync(FakeServer.ExistingTableName);
        var view = table!.RecordBinaryView;
        var cts = new CancellationTokenSource();

        var task = view.StreamDataAsync(GetTuples(), DataStreamerOptions.Default with { PageSize = 10 }, cts.Token);

        AssertMetricGreaterOrEqual(MetricNames.StreamerBatchesSent, 1);
        cts.Cancel();
        Assert.CatchAsync<OperationCanceledException>(async () => await task);

        AssertMetricGreaterOrEqual(MetricNames.StreamerBatchesSent, 1);
        AssertMetric(MetricNames.StreamerBatchesActive, 0);
        AssertMetric(MetricNames.StreamerItemsQueued, 0);

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

    [Test]
    public async Task TestMetricNames()
    {
        using var server = new FakeServer();
        using var client = await server.ConnectClientAsync();

        var publicNames = typeof(MetricNames).GetFields()
            .Where(x => x is { IsPublic: true, IsStatic: true, IsLiteral: true } && x.FieldType == typeof(string))
            .Where(x => x.Name != nameof(MetricNames.MeterName) && x.Name != nameof(MetricNames.MeterVersion))
            .Select(x => x.GetValue(null))
            .Cast<string>()
            .OrderBy(x => x)
            .ToList();

        var instrumentNames = _listener.MetricNames;

        CollectionAssert.AreEquivalent(publicNames, instrumentNames);
        CollectionAssert.AreEquivalent(publicNames, MetricNames.All);
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

    private void AssertTaggedMetric(string name, int value, IIgniteClient client) =>
        AssertTaggedMetric(name, value, client.GetConnections().Single().Node.Address.ToString());

    private void AssertTaggedMetric(string name, int value, string nodeAddr) =>
        _listener.AssertTaggedMetric(name, value, nodeAddr);

    private void AssertMetricGreaterOrEqual(string name, int value, int timeoutMs = 1000) =>
        _listener.AssertMetricGreaterOrEqual(name, value, timeoutMs);

    internal sealed class Listener : IDisposable
    {
        private readonly MeterListener _listener = new();

        private readonly ConcurrentDictionary<string, long> _metrics = new();

        private readonly ConcurrentDictionary<string, long> _metricsWithTags = new();

        public Listener()
        {
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == "Apache.Ignite")
                {
                    listener.EnableMeasurementEvents(instrument);
                    _metrics[instrument.Name] = 0;
                }
            };

            _listener.SetMeasurementEventCallback<long>(Handle);
            _listener.SetMeasurementEventCallback<int>(Handle);
            _listener.Start();
        }

        public ICollection<string> MetricNames => _metrics.Keys;

        public int GetMetric(string name)
        {
            _listener.RecordObservableInstruments();
            return _metrics.TryGetValue(name, out var val) ? (int)val : 0;
        }

        public void AssertMetric(string name, int value, int timeoutMs = 1000)
        {
            TestUtils.WaitForCondition(
                condition: () => GetMetric(name) == value,
                timeoutMs: timeoutMs,
                messageFactory: () => $"{name}: expected '{value}', but was '{GetMetric(name)}'");
        }

        public void AssertTaggedMetric(string name, int value, string nodeAddr)
        {
            var taggedName = $"{name}_{MetricTags.NodeAddress}={nodeAddr}";
            Assert.AreEqual(value, _metricsWithTags[taggedName]);
        }

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

                var taggedName = $"{instrument.Name}_{string.Join(",", tags.ToArray().Select(x => $"{x.Key}={x.Value}"))}";
                _metricsWithTags.AddOrUpdate(taggedName, newVal, (_, val) => val + newVal);
            }
        }
    }
}
