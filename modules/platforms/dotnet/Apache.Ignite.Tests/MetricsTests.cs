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
using Internal;
using NUnit.Framework;

/// <summary>
/// Tests client metrics.
/// </summary>
public class MetricsTests
{
    private const int DefaultTimeoutMs = 3000;

    private volatile Listener _listener = null!;

    [SetUp]
    public void SetUp()
    {
        _listener = new Listener(TestContext.CurrentContext.Test.Name);
        AssertMetric(MetricNames.ConnectionsActive, 0);
    }

    [TearDown]
    public void TearDown()
    {
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

        var client1 = await server.ConnectClientAsync();
        using (client1)
        {
            AssertMetric(MetricNames.ConnectionsEstablished, 1);
            AssertMetric(MetricNames.ConnectionsActive, 1);
        }

        AssertMetric(MetricNames.ConnectionsActive, 0);

        var client2 = await server.ConnectClientAsync();
        client2.Dispose();

        AssertMetric(MetricNames.ConnectionsEstablished, 2);
        AssertMetric(MetricNames.ConnectionsActive, 0);

        AssertTaggedMetric(MetricNames.ConnectionsEstablished, 1, server, client1);
        AssertTaggedMetric(MetricNames.ConnectionsEstablished, 1, server, client2);
    }

    [Test]
    public async Task TestBytesSentReceived()
    {
        using var server = new FakeServer
        {
            DisableRandomHandshake = true
        };

        AssertMetric(MetricNames.BytesSent, 0);
        AssertMetric(MetricNames.BytesReceived, 0);

        using var client = await server.ConnectClientAsync();

        AssertMetric(MetricNames.BytesSent, 16);
        AssertMetric(MetricNames.BytesReceived, 91);

        await client.Tables.GetTablesAsync();

        AssertMetric(MetricNames.BytesSent, 22);
        AssertMetric(MetricNames.BytesReceived, 100);

        AssertTaggedMetric(MetricNames.BytesSent, 22, server, client);
        AssertTaggedMetric(MetricNames.BytesReceived, 100, server, client);
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

        AssertTaggedMetric(MetricNames.ConnectionsLost, 1, server, client);
    }

    [Test]
    public async Task TestConnectionsLostTimeout()
    {
        using var server = new FakeServer { HeartbeatDelay = TimeSpan.FromSeconds(3) };
        using var client = await server.ConnectClientAsync(GetConfigWithDelay());

        AssertMetric(MetricNames.ConnectionsLostTimeout, 0);
        AssertMetric(MetricNames.ConnectionsLostTimeout, 1, timeoutMs: 10_000);

        AssertTaggedMetric(MetricNames.ConnectionsLostTimeout, 1, server, client);
    }

    [Test]
    public void TestHandshakesFailed()
    {
        using var server = new FakeServer { SendInvalidMagic = true };

        Assert.ThrowsAsync<IgniteClientConnectionException>(async () => await server.ConnectClientAsync(GetConfig()));
        AssertMetricGreaterOrEqual(MetricNames.HandshakesFailed, 1);
        AssertMetric(MetricNames.HandshakesFailedTimeout, 0);
        AssertMetric(MetricNames.ConnectionsActive, 0);

        AssertTaggedMetric(MetricNames.HandshakesFailed, 1, server, null);
    }

    [Test]
    public void TestHandshakesFailedTimeout()
    {
        using var server = new FakeServer { HandshakeDelay = TimeSpan.FromSeconds(1) };

        Assert.ThrowsAsync<IgniteClientConnectionException>(async () => await server.ConnectClientAsync(GetConfigWithDelay()));
        AssertMetric(MetricNames.HandshakesFailedTimeout, 1);

        AssertTaggedMetric(MetricNames.HandshakesFailedTimeout, 1, server, null);
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

        Assert.ThrowsAsync<IgniteException>(async () => await client.Tables.GetTableAsync("bad_table"));

        AssertMetric(MetricNames.RequestsSent, 2);
        AssertMetric(MetricNames.RequestsFailed, 1);
        AssertMetric(MetricNames.RequestsCompleted, 1);

        AssertTaggedMetric(MetricNames.RequestsSent, 2, server, client);
        AssertTaggedMetric(MetricNames.RequestsFailed, 1, server, client);
        AssertTaggedMetric(MetricNames.RequestsCompleted, 1, server, client);
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

        AssertTaggedMetric(MetricNames.RequestsSent, 1, server, client);
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

        AssertTaggedMetric(MetricNames.RequestsRetried, 3, server, client);
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

        AssertTaggedMetric(MetricNames.StreamerBatchesSent, 1, server, client);
        AssertTaggedMetric(MetricNames.StreamerItemsSent, 2, server, client);

        IEnumerable<IIgniteTuple> GetTuples()
        {
            AssertMetric(MetricNames.StreamerBatchesActive, 0);
            AssertMetric(MetricNames.StreamerItemsQueued, 0);

            yield return new IgniteTuple { ["ID"] = 1 };

            AssertMetric(MetricNames.StreamerBatchesActive, 1);
            AssertMetric(MetricNames.StreamerItemsQueued, 1);

            yield return new IgniteTuple { ["ID"] = 2 };
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

        var ex = Assert.ThrowsAsync<DataStreamerException>(async () => await task);
        Assert.IsInstanceOf<OperationCanceledException>(ex.InnerException);
        Assert.AreEqual(ErrorGroups.Common.Internal, ex.Code);

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
            RetryPolicy = new RetryNonePolicy(),
            ReconnectInterval = TimeSpan.Zero
        };

    private static IgniteClientConfiguration GetConfigWithDelay() =>
        new()
        {
            HeartbeatInterval = TimeSpan.FromMilliseconds(50),
            SocketTimeout = TimeSpan.FromMilliseconds(50),
            RetryPolicy = new RetryNonePolicy(),
            ReconnectInterval = TimeSpan.Zero
        };

    private static Guid? GetClientId(IIgniteClient? client) => ((IgniteClientInternal?)client)?.Socket.ClientId;

    private void AssertMetric(string name, int value, int timeoutMs = DefaultTimeoutMs) =>
        _listener.AssertMetric(name, value, timeoutMs);

    private void AssertTaggedMetric(string name, int value, FakeServer server, IIgniteClient? client) =>
        AssertTaggedMetric(name, value, server.Node.Address.ToString()!, GetClientId(client));

    private void AssertTaggedMetric(string name, int value, string nodeAddr, Guid? clientId) =>
        _listener.AssertTaggedMetric(name, value, nodeAddr, clientId);

    private void AssertMetricGreaterOrEqual(string name, int value, int timeoutMs = DefaultTimeoutMs) =>
        _listener.AssertMetricGreaterOrEqual(name, value, timeoutMs);

    internal sealed class Listener : IDisposable
    {
        private readonly string _name;

        private readonly MeterListener _listener = new();

        private readonly ConcurrentDictionary<string, long> _metrics = new();

        private readonly ConcurrentDictionary<string, long> _metricsWithTags = new();

        private readonly int _initialConnectionsActive;

        public Listener(string name)
        {
            _name = name;

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

            _initialConnectionsActive = GetMetric(Apache.Ignite.MetricNames.ConnectionsActive);
        }

        public ICollection<string> MetricNames => _metrics.Keys;

        public int GetMetric(string name)
        {
            _listener.RecordObservableInstruments();

            var res = _metrics.TryGetValue(name, out var val)
                ? (int)val
                : 0;

            // Workaround for initial connections active not being zero.
            return name == Apache.Ignite.MetricNames.ConnectionsActive
                ? res - _initialConnectionsActive
                : res;
        }

        public int GetTaggedMetric(string name, string nodeAddr, Guid? clientId)
        {
            _listener.RecordObservableInstruments();

            if (clientId != null)
            {
                var taggedName = $"{name}_{MetricTags.ClientId}={clientId},{MetricTags.NodeAddress}={nodeAddr}";
                return _metricsWithTags.TryGetValue(taggedName, out var val) ? (int)val : 0;
            }

            // Client id is not known, find by name and node address.
            return _metricsWithTags
                .Where(x =>
                    x.Key.StartsWith($"{name}_{MetricTags.ClientId}=", StringComparison.Ordinal) &&
                    x.Key.EndsWith($",{MetricTags.NodeAddress}={nodeAddr}", StringComparison.Ordinal))
                .Select(x => (int)x.Value)
                .SingleOrDefault();
        }

        public void AssertMetric(string name, int value, int timeoutMs = DefaultTimeoutMs)
        {
            TestUtils.WaitForCondition(
                condition: () => GetMetric(name) == value,
                timeoutMs: timeoutMs,
                messageFactory: () => $"{name}: expected '{value}', but was '{GetMetric(name)}'");
        }

        public void AssertTaggedMetric(string name, int value, string nodeAddr, Guid? clientId, int timeoutMs = DefaultTimeoutMs)
        {
            TestUtils.WaitForCondition(
                condition: () => GetTaggedMetric(name, nodeAddr, clientId) == value,
                timeoutMs: timeoutMs,
                messageFactory: () => $"{name} for {nodeAddr} ({clientId}): expected '{value}', " +
                                      $"but was '{GetTaggedMetric(name, nodeAddr, clientId)}'");
        }

        public void AssertMetricGreaterOrEqual(string name, int value, int timeoutMs = DefaultTimeoutMs) =>
            TestUtils.WaitForCondition(
                condition: () => GetMetric(name) >= value,
                timeoutMs: timeoutMs,
                messageFactory: () => $"{name}: expected '>= {value}', but was '{GetMetric(name)}'");

        public void Dispose()
        {
            Console.WriteLine("Disposing listener: " + _name);
            _listener.Dispose();
            Console.WriteLine("Disposed listener: " + _name);
        }

        private void Handle<T>(Instrument instrument, T measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
        {
            lock (_listener)
            {
                var newVal = Convert.ToInt64(measurement);

                if (instrument.IsObservable)
                {
                    _metrics[instrument.Name] = newVal;
                    Console.WriteLine($"{_name} observable measurement: {instrument.Name} = {newVal}");
                }
                else
                {
                    var res = _metrics.AddOrUpdate(instrument.Name, newVal, (_, val) => val + newVal);
                    Console.WriteLine($"{_name} observable measurement: {instrument.Name} = {res - newVal} + {newVal} = {res}");

                    var taggedName = $"{instrument.Name}_{string.Join(",", tags.ToArray().Select(x => $"{x.Key}={x.Value}"))}";
                    _metricsWithTags.AddOrUpdate(taggedName, newVal, (_, val) => val + newVal);
                }
            }
        }
    }
}
