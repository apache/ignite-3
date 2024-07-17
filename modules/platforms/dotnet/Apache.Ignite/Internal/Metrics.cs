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

namespace Apache.Ignite.Internal;

using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Metrics;
using System.Threading;

/// <summary>
/// Ignite.NET client metrics.
/// </summary>
[SuppressMessage(
    "StyleCop.CSharp.OrderingRules",
    "SA1202:Elements should be ordered by access",
    Justification = "Meter should be private and comes before metrics.")]
internal static class Metrics
{
    private static readonly Meter Meter = new(name: MetricNames.MeterName, version: MetricNames.MeterVersion);

    private static int _connectionsActive;

    private static int _requestsActive;

    private static int _streamerBatchesActive;

    private static int _streamerItemsQueued;

    /// <summary>
    /// Currently active connections.
    /// </summary>
    public static readonly ObservableGauge<int> ConnectionsActive = Meter.CreateObservableGauge(
        name: MetricNames.ConnectionsActive,
        observeValue: () => Interlocked.CompareExchange(ref _connectionsActive, 0, 0),
        unit: "connections",
        description: "Total number of currently active connections");

    /// <summary>
    /// Total number of connections established.
    /// </summary>
    public static readonly Counter<long> ConnectionsEstablished = Meter.CreateCounter<long>(
        name: MetricNames.ConnectionsEstablished,
        unit: "connections",
        description: "Total number of established connections");

    /// <summary>
    /// Total number of connections lost.
    /// </summary>
    public static readonly Counter<long> ConnectionsLost = Meter.CreateCounter<long>(
        name: MetricNames.ConnectionsLost,
        unit: "connections",
        description: "Total number of lost connections");

    /// <summary>
    /// Total number of connections lost due to timeout.
    /// </summary>
    public static readonly Counter<long> ConnectionsLostTimeout = Meter.CreateCounter<long>(
        name: MetricNames.ConnectionsLostTimeout,
        unit: "connections",
        description: "Total number of lost connections due to network timeout");

    /// <summary>
    /// Handshakes failed.
    /// </summary>
    public static readonly Counter<long> HandshakesFailed = Meter.CreateCounter<long>(
        name: MetricNames.HandshakesFailed,
        unit: "handshakes",
        description: "Total number of failed handshakes (due to version mismatch, auth failure, or other problems)");

    /// <summary>
    /// Handshakes failed due to a timeout.
    /// </summary>
    public static readonly Counter<long> HandshakesFailedTimeout = Meter.CreateCounter<long>(
        name: MetricNames.HandshakesFailedTimeout,
        unit: "handshakes",
        description: "Total number of failed handshakes due to a network timeout");

    /// <summary>
    /// Currently active requests (request sent, waiting for response).
    /// </summary>
    public static readonly ObservableGauge<int> RequestsActive = Meter.CreateObservableGauge(
        name: MetricNames.RequestsActive,
        observeValue: () => Interlocked.CompareExchange(ref _requestsActive, 0, 0),
        unit: "requests",
        description: "Currently active requests (requests being sent to the socket or waiting for response)");

    /// <summary>
    /// Requests sent.
    /// </summary>
    public static readonly Counter<long> RequestsSent = Meter.CreateCounter<long>(
        name: MetricNames.RequestsSent,
        unit: "requests",
        description: "Total number of requests sent");

    /// <summary>
    /// Requests completed (response received).
    /// </summary>
    public static readonly Counter<long> RequestsCompleted = Meter.CreateCounter<long>(
        name: MetricNames.RequestsCompleted,
        unit: "requests",
        description: "Total number of requests completed (response received)");

    /// <summary>
    /// Total number of retried requests.
    /// </summary>
    public static readonly Counter<long> RequestsRetried = Meter.CreateCounter<long>(
        name: MetricNames.RequestsRetried,
        unit: "requests",
        description: "Total number of retried requests");

    /// <summary>
    /// Requests failed.
    /// </summary>
    public static readonly Counter<long> RequestsFailed = Meter.CreateCounter<long>(
        name: MetricNames.RequestsFailed,
        unit: "requests",
        description: "Total number of failed requests (completed with error or failed to send)");

    /// <summary>
    /// Bytes sent.
    /// </summary>
    public static readonly Counter<long> BytesSent = Meter.CreateCounter<long>(
        name: MetricNames.BytesSent,
        unit: "bytes",
        description: "Total number of bytes sent");

    /// <summary>
    /// Bytes received.
    /// </summary>
    public static readonly Counter<long> BytesReceived = Meter.CreateCounter<long>(
        name: MetricNames.BytesReceived,
        unit: "bytes",
        description: "Total number of bytes received");

    /// <summary>
    /// Data streamer batches sent.
    /// </summary>
    public static readonly Counter<long> StreamerBatchesSent = Meter.CreateCounter<long>(
        name: MetricNames.StreamerBatchesSent,
        unit: "batches",
        description: "Total number of data streamer batches sent.");

    /// <summary>
    /// Data streamer items sent.
    /// </summary>
    public static readonly Counter<long> StreamerItemsSent = Meter.CreateCounter<long>(
        name: MetricNames.StreamerItemsSent,
        unit: "batches",
        description: "Total number of data streamer items sent.");

    /// <summary>
    /// Data streamer batches active.
    /// </summary>
    public static readonly ObservableGauge<int> StreamerBatchesActive = Meter.CreateObservableGauge(
        name: MetricNames.StreamerBatchesActive,
        observeValue: () => Interlocked.CompareExchange(ref _streamerBatchesActive, 0, 0),
        unit: "batches",
        description: "Total number of existing data streamer batches.");

    /// <summary>
    /// Data streamer items (rows) queued.
    /// </summary>
    public static readonly ObservableGauge<int> StreamerItemsQueued = Meter.CreateObservableGauge(
        name: MetricNames.StreamerItemsQueued,
        observeValue: () => new Measurement<int>(Interlocked.CompareExchange(ref _streamerItemsQueued, 0, 0)),
        unit: "items",
        description: "Total number of queued data streamer items (rows).");

    /// <summary>
    /// Increments active connections.
    /// </summary>
    public static void ConnectionsActiveIncrement() => Interlocked.Increment(ref _connectionsActive);

    /// <summary>
    /// Decrements active connections.
    /// </summary>
    public static void ConnectionsActiveDecrement() => Interlocked.Decrement(ref _connectionsActive);

    /// <summary>
    /// Increments active requests.
    /// </summary>
    public static void RequestsActiveIncrement() => Interlocked.Increment(ref _requestsActive);

    /// <summary>
    /// Decrements active requests.
    /// </summary>
    public static void RequestsActiveDecrement() => Interlocked.Decrement(ref _requestsActive);

    /// <summary>
    /// Increments active streamer batches.
    /// </summary>
    public static void StreamerBatchesActiveIncrement() => Interlocked.Increment(ref _streamerBatchesActive);

    /// <summary>
    /// Decrements active streamer batches.
    /// </summary>
    public static void StreamerBatchesActiveDecrement() => Interlocked.Decrement(ref _streamerBatchesActive);

    /// <summary>
    /// Increments streamer items queued.
    /// </summary>
    public static void StreamerItemsQueuedIncrement() => Interlocked.Increment(ref _streamerItemsQueued);

    /// <summary>
    /// Decrements streamer items queued.
    /// </summary>
    /// <param name="count">The count.</param>
    public static void StreamerItemsQueuedDecrement(int count) => Interlocked.Add(ref _streamerItemsQueued, -count);
}
