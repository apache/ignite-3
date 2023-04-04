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
    private static readonly Meter Meter = new(name: "Apache.Ignite", version: "3.0.0");

    private static int _connectionsActive;

    private static int _requestsActive;

    /// <summary>
    /// Currently active connections.
    /// </summary>
    public static readonly ObservableCounter<int> ConnectionsActive = Meter.CreateObservableCounter(
        name: "connections-active",
        observeValue: () => Interlocked.CompareExchange(ref _connectionsActive, 0, 0),
        unit: "connections",
        description: "Currently active connections");

    /// <summary>
    /// Total number of connections established.
    /// </summary>
    public static readonly Counter<long> ConnectionsEstablished = Meter.CreateCounter<long>(
        name: "connections-established",
        unit: "connections",
        description: "Total number of established connections");

    /// <summary>
    /// Total number of connections lost.
    /// </summary>
    public static readonly Counter<long> ConnectionsLost = Meter.CreateCounter<long>(
        name: "connections-lost",
        unit: "connections",
        description: "Total number of lost connections");

    /// <summary>
    /// Total number of connections lost due to timeout.
    /// </summary>
    public static readonly Counter<long> ConnectionsLostTimeout = Meter.CreateCounter<long>(
        name: "connections-lost-timeout",
        unit: "connections",
        description: "Total number of lost connections due to network timeout");

    /// <summary>
    /// Handshakes failed.
    /// </summary>
    public static readonly Counter<long> HandshakesFailed = Meter.CreateCounter<long>(
        name: "handshakes-failed",
        unit: "handshakes",
        description: "Total number of failed handshakes (due to version mismatch, auth failure, etc)");

    /// <summary>
    /// Handshakes failed due to a timeout.
    /// </summary>
    public static readonly Counter<long> HandshakesFailedTimeout = Meter.CreateCounter<long>(
        name: "handshakes-failed-timeout",
        unit: "handshakes",
        description: "Total number of failed handshakes due to network timeout");

    /// <summary>
    /// Currently active requests (request sent, waiting for response).
    /// </summary>
    public static readonly ObservableCounter<int> RequestsActive = Meter.CreateObservableCounter(
        name: "requests-active",
        observeValue: () => Interlocked.CompareExchange(ref _requestsActive, 0, 0),
        unit: "requests",
        description: "Currently active requests (request sent, waiting for response)");

    /// <summary>
    /// Requests sent.
    /// </summary>
    public static readonly Counter<long> RequestsSent = Meter.CreateCounter<long>(
        name: "requests-sent",
        unit: "requests",
        description: "Total number of requests sent");

    /// <summary>
    /// Requests completed (response received).
    /// </summary>
    public static readonly Counter<long> RequestsCompleted = Meter.CreateCounter<long>(
        name: "requests-completed",
        unit: "requests",
        description: "Total number of requests completed (response received)");

    /// <summary>
    /// Total number of request retries.
    /// </summary>
    public static readonly Counter<long> RequestsRetried = Meter.CreateCounter<long>(
        name: "requests-retried",
        unit: "requests",
        description: "Total number of request retries");

    /// <summary>
    /// Requests failed.
    /// </summary>
    public static readonly Counter<long> RequestsFailed = Meter.CreateCounter<long>(
        name: "requests-failed",
        unit: "requests",
        description: "Total number of failed requests (failed to send, or completed with error)");

    /// <summary>
    /// Bytes sent.
    /// </summary>
    public static readonly Counter<long> BytesSent = Meter.CreateCounter<long>(
        name: "bytes-sent",
        unit: "bytes",
        description: "Total number of bytes sent");

    /// <summary>
    /// Bytes received.
    /// </summary>
    public static readonly Counter<long> BytesReceived = Meter.CreateCounter<long>(
        name: "bytes-received",
        unit: "bytes",
        description: "Total number of bytes received");

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
}
