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

    /// <summary>
    /// Currently active connections.
    /// </summary>
    public static readonly Counter<long> ConnectionsActive = Meter.CreateCounter<long>("connections-active");

    /// <summary>
    /// Total number of connections established.
    /// </summary>
    public static readonly Counter<long> ConnectionsEstablished = Meter.CreateCounter<long>("connections-established");

    /// <summary>
    /// Total number of connections lost.
    /// </summary>
    public static readonly Counter<long> ConnectionsLost = Meter.CreateCounter<long>("connections-lost");

    /// <summary>
    /// Total number of connections lost due to timeout.
    /// </summary>
    public static readonly Counter<long> ConnectionsLostTimeout = Meter.CreateCounter<long>("connections-lost-timeout");

    /// <summary>
    /// Handshakes failed.
    /// </summary>
    public static readonly Counter<long> HandshakesFailed = Meter.CreateCounter<long>("handshakes-failed");

    /// <summary>
    /// Handshakes failed due to a timeout.
    /// </summary>
    public static readonly Counter<long> HandshakesFailedTimeout = Meter.CreateCounter<long>("handshakes-failed-timeout");

    /// <summary>
    /// Currently active requests (request sent, waiting for response).
    /// </summary>
    public static readonly Counter<long> RequestsActive = Meter.CreateCounter<long>("requests-active");

    /// <summary>
    /// Requests sent.
    /// </summary>
    public static readonly Counter<long> RequestsSent = Meter.CreateCounter<long>("requests-sent");

    /// <summary>
    /// Requests completed (response received).
    /// </summary>
    public static readonly Counter<long> RequestsCompleted = Meter.CreateCounter<long>("requests-completed");

    /// <summary>
    /// Total number of request retries.
    /// </summary>
    public static readonly Counter<long> RequestsRetried = Meter.CreateCounter<long>("requests-retried");

    /// <summary>
    /// Requests failed.
    /// </summary>
    public static readonly Counter<long> RequestsFailed = Meter.CreateCounter<long>("requests-failed");

    /// <summary>
    /// Bytes sent.
    /// </summary>
    public static readonly Counter<long> BytesSent = Meter.CreateCounter<long>("bytes-sent");

    /// <summary>
    /// Bytes received.
    /// </summary>
    public static readonly Counter<long> BytesReceived = Meter.CreateCounter<long>("bytes-received");
}
