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

namespace Apache.Ignite;

using System.Collections.Generic;

/// <summary>
/// Ignite.NET client metrics.
/// <para />
/// CLI usage example:
/// <code>
/// dotnet-counters monitor --counters Apache.Ignite,System.Runtime --process-id PID
/// </code>
/// See https://learn.microsoft.com/en-us/dotnet/core/diagnostics/dotnet-counters for details.
/// <para />
/// Code usage example:
/// <code>
/// var meterListener = new MeterListener();
///
/// meterListener.InstrumentPublished = (instrument, listener) =>
/// {
///     if (instrument.Meter.Name == MetricNames.MeterName)
///     {
///         listener.EnableMeasurementEvents(instrument);
///         Console.WriteLine($"Instrument enabled: {instrument.Name}");
///     }
/// };
///
/// meterListener.SetMeasurementEventCallback&lt;long&gt;((instrument, measurement, tags, state) =>
///     Console.WriteLine($"{instrument.Name}: {measurement}"));
///
/// meterListener.SetMeasurementEventCallback&lt;int&gt;((instrument, measurement, tags, state) =>
///     Console.WriteLine($"{instrument.Name}: {measurement}"));
///
/// meterListener.Start();
/// </code>
/// See https://learn.microsoft.com/en-us/dotnet/core/diagnostics/metrics for details.
/// </summary>
public static class MetricNames
{
    /// <summary>
    /// Meter name.
    /// </summary>
    public const string MeterName = "Apache.Ignite";

    /// <summary>
    /// Meter version.
    /// </summary>
    public const string MeterVersion = "3.0.0";

    /// <summary>
    /// Total number of currently active connections.
    /// </summary>
    public const string ConnectionsActive = "connections-active";

    /// <summary>
    /// Total number of connections established (unlike <see cref="ConnectionsActive"/>, this metric only goes up).
    /// </summary>
    public const string ConnectionsEstablished = "connections-established";

    /// <summary>
    /// Total number of connections lost.
    /// </summary>
    public const string ConnectionsLost = "connections-lost";

    /// <summary>
    /// Total number of connections lost due to a timeout.
    /// </summary>
    public const string ConnectionsLostTimeout = "connections-lost-timeout";

    /// <summary>
    /// Total number of failed handshakes (due to version mismatch, auth failure, or other problems).
    /// </summary>
    public const string HandshakesFailed = "handshakes-failed";

    /// <summary>
    /// Total number of failed handshakes due to a network timeout.
    /// </summary>
    public const string HandshakesFailedTimeout = "handshakes-failed-timeout";

    /// <summary>
    /// Currently active requests (requests being sent to the socket or waiting for response).
    /// </summary>
    public const string RequestsActive = "requests-active";

    /// <summary>
    /// Total number of requests sent.
    /// </summary>
    public const string RequestsSent = "requests-sent";

    /// <summary>
    /// Total number of requests completed (response received).
    /// </summary>
    public const string RequestsCompleted = "requests-completed";

    /// <summary>
    /// Total number of retried requests.
    /// </summary>
    public const string RequestsRetried = "requests-retried";

    /// <summary>
    /// Total number of failed requests (completed with error or failed to send).
    /// </summary>
    public const string RequestsFailed = "requests-failed";

    /// <summary>
    /// Total number of bytes sent.
    /// </summary>
    public const string BytesSent = "bytes-sent";

    /// <summary>
    /// Total number of bytes received.
    /// </summary>
    public const string BytesReceived = "bytes-received";

    /// <summary>
    /// Total number of data streamer batches sent.
    /// </summary>
    public const string StreamerBatchesSent = "streamer-batches-sent";

    /// <summary>
    /// Total number of data streamer items sent.
    /// </summary>
    public const string StreamerItemsSent = "streamer-items-sent";

    /// <summary>
    /// Total number of active (in-memory) data streamer batches.
    /// </summary>
    public const string StreamerBatchesActive = "streamer-batches-active";

    /// <summary>
    /// Total number of queued data streamer items (rows).
    /// </summary>
    public const string StreamerItemsQueued = "streamer-items-queued";

    /// <summary>
    /// All metric names.
    /// </summary>
    public static readonly IReadOnlyCollection<string> All = new[]
    {
        ConnectionsActive,
        ConnectionsEstablished,
        ConnectionsLost,
        ConnectionsLostTimeout,
        HandshakesFailed,
        HandshakesFailedTimeout,
        RequestsActive,
        RequestsSent,
        RequestsCompleted,
        RequestsRetried,
        RequestsFailed,
        BytesSent,
        BytesReceived,
        StreamerBatchesSent,
        StreamerItemsSent,
        StreamerBatchesActive,
        StreamerItemsQueued
    };
}
