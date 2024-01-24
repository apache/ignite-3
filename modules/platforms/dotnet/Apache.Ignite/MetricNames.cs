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

/// <summary>
/// Ignite.NET client metrics.
///
/// TODO: brief description of the usage.
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
    /// Currently active connections.
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
}
