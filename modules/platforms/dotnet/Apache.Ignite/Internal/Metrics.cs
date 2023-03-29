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
    public static readonly Counter<int> ConnectionsActive = Meter.CreateCounter<int>("connections-active");

    /// <summary>
    /// Total number of connections established.
    /// </summary>
    public static readonly Counter<int> ConnectionsEstablished = Meter.CreateCounter<int>("connections-established");
}
