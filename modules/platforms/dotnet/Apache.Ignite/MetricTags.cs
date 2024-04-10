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
/// Metric tag names. See also <see cref="MetricNames"/>.
/// </summary>
public static class MetricTags
{
    // Naming guidelines: https://github.com/open-telemetry/semantic-conventions/blob/main/docs/general/attribute-naming.md
    // https://learn.microsoft.com/en-us/dotnet/core/diagnostics/metrics-instrumentation#best-practices-4

    /// <summary>
    /// Client id.
    /// </summary>
    public const string ClientId = "client.id";

    /// <summary>
    /// Node address.
    /// </summary>
    public const string NodeAddress = "node.addr";
}
