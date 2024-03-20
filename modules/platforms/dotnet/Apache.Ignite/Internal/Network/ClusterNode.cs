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

namespace Apache.Ignite.Internal.Network
{
    using System;
    using System.Collections.Generic;
    using System.Net;
    using Ignite.Network;

    /// <summary>
    /// Cluster node.
    /// </summary>
    internal sealed record ClusterNode(string Id, string Name, IPEndPoint Address) : IClusterNode
    {
        /** Cached metric tags. */
        private KeyValuePair<string, object?>[]? _metricTags;

        /// <summary>
        /// Gets the metric tags.
        /// </summary>
        /// <returns>Metric tags for this node.</returns>
        internal ReadOnlySpan<KeyValuePair<string, object?>> GetMetricTags() =>
            _metricTags ??= new[]
            {
                new KeyValuePair<string, object?>(MetricTags.NodeAddress, Address.ToString()),
                new KeyValuePair<string, object?>(MetricTags.NodeName, Name)
            };
    }
}
