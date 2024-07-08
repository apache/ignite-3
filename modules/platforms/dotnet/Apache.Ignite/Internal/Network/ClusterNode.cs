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
    using System.Diagnostics;
    using System.Net;
    using Ignite.Network;
    using Proto.MsgPack;

    /// <summary>
    /// Cluster node.
    /// </summary>
    internal sealed record ClusterNode : IClusterNode
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ClusterNode"/> class.
        /// </summary>
        /// <param name="id">Id.</param>
        /// <param name="name">Name.</param>
        /// <param name="endpoint">Endpoint.</param>
        /// <param name="metricsContext">Metrics context.</param>
        internal ClusterNode(string id, string name, IPEndPoint endpoint, MetricsContext? metricsContext = null)
        {
            Id = id;
            Name = name;
            Address = endpoint;
            MetricsContext = metricsContext;
        }

        /// <inheritdoc/>
        public string Id { get; }

        /// <inheritdoc/>
        public string Name { get; }

        /// <inheritdoc/>
        public IPEndPoint Address { get; }

        /// <summary>
        /// Gets the metric tags.
        /// </summary>
        /// <returns>Metric tags for this node.</returns>
        internal MetricsContext? MetricsContext { get; }

        /// <inheritdoc/>
        public bool Equals(ClusterNode? other)
        {
            if (ReferenceEquals(null, other))
            {
                return false;
            }

            if (ReferenceEquals(this, other))
            {
                return true;
            }

            return Id == other.Id && Name == other.Name && Address.Equals(other.Address);
        }

        /// <inheritdoc/>
        public override int GetHashCode() => HashCode.Combine(Id, Name, Address);

        /// <summary>
        /// Read node from reader.
        /// </summary>
        /// <param name="r">Reader.</param>
        /// <returns>Cluster node.</returns>
        internal static ClusterNode Read(ref MsgPackReader r)
        {
            var fieldCount = r.ReadInt32();
            Debug.Assert(fieldCount == 4, "fieldCount == 4");

            var id = r.ReadString();
            var name = r.ReadString();
            var addr = r.ReadString();
            var port = r.ReadInt32();

            return new ClusterNode(id, name, new IPEndPoint(IPAddress.Parse(addr), port));
        }
    }
}
