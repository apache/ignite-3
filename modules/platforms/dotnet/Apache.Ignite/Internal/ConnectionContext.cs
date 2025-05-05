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

namespace Apache.Ignite.Internal
{
    using System;
    using System.Collections.Generic;
    using Ignite.Network;
    using Network;
    using Proto;

    /// <summary>
    /// Socket connection context.
    /// </summary>
    /// <param name="Version">Protocol version.</param>
    /// <param name="IdleTimeout">Server idle timeout.</param>
    /// <param name="ClusterNode">Cluster node.</param>
    /// <param name="ClusterIds">Cluster ids, from oldest to newest.</param>
    /// <param name="ClusterName">Cluster name.</param>
    /// <param name="SslInfo">SSL info.</param>
    /// <param name="Features">Protocol features.</param>
    internal record ConnectionContext(
        ClientProtocolVersion Version,
        TimeSpan IdleTimeout,
        ClusterNode ClusterNode,
        IReadOnlyList<Guid> ClusterIds,
        string ClusterName,
        ISslInfo? SslInfo,
        ProtocolBitmaskFeature Features)
    {
        /// <summary>
        /// Gets the current cluster id.
        /// </summary>
        public Guid ClusterId => ClusterIds[^1];

        /// <summary>
        /// Gets a value indicating whether the server supports the specified feature.
        /// </summary>
        /// <param name="feature">Feature flag.</param>
        /// <returns>True if the server supports the specified feature; false otherwise.</returns>
        public bool ServerHasFeature(ProtocolBitmaskFeature feature) =>
            (Features & feature) == feature;
    }
}
