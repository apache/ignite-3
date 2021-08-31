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
    using System.Diagnostics;
    using System.Linq;
    using System.Net;
    using System.Net.Sockets;
    using System.Threading;
    using System.Threading.Tasks;
    using Log;

    /// <summary>
    /// Client socket wrapper with reconnect/failover functionality.
    /// </summary>
    internal class ClientFailoverSocket
    {
        /** Client configuration. */
        private readonly IgniteClientConfiguration _configuration;

        /** Logger. */
        private readonly IIgniteLogger? _logger;

        /** Endpoints with corresponding hosts - from configuration. */
        private readonly IReadOnlyList<SocketEndpoint> _endPoints;

        /// <summary>
        /// Initializes a new instance of the <see cref="ClientFailoverSocket"/> class.
        /// </summary>
        /// <param name="configuration">Client configuration.</param>
        public ClientFailoverSocket(IgniteClientConfiguration configuration)
        {
            _configuration = new IgniteClientConfiguration(configuration);
            _logger = _configuration.Logger;
            _endPoints = GetIpEndPoints(configuration).ToList();
        }

        /// <summary>
        /// Gets next connected socket, or connects a new one.
        /// </summary>
        private ClientSocket GetNextSocket()
        {
            List<Exception> errors = null;
            var startIdx = (int) Interlocked.Increment(ref _endPointIndex);

            // Check socket map first, if available: it includes all cluster nodes.
            var map = _nodeSocketMap;
            foreach (var socket in map.Values)
            {
                if (!socket.IsDisposed)
                {
                    return socket;
                }
            }

            // Fall back to initially known endpoints.
            for (var i = 0; i < _endPoints.Count; i++)
            {
                var idx = (startIdx + i) % _endPoints.Count;
                var endPoint = _endPoints[idx];

                if (endPoint.Socket != null && !endPoint.Socket.IsDisposed)
                {
                    return endPoint.Socket;
                }

                try
                {
                    return Connect(endPoint);
                }
                catch (SocketException e)
                {
                    if (errors == null)
                    {
                        errors = new List<Exception>();
                    }

                    errors.Add(e);
                }
            }

            throw new AggregateException(
                "Failed to establish Ignite thin client connection, examine inner exceptions for details.", errors);
        }

        /// <summary>
        /// Connects to the given endpoint.
        /// </summary>
        private async Task<ClientSocket> ConnectAsync(SocketEndpoint endPoint)
        {
            var socket = await ClientSocket.ConnectAsync(endPoint.EndPoint, _logger).ConfigureAwait(false);

            endPoint.Socket = socket;

            return socket;
        }

        /// <summary>
        /// Gets the endpoints: all combinations of IP addresses and ports according to configuration.
        /// </summary>
        private IEnumerable<SocketEndpoint> GetIpEndPoints(IgniteClientConfiguration cfg)
        {
            foreach (var e in Endpoint.GetEndpoints(cfg))
            {
                var host = e.Host;
                Debug.Assert(host != null, "host != null");  // Checked by GetEndpoints.

                for (var port = e.Port; port <= e.PortRange + e.Port; port++)
                {
                    foreach (var ip in GetIps(e.Host))
                    {
                        yield return new SocketEndpoint(new IPEndPoint(ip, port), e.Host);
                    }
                }
            }
        }

        /// <summary>
        /// Gets IP address list from a given host.
        /// When host is an IP already - parses it. Otherwise, resolves DNS name to IPs.
        /// </summary>
        private IEnumerable<IPAddress> GetIps(string host, bool suppressExceptions = false)
        {
            try
            {
                IPAddress ip;

                // GetHostEntry accepts IPs, but TryParse is a more efficient shortcut.
                return IPAddress.TryParse(host, out ip) ? new[] {ip} : Dns.GetHostEntry(host).AddressList;
            }
            catch (SocketException e)
            {
                _logger?.Debug(e, "Failed to parse host: " + host);

                if (suppressExceptions)
                {
                    return Enumerable.Empty<IPAddress>();
                }

                throw;
            }
        }
    }
}
