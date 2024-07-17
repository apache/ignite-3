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
    using System.Collections.Generic;
    using System.ComponentModel;
    using Common;

    /// <summary>
    /// Internal representation of a client endpoint.
    /// </summary>
    internal record Endpoint
    {
        /** */
        private const char HostSeparator = ':';

        /// <summary>
        /// Initializes a new instance of the <see cref="Endpoint"/> class.
        /// </summary>
        private Endpoint(string host, int port = IgniteClientConfiguration.DefaultPort)
        {
            Host = IgniteArgumentCheck.NotNullOrEmpty(host);
            Port = port;
        }

        /// <summary>
        /// Gets the host.
        /// </summary>
        public string Host { get; }

        /// <summary>
        /// Gets the port.
        /// </summary>
        [DefaultValue(IgniteClientConfiguration.DefaultPort)]
        public int Port { get; }

        /// <summary>
        /// Gets the client endpoints from given configuration.
        /// </summary>
        /// <param name="cfg">Client configuration.</param>
        /// <returns>Parsed endpoints.</returns>
        public static IEnumerable<Endpoint> GetEndpoints(IgniteClientConfiguration cfg)
        {
            foreach (var endpoint in cfg.Endpoints)
            {
                yield return ParseEndpoint(endpoint);
            }
        }

        /// <summary>
        /// Parses the endpoint string.
        /// </summary>
        /// <param name="endpoint">Endpoint.</param>
        /// <returns>Parsed endpoint.</returns>
        public static Endpoint ParseEndpoint(string endpoint)
        {
            if (string.IsNullOrWhiteSpace(endpoint))
            {
                throw new IgniteClientException(
                    ErrorGroups.Client.Configuration,
                    "IgniteClientConfiguration.Endpoints[...] can't be null or whitespace.");
            }

            var idx = endpoint.LastIndexOf(HostSeparator);

            if (idx == -1)
            {
                return new Endpoint(endpoint);
            }

            var host = endpoint.Substring(0, idx);
            var port = endpoint.Substring(idx + 1);

            return new Endpoint(host, ParsePort(endpoint, port));
        }

        /// <summary>
        /// Parses the port string.
        /// </summary>
        private static int ParsePort(string endpoint, string portString)
        {
            int port;

            if (int.TryParse(portString, out port))
            {
                return port;
            }

            throw new IgniteClientException(
                ErrorGroups.Client.Configuration,
                $"Unrecognized format of IgniteClientConfiguration.Endpoint, failed to parse port: '{endpoint}'");
        }
    }
}
