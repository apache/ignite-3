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

namespace Apache.Ignite
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Linq;
    using System.Threading;
    using Internal.Common;
    using Microsoft.Extensions.Logging;
    using Microsoft.Extensions.Logging.Abstractions;

    /// <summary>
    /// Ignite client driver configuration.
    /// </summary>
    public sealed record IgniteClientConfiguration
    {
        /// <summary>
        /// Default port.
        /// </summary>
        public const int DefaultPort = 10800;

        /// <summary>
        /// Default socket timeout.
        /// </summary>
        public static readonly TimeSpan DefaultSocketTimeout = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Default socket timeout.
        /// </summary>
        public static readonly TimeSpan DefaultOperationTimeout = Timeout.InfiniteTimeSpan;

        /// <summary>
        /// Default heartbeat interval.
        /// </summary>
        public static readonly TimeSpan DefaultHeartbeatInterval = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Default reconnect interval.
        /// </summary>
        public static readonly TimeSpan DefaultReconnectInterval = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteClientConfiguration"/> class.
        /// </summary>
        public IgniteClientConfiguration()
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteClientConfiguration"/> class.
        /// </summary>
        /// <param name="endpoints">Endpoints.</param>
        public IgniteClientConfiguration(params string[] endpoints)
            : this()
        {
            IgniteArgumentCheck.NotNull(endpoints);

            foreach (var endpoint in endpoints)
            {
                Endpoints.Add(endpoint);
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteClientConfiguration"/> class.
        /// </summary>
        /// <param name="other">Other configuration.</param>
        public IgniteClientConfiguration(IgniteClientConfiguration other)
        {
            IgniteArgumentCheck.NotNull(other);

            LoggerFactory = other.LoggerFactory;
            SocketTimeout = other.SocketTimeout;
            OperationTimeout = other.OperationTimeout;
            Endpoints = other.Endpoints.ToList();
            RetryPolicy = other.RetryPolicy;
            HeartbeatInterval = other.HeartbeatInterval;
            ReconnectInterval = other.ReconnectInterval;
            SslStreamFactory = other.SslStreamFactory;
            Authenticator = other.Authenticator;
        }

        /// <summary>
        /// Gets or sets the logger factory. Default is <see cref="NullLoggerFactory.Instance"/>.
        /// </summary>
        public ILoggerFactory LoggerFactory { get; set; } = NullLoggerFactory.Instance;

        /// <summary>
        /// Gets or sets the socket timeout.
        /// <para />
        /// The timeout applies to the initial handshake procedure and heartbeats (see <see cref="HeartbeatInterval"/>).
        /// If the server does not respond to the initial handshake message or a periodic heartbeat in the specified time,
        /// the connection is closed with a <see cref="TimeoutException"/>.
        /// <para />
        /// Use <see cref="Timeout.InfiniteTimeSpan"/> for infinite timeout.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "00:00:30")]
        public TimeSpan SocketTimeout { get; set; } = DefaultSocketTimeout;

        /// <summary>
        /// Gets or sets the operation timeout. Default is <see cref="Timeout.InfiniteTimeSpan"/> (no timeout).
        /// <para />
        /// An "operation" is a single client request to the server. Some public API calls may involve multiple operations, in
        /// which case the operation timeout is applied to each individual network call.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "-00:00:00.001")]
        public TimeSpan OperationTimeout { get; set; } = DefaultOperationTimeout;

        /// <summary>
        /// Gets endpoints to connect to.
        /// <para />
        /// Providing addresses of multiple nodes in the cluster will improve performance:
        /// Ignite will balance requests across all connections, and use partition awareness to send key-based requests
        /// directly to the primary node.
        /// <para />
        /// Examples of supported formats:
        ///  * 192.168.1.25 (default port is used, see <see cref="DefaultPort"/>).
        ///  * 192.168.1.25:780 (custom port)
        ///  * 192.168.1.25:780..787 (custom port range)
        ///  * my-host.com (default port is used, see <see cref="DefaultPort"/>).
        ///  * my-host.com:780 (custom port)
        ///  * my-host.com:780..787 (custom port range).
        /// </summary>
        public IList<string> Endpoints { get; } = new List<string>();

        /// <summary>
        /// Gets or sets the retry policy. When a request fails due to a connection error,
        /// Ignite will retry the request if the specified policy allows it.
        /// <para />
        /// Default is <see cref="RetryReadPolicy"/> - retry read operations up to <see cref="RetryLimitPolicy.DefaultRetryLimit"/> times.
        /// <para />
        /// See also <see cref="RetryLimitPolicy"/>, <see cref="RetryReadPolicy"/>, <see cref="RetryNonePolicy"/>,
        /// <see cref="RetryLimitPolicy.RetryLimit"/>.
        /// </summary>
        public IRetryPolicy RetryPolicy { get; set; } = new RetryReadPolicy();

        /// <summary>
        /// Gets or sets the heartbeat message interval.
        /// <para />
        /// Default is <see cref="DefaultHeartbeatInterval"/>.
        /// <para />
        /// When server-side idle timeout is not zero, effective heartbeat
        /// interval is set to <c>Min(HeartbeatInterval, IdleTimeout / 3)</c>.
        /// <para />
        /// When client connection is idle (no operations are performed), heartbeat messages are sent periodically
        /// to keep the connection alive and detect potential half-open state.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "00:00:30")]
        public TimeSpan HeartbeatInterval { get; set; } = DefaultHeartbeatInterval;

        /// <summary>
        /// Gets or sets the background reconnect interval.
        /// <para />
        /// Default is <see cref="DefaultReconnectInterval"/>. Set to <see cref="TimeSpan.Zero"/> to disable periodic reconnect.
        /// <para />
        /// Ignite balances requests across all healthy connections (when multiple endpoints are configured).
        /// Ignite also repairs connections on demand (when a request is made).
        /// However, "secondary" connections can be lost (due to network issues, or node restarts). This property controls how ofter Ignite
        /// client will check all configured endpoints and try to reconnect them in case of failure.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "00:00:30")]
        public TimeSpan ReconnectInterval { get; set; } = DefaultReconnectInterval;

        /// <summary>
        /// Gets or sets the SSL stream factory.
        /// <para />
        /// When not null, secure socket connection will be established.
        /// <para />
        /// See <see cref="SslStreamFactory"/>.
        /// </summary>
        public ISslStreamFactory? SslStreamFactory { get; set; }

        /// <summary>
        /// Gets or sets the authenticator. When null, no authentication is performed.
        /// <para />
        /// See <see cref="BasicAuthenticator"/>.
        /// </summary>
        public IAuthenticator? Authenticator { get; set; }
    }
}
