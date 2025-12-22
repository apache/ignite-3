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

namespace Apache.Ignite.Sql;

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;

/// <summary>
/// Ignite connection string builder.
/// </summary>
[SuppressMessage("Design", "CA1010:Generic interface should also be implemented", Justification = "Reviewed.")]
public sealed class IgniteDbConnectionStringBuilder : DbConnectionStringBuilder
{
    /// <summary>
    /// Gets the character used to separate multiple endpoints in the connection string.
    /// </summary>
    public const char EndpointSeparator = ',';

    private static readonly IReadOnlySet<string> KnownKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        nameof(Endpoints),
        nameof(SocketTimeout),
        nameof(OperationTimeout),
        nameof(HeartbeatInterval),
        nameof(ReconnectInterval),
        nameof(SslEnabled),
        nameof(Username),
        nameof(Password),
        nameof(ReResolveAddressesInterval)
    };

    /// <summary>
    /// Initializes a new instance of the <see cref="IgniteDbConnectionStringBuilder"/> class.
    /// </summary>
    public IgniteDbConnectionStringBuilder()
    {
        // No-op.
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="IgniteDbConnectionStringBuilder"/> class.
    /// </summary>
    /// <param name="connectionString">Connection string.</param>
    public IgniteDbConnectionStringBuilder(string connectionString)
    {
        ConnectionString = connectionString;
    }

    /// <summary>
    /// Gets or sets the Ignite endpoints.
    /// Multiple endpoints can be specified separated by comma, e.g. "localhost:10800,localhost:10801".
    /// If the port is not specified, the default port 10800 is used.
    /// </summary>
    [SuppressMessage("Usage", "CA2227:Collection properties should be read only", Justification = "Reviewed.")]
    public IList<string> Endpoints
    {
        get => this[nameof(Endpoints)] is string endpoints
            ? endpoints.Split(EndpointSeparator, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            : [];
        set => this[nameof(Endpoints)] = string.Join(EndpointSeparator, value);
    }

    /// <summary>
    /// Gets or sets the socket timeout. See <see cref="IgniteClientConfiguration.SocketTimeout"/> for more details.
    /// </summary>
    public TimeSpan SocketTimeout
    {
        get => GetString(nameof(SocketTimeout)) is { } s
            ? TimeSpan.Parse(s, CultureInfo.InvariantCulture)
            : IgniteClientConfiguration.DefaultSocketTimeout;
        set => this[nameof(SocketTimeout)] = value.ToString();
    }

    /// <summary>
    /// Gets or sets the operation timeout. See <see cref="IgniteClientConfiguration.OperationTimeout"/> for more details.
    /// </summary>
    public TimeSpan OperationTimeout
    {
        get => GetString(nameof(OperationTimeout)) is { } s
            ? TimeSpan.Parse(s, CultureInfo.InvariantCulture)
            : IgniteClientConfiguration.DefaultOperationTimeout;
        set => this[nameof(OperationTimeout)] = value.ToString();
    }

    /// <summary>
    /// Gets or sets the heartbeat interval. See <see cref="IgniteClientConfiguration.HeartbeatInterval"/> for more details.
    /// </summary>
    public TimeSpan HeartbeatInterval
    {
        get => GetString(nameof(HeartbeatInterval)) is { } s
            ? TimeSpan.Parse(s, CultureInfo.InvariantCulture)
            : IgniteClientConfiguration.DefaultHeartbeatInterval;
        set => this[nameof(HeartbeatInterval)] = value.ToString();
    }

    /// <summary>
    /// Gets or sets the reconnect interval. See <see cref="IgniteClientConfiguration.ReconnectInterval"/> for more details.
    /// </summary>
    public TimeSpan ReconnectInterval
    {
        get => GetString(nameof(ReconnectInterval)) is { } s
            ? TimeSpan.Parse(s, CultureInfo.InvariantCulture)
            : IgniteClientConfiguration.DefaultReconnectInterval;
        set => this[nameof(ReconnectInterval)] = value.ToString();
    }

    /// <summary>
    /// Gets or sets a value indicating whether SSL is enabled.
    /// </summary>
    public bool SslEnabled
    {
        get => GetString(nameof(SslEnabled)) is { } s && bool.Parse(s);
        set => this[nameof(SslEnabled)] = value.ToString();
    }

    /// <summary>
    /// Gets or sets the username for authentication.
    /// </summary>
    public string? Username
    {
        get => GetString(nameof(Username));
        set => this[nameof(Username)] = value;
    }

    /// <summary>
    /// Gets or sets the password for authentication.
    /// </summary>
    public string? Password
    {
        get => GetString(nameof(Password));
        set => this[nameof(Password)] = value;
    }

    /// <summary>
    /// Gets or sets the re-resolve interval. See <see cref="IgniteClientConfiguration.ReResolveAddressesInterval"/> for more details.
    /// </summary>
    public TimeSpan ReResolveAddressesInterval
    {
        get => GetString(nameof(ReResolveAddressesInterval)) is { } s
            ? TimeSpan.Parse(s, CultureInfo.InvariantCulture)
            : IgniteClientConfiguration.DefaultReResolveAddressesInterval;
        set => this[nameof(ReResolveAddressesInterval)] = value.ToString();
    }

    /// <inheritdoc />
    [AllowNull]
    public override object this[string keyword]
    {
        get => base[keyword];
        set
        {
            if (!KnownKeys.Contains(keyword))
            {
                throw new ArgumentException($"Unknown connection string key: '{keyword}'.", nameof(keyword));
            }

            base[keyword] = value;
        }
    }

    /// <summary>
    /// Converts this instance to <see cref="IgniteClientConfiguration"/>.
    /// </summary>
    /// <returns>Ignite client configuration.</returns>
    public IgniteClientConfiguration ToIgniteClientConfiguration()
    {
        return new IgniteClientConfiguration([.. Endpoints])
        {
            SocketTimeout = SocketTimeout,
            OperationTimeout = OperationTimeout,
            HeartbeatInterval = HeartbeatInterval,
            ReconnectInterval = ReconnectInterval,
            SslStreamFactory = SslEnabled ? new SslStreamFactory() : null,
            Authenticator = Username is null && Password is null ? null : new BasicAuthenticator
            {
                Username = Username ?? string.Empty,
                Password = Password ?? string.Empty
            },
            ReResolveAddressesInterval = ReResolveAddressesInterval
        };
    }

    private string? GetString(string key) => TryGetValue(key, out var s) ? (string?)s : null;
}
