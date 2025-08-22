// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

namespace Apache.Ignite.Sql;

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using Internal.Common;

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
    public ICollection<string> Endpoints
    {
        get => this[nameof(IgniteClientConfiguration.Endpoints)] is string endpoints
            ? endpoints.Split(EndpointSeparator, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            : [];
        set => this[nameof(IgniteClientConfiguration.Endpoints)] = string.Join(EndpointSeparator, value);
    }

    /// <summary>
    /// Gets or sets the socket timeout. See <see cref="IgniteClientConfiguration.SocketTimeout"/> for more details.
    /// </summary>
    public TimeSpan SocketTimeout
    {
        get => this[nameof(IgniteClientConfiguration.SocketTimeout)] is string s
            ? TimeSpan.Parse(s, CultureInfo.InvariantCulture)
            : IgniteClientConfiguration.DefaultSocketTimeout;
        set => this[nameof(IgniteClientConfiguration.SocketTimeout)] = value.ToString();
    }

    /// <summary>
    /// Converts this instance to <see cref="IgniteClientConfiguration"/>.
    /// </summary>
    /// <returns>Ignite client configuration.</returns>
    public IgniteClientConfiguration ToIgniteClientConfiguration()
    {
        return new IgniteClientConfiguration([.. Endpoints])
        {
            SocketTimeout = SocketTimeout
        };
    }
}
