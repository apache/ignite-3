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

using System;
using Internal.Common;
using Internal.Proto;

/// <summary>
/// Basic authenticator with username and password.
/// <para />
/// Credentials are sent to the server in plain text, unless SSL/TLS is enabled - see
/// <see cref="IgniteClientConfiguration.SslStreamFactory"/>.
/// </summary>
public sealed class BasicAuthenticator : IAuthenticator
{
    private string _username = string.Empty;

    private string _password = string.Empty;

    /// <summary>
    /// Gets or sets the username.
    /// </summary>
    public string Username
    {
        get => _username;
        set => _username = value ?? throw new ArgumentNullException(nameof(value));
    }

    /// <summary>
    /// Gets or sets the password.
    /// </summary>
    public string Password
    {
        get => _password;
        set => _password = value ?? throw new ArgumentNullException(nameof(value));
    }

    /// <inheritdoc />
    public string Type => HandshakeExtensions.AuthenticationTypeBasic;

    /// <inheritdoc />
    public object Identity => Username;

    /// <inheritdoc />
    public object Secret => Password;

    /// <inheritdoc />
    public override string ToString() =>
        new IgniteToStringBuilder(GetType())
            .Append(Username) // Password is not included intentionally.
            .ToString();
}
