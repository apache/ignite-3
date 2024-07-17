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

namespace Apache.Ignite.Network;

using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;

/// <summary>
/// SSL info.
/// </summary>
public interface ISslInfo
{
    /// <summary>
    /// Gets the cipher suite which was negotiated for this connection.
    /// </summary>
    string NegotiatedCipherSuiteName { get; }

    /// <summary>
    /// Gets the certificate used to authenticate the local endpoint.
    /// </summary>
    X509Certificate? LocalCertificate { get; }

    /// <summary>
    /// Gets the certificate used to authenticate the remote endpoint.
    /// </summary>
    X509Certificate? RemoteCertificate { get; }

    /// <summary>
    /// Gets the name of the server the client is trying to connect to. That name is used for server certificate validation.
    /// It can be a DNS name or an IP address.
    /// </summary>
    string TargetHostName { get; }

    /// <summary>
    /// Gets a value indicating whether both server and client have been authenticated.
    /// </summary>
    bool IsMutuallyAuthenticated { get; }

    /// <summary>
    /// Gets the SSL protocol.
    /// </summary>
    SslProtocols SslProtocol { get; }
}
