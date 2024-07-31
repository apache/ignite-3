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

namespace Apache.Ignite.Internal.Network;

using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using Ignite.Network;

/// <summary>
/// SSL info.
/// </summary>
/// <param name="TargetHostName">Target host name.</param>
/// <param name="NegotiatedCipherSuiteName">Negotiated cipher suite name.</param>
/// <param name="IsMutuallyAuthenticated">Whether client and server are mutually authenticated.</param>
/// <param name="LocalCertificate">Local certificate.</param>
/// <param name="RemoteCertificate">Remote certificate.</param>
/// <param name="SslProtocol">SSL protocol.</param>
internal sealed record SslInfo(
    string TargetHostName,
    string NegotiatedCipherSuiteName,
    bool IsMutuallyAuthenticated,
    X509Certificate? LocalCertificate,
    X509Certificate? RemoteCertificate,
    SslProtocols SslProtocol) : ISslInfo;
