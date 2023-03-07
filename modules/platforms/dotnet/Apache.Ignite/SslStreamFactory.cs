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

using System.ComponentModel;
using System.IO;
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using Internal.Common;

/// <summary>
/// Standard SSL stream factory. Can be used with or without client-side certificates.
/// </summary>
public sealed class SslStreamFactory : ISslStreamFactory
{
    /// <summary>
    /// Default SSL protocols.
    /// </summary>
    public const SslProtocols DefaultSslProtocols = SslProtocols.None;

    /// <summary>
    /// Gets or sets the certificate file path (see <see cref="X509Certificate2"/>).
    /// </summary>
    public string? CertificatePath { get; set; }

    /// <summary>
    /// Gets or sets the certificate file password (see <see cref="X509Certificate2"/>).
    /// </summary>
    public string? CertificatePassword { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether to ignore invalid remote (server) certificates.
    /// This may be useful for testing with self-signed certificates.
    /// </summary>
    public bool SkipServerCertificateValidation { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether the certificate revocation list is checked during authentication.
    /// </summary>
    public bool CheckCertificateRevocation { get; set; }

    /// <summary>
    /// Gets or sets the SSL protocols.
    /// </summary>
    [DefaultValue(DefaultSslProtocols)]
    public SslProtocols SslProtocols { get; set; } = DefaultSslProtocols;

    /// <inheritdoc />
    public SslStream Create(Stream stream, string targetHost)
    {
        IgniteArgumentCheck.NotNull(stream, "stream");

        var sslStream = new SslStream(stream, false, ValidateServerCertificate, null);

        var cert = string.IsNullOrEmpty(CertificatePath)
            ? null
            : new X509Certificate2(CertificatePath, CertificatePassword);

        var certs = cert == null
            ? null
            : new X509CertificateCollection(new X509Certificate[] { cert });

        sslStream.AuthenticateAsClient(targetHost, certs, SslProtocols, CheckCertificateRevocation);

        return sslStream;
    }

    /// <summary>
    /// Validates the server certificate.
    /// </summary>
    private bool ValidateServerCertificate(
        object sender,
        X509Certificate? certificate,
        X509Chain? chain,
        SslPolicyErrors sslPolicyErrors)
    {
        if (SkipServerCertificateValidation)
        {
            return true;
        }

        if (sslPolicyErrors == SslPolicyErrors.None)
        {
            return true;
        }

        return false;
    }
}
