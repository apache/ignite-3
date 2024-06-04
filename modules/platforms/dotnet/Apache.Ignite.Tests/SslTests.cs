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

namespace Apache.Ignite.Tests;

using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NUnit.Framework;

/// <summary>
/// SSL tests.
/// </summary>
[SuppressMessage("Security", "CA5359:Do Not Disable Certificate Validation", Justification = "Tests.")]
public class SslTests : IgniteTestsBase
{
    private const string CertificatePassword = "123456";

    private const string CertificateIssuer =
        "E=dev@ignite.apache.org, OU=Apache Ignite CA, O=The Apache Software Foundation, CN=ignite.apache.org";

    private static readonly string CertificatePath = Path.Combine(
        TestUtils.RepoRootDir, "modules", "runner", "src", "integrationTest", "resources", "ssl", "client.pfx");

    private static string SslEndpoint => "localhost:" + (ServerPort + 2);

    private static string SslEndpointWithClientAuth => "127.0.0.1:" + (ServerPort + 3);

    [Test]
    [SuppressMessage("Security", "CA5398:Avoid hardcoded SslProtocols values", Justification = "Tests")]
    public async Task TestSslWithoutClientAuthentication()
    {
        var cfg = new IgniteClientConfiguration
        {
            Endpoints = { SslEndpoint },
            SslStreamFactory = new SslStreamFactory
            {
                SslClientAuthenticationOptions = new SslClientAuthenticationOptions
                {
                    RemoteCertificateValidationCallback = (_, _, _, _) => true
                }
            }
        };

        using var client = await IgniteClient.StartAsync(cfg);

        var connection = client.GetConnections().Single();
        var sslInfo = connection.SslInfo;

        Assert.IsNotNull(sslInfo);
        Assert.IsFalse(sslInfo!.IsMutuallyAuthenticated);
        StringAssert.StartsWith("TLS_", sslInfo.NegotiatedCipherSuiteName);
        CollectionAssert.Contains(new[] { SslProtocols.Tls13, SslProtocols.Tls12 }, sslInfo.SslProtocol);
        Assert.AreEqual("localhost", sslInfo.TargetHostName);
        Assert.IsNull(sslInfo.LocalCertificate);
        StringAssert.Contains(CertificateIssuer, sslInfo.RemoteCertificate!.Issuer);
    }

    [Test]
    public async Task TestSslWithClientAuthentication()
    {
        var cfg = new IgniteClientConfiguration
        {
            Endpoints = { SslEndpointWithClientAuth },
            SslStreamFactory = new SslStreamFactory
            {
                SslClientAuthenticationOptions = new()
                {
                    RemoteCertificateValidationCallback = (_, _, _, _) => true,
                    ClientCertificates = new X509Certificate2Collection(new X509Certificate2(CertificatePath, CertificatePassword))
                }
            },
            LoggerFactory = TestUtils.GetConsoleLoggerFactory(LogLevel.Trace)
        };

        using var client = await IgniteClient.StartAsync(cfg);

        var connection = client.GetConnections().Single();
        var sslInfo = connection.SslInfo;

        Assert.IsNotNull(sslInfo);
        Assert.IsTrue(sslInfo!.IsMutuallyAuthenticated);
        StringAssert.StartsWith("TLS_", sslInfo.NegotiatedCipherSuiteName);
        Assert.AreEqual("127.0.0.1", sslInfo.TargetHostName);
        StringAssert.Contains(CertificateIssuer, sslInfo.RemoteCertificate!.Issuer);
        StringAssert.Contains(CertificateIssuer, sslInfo.LocalCertificate!.Issuer);
    }

    [Test]
    [SuppressMessage("Security", "CA5398:Avoid hardcoded SslProtocols values", Justification = "Tests.")]
    public void TestSslOnClientWithoutSslOnServerThrows()
    {
        var cfg = GetConfig();
        cfg.SslStreamFactory = new SslStreamFactory();

        var ex = Assert.ThrowsAsync<AggregateException>(async () => await IgniteClient.StartAsync(cfg));
        Assert.IsInstanceOf<IgniteClientConnectionException>(ex?.InnerException);
    }

    [Test]
    public void TestSslOnServerWithoutSslOnClientThrows()
    {
        var cfg = new IgniteClientConfiguration
        {
            Endpoints = { SslEndpoint }
        };

        var ex = Assert.ThrowsAsync<AggregateException>(async () => await IgniteClient.StartAsync(cfg));
        Assert.IsInstanceOf<IgniteClientConnectionException>(ex?.InnerException);
    }

    [Test]
    public void TestMissingClientCertThrows()
    {
        var cfg = new IgniteClientConfiguration
        {
            Endpoints = { SslEndpointWithClientAuth },
            SslStreamFactory = new SslStreamFactory
            {
                SslClientAuthenticationOptions = new()
                {
                    RemoteCertificateValidationCallback = (_, _, _, _) => true,
                    CertificateRevocationCheckMode = X509RevocationMode.NoCheck
                }
            }
        };

        Assert.CatchAsync<Exception>(async () => await IgniteClient.StartAsync(cfg));
    }

    [Test]
    public async Task TestCustomSslStreamFactory()
    {
        var cfg = new IgniteClientConfiguration
        {
            Endpoints = { SslEndpoint },
            SslStreamFactory = new CustomSslStreamFactory()
        };

        using var client = await IgniteClient.StartAsync(cfg);

        var connection = client.GetConnections().Single();
        var sslInfo = connection.SslInfo;

        Assert.IsNotNull(sslInfo);
        Assert.IsFalse(sslInfo!.IsMutuallyAuthenticated);
    }

    [Test]
    public async Task TestSslStreamFactoryReturnsNullDisablesSsl()
    {
        var cfg = GetConfig();
        cfg.SslStreamFactory = new NullSslStreamFactory();

        using var client = await IgniteClient.StartAsync(cfg);
        Assert.IsNull(client.GetConnections().First().SslInfo);
    }

    [Test]
    public async Task TestCustomCipherSuite()
    {
        var cfg = new IgniteClientConfiguration
        {
            Endpoints = { SslEndpoint },
            SslStreamFactory = new SslStreamFactory
            {
                SslClientAuthenticationOptions = new SslClientAuthenticationOptions
                {
                    RemoteCertificateValidationCallback = (_, _, _, _) => true,
                    CipherSuitesPolicy = new CipherSuitesPolicy(new[]
                    {
                        TlsCipherSuite.TLS_AES_128_GCM_SHA256
                    })
                }
            }
        };

        using var client = await IgniteClient.StartAsync(cfg);

        var connection = client.GetConnections().Single();
        var sslInfo = connection.SslInfo;

        Assert.IsNotNull(sslInfo);
        Assert.IsFalse(sslInfo!.IsMutuallyAuthenticated);
        Assert.AreEqual(TlsCipherSuite.TLS_AES_128_GCM_SHA256.ToString(), sslInfo.NegotiatedCipherSuiteName);
    }

    private class NullSslStreamFactory : ISslStreamFactory
    {
        public Task<SslStream?> CreateAsync(Stream stream, string targetHost, CancellationToken cancellationToken) =>
            Task.FromResult<SslStream?>(null);
    }

    private class CustomSslStreamFactory : ISslStreamFactory
    {
        public async Task<SslStream?> CreateAsync(Stream stream, string targetHost, CancellationToken cancellationToken)
        {
            var sslStream = new SslStream(
                innerStream: stream,
                leaveInnerStreamOpen: false,
                userCertificateValidationCallback: (_, certificate, _, _) => certificate!.Issuer.Contains("ignite"),
                userCertificateSelectionCallback: null);

            await sslStream.AuthenticateAsClientAsync(targetHost, null, SslProtocols.None, false);

            return sslStream;
        }
    }
}
