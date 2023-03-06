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
using System.Threading.Tasks;
using Log;
using NUnit.Framework;

/// <summary>
/// SSL tests.
/// </summary>
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
            SslStreamFactory = new SslStreamFactory { SkipServerCertificateValidation = true }
        };

        using var client = await IgniteClient.StartAsync(cfg);

        var connection = client.GetConnections().Single();
        var sslInfo = connection.SslInfo;

        Assert.IsNotNull(sslInfo);
        Assert.IsFalse(sslInfo!.IsMutuallyAuthenticated);
        Assert.AreEqual(TlsCipherSuite.TLS_AES_256_GCM_SHA384.ToString(), sslInfo.NegotiatedCipherSuiteName);
        Assert.AreEqual(SslProtocols.Tls13, sslInfo.SslProtocol);
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
                SkipServerCertificateValidation = true,
                CertificatePath = CertificatePath,
                CertificatePassword = CertificatePassword
            },
            Logger = new ConsoleLogger { MinLevel = LogLevel.Trace }
        };

        using var client = await IgniteClient.StartAsync(cfg);

        var connection = client.GetConnections().Single();
        var sslInfo = connection.SslInfo;

        Assert.IsNotNull(sslInfo);
        Assert.IsTrue(sslInfo!.IsMutuallyAuthenticated);
        Assert.AreEqual(TlsCipherSuite.TLS_AES_256_GCM_SHA384.ToString(), sslInfo.NegotiatedCipherSuiteName);
        Assert.AreEqual("127.0.0.1", sslInfo.TargetHostName);
        StringAssert.Contains(CertificateIssuer, sslInfo.RemoteCertificate!.Issuer);
        StringAssert.Contains(CertificateIssuer, sslInfo.LocalCertificate!.Issuer);
    }

    [Test]
    [SuppressMessage("Security", "CA5398:Avoid hardcoded SslProtocols values", Justification = "Tests.")]
    public void TestSslOnClientWithoutSslOnServerThrows()
    {
        var cfg = GetConfig();
        cfg.SslStreamFactory = new SslStreamFactory
        {
            SslProtocols = SslProtocols.Tls13 | SslProtocols.Tls12
        };

        var ex = Assert.CatchAsync<Exception>(async () => await IgniteClient.StartAsync(cfg));
        Assert.IsInstanceOf<IgniteClientConnectionException>(ex?.GetBaseException());
    }

    [Test]
    public void TestSslOnServerWithoutSslOnClientThrows()
    {
        var cfg = new IgniteClientConfiguration
        {
            Endpoints = { SslEndpoint }
        };

        var ex = Assert.CatchAsync<Exception>(async () => await IgniteClient.StartAsync(cfg));
        Assert.IsInstanceOf<IgniteClientConnectionException>(ex?.GetBaseException());
    }

    [Test]
    public void TestMissingClientCertThrows()
    {
        var cfg = new IgniteClientConfiguration
        {
            Endpoints = { SslEndpointWithClientAuth },
            SslStreamFactory = new SslStreamFactory
            {
                SkipServerCertificateValidation = true,
                CheckCertificateRevocation = true
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
        Assert.IsNull(client.GetConnections().Single().SslInfo);
    }

    private class NullSslStreamFactory : ISslStreamFactory
    {
        public SslStream? Create(Stream stream, string targetHost) => null;
    }

    private class CustomSslStreamFactory : ISslStreamFactory
    {
        public SslStream Create(Stream stream, string targetHost)
        {
            var sslStream = new SslStream(
                innerStream: stream,
                leaveInnerStreamOpen: false,
                userCertificateValidationCallback: (_, certificate, _, _) => certificate!.Issuer.Contains("ignite"),
                userCertificateSelectionCallback: null);

            sslStream.AuthenticateAsClient(targetHost, null, SslProtocols.None, false);

            return sslStream;
        }
    }
}
