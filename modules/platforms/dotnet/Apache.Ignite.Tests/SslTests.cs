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
    [Test]
    [SuppressMessage("Security", "CA5398:Avoid hardcoded SslProtocols values", Justification = "Tests")]
    public async Task TestSslWithoutClientAuthentication()
    {
        var cfg = new IgniteClientConfiguration
        {
            Endpoints = { "127.0.0.1:" + (ServerPort + 1) },
            SslStreamFactory = new SslStreamFactory { SkipServerCertificateValidation = true }
        };

        using var client = await IgniteClient.StartAsync(cfg);

        var connection = client.GetConnections().Single();
        var sslInfo = connection.SslInfo;

        Assert.IsNotNull(sslInfo);
        Assert.IsFalse(sslInfo!.IsMutuallyAuthenticated);
        Assert.AreEqual(TlsCipherSuite.TLS_AES_256_GCM_SHA384.ToString(), sslInfo.NegotiatedCipherSuiteName);
        Assert.AreEqual(SslProtocols.Tls13, sslInfo.SslProtocol);
        Assert.AreEqual("127.0.0.1", sslInfo.TargetHostName);
        Assert.IsNull(sslInfo.LocalCertificate);

        StringAssert.Contains(
            "E=dev@ignite.apache.org, OU=Apache Ignite CA, O=The Apache Software Foundation, CN=ignite.apache.org",
            sslInfo.RemoteCertificate!.Issuer);
    }

    [Test]
    public async Task TestSslWithClientAuthentication()
    {
        var cfg = new IgniteClientConfiguration
        {
            Endpoints = { "127.0.0.1:" + (ServerPort + 2) },
            SslStreamFactory = new SslStreamFactory
            {
                SkipServerCertificateValidation = true,
                CertificatePath = Path.Combine(
                    TestUtils.RepoRootDir, "modules", "runner", "src", "integrationTest", "resources", "ssl", "client.pfx"),
                CertificatePassword = "123456"
            }
        };

        using var client = await IgniteClient.StartAsync(cfg);

        var connection = client.GetConnections().Single();
        var sslInfo = connection.SslInfo;

        Assert.IsNotNull(sslInfo);
        Assert.IsTrue(sslInfo!.IsMutuallyAuthenticated);
        Assert.AreEqual(TlsCipherSuite.TLS_AES_256_GCM_SHA384.ToString(), sslInfo.NegotiatedCipherSuiteName);
        Assert.AreEqual("127.0.0.1", sslInfo.TargetHostName);

        StringAssert.Contains(
            "E=dev@ignite.apache.org, OU=Apache Ignite CA, O=The Apache Software Foundation, CN=ignite.apache.org",
            sslInfo.RemoteCertificate!.Issuer);

        StringAssert.Contains(
            "E=dev@ignite.apache.org, OU=Apache Ignite CA, O=The Apache Software Foundation, CN=ignite.apache.org",
            sslInfo.LocalCertificate!.Issuer);
    }

    [Test]
    public void TestSslOnClientWithoutSslOnServerThrows()
    {
        var cfg = GetConfig();

        cfg.SslStreamFactory = new SslStreamFactory();

        var ex = Assert.ThrowsAsync<AggregateException>(async () => await IgniteClient.StartAsync(cfg));
        Assert.IsInstanceOf<IgniteClientConnectionException>(ex?.GetBaseException());
    }

    [Test]
    public void TestMissingClientCertThrows()
    {
        Assert.Fail("TODO");
    }

    [Test]
    public void TestCustomSslStreamFactory()
    {
        // TODO: Test server cert validation?
        Assert.Fail("TODO");
    }
}
