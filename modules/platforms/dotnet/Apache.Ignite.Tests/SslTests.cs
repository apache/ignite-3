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
using System.Linq;
using System.Net.Security;
using System.Threading.Tasks;
using Log;
using NUnit.Framework;

/// <summary>
/// SSL tests.
/// </summary>
public class SslTests : IgniteTestsBase
{
    [Test]
    public async Task TestSslWithoutClientAuthentication()
    {
        var cfg = new IgniteClientConfiguration
        {
            Endpoints = { "127.0.0.1:" + (ServerPort + 1) },
            Logger = new ConsoleLogger { MinLevel = LogLevel.Trace },
            SslStreamFactory = new SslStreamFactory { SkipServerCertificateValidation = true }
        };

        using var client = await IgniteClient.StartAsync(cfg);

        var connection = client.GetConnections().Single();
        var sslInfo = connection.SslInfo;

        Assert.IsNotNull(sslInfo);
        Assert.IsFalse(sslInfo!.IsMutuallyAuthenticated);
        Assert.AreEqual(TlsCipherSuite.TLS_AES_256_GCM_SHA384.ToString(), sslInfo.NegotiatedCipherSuiteName);
        Assert.AreEqual("127.0.0.1", sslInfo.TargetHostName);
        Assert.IsNull(sslInfo.LocalCertificate);
        Assert.AreEqual(
            "E=dev@ignite.apache.org, CN=ignite.apache.org, OU=dev, O=apache ignite, L=London, S=US, C=US",
            sslInfo.RemoteCertificate!.Issuer);
    }

    [Test]
    public async Task TestSslWithClientAuthentication()
    {
        var cfg = new IgniteClientConfiguration
        {
            Endpoints = { "127.0.0.1:" + (ServerPort + 2) },
            Logger = new ConsoleLogger { MinLevel = LogLevel.Trace },
            SslStreamFactory = new SslStreamFactory
            {
                SkipServerCertificateValidation = true,
                CertificatePath = "TODO",
                CertificatePassword = "changeit"
            }
        };

        using var client = await IgniteClient.StartAsync(cfg);

        var connection = client.GetConnections().Single();
        var sslInfo = connection.SslInfo;

        Assert.IsNotNull(sslInfo);
        Assert.IsFalse(sslInfo!.IsMutuallyAuthenticated);
        Assert.AreEqual(TlsCipherSuite.TLS_AES_256_GCM_SHA384.ToString(), sslInfo.NegotiatedCipherSuiteName);
        Assert.AreEqual("127.0.0.1", sslInfo.TargetHostName);
        Assert.IsNull(sslInfo.LocalCertificate);
        Assert.AreEqual(
            "E=dev@ignite.apache.org, CN=ignite.apache.org, OU=dev, O=apache ignite, L=London, S=US, C=US",
            sslInfo.RemoteCertificate!.Issuer);
    }

    [Test]
    public void TestSslOnClientWithoutSslOnServerThrows()
    {
        var cfg = GetConfig();

        cfg.SslStreamFactory = new SslStreamFactory();

        var ex = Assert.ThrowsAsync<AggregateException>(async () => await IgniteClient.StartAsync(cfg));
        Assert.IsInstanceOf<IgniteClientConnectionException>(ex?.GetBaseException());
    }
}
