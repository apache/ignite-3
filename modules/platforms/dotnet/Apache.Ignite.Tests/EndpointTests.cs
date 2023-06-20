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

namespace Apache.Ignite.Tests
{
    using System.Linq;
    using Internal;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="Endpoint"/> class.
    /// </summary>
    public class EndpointTests
    {
        [Test]
        public void GetEndpointsInvalidConfigFormatThrowsIgniteClientException()
        {
            var ex = AssertThrowsClientException(string.Empty);
            Assert.AreEqual("IgniteClientConfiguration.Endpoints[...] can't be null or whitespace.", ex.Message);

            ex = AssertThrowsClientException("host:");
            Assert.AreEqual(
                "Unrecognized format of IgniteClientConfiguration.Endpoint, failed to parse port: 'host:'",
                ex.Message);

            ex = AssertThrowsClientException("host:port");
            Assert.AreEqual(
                "Unrecognized format of IgniteClientConfiguration.Endpoint, failed to parse port: 'host:port'",
                ex.Message);

            ex = AssertThrowsClientException("host:1..");
            Assert.AreEqual(
                "Unrecognized format of IgniteClientConfiguration.Endpoint, failed to parse port: 'host:1..'",
                ex.Message);

            ex = AssertThrowsClientException("host:1..2..3");
            Assert.AreEqual(
                "Unrecognized format of IgniteClientConfiguration.Endpoint, failed to parse port: 'host:1..2..3'",
                ex.Message);
        }

        [Test]
        public void GetEndpointsParsesPortsAndRanges()
        {
            const string ip = "1.2.3.4";
            const string host = "example.com";
            const int port = 678;

            var ipWithDefaultPort = Endpoint.GetEndpoints(new IgniteClientConfiguration(ip)).Single();
            Assert.AreEqual(ip, ipWithDefaultPort.Host);
            Assert.AreEqual(IgniteClientConfiguration.DefaultPort, ipWithDefaultPort.Port);

            var ipWithCustomPort = Endpoint
                .GetEndpoints(new IgniteClientConfiguration($"{ip}:{port}"))
                .Single();
            Assert.AreEqual(ip, ipWithCustomPort.Host);
            Assert.AreEqual(port, ipWithCustomPort.Port);

            var hostWithDefaultPort = Endpoint.GetEndpoints(new IgniteClientConfiguration(host)).Single();
            Assert.AreEqual(host, hostWithDefaultPort.Host);
            Assert.AreEqual(IgniteClientConfiguration.DefaultPort, hostWithDefaultPort.Port);

            var hostWithCustomPort = Endpoint
                .GetEndpoints(new IgniteClientConfiguration($"{host}:{port}"))
                .Single();
            Assert.AreEqual(host, hostWithCustomPort.Host);
            Assert.AreEqual(port, hostWithCustomPort.Port);
        }

        private static IgniteClientException AssertThrowsClientException(string endpoint)
        {
            var endpoints = Endpoint.GetEndpoints(new IgniteClientConfiguration(endpoint));

            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            return Assert.Throws<IgniteClientException>(() => endpoints.ToList())!;
        }
    }
}
