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
    using System.Threading.Tasks;
    using Internal;
    using Internal.Buffers;
    using NUnit.Framework;

    /// <summary>
    /// Client startup and usage tests.
    /// </summary>
    public class IgniteClientTests : IgniteTestsBase
    {
        [Test]
        public void TestStartAsyncThrowsExceptionOnEmptyEndpoints()
        {
            var ex = Assert.ThrowsAsync<IgniteClientException>(
                async () => await IgniteClient.StartAsync(new IgniteClientConfiguration()));

            Assert.AreEqual("Invalid IgniteClientConfiguration: Endpoints is empty. Nowhere to connect.", ex!.Message);
        }

        [Test]
        public async Task TestStartAsyncConnectsToServer()
        {
            using var client = await IgniteClient.StartAsync(GetConfig());

            Assert.IsNotNull(client);

            var tables = await client.Tables.GetTablesAsync();

            Assert.Greater(tables.Count, 0);
        }

        [Test]
        public async Task TestToString()
        {
            var address = "127.0.0.1:" + ServerPort;
            var cfg = new IgniteClientConfiguration { Endpoints = { address } };
            using var client = await IgniteClient.StartAsync(cfg);
            var id = client.GetConnections().Single().Node.Id;

            var expected = $"IgniteClientInternal {{ Connections = [ ClusterNode {{ " +
                           $"Id = {id}, " +
                           $"Name = org.apache.ignite.internal.runner.app.PlatformTestNodeRunner, " +
                           $"Address = {address} }} ] }}";

            Assert.AreEqual(expected, client.ToString());
        }

        [Test]
        public async Task TestHeartbeat()
        {
            using var client = await IgniteClient.StartAsync(GetConfig());
            IgniteClientInternal clientInternal = (IgniteClientInternal)client;

            var sockets = clientInternal.Socket.GetSockets().ToList();

            using var payload = new PooledArrayBuffer();
            payload.MessageWriter.Write("foo bar baz");

            foreach (var socket in sockets)
            {
                await socket.HeartbeatAsync();
                await socket.HeartbeatAsync(payload);
            }
        }
    }
}
