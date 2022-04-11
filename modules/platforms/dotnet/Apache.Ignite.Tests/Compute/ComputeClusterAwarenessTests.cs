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

namespace Apache.Ignite.Tests.Compute
{
    using System.Linq;
    using System.Threading.Tasks;
    using Internal.Proto;
    using NUnit.Framework;

    /// <summary>
    /// Tests compute cluster awareness: client requests can be sent to correct server nodes when a direct connection is available.
    /// TODO: Test retry behavior.
    /// </summary>
    public class ComputeClusterAwarenessTests
    {
        [Test]
        public async Task TestClientSendsComputeJobToTargetNodeWhenDirectConnectionExists()
        {
            using var server1 = new FakeServer(nodeName: "s1");
            using var server2 = new FakeServer(nodeName: "s2");
            using var server3 = new FakeServer(nodeName: "s3");

            var clientCfg = new IgniteClientConfiguration
            {
                Endpoints = { server1.Node.Address.ToString(), server2.Node.Address.ToString(), server3.Node.Address.ToString() }
            };

            using var client = await IgniteClient.StartAsync(clientCfg);

            // ReSharper disable once AccessToDisposedClosure
            TestUtils.WaitForCondition(() => client.GetConnections().Count == 3);

            var res = await client.Compute.ExecuteAsync<string>(nodes: new[] { server3.Node }, jobClassName: string.Empty);

            Assert.AreEqual("s3", res);
            Assert.AreEqual(ClientOp.ComputeExecute, server3.ClientOps.Last());

            Assert.IsEmpty(server1.ClientOps);
            Assert.IsEmpty(server2.ClientOps);
        }

        [Test]
        public async Task TestClientSendsComputeJobToDefaultNodeWhenDirectConnectionToTargetDoesNotExist()
        {
            using var server1 = new FakeServer(nodeName: "s1");
            using var server2 = new FakeServer(nodeName: "s2");
            using var server3 = new FakeServer(nodeName: "s3");

            var clientCfg = new IgniteClientConfiguration
            {
                Endpoints = { server1.Node.Address.ToString(), server2.Node.Address.ToString() }
            };

            using var client = await IgniteClient.StartAsync(clientCfg);

            // ReSharper disable once AccessToDisposedClosure
            TestUtils.WaitForCondition(() => client.GetConnections().Count == 2);

            var res = await client.Compute.ExecuteAsync<string>(nodes: new[] { server3.Node }, jobClassName: string.Empty);

            Assert.AreEqual("s2", res);
            Assert.AreEqual(ClientOp.ComputeExecute, server2.ClientOps.Last());

            Assert.IsEmpty(server1.ClientOps);
            Assert.IsEmpty(server3.ClientOps);
        }
    }
}
