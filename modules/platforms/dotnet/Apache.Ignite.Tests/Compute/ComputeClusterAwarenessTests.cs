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
    /// </summary>
    public class ComputeClusterAwarenessTests
    {
        [Test]
        public async Task TestClusterAwareness()
        {
            // TODO: Test with fake server (start 3 of them):
            // - Check that request arrives to proper node
            // - Check that retry works properly for cluster aware calls
            // - Check that default node is used when no direct connection exists
            using var server1 = new FakeServer(nodeName: "s1");
            using var server2 = new FakeServer(nodeName: "s2");

            var clientCfg = new IgniteClientConfiguration
            {
                Endpoints = { server1.Node.Address.ToString(), server2.Node.Address.ToString() }
            };
            using var client = await IgniteClient.StartAsync(clientCfg);

            var res = await client.Compute.ExecuteAsync<string>(nodes: new[] { server1.Node }, jobClassName: string.Empty);
            Assert.AreEqual("s1", res);
            Assert.AreEqual(ClientOp.ComputeExecute, server1.ClientOps.Last());
        }
    }
}
