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
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Ignite.Compute;
    using Internal.Proto;
    using NUnit.Framework;

    /// <summary>
    /// Tests compute cluster awareness: client requests can be sent to correct server nodes when a direct connection is available.
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
            client.WaitForConnections(3);

            var job = new JobDescriptor<object?, string>(string.Empty);

            IJobExecution<string> exec2 = await client.Compute.SubmitAsync(JobTarget.Node(server2.Node), job, null);
            IJobExecution<string> exec3 = await client.Compute.SubmitAsync(JobTarget.Node(server3.Node), job, null);

            Assert.AreEqual("s2", await exec2.GetResultAsync());
            Assert.AreEqual("s3", await exec3.GetResultAsync());

            Assert.AreEqual(ClientOp.ComputeExecute, server2.ClientOps.Single());
            Assert.AreEqual(ClientOp.ComputeExecute, server3.ClientOps.Single());

            Assert.IsEmpty(server1.ClientOps);
        }

        [Test]
        public async Task TestClientSendsComputeJobToDefaultNodeWhenDirectConnectionToTargetDoesNotExist()
        {
            using var server1 = new FakeServer(nodeName: "s1");
            using var server2 = new FakeServer(nodeName: "s2");
            using var server3 = new FakeServer(nodeName: "s3");

            using var client = await server1.ConnectClientAsync();
            var job = new JobDescriptor<object?, string>(string.Empty);

            IJobExecution<string> exec2 = await client.Compute.SubmitAsync(JobTarget.Node(server2.Node), job, null);
            IJobExecution<string> exec3 = await client.Compute.SubmitAsync(JobTarget.Node(server3.Node), job, null);

            Assert.AreEqual("s1", await exec2.GetResultAsync());
            Assert.AreEqual("s1", await exec3.GetResultAsync());
            Assert.AreEqual(new[] { ClientOp.ComputeExecute, ClientOp.ComputeExecute }, server1.ClientOps);

            Assert.IsEmpty(server2.ClientOps);
            Assert.IsEmpty(server3.ClientOps);

            Assert.AreEqual(server1.Node, client.GetConnections().Single().Node);
        }

        [Test]
        public async Task TestClientRetriesComputeJobOnPrimaryAndDefaultNodes()
        {
            using var server1 = new FakeServer(shouldDropConnection: ctx => ctx.RequestCount % 2 == 0, nodeName: "s1");
            using var server2 = new FakeServer(shouldDropConnection: ctx => ctx.RequestCount % 2 == 0, nodeName: "s2");

            var clientCfg = new IgniteClientConfiguration
            {
                Endpoints = { server1.Node.Address.ToString(), server2.Node.Address.ToString() },
                RetryPolicy = new RetryLimitPolicy { RetryLimit = 2 }
            };

            using var client = await IgniteClient.StartAsync(clientCfg);
            client.WaitForConnections(2);

            var nodeNames = new HashSet<string>();
            var job = new JobDescriptor<object?, string>(string.Empty);

            for (int i = 0; i < 100; i++)
            {
                var node = i % 2 == 0 ? server1.Node : server2.Node;

                IJobExecution<string> jobExecution = await client.Compute.SubmitAsync(JobTarget.Node(node), job, null);

                string res = await jobExecution.GetResultAsync();

                nodeNames.Add(res);
            }

            CollectionAssert.AreEquivalent(new[] { "s1", "s2" }, nodeNames);
        }
    }
}
