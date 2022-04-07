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
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Threading.Tasks;
    using Network;
    using NUnit.Framework;

    /// <summary>
    /// TODO.
    /// </summary>
    public class ComputeTests : IgniteTestsBase
    {
        private const string NodeNameJob = "org.apache.ignite.internal.runner.app.client.ItThinClientComputeTest$NodeNameJob";

        private const string PlatformTestNodeRunner = "org.apache.ignite.internal.runner.app.PlatformTestNodeRunner";

        [Test]
        public async Task TestGetClusterNodes()
        {
            var res = await Client.GetClusterNodesAsync();

            Assert.AreEqual(1, res.Count);
            Assert.AreEqual(PlatformTestNodeRunner, res[0].Name);
            Assert.IsNotEmpty(res[0].Id);
            Assert.AreEqual(3344, res[0].Address.Port);
            Assert.IsTrue(IPAddress.IsLoopback(res[0].Address.Address), res[0].Address.ToString());
        }

        [Test]
        public async Task TestExecuteOnSpecificNode()
        {
            var res1 = await Client.Compute.ExecuteAsync<string>(await GetNode(0), NodeNameJob);
            var res2 = await Client.Compute.ExecuteAsync<string>(await GetNode(1), NodeNameJob);

            Assert.AreEqual(PlatformTestNodeRunner, res1);
            Assert.AreEqual(PlatformTestNodeRunner + "_2", res2);
        }

        [Test]
        public void TestExecuteResultTypeMismatchThrowsInvalidCastException()
        {
            Assert.ThrowsAsync<InvalidCastException>(async () =>
                await Client.Compute.ExecuteAsync<Guid>(await Client.GetClusterNodesAsync(), NodeNameJob));
        }

        private async Task<IEnumerable<IClusterNode>> GetNode(int index) =>
            (await Client.GetClusterNodesAsync()).OrderBy(n => n.Name).Skip(index).Take(1);
    }
}
