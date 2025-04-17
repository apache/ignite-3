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

namespace Apache.Ignite.Tests.Compute;

using System.Linq;
using System.Threading.Tasks;
using Ignite.Compute;
using Network;
using NUnit.Framework;

/// <summary>
/// Tests for platform compute (non-Java jobs).
/// </summary>
public class PlatformComputeTests : IgniteTestsBase
{
    private const string TestOnlyDotnetJobEcho = "TEST_ONLY_DOTNET_JOB:ECHO";

    [Test]
    public async Task TestDotNetEchoJob([Values(true, false)] bool withSsl)
    {
        var target = JobTarget.Node(await GetClusterNodeAsync(withSsl ? "_3" : string.Empty));
        var desc = new JobDescriptor<string, string>(TestOnlyDotnetJobEcho);

        var jobExec = await Client.Compute.SubmitAsync(target, desc, "Hello world!");
        var result = await jobExec.GetResultAsync();

        Assert.AreEqual("Hello world!", result);
    }

    [Test]
    public async Task TestDotNetJobFailsOnServerWithClientCertificate()
    {
        var target = JobTarget.Node(await GetClusterNodeAsync("_4"));
        var desc = new JobDescriptor<string, string>(TestOnlyDotnetJobEcho);

        var jobExec = await Client.Compute.SubmitAsync(target, desc, "Hello world!");
        var result = await jobExec.GetResultAsync();

        Assert.AreEqual("Hello world!", result);
    }

    private async Task<IClusterNode> GetClusterNodeAsync(string suffix)
    {
        var nodeName = ComputeTests.PlatformTestNodeRunner + suffix;

        var nodes = await Client.GetClusterNodesAsync();
        return nodes.First(n => n.Name == nodeName);
    }
}
