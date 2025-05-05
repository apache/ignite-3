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

namespace Apache.Ignite.Benchmarks.Compute;

using System;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Ignite.Compute;
using Network;
using Tests.Compute;
using Tests.TestHelpers;

/// <summary>
/// .NET vs Java job benchmarks.
/// </summary>
public class PlatformJobBenchmarks : ServerBenchmarkBase
{
    private static readonly DeploymentUnit Unit = new(nameof(PlatformJobBenchmarks), "1.0.0");

    private JobDescriptor<object?, object?> _echoJobJava = null!;

    private JobDescriptor<object?, object?> _echoJobDotNet = null!;

    private IJobTarget<IClusterNode> _jobTarget = null!;

    public override async Task GlobalSetup()
    {
        await base.GlobalSetup();

        var testsDll = typeof(PlatformComputeTests).Assembly.Location;
        var benchDll = typeof(PlatformJobBenchmarks).Assembly.Location;

        await ManagementApi.UnitDeploy(Unit.Name, Unit.Version, [testsDll, benchDll]);

        _echoJobJava = new(
            "org.apache.ignite.internal.runner.app.client.ItThinClientComputeTest$EchoJob", [Unit]);

        _echoJobDotNet = DotNetJobs.Echo with { DeploymentUnits = [Unit] };

        var nodes = await Client.GetClusterNodesAsync();
        var firstNode = nodes.Single(x => x.Name.EndsWith("PlatformTestNodeRunner", StringComparison.Ordinal));
        _jobTarget = JobTarget.Node(firstNode);
    }

    public override async Task GlobalCleanup()
    {
        await base.GlobalCleanup();

        await ManagementApi.UnitUndeploy(Unit);
    }

    [Benchmark]
    public async Task DotNetJob() => await ExecJobAsync(_echoJobDotNet);

    [Benchmark]
    public async Task JavaJob() => await ExecJobAsync(_echoJobJava);

    private async Task ExecJobAsync(JobDescriptor<object?, object?> desc)
    {
        var exec = await Client.Compute.SubmitAsync(_jobTarget, desc, "Hello world!");

        await exec.GetResultAsync();
    }
}
