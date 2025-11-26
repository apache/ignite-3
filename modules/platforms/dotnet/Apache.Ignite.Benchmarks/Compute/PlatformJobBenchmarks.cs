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
using Tests.Compute.Executor;
using Tests.TestHelpers;

/// <summary>
/// .NET vs Java job benchmarks.
/// Not entirely fair, because the Java job does not require class loading.
/// Results on i9-12900H, .NET SDK 8.0.15, Ubuntu 22.04:
///
/// | Method    | Mean     | Error    | StdDev   | Ratio | RatioSD |
/// |---------- |---------:|---------:|---------:|------:|--------:|
/// | DotNetJob | 170.1 us |  3.40 us |  9.30 us |  0.91 |    0.12 |
/// | JavaJob   | 191.2 us | 10.18 us | 27.34 us |  1.02 |    0.19 |
///
/// Notes:
/// 1. Change DotNetComputeExecutor$resolveDotNetBinaryDir in Java to use Release executor in dev mode.
/// 2. Disable Netty leak detector.
/// </summary>
public class PlatformJobBenchmarks : ServerBenchmarkBase
{
    private DeploymentUnit _unit = null!;

    private JobDescriptor<object?, object?> _echoJobJava = null!;

    private JobDescriptor<object?, object?> _echoJobDotNet = null!;

    private IJobTarget<IClusterNode> _jobTarget = null!;

    private TempDir _tempDir = null!;

    public override async Task GlobalSetup()
    {
        await base.GlobalSetup();

        _tempDir = new TempDir();
        var asmName = nameof(PlatformJobBenchmarks);
        var asmDll = JobGenerator.EmitEchoJob(_tempDir, asmName);

        _unit = await ManagementApi.UnitDeploy(nameof(PlatformJobBenchmarks), ManagementApi.GetRandomUnitVersion(), [asmDll]);

        _echoJobDotNet = new JobDescriptor<object?, object?>(
            JobClassName: $"TestNamespace.EchoJob, {asmName}",
            DeploymentUnits: [_unit],
            Options: new() { ExecutorType = JobExecutorType.DotNetSidecar });

        _echoJobJava = new(
            "org.apache.ignite.internal.runner.app.client.ItThinClientComputeTest$EchoJob", [_unit]);

        var nodes = await Client.GetClusterNodesAsync();
        var firstNode = nodes.Single(x => x.Name.EndsWith("PlatformTestNodeRunner", StringComparison.Ordinal));
        _jobTarget = JobTarget.Node(firstNode);
    }

    public override async Task GlobalCleanup()
    {
        await base.GlobalCleanup();

        await ManagementApi.UnitUndeploy(_unit);

        _tempDir.Dispose();
    }

    [Benchmark]
    public async Task DotNetJob() => await ExecJobAsync(_echoJobDotNet);

    [Benchmark(Baseline = true)]
    public async Task JavaJob() => await ExecJobAsync(_echoJobJava);

    private async Task ExecJobAsync(JobDescriptor<object?, object?> desc)
    {
        var exec = await Client.Compute.SubmitAsync(_jobTarget, desc, "Hello world!");

        await exec.GetResultAsync();
    }
}
