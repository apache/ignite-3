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

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading.Tasks;
using Ignite.Compute;
using Ignite.Marshalling;
using Network;
using NodaTime;
using NUnit.Framework;
using TestHelpers;

/// <summary>
/// Tests for platform compute (non-Java jobs).
/// </summary>
public class PlatformComputeTests : IgniteTestsBase
{
    private static readonly JobDescriptor<JobInfo, object?> JobRunnerJob = new(ComputeTests.PlatformTestNodeRunner + "$JobRunnerJob")
    {
        ArgMarshaller = new JsonMarshaller<JobInfo>()
    };

    private DeploymentUnit _defaultTestUnit = null!;

    [OneTimeSetUp]
    public async Task DeployDefaultUnit() => _defaultTestUnit = await DeployTestsAssembly();

    [OneTimeTearDown]
    public async Task UndeployDefaultUnit() => await ManagementApi.UnitUndeploy(_defaultTestUnit);

    [Test]
    public async Task TestEchoJob([Values(true, false)] bool withSsl)
    {
        var jobDesc = DotNetJobs.Echo with { DeploymentUnits = [_defaultTestUnit] };
        var jobTarget = JobTarget.Node(await GetClusterNodeAsync(withSsl ? "_3" : string.Empty));

        var jobExec = await Client.Compute.SubmitAsync(
            jobTarget,
            jobDesc,
            "Hello world!");

        var result = await jobExec.GetResultAsync();

        Assert.AreEqual("Hello world!", result);
    }

    [Test]
    [TestCaseSource(nameof(ArgTypesTestCases))]
    public async Task TestAllSupportedArgTypes(object val)
    {
        var result = await ExecJobAsync(DotNetJobs.Echo, val);

        if (val is decimal dec)
        {
            val = new BigDecimal(dec);
        }

        Assert.AreEqual(val, result);
    }

    [Test]
    public async Task TestMissingClass()
    {
        var target = JobTarget.Node(await GetClusterNodeAsync());
        var desc = new JobDescriptor<string, string>(
            "MyNamespace.MyJob",
            [_defaultTestUnit],
            new JobExecutionOptions { ExecutorType = JobExecutorType.DotNetSidecar });

        var jobExec = await Client.Compute.SubmitAsync(target, desc, "arg");
        var ex = Assert.ThrowsAsync<IgniteException>(async () => await jobExec.GetResultAsync());

        Assert.AreEqual("Type 'MyNamespace.MyJob' not found in the specified deployment units.", ex.Message);
        Assert.AreEqual("IGN-COMPUTE-9", ex.CodeAsString);
    }

    [Test]
    public async Task TestMissingAssembly()
    {
        // Run without providing deployment units.
        var target = JobTarget.Node(await GetClusterNodeAsync(string.Empty));
        var jobExec = await Client.Compute.SubmitAsync(target, DotNetJobs.Echo, "Hello world!");

        var ex = Assert.ThrowsAsync<IgniteException>(async () => await jobExec.GetResultAsync());
        StringAssert.StartsWith("Could not load file or assembly 'Apache.Ignite.Tests", ex.Message);
        Assert.AreEqual("IGN-COMPUTE-9", ex.CodeAsString);
    }

    [Test]
    public async Task TestJobError()
    {
        var target = JobTarget.Node(await GetClusterNodeAsync(string.Empty));
        var desc = DotNetJobs.Error with { DeploymentUnits = [_defaultTestUnit] };

        var jobExec = await Client.Compute.SubmitAsync(target, desc, "arg");
        var ex = Assert.ThrowsAsync<IgniteException>(async () => await jobExec.GetResultAsync());

        Assert.AreEqual("Test exception: arg", ex.Message);
        Assert.AreEqual("IGN-COMPUTE-9", ex.CodeAsString);

        StringAssert.Contains(
            "System.ArithmeticException: Test exception: arg" +
            $"{Environment.NewLine}   at Apache.Ignite.Tests.Compute.DotNetJobs.ErrorJob.Throw(Object arg)" +
            $"{Environment.NewLine}   at Apache.Ignite.Tests.Compute.DotNetJobs.ErrorJob.ExecuteAsync",
            ex.InnerException?.Message);
    }

    [Test]
    public async Task TestDotNetJobFailsOnServerWithClientCertificate()
    {
        var target = JobTarget.Node(await GetClusterNodeAsync("_4"));
        var desc = new JobDescriptor<string, string>(
            "SomeJob",
            [_defaultTestUnit],
            new JobExecutionOptions { ExecutorType = JobExecutorType.DotNetSidecar });

        var jobExec = await Client.Compute.SubmitAsync(target, desc, "Hello world!");
        var ex = Assert.ThrowsAsync<IgniteException>(async () => await jobExec.GetResultAsync());

        // TODO IGNITE-25181 Support client certs with .NET compute executor.
        Assert.AreEqual("Could not start .NET executor process in 2 attempts", ex.Message);
    }

    [Test]
    public async Task TestCallDotNetJobFromJava()
    {
        var targetNode = await GetClusterNodeAsync();
        var target = JobTarget.Node(targetNode);

        var arg = new JobInfo(
            TypeName: typeof(DotNetJobs.EchoJob).AssemblyQualifiedName!,
            Arg: "arg1",
            DeploymentUnits: [$"{_defaultTestUnit.Name}:{_defaultTestUnit.Version}"],
            NodeId: targetNode.Id,
            JobExecutorType: "DOTNET_SIDECAR");

        var jobExec = await Client.Compute.SubmitAsync(target, JobRunnerJob, arg);
        var res = await jobExec.GetResultAsync();

        Assert.AreEqual("arg1", res);
    }

    [Test]
    public async Task TestDotNetJobRunsInAnotherProcess()
    {
        var jobProcessId = await ExecJobAsync(DotNetJobs.ProcessId);

        Assert.AreNotEqual(Environment.ProcessId, jobProcessId);
    }

    [Test]
    public async Task TestDotNetSidecarProcessIsRestartedOnExit()
    {
        // Get executor process id.
        int jobProcessId1 = await ExecJobAsync(DotNetJobs.ProcessId);

        // Run a job that exits the process. This job fails because the process exits before the result is returned.
        var ex = Assert.ThrowsAsync<IgniteException>(async () => await ExecJobAsync(DotNetJobs.ProcessExit));

        // Run another job - the process should be restarted automatically.
        int jobProcessId2 = await ExecJobAsync(DotNetJobs.ProcessId);

        Assert.AreNotEqual(jobProcessId1, jobProcessId2);
        Assert.AreEqual(".NET compute executor connection lost", ex.Message);
        Assert.AreEqual("IGN-CLIENT-9", ex.CodeAsString);
    }

    [Test]
    public async Task TestIgniteApiAccessFromJob()
    {
        var apiRes = await ExecJobAsync(DotNetJobs.ApiTest, "Hello world!");

        Assert.AreEqual(
            "Arg: Hello world!|SQL result: IgniteTuple { ANSWER = 42 }|Table result: Option { HasValue = True, Value = Hello }",
            apiRes);
    }

    private static async Task<DeploymentUnit> DeployTestsAssembly(string? unitId = null, string? unitVersion = null)
    {
        var testsDll = typeof(PlatformComputeTests).Assembly.Location;

        var unitId0 = unitId ?? TestContext.CurrentContext.Test.FullName;
        var unitVersion0 = unitVersion ?? DateTime.Now.TimeOfDay.ToString(@"m\.s\.f");

        await ManagementApi.UnitDeploy(
            unitId: unitId0,
            unitVersion: unitVersion0,
            unitContent: [testsDll]);

        return new DeploymentUnit(unitId0, unitVersion0);
    }

    private static IEnumerable<object> ArgTypesTestCases() => [
        sbyte.MinValue,
        sbyte.MaxValue,
        short.MinValue,
        short.MaxValue,
        int.MinValue,
        int.MaxValue,
        long.MinValue,
        long.MaxValue,
        float.MinValue,
        float.MaxValue,
        double.MinValue,
        double.MaxValue,
        123.456m,
        -123.456m,
        decimal.MinValue,
        decimal.MaxValue,
        new BigDecimal(long.MinValue, 10),
        new BigDecimal(long.MaxValue, 20),
        new byte[] { 1, 255 },
        "Ignite 🔥",
        LocalDate.MinIsoValue,
        LocalTime.Noon,
        LocalDateTime.MaxIsoValue,
        Instant.FromUtc(2001, 3, 4, 5, 6),
        Guid.Empty,
        new Guid(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 }),
    ];

    private async Task<IClusterNode> GetClusterNodeAsync(string? suffix = null)
    {
        var nodeName = ComputeTests.PlatformTestNodeRunner + suffix;

        var nodes = await Client.GetClusterNodesAsync();
        return nodes.First(n => n.Name == nodeName);
    }

    private async Task<TRes> ExecJobAsync<TArg, TRes>(JobDescriptor<TArg, TRes> desc, TArg arg = default!)
    {
        var jobDesc = desc with { DeploymentUnits = [_defaultTestUnit] };
        var jobTarget = JobTarget.Node(await GetClusterNodeAsync());

        var jobExec = await Client.Compute.SubmitAsync(
            jobTarget,
            jobDesc,
            arg: arg);

        return await jobExec.GetResultAsync();
    }

    [SuppressMessage("ReSharper", "NotAccessedPositionalProperty.Local", Justification = "JSON")]
    private record JobInfo(string TypeName, object Arg, List<string> DeploymentUnits, Guid NodeId, string JobExecutorType);
}
