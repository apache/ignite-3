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
using Ignite.Table;
using Network;
using NUnit.Framework;
using TestHelpers;

/// <summary>
/// Tests for platform compute (non-Java jobs).
/// <para />
/// Development:
/// - Changing test code, job code: no need to restart Ignite.
/// - Changing core code: restart Ignite servers and do a full .NET solution rebuild to reflect the changes in .NET compute executor.
/// <para />
/// Debugging:
/// - Run tests once so that .NET executor processes are started.
/// - Attach to the executor processes.
/// - Run tests again to debug the executor.
/// </summary>
public class PlatformComputeTests : IgniteTestsBase
{
    private static readonly JobDescriptor<JobInfo, object?> JobRunnerJob = new(ComputeTests.PlatformTestNodeRunner + "$JobRunnerJob")
    {
        ArgMarshaller = new JsonMarshaller<JobInfo>()
    };

    private DeploymentUnit _defaultTestUnit = null!;

    [OneTimeSetUp]
    public async Task DeployDefaultUnit() => _defaultTestUnit = await ManagementApi.DeployTestsAssembly();

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
    public async Task TestBroadcastJob()
    {
        var jobDesc = DotNetJobs.Echo with { DeploymentUnits = [_defaultTestUnit] };
        var jobTarget = BroadcastJobTarget.Nodes(
            await GetClusterNodeAsync(),
            await GetClusterNodeAsync("_2"),
            await GetClusterNodeAsync("_3"));

        var jobExec = await Client.Compute.SubmitBroadcastAsync(
            jobTarget,
            jobDesc,
            "Hello world!");

        foreach (var job in jobExec.JobExecutions)
        {
            var res = await job.GetResultAsync();
            Assert.AreEqual("Hello world!", res);
        }
    }

    [Test]
    [TestCaseSource(typeof(TestCases), nameof(TestCases.SupportedArgs))]
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

        StringAssert.StartsWith(".NET job failed: Failed to load type 'MyNamespace.MyJob'", ex.Message);
        StringAssert.Contains("Could not resolve type 'MyNamespace.MyJob' in assembly 'Apache.Ignite", ex.Message);
        Assert.AreEqual("IGN-COMPUTE-9", ex.CodeAsString);
    }

    [Test]
    public async Task TestMissingAssembly()
    {
        // Run without providing deployment units.
        var target = JobTarget.Node(await GetClusterNodeAsync(string.Empty));
        var jobExec = await Client.Compute.SubmitAsync(target, DotNetJobs.Echo, "Hello world!");

        var ex = Assert.ThrowsAsync<IgniteException>(async () => await jobExec.GetResultAsync());
        StringAssert.StartsWith(".NET job failed: Failed to load type 'Apache.Ignite.Tests.Compute.DotNetJobs+EchoJob", ex.Message);
        StringAssert.Contains("Could not load file or assembly 'Apache.Ignite.Tests", ex.Message);
        Assert.AreEqual("IGN-COMPUTE-9", ex.CodeAsString);
    }

    [Test]
    public async Task TestJobError()
    {
        var target = JobTarget.Node(await GetClusterNodeAsync(string.Empty));
        var desc = DotNetJobs.Error with { DeploymentUnits = [_defaultTestUnit] };

        var jobExec = await Client.Compute.SubmitAsync(target, desc, "arg");
        var ex = Assert.ThrowsAsync<IgniteException>(async () => await jobExec.GetResultAsync());

        Assert.AreEqual(".NET job failed: Test exception: arg", ex.Message);
        Assert.AreEqual("IGN-COMPUTE-9", ex.CodeAsString);

        StringAssert.Contains(
            "System.ArithmeticException: Test exception: arg" +
            $"{Environment.NewLine}   at Apache.Ignite.Tests.Compute.DotNetJobs.ErrorJob.Throw(Object arg)" +
            $"{Environment.NewLine}   at Apache.Ignite.Tests.Compute.DotNetJobs.ErrorJob.ExecuteAsync",
            ex.InnerException?.Message);
    }

    [Test]
    [Ignore("IGNITE-25181")]
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
        var jobTimeout = TimeSpan.FromSeconds(5);

        // Get executor process id.
        int jobProcessId1 = await ExecJobAsync(DotNetJobs.ProcessId).WaitAsync(jobTimeout);

        // Run a job that exits the process. This job fails because the process exits before the result is returned.
        var ex = Assert.ThrowsAsync<IgniteException>(
            async () => await ExecJobAsync(DotNetJobs.ProcessExit).WaitAsync(jobTimeout));

        // Run another job - the process should be restarted automatically.
        int jobProcessId2 = await ExecJobAsync(DotNetJobs.ProcessId).WaitAsync(jobTimeout);

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

    [Test]
    public async Task TestManyJobsAssemblyLoadContextUnload()
    {
        int jobCount = 100;

        var jobTasks = Enumerable
            .Range(0, jobCount)
            .Select(x => ExecJobAsync(DotNetJobs.Echo, x))
            .ToArray();

        await Task.WhenAll(jobTasks);

        var assemblyLoadContextCount = await ExecJobAsync(DotNetJobs.AssemblyLoadContextCount);

        // Default context + current job context, all others should be unloaded.
        Assert.AreEqual(2, assemblyLoadContextCount);
    }

    [Test]
    public async Task TestTupleWithSchemaRoundTrip()
    {
        var tuple = TestCases.GetTupleWithAllFieldTypes();
        tuple["nested_tuple"] = TestCases.GetTupleWithAllFieldTypes(x => x is not decimal);

        var expectedTuple = Enumerable.Range(0, tuple.FieldCount).Aggregate(
            seed: new IgniteTuple(),
            (acc, i) =>
            {
                acc[tuple.GetName(i)] = tuple[i] is decimal d ? new BigDecimal(d) : tuple[i];
                return acc;
            });

        var res = (IIgniteTuple)(await ExecJobAsync(DotNetJobs.Echo, tuple))!;

        Assert.AreEqual(expectedTuple, res);
    }

    [Test]
    public async Task TestDeepNestedTupleWithSchemaRoundTrip()
    {
        var tuple = TestCases.GetNestedTuple(100);
        var res = await ExecJobAsync(DotNetJobs.Echo, tuple);

        Assert.AreEqual(tuple, res);
        StringAssert.Contains("CHILD99 = IgniteTuple { ID = 99, CHILD100 = IgniteTuple { ID = 100 } } } } } } } }", res?.ToString());
    }

    [Test]
    public async Task TestPlatformExecutorWithOldServerThrowsCompatibilityError()
    {
        using var server = new FakeServer();
        using var client = await server.ConnectClientAsync();

        var ex = Assert.ThrowsAsync<IgniteClientException>(async () => await ExecJobAsync(DotNetJobs.Echo, arg: "test", client: client));
        Assert.AreEqual("Job executor type 'DotNetSidecar' is not supported by the server.", ex.Message);
    }

    [Test]
    public async Task TestNewerDotnetVersionAssembly()
    {
        await ExecJobAsync(DotNetJobs.NewerDotNetJob, "test");
    }

    private async Task<IClusterNode> GetClusterNodeAsync(string? suffix = null)
    {
        var nodeName = ComputeTests.PlatformTestNodeRunner + suffix;

        var nodes = await Client.GetClusterNodesAsync();
        return nodes.First(n => n.Name == nodeName);
    }

    private async Task<TRes> ExecJobAsync<TArg, TRes>(JobDescriptor<TArg, TRes> desc, TArg arg = default!, IIgniteClient? client = null)
    {
        var jobDesc = desc with { DeploymentUnits = [_defaultTestUnit] };
        var jobTarget = JobTarget.Node(await GetClusterNodeAsync());

        client ??= Client;

        var jobExec = await client.Compute.SubmitAsync(
            jobTarget,
            jobDesc,
            arg: arg);

        return await jobExec.GetResultAsync();
    }

    [SuppressMessage("ReSharper", "NotAccessedPositionalProperty.Local", Justification = "JSON")]
    private record JobInfo(string TypeName, object Arg, List<string> DeploymentUnits, Guid NodeId, string JobExecutorType);
}
