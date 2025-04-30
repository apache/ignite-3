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
using System.Linq;
using System.Threading.Tasks;
using Ignite.Compute;
using Network;
using NodaTime;
using NUnit.Framework;

/// <summary>
/// Tests for platform compute (non-Java jobs).
/// </summary>
public class PlatformComputeTests : IgniteTestsBase
{
    private DeploymentUnit _defaultTestUnit = null!;

    [OneTimeSetUp]
    public async Task DeployDefaultUnit() => _defaultTestUnit = await DeployTestsAssembly();

    [OneTimeTearDown]
    public async Task UndeployDefaultUnit() => await ManagementApi.UnitUndeploy(_defaultTestUnit);

    [Test]
    public async Task TestSystemInfoJob([Values(true, false)] bool withSsl)
    {
        var target = JobTarget.Node(await GetClusterNodeAsync(withSsl ? "_3" : string.Empty));
        var desc = new JobDescriptor<object?, string>(
            DotNetJobs.TempJobPrefix + "Apache.Ignite.Internal.ComputeExecutor.SystemInfoJob, Apache.Ignite.Internal.ComputeExecutor");

        var jobExec = await Client.Compute.SubmitAsync(target, desc, "Hello world!");
        var result = await jobExec.GetResultAsync();

        StringAssert.StartsWith("SystemInfoJob [CLR=", result);
        StringAssert.EndsWith("JobArg=Hello world!]", result);
    }

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
        var jobDesc = DotNetJobs.Echo with { DeploymentUnits = [_defaultTestUnit] };
        var jobTarget = JobTarget.Node(await GetClusterNodeAsync());

        var jobExec = await Client.Compute.SubmitAsync(
            jobTarget,
            jobDesc,
            val);

        var result = await jobExec.GetResultAsync();

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
        var desc = new JobDescriptor<string, string>(DotNetJobs.TempJobPrefix + "MyNamespace.MyJob");

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
            "\n   at Apache.Ignite.Tests.Compute.DotNetJobs.ErrorJob.Throw(Object arg)" +
            "\n   at Apache.Ignite.Tests.Compute.DotNetJobs.ErrorJob.ExecuteAsync",
            ex.InnerException?.Message);
    }

    [Test]
    public async Task TestDotNetJobFailsOnServerWithClientCertificate()
    {
        var target = JobTarget.Node(await GetClusterNodeAsync("_4"));
        var desc = new JobDescriptor<string, string>(DotNetJobs.TempJobPrefix + "TODO");

        var jobExec = await Client.Compute.SubmitAsync(target, desc, "Hello world!");
        var ex = Assert.ThrowsAsync<IgniteException>(async () => await jobExec.GetResultAsync());

        // TODO IGNITE-25181 Support client certs with .NET compute executor.
        Assert.AreEqual("Could not start .NET executor process in 2 attempts", ex.Message);
    }

    private static async Task<DeploymentUnit> DeployTestsAssembly(string? unitId = null, string? unitVersion = null)
    {
        var testsDll = typeof(PlatformComputeTests).Assembly.Location;

        var unitId0 = unitId ?? TestContext.CurrentContext.Test.FullName;
        var unitVersion0 = unitVersion ?? DateTime.Now.TimeOfDay.ToString(@"m\.s\.fff");

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
}
