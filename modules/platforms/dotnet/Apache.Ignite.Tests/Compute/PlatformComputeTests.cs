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
    public async Task TestDotNetSystemInfoJob([Values(true, false)] bool withSsl)
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
    public async Task TestDotNetEchoJob([Values(true, false)] bool withSsl)
    {
        var jobDesc = DotNetJobs.Echo with { DeploymentUnits = [_defaultTestUnit] };
        var jobTarget = JobTarget.Node(await GetClusterNodeAsync(withSsl ? "_3" : string.Empty));

        // TODO: Test all arg types.
        var jobExec = await Client.Compute.SubmitAsync(
            jobTarget,
            jobDesc,
            "Hello world!");

        var result = await jobExec.GetResultAsync();

        Assert.AreEqual("Hello world!", result);
    }

    [Test]
    public async Task TestAllSupportedArgTypes()
    {
        await Test(sbyte.MinValue);
        await Test(sbyte.MaxValue);
        await Test(short.MinValue);
        await Test(short.MaxValue);
        await Test(int.MinValue);
        await Test(int.MaxValue);
        await Test(long.MinValue);
        await Test(long.MaxValue);
        await Test(float.MinValue);
        await Test(float.MaxValue);
        await Test(double.MinValue);
        await Test(double.MaxValue);

        await Test(123.456m);
        await Test(-123.456m);
        await Test(decimal.MinValue);
        await Test(decimal.MaxValue);
        await Test(new BigDecimal(long.MinValue, 10));
        await Test(new BigDecimal(long.MaxValue, 20));

        await Test(new byte[] { 1, 255 });
        await Test("Ignite 🔥");
        await Test(LocalDate.MinIsoValue);
        await Test(LocalTime.Noon);
        await Test(LocalDateTime.MaxIsoValue);
        await Test(Instant.FromUtc(2001, 3, 4, 5, 6));

        await Test(Guid.Empty);
        await Test(new Guid(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 }));
        await Test(Guid.NewGuid());

        async Task Test(object val)
        {
            var jobDesc = DotNetJobs.Echo with { DeploymentUnits = [_defaultTestUnit] };
            var jobTarget = JobTarget.Node(await GetClusterNodeAsync());

            var jobExec = await Client.Compute.SubmitAsync(
                jobTarget,
                jobDesc,
                val);

            var result = await jobExec.GetResultAsync();

            Assert.AreEqual(val, result);
        }
    }

    [Test]
    public async Task TestNonExistentJob()
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

    private async Task<IClusterNode> GetClusterNodeAsync(string? suffix = null)
    {
        var nodeName = ComputeTests.PlatformTestNodeRunner + suffix;

        var nodes = await Client.GetClusterNodesAsync();
        return nodes.First(n => n.Name == nodeName);
    }
}
