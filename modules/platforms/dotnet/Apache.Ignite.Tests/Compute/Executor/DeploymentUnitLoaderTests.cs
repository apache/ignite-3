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

namespace Apache.Ignite.Tests.Compute.Executor;

using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Threading.Tasks;
using Internal.Compute.Executor;
using NUnit.Framework;
using TestHelpers;

/// <summary>
/// Tests for <see cref="DeploymentUnitLoader"/>.
/// </summary>
[SuppressMessage("StyleCop.CSharp.ReadabilityRules", "SA1118:Parameter should not span multiple lines", Justification = "Tests")]
public class DeploymentUnitLoaderTests
{
    [Test]
    public async Task TestSingleAssemblyDeploymentUnit()
    {
        using var tempDir = new TempDir();
        var asmName = nameof(TestSingleAssemblyDeploymentUnit);
        EmitEchoJob(tempDir, asmName);

        using JobLoadContext jobCtx = DeploymentUnitLoader.GetJobLoadContext(new DeploymentUnitPaths([tempDir.Path]));
        IComputeJobWrapper jobWrapper = jobCtx.CreateJobWrapper($"TestNamespace.EchoJob, {asmName}");

        var jobRes = await JobWrapperHelper.ExecuteAsync<string, string>(jobWrapper, "Hello, world!");
        Assert.AreEqual("Echo: Hello, world!", jobRes);
    }

    [Test]
    public async Task TestMultiAssemblyDeploymentUnit()
    {
        // TODO: Build multiple assemblies with different type names and make sure all of them can be loaded.
        await Task.Delay(1);
        Assert.Fail();
    }

    [Test]
    public async Task TestUnitIsolation()
    {
        using var tempDir = new TempDir();
        var asmName = nameof(TestUnitIsolation);
        EmitGetAndSetStaticFieldJob(tempDir, asmName);

        var typeName = $"TestNamespace.GetAndSetStaticFieldJob, {asmName}";
        var deploymentUnitPaths = new DeploymentUnitPaths([tempDir.Path]);

        using JobLoadContext jobCtx1 = DeploymentUnitLoader.GetJobLoadContext(deploymentUnitPaths);
        using JobLoadContext jobCtx2 = DeploymentUnitLoader.GetJobLoadContext(deploymentUnitPaths);

        IComputeJobWrapper jobWrapper1 = jobCtx1.CreateJobWrapper(typeName);
        IComputeJobWrapper jobWrapper2 = jobCtx2.CreateJobWrapper(typeName);

        var job1Res1 = await JobWrapperHelper.ExecuteAsync<string, string>(jobWrapper1, "Job1val1");
        var job2Res1 = await JobWrapperHelper.ExecuteAsync<string, string>(jobWrapper2, "Job2val1");

        var job1Res2 = await JobWrapperHelper.ExecuteAsync<string, string>(jobWrapper1, "Job1val2");
        var job2Res2 = await JobWrapperHelper.ExecuteAsync<string, string>(jobWrapper2, "Job2val2");

        // Static fields are isolated between different job contexts.
        Assert.AreEqual("Initial", job1Res1);
        Assert.AreEqual("Job1val1", job1Res2);

        Assert.AreEqual("Initial", job2Res1);
        Assert.AreEqual("Job2val1", job2Res2);
    }

    [Test]
    public async Task TestAssemblyLoadOrder()
    {
        // TODO: Test that assemblies are loaded in the same order as specified deployment units.
        await Task.Delay(1);
        Assert.Fail();
    }

    private static void EmitEchoJob(TempDir tempDir, string asmName) =>
        EmitJob(
            tempDir,
            asmName,
            """
            public class EchoJob : IComputeJob<string, string>
            {
                public ValueTask<string> ExecuteAsync(IJobExecutionContext context, string arg, CancellationToken cancellationToken) =>
                    ValueTask.FromResult("Echo: " + arg);
            }
            """);

    private static void EmitGetAndSetStaticFieldJob(TempDir tempDir, string asmName) =>
        EmitJob(
            tempDir,
            asmName,
            """
            public class GetAndSetStaticFieldJob : IComputeJob<string, string>
            {
                public static string StaticField { get; set; } = "Initial";
                
                public ValueTask<string> ExecuteAsync(IJobExecutionContext context, string arg, CancellationToken cancellationToken)
                {
                    var oldValue = StaticField;
                    StaticField = arg;
                    
                    return ValueTask.FromResult(oldValue);
                }
                    
            }
            """);

    private static void EmitJob(TempDir tempDir, string asmName, [StringSyntax("C#")] string jobCode)
    {
        AssemblyGenerator.EmitClassLib(
            Path.Combine(tempDir.Path, $"{asmName}.dll"),
            $$"""
              using System;
              using System.Threading;
              using System.Threading.Tasks;
              using Apache.Ignite.Compute;

              namespace TestNamespace
              {
                  {{jobCode}}
              }
              """);
    }
}
