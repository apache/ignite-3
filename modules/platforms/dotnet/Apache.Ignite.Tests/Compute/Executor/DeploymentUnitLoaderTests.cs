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
        var asmName = "TestSingleAssemblyDeploymentUnit.dll";

        AssemblyGenerator.EmitClassLib(
            Path.Combine(tempDir.Path, asmName),
            @"
                    using System;
                    using System.Threading;
                    using System.Threading.Tasks;
                    using Apache.Ignite.Compute;

                    namespace TestNamespace
                    {
                        public class EchoJob : IComputeJob<object, object>
                        {
                            public ValueTask<object> ExecuteAsync(IJobExecutionContext context, object arg, CancellationToken cancellationToken) =>
                                ValueTask.FromResult(arg);
                        }
                    }");

        using JobLoadContext jobCtx = DeploymentUnitLoader.GetJobLoadContext(new DeploymentUnitPaths([tempDir.Path]));
        IComputeJobWrapper jobWrapper = jobCtx.CreateJobWrapper($"TestNamespace.EchoJob, {asmName}");

        var jobRes = await JobWrapperHelper.ExecuteAsync<object, object>(jobWrapper, "Hello, world!");
        Assert.AreEqual("Hello, world!", jobRes);
    }

    [Test]
    public async Task TestMultiAssemblyDeploymentUnit()
    {
        // TODO: Build multiple assemblies with different type names and make sure all of them can be loaded.
        // TODO: Test isolation and versioning.
        await Task.Delay(1);
        Assert.Fail();
    }
}
