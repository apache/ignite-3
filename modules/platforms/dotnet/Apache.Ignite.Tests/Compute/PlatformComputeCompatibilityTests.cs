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
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Executor;
using Ignite.Compute;
using NUnit.Framework;
using TestHelpers;

public class PlatformComputeCompatibilityTests : IgniteTestsBase
{
    private const string JobAssemblyName = nameof(PlatformComputeCompatibilityTests);

    private DeploymentUnit _unit;

    [OneTimeSetUp]
    public async Task UnitDeploy()
    {
        using var igniteBuildDir = new TempDir();
        using var jobBuildDir = new TempDir();

        // Build Ignite with some unlikely future version.
        BuildIgniteWithVersion(igniteBuildDir.Path, "11.22.33");

        var jobDllPath = JobGenerator.EmitEchoJob(
            jobBuildDir,
            asmName: JobAssemblyName,
            igniteDllPath: Path.Combine(igniteBuildDir.Path, "Apache.Ignite.dll"));

        _unit = await ManagementApi.UnitDeploy($"unit-{JobAssemblyName}-{Guid.NewGuid()}", "1.0.0", [jobDllPath]);
    }

    [OneTimeTearDown]
    public async Task UnitUndeploy() => await ManagementApi.UnitUndeploy(_unit);

    [Test]
    public async Task TestDotNetJobCompiledAgainstNewIgniteVersion()
    {
        await Task.Delay(1);
    }

    private static void BuildIgniteWithVersion(string targetPath, string version)
    {
        var process = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = "dotnet",
                ArgumentList =
                {
                    "publish",
                    "--no-restore",
                    "-c", "Release",
                    "-o", targetPath,
                    "/p:Version=" + version,
                },
                CreateNoWindow = true,
                UseShellExecute = false,
                WorkingDirectory = Path.Combine(TestUtils.SolutionDir, "Apache.Ignite"),
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                RedirectStandardInput = true
            }
        };

        if (!process.Start())
        {
            throw new InvalidOperationException("Failed to start process: " + process.StartInfo.FileName);
        }

        if (!process.WaitForExit(TimeSpan.FromSeconds(30)))
        {
            throw new TimeoutException("Process did not complete in time: " + process.StartInfo.FileName);
        }

        Console.WriteLine(process.StandardOutput.ReadToEnd());
        Console.WriteLine(process.StandardError.ReadToEnd());
    }
}
