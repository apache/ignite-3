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
using System.Linq;
using System.Threading.Tasks;
using Executor;
using Ignite.Compute;
using NUnit.Framework;
using TestHelpers;

[Platform("Linux", Reason = "File locking on Windows prevents build.")]
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
        var jobDesc = new JobDescriptor<object, object>(
            JobClassName: $"TestNamespace.EchoJob, {JobAssemblyName}",
            DeploymentUnits: [_unit],
            Options: new JobExecutionOptions(ExecutorType: JobExecutorType.DotNetSidecar));

        var nodes = await Client.GetClusterNodesAsync();
        var target = JobTarget.Node(nodes.Single(x => x.Name == ComputeTests.PlatformTestNodeRunner));

        var jobExec = await Client.Compute.SubmitAsync(target, jobDesc, "test1");
        var result = await jobExec.GetResultAsync();

        Assert.AreEqual("Echo: test1", result);
    }

    private static void BuildIgniteWithVersion(string targetPath, string version)
    {
        // Copy the Ignite solution to a temporary directory.
        // Skip Directory.Build.props to get rid of GitVersioning.
        using var tempRepoDir = new TempDir();
        CopyFilesAndDirectories(
            sourcePath: TestUtils.SolutionDir,
            targetPath: tempRepoDir.Path,
            predicate: s => !s.EndsWith("Directory.Build.props", StringComparison.OrdinalIgnoreCase));

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
                WorkingDirectory = Path.Combine(tempRepoDir.Path, "Apache.Ignite"),
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

    private static void CopyFilesAndDirectories(string sourcePath, string targetPath, Func<string, bool> predicate)
    {
        foreach (var dir in Directory.GetDirectories(sourcePath, "*", SearchOption.AllDirectories))
        {
            if (!predicate(dir))
            {
                continue;
            }

            Directory.CreateDirectory(GetTargetPath(dir));
        }

        foreach (var file in Directory.GetFiles(sourcePath, "*", SearchOption.AllDirectories))
        {
            if (!predicate(file))
            {
                continue;
            }

            File.Copy(file, GetTargetPath(file));
        }

        string GetTargetPath(string path)
        {
            var relative = Path.GetRelativePath(sourcePath, path);

            return Path.Combine(targetPath, relative);
        }
    }
}
