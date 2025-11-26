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

    private const string FutureIgniteVersion = "11.22.33";

    private DeploymentUnit _unit;

    [OneTimeSetUp]
    public async Task UnitDeploy()
    {
        using var igniteBuildDir = new TempDir();
        using var jobBuildDir = new TempDir();

        // Build Ignite with some unlikely future version.
        BuildIgniteWithVersion(igniteBuildDir.Path, FutureIgniteVersion);

        var jobDllPath = JobGenerator.EmitGetReferencedIgniteAssemblyJob(
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
        var jobDesc = new JobDescriptor<string, string>(
            JobClassName: $"TestNamespace.GetReferencedIgniteAssemblyJob, {JobAssemblyName}",
            DeploymentUnits: [_unit],
            Options: new JobExecutionOptions(ExecutorType: JobExecutorType.DotNetSidecar));

        var nodes = await Client.GetClusterNodesAsync();
        var target = JobTarget.Node(nodes.Single(x => x.Name == ComputeTests.PlatformTestNodeRunner));

        var jobExec = await Client.Compute.SubmitAsync(target, jobDesc, "test1");
        var result = await jobExec.GetResultAsync();

        // Verify that the job references a future Ignite version but still works.
        StringAssert.StartsWith($"Apache.Ignite, Version={FutureIgniteVersion}", result);
    }

    private static void BuildIgniteWithVersion(string targetPath, string version)
    {
        // Copy the Ignite solution and override build props to skip GitVersioning.
        var slnDirCopy = Path.Combine(Directory.GetParent(TestUtils.SolutionDir)!.FullName, Guid.NewGuid().ToString());
        using var disposeSln = new DisposeAction(() => Directory.Delete(slnDirCopy, true));

        CopyFilesAndDirectories(sourcePath: TestUtils.SolutionDir, targetPath: slnDirCopy);

        var buildPropsOverride = """
                                 <Project>
                                     <PropertyGroup>
                                         <LangVersion>12</LangVersion>
                                         <EnableNETAnalyzers>true</EnableNETAnalyzers>
                                         <Nullable>enable</Nullable>
                                         <AnalysisMode>None</AnalysisMode>
                                     </PropertyGroup>
                                 </Project>
                                 """;

        File.WriteAllText(Path.Combine(slnDirCopy, "Directory.Build.props"), buildPropsOverride);

        // Build the solution with the specified version.
        var process = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = "dotnet",
                ArgumentList =
                {
                    "publish",
                    "-c", "Release",
                    "-o", targetPath,
                    "/p:Version=" + version,
                    "/p:VersionSuffix=" + version
                },
                CreateNoWindow = true,
                UseShellExecute = false,
                WorkingDirectory = Path.Combine(slnDirCopy, "Apache.Ignite"),
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                RedirectStandardInput = true
            }
        };

        if (!process.Start())
        {
            throw new InvalidOperationException("Failed to start process: " + process.StartInfo.FileName);
        }

        var output = GetOutput();
        Console.WriteLine(output);

        if (!process.WaitForExit(TimeSpan.FromSeconds(120)))
        {
            throw new TimeoutException($"Process did not complete in time: {GetOutput()}");
        }

        if (process.ExitCode != 0)
        {
            throw new InvalidOperationException($"Process failed with exit code {process.ExitCode}: {output}");
        }

        string GetOutput() => process.StandardOutput.ReadToEnd() + process.StandardError.ReadToEnd();
    }

    private static void CopyFilesAndDirectories(string sourcePath, string targetPath)
    {
        foreach (var dir in Directory.GetDirectories(sourcePath, "*", SearchOption.AllDirectories))
        {
            Directory.CreateDirectory(GetTargetPath(dir));
        }

        foreach (var file in Directory.GetFiles(sourcePath, "*", SearchOption.AllDirectories))
        {
            File.Copy(file, GetTargetPath(file));
        }

        string GetTargetPath(string path)
        {
            var relative = Path.GetRelativePath(sourcePath, path);

            return Path.Combine(targetPath, relative);
        }
    }
}
