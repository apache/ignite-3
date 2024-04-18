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

namespace Apache.Ignite.Tests;

using System.IO;
using System.Text.RegularExpressions;
using Internal;
using NUnit.Framework;

public class VersionTests
{
    [Test]
    public void TestAssemblyInformationalVersionHasGitCommitHash()
    {
        var informationalVersion = VersionUtils.InformationalVersion;
        StringAssert.StartsWith(GetAssemblyVersion(), informationalVersion);

        var parts = informationalVersion.Split('+');
        Assert.AreEqual(2, parts.Length, informationalVersion);
        StringAssert.IsMatch("[0-9a-f]{7}", parts[1], informationalVersion);
    }

    [Test]
    public void TestAssemblyVersionMatchesJavaServerVersion()
    {
        var buildGradle = Path.Combine(TestUtils.RepoRootDir, "build.gradle");
        var buildGradleText = File.ReadAllText(buildGradle);
        var versionMatch = Regex.Match(buildGradleText, @"version\s*=\s*""(.*?)""");

        Assert.IsTrue(versionMatch.Success);

        var gradleVersion = versionMatch.Groups[1].Value.Replace("-SNAPSHOT", string.Empty);

        Assert.AreEqual(gradleVersion, GetAssemblyVersion());
    }

    private static string GetAssemblyVersion()
    {
        var asm = typeof(IIgnite).Assembly;
        var asmVersion = asm.GetName().Version!;

        return $"{asmVersion.Major}.{asmVersion.Minor}.{asmVersion.Build}";
    }
}
