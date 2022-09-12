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

namespace Apache.Ignite.Tests
{
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Text.RegularExpressions;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="ErrorGroup"/>.
    /// </summary>
    public class ErrorGroupTests
    {
        private static readonly string JavaErrorGroupsFile = Path.Combine(
            TestUtils.RepoRootDir, "modules", "core", "src", "main", "java", "org", "apache", "ignite", "lang", "ErrorGroups.java");

        [Test]
        public void TestErrorGroupCodesAreUnique()
        {
            var existingCodes = new Dictionary<int, string>();

            foreach (var (code, name) in GetErrorGroups())
            {
                if (existingCodes.TryGetValue(code, out var existingGroupName))
                {
                    Assert.Fail($"Duplicate group code: {code} ({existingGroupName} and {name})");
                }

                existingCodes.Add(code, name);
            }
        }

        [Test]
        public void TestGetGroupNameReturnsUniqueNames()
        {
            var existingNames = new Dictionary<string, string>();

            foreach (var (code, className) in GetErrorGroups())
            {
                var name = ErrorGroup.GetGroupName(code);

                if (existingNames.TryGetValue(name, out var existingClassName))
                {
                    Assert.Fail($"Duplicate group name: {name} ({existingClassName} and {className})");
                }

                existingNames.Add(name, className);
            }
        }

        [Test]
        public void TestErrorCodesAreUnique()
        {
            var duplicateCodes = GetErrorCodes()
                .GroupBy(x => x.Code)
                .Select(x => x.ToList())
                .Where(x => x.Count > 1)
                .ToList();

            Assert.Multiple(() => duplicateCodes.ForEach(
                x => Assert.Fail($"Duplicate error code: {x[0].Code} ({string.Join(", ", x.Select(y => y.Name))})")));
        }

        [Test]
        public void TestErrorCodeMatchesParentGroup()
        {
            foreach (var (code, group, name) in GetErrorCodes())
            {
                var expectedGroup = ErrorGroup.GetGroupCode(code);

                Assert.AreEqual(expectedGroup, group, $"Code {code} ({name}) has incorrect group. Expected {expectedGroup}, got {group}.");
            }
        }

        [Test]
        public void TestJavaErrorGroupsAndCodesHaveDotNetCounterparts()
        {
            var dotNetErrorCodes = GetErrorCodes().ToDictionary(x => x.Code, x => (x.Name, x.GroupCode));

            var javaErrorGroupsText = File.ReadAllText(JavaErrorGroupsFile);

            // ErrorGroup TX_ERR_GROUP = ErrorGroup.newGroup("TX", 7);
            var javaErrorGroups = Regex.Matches(
                javaErrorGroupsText,
                @"ErrorGroup ([\w_]+)_ERR_GROUP = ErrorGroup.newGroup\(""(\w+)"", (\d+)\);")
                .Select(x => (Name: x.Groups[1].Value, ShortName: x.Groups[2].Value, Code: int.Parse(x.Groups[3].Value, CultureInfo.InvariantCulture)))
                .ToList();

            Assert.GreaterOrEqual(javaErrorGroups.Count, 7);

            foreach (var (grpName, grpShortName, grpCode) in javaErrorGroups)
            {
                var dotNetName = ErrorGroup.GetGroupName(grpCode);

                Assert.AreEqual(grpShortName, dotNetName, $"Java and .NET error group '{grpName}' names do not match");

                // TX_STATE_STORAGE_CREATE_ERR = TX_ERR_GROUP.registerErrorCode(1)
                var javaErrors = Regex.Matches(
                        javaErrorGroupsText,
                        @"([\w_]+) = " + grpName + @"_ERR_GROUP\.registerErrorCode\((\d+)\);")
                    .Select(x => (Name: x.Groups[1].Value, Code: int.Parse(x.Groups[2].Value, CultureInfo.InvariantCulture)))
                    .ToList();

                Assert.IsNotEmpty(javaErrors);

                foreach (var (errName, errCode) in javaErrors)
                {
                    var fullErrCode = ErrorGroup.GetFullCode(grpCode, errCode);
                    var expectedDotNetName = errName.SnakeToCamelCase()[..^3];

                    if (!dotNetErrorCodes.TryGetValue(fullErrCode, out var dotNetError))
                    {
                        Assert.Fail(
                            $"Java error '{errName}' ('{errCode}') in group '{grpName}' ('{grpCode}') has no .NET counterpart.\n" +
                            $"public static readonly int {expectedDotNetName} = GetFullCode(GroupCode, {errCode});");
                    }

                    Assert.AreEqual(grpCode, dotNetError.GroupCode);
                    Assert.AreEqual(expectedDotNetName, dotNetError.Name);
                }
            }
        }

        [Test]
        public void TestGeneratedErrorGroups()
        {
            Assert.AreEqual(1, ErrorGroup.Common.GroupCode);
            Assert.AreEqual("CMN", ErrorGroup.Common.GroupName);
            Assert.AreEqual("CMN", ErrorGroup.GetGroupName(1));

            Assert.AreEqual(2, ErrorGroup.Table.GroupCode);
            Assert.AreEqual("TBL", ErrorGroup.Table.GroupName);
            Assert.AreEqual("TBL", ErrorGroup.GetGroupName(2));

            Assert.AreEqual(3, ErrorGroup.Client.GroupCode);
            Assert.AreEqual("CLIENT", ErrorGroup.Client.GroupName);
            Assert.AreEqual("CLIENT", ErrorGroup.GetGroupName(3));

            Assert.AreEqual(4, ErrorGroup.Sql.GroupCode);
            Assert.AreEqual("SQL", ErrorGroup.Sql.GroupName);
            Assert.AreEqual("SQL", ErrorGroup.GetGroupName(4));

            Assert.AreEqual(5, ErrorGroup.MetaStorage.GroupCode);
            Assert.AreEqual("META", ErrorGroup.MetaStorage.GroupName);
            Assert.AreEqual("META", ErrorGroup.GetGroupName(5));

            Assert.AreEqual(6, ErrorGroup.Index.GroupCode);
            Assert.AreEqual("IDX", ErrorGroup.Index.GroupName);
            Assert.AreEqual("IDX", ErrorGroup.GetGroupName(6));

            Assert.AreEqual(7, ErrorGroup.Transactions.GroupCode);
            Assert.AreEqual("TX", ErrorGroup.Transactions.GroupName);
            Assert.AreEqual("TX", ErrorGroup.GetGroupName(7));
        }

        private static IEnumerable<(int Code, string Name)> GetErrorGroups() => typeof(ErrorGroup).GetNestedTypes()
                .Select(x => ((int) x.GetField("GroupCode")!.GetValue(null)!, x.Name));

        private static IEnumerable<(int Code, int GroupCode, string Name)> GetErrorCodes() => typeof(ErrorGroup).GetNestedTypes()
                .SelectMany(groupClass =>
                {
                    var groupCode = (int)groupClass.GetField("GroupCode")!.GetValue(null)!;

                    return groupClass
                        .GetFields()
                        .Where(x => x.Name != "GroupCode" && x.Name != "GroupName")
                        .Select(errCode => ((int)errCode.GetValue(null)!, groupCode, errCode.Name));
                });
    }
}
