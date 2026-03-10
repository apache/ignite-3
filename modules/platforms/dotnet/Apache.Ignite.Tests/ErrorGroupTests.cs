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
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Text.RegularExpressions;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="ErrorGroups"/>.
    /// </summary>
    public class ErrorGroupTests
    {
        private static readonly string JavaErrorGroupsFile = Path.Combine(
            TestUtils.RepoRootDir, "modules", "api", "src", "main", "java", "org", "apache", "ignite", "lang", "ErrorGroups.java");

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
                var name = ErrorGroups.GetGroupName(code);

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
                var expectedGroup = ErrorGroups.GetGroupCode(code);

                Assert.AreEqual(expectedGroup, group, $"Code {code} ({name}) has incorrect group. Expected {expectedGroup}, got {group}.");
            }
        }

        [Test]
        public void TestJavaErrorGroupsAndCodesHaveDotNetCounterparts()
        {
            var dotNetErrorCodes = GetErrorCodes().ToDictionary(x => x.Code, x => (x.Name, x.GroupCode));

            var javaErrorGroupsText = File.ReadAllText(JavaErrorGroupsFile);

            // ErrorGroup TX_ERR_GROUP = registerGroup("TX", 7);
            var javaErrorGroups = Regex.Matches(
                javaErrorGroupsText,
                @"ErrorGroup ([\w_]+)_ERR_GROUP = registerGroup\(""(\w+)"", \(short\)\s*(\d+)\);")
                .Select(x => (Name: x.Groups[1].Value, ShortName: x.Groups[2].Value, Code: short.Parse(x.Groups[3].Value, CultureInfo.InvariantCulture)))
                .ToList();

            Assert.GreaterOrEqual(javaErrorGroups.Count, 7);

            foreach (var (grpName, grpShortName, grpCode) in javaErrorGroups)
            {
                var dotNetName = ErrorGroups.GetGroupName(grpCode);

                Assert.AreEqual(grpShortName, dotNetName, $"Java and .NET error group '{grpName}' names do not match");

                // TX_STATE_STORAGE_CREATE_ERR = TX_ERR_GROUP.registerErrorCode(1)
                var javaErrors = Regex.Matches(
                        javaErrorGroupsText,
                        @"([\w_]+) = " + grpName + @"_ERR_GROUP\.registerErrorCode\(\(short\)\s*(\d+)\);")
                    .Select(x => (Name: x.Groups[1].Value, Code: short.Parse(x.Groups[2].Value, CultureInfo.InvariantCulture)))
                    .ToList();

                Assert.IsNotEmpty(javaErrors);

                foreach (var (errName, errCode) in javaErrors)
                {
                    var fullErrCode = ErrorGroups.GetFullCode(grpCode, errCode);
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
            Assert.AreEqual(1, ErrorGroups.Common.GroupCode);
            Assert.AreEqual("CMN", ErrorGroups.Common.GroupName);
            Assert.AreEqual("CMN", ErrorGroups.GetGroupName(1));

            Assert.AreEqual(2, ErrorGroups.Table.GroupCode);
            Assert.AreEqual("TBL", ErrorGroups.Table.GroupName);
            Assert.AreEqual("TBL", ErrorGroups.GetGroupName(2));

            Assert.AreEqual(3, ErrorGroups.Client.GroupCode);
            Assert.AreEqual("CLIENT", ErrorGroups.Client.GroupName);
            Assert.AreEqual("CLIENT", ErrorGroups.GetGroupName(3));

            Assert.AreEqual(4, ErrorGroups.Sql.GroupCode);
            Assert.AreEqual("SQL", ErrorGroups.Sql.GroupName);
            Assert.AreEqual("SQL", ErrorGroups.GetGroupName(4));

            Assert.AreEqual(5, ErrorGroups.MetaStorage.GroupCode);
            Assert.AreEqual("META", ErrorGroups.MetaStorage.GroupName);
            Assert.AreEqual("META", ErrorGroups.GetGroupName(5));

            Assert.AreEqual(6, ErrorGroups.Index.GroupCode);
            Assert.AreEqual("IDX", ErrorGroups.Index.GroupName);
            Assert.AreEqual("IDX", ErrorGroups.GetGroupName(6));

            Assert.AreEqual(7, ErrorGroups.Transactions.GroupCode);
            Assert.AreEqual("TX", ErrorGroups.Transactions.GroupName);
            Assert.AreEqual("TX", ErrorGroups.GetGroupName(7));

            Assert.AreEqual(8, ErrorGroups.Replicator.GroupCode);
            Assert.AreEqual("REP", ErrorGroups.Replicator.GroupName);
            Assert.AreEqual("REP", ErrorGroups.GetGroupName(8));

            Assert.AreEqual(10, ErrorGroups.DistributionZones.GroupCode);
            Assert.AreEqual("DISTRZONES", ErrorGroups.DistributionZones.GroupName);
            Assert.AreEqual("DISTRZONES", ErrorGroups.GetGroupName(10));
        }

        [Test]
        public void TestGetGroupNameForUnknownGroupCode()
        {
            // Newer servers may return unknown to us error codes.
            Assert.AreEqual("UNKNOWN-1", ErrorGroups.GetGroupName(-1));
            Assert.AreEqual("UNKNOWN9999", ErrorGroups.GetGroupName(9999));
        }

        [Test]
        public void TestExceptionProperties()
        {
            var ex = new IgniteException(Guid.Empty, ErrorGroups.Common.Internal, "msg");

            Assert.AreEqual(ErrorGroups.Common.Internal, ex.Code);

            Assert.AreEqual(-1, ex.ErrorCode);
            Assert.AreEqual("IGN-CMN-65535", ex.CodeAsString);

            Assert.AreEqual(1, ex.GroupCode);
            Assert.AreEqual("CMN", ex.GroupName);
        }

        [Test]
        public void TestUnknownErrorCode()
        {
            int unknownCode = (999 << 16) | 1;
            var traceId = Guid.NewGuid();
            string message = "Error from unknown group";

            var ex = new IgniteException(traceId, unknownCode, message);

            Assert.AreEqual(unknownCode, ex.Code);
            Assert.AreEqual(999, ex.GroupCode);
            Assert.AreEqual(1, ex.ErrorCode);
            Assert.AreEqual(traceId, ex.TraceId);
            Assert.AreEqual(message, ex.Message);
            Assert.AreEqual("UNKNOWN-UNKNOWN999-1", ex.CodeAsString);
            Assert.AreEqual("UNKNOWN999", ex.GroupName);
        }

        private static IEnumerable<(short Code, string Name)> GetErrorGroups() => typeof(ErrorGroups).GetNestedTypes()
                .Select(x => ((short) x.GetField("GroupCode")!.GetValue(null)!, x.Name));

        private static IEnumerable<(int Code, short GroupCode, string Name)> GetErrorCodes() => typeof(ErrorGroups).GetNestedTypes()
                .SelectMany(groupClass =>
                {
                    var groupCode = (short)groupClass.GetField("GroupCode")!.GetValue(null)!;

                    return groupClass
                        .GetFields()
                        .Where(x => x.Name != "GroupCode" && x.Name != "GroupName" && x.Name != "ErrorPrefix" &&
                                    x.GetCustomAttributes(typeof(ObsoleteAttribute), false).Length == 0)
                        .Select(errCode => ((int)errCode.GetValue(null)!, groupCode, errCode.Name));
                });
    }
}
