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
    using System.Linq;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="ErrorGroup"/>.
    /// </summary>
    public class ErrorGroupTests
    {
        // TODO:
        // 2. Test that all Java groups and codes are mapped to .NET.
        // 3. Test that all Java exception classes have .NET counterparts
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
            Assert.Fail("TODO");
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

        private static IEnumerable<(int Code, string Name)> GetErrorGroups() => typeof(ErrorGroup).GetNestedTypes()
                .Select(x => ((int) x.GetField("GroupCode")!.GetValue(null)!, x.Name));

        private static IEnumerable<(int Code, int GroupCode, string Name)> GetErrorCodes() => typeof(ErrorGroup).GetNestedTypes()
                .SelectMany(groupClass =>
                {
                    var groupCode = (int)groupClass.GetField("GroupCode")!.GetValue(null)!;

                    return groupClass
                        .GetFields()
                        .Where(x => x.Name != "GroupCode")
                        .Select(errCode => ((int)errCode.GetValue(null)!, groupCode, errCode.Name));
                });
    }
}
