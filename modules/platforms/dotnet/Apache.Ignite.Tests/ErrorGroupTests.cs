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
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="ErrorGroup"/>.
    /// </summary>
    public class ErrorGroupTests
    {
        // TODO:
        // 1. Test that all group and error codes are unique.
        // 2. Test that all Java groups and codes are mapped to .NET.
        // 3. Test that all Java exception classes have .NET counterparts
        [Test]
        public void TestErrorGroupCodesAreUnique()
        {
            var errorGroups = typeof(ErrorGroup).GetNestedTypes();
            var existingCodes = new Dictionary<int, string>();

            Assert.IsNotEmpty(errorGroups);

            foreach (var errorGroup in errorGroups)
            {
                var groupCodeField = errorGroup.GetField("GroupCode");
                Assert.IsNotNull(groupCodeField);

                var groupCode = (int)groupCodeField!.GetValue(null)!;

                if (existingCodes.TryGetValue(groupCode, out var existingGroupName))
                {
                    Assert.Fail($"Duplicate group code: {groupCode} ({existingGroupName} and {errorGroup.Name})");
                }

                existingCodes.Add(groupCode, errorGroup.Name);
            }
        }

        [Test]
        public void TestErrorCodesAreUnique()
        {
        }
    }
}
