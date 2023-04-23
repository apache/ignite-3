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

namespace Apache.Ignite.Tests.Common;

using System.Text;
using Internal.Common;
using NUnit.Framework;

/// <summary>
/// Tests for <see cref="StringBuilderExtensions"/>.
/// </summary>
public class StringBuilderExtensionsTests
{
    [Test]
    [TestCase("abc", "abc")]
    [TestCase(" abc ", " abc")]
    [TestCase("  abc  ", "  abc")]
    public void TestTrimEnd(string input, string expected)
    {
        var sb = new StringBuilder(input);
        sb.TrimEnd();

        Assert.AreEqual(expected, sb.ToString());
    }

    [Test]
    [TestCase("abc", "abc def")]
    [TestCase("abc ", "abc def")]
    [TestCase("abc  ", "abc  def")]
    public void TestAppendWithSpace(string input, string expected)
    {
        var sb = new StringBuilder(input);
        sb.AppendWithSpace("def");

        Assert.AreEqual(expected, sb.ToString());
    }
}
