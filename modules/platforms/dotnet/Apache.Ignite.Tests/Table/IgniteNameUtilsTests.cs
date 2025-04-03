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

namespace Apache.Ignite.Tests.Table;

using System;
using Internal.Table;
using NUnit.Framework;

/// <summary>
/// Tests for <see cref="IgniteNameUtils"/>.
/// </summary>
public class IgniteNameUtilsTests
{
    [TestCaseSource(nameof(ValidUnquotedIdentifiers))]
    [TestCaseSource(nameof(ValidQuotedIdentifiers))]
    public void TestValidIdentifiers(string source, string expected)
    {
        string parsed = IgniteNameUtils.ParseIdentifier(source);

        Assert.AreEqual(expected, parsed);
        Assert.AreEqual(parsed, IgniteNameUtils.ParseIdentifier(IgniteNameUtils.QuoteIfNeeded(parsed)));
    }

    [TestCaseSource(nameof(MalformedIdentifiers))]
    public void TestMalformedIdentifiers(string source)
    {
        Assert.Throws<ArgumentException>(() => IgniteNameUtils.ParseIdentifier(source));
    }

    [TestCaseSource(nameof(QuoteIfNeeded))]
    public void TestQuoteIfNeeded(string source, string expected)
    {
        string quoted = IgniteNameUtils.QuoteIfNeeded(source);

        Assert.AreEqual(expected, quoted);
        Assert.AreEqual(expected, IgniteNameUtils.QuoteIfNeeded(IgniteNameUtils.ParseIdentifier(quoted)));
    }

    private static TestCaseData[] QuoteIfNeeded() =>
    [
        new("foo", "\"foo\""),
        new("fOo", "\"fOo\""),
        new("FOO", "FOO"),
        new("1o0", "\"1o0\""),
        new("@#$", "\"@#$\""),
        new("f16", "\"f16\""),
        new("F16", "F16"),
        new("Ff16", "\"Ff16\""),
        new("FF16", "FF16"),
        new(" ", "\" \""),
        new(" F", "\" F\""),
        new(" ,", "\" ,\""),
        new("😅", "\"😅\""),
        new("\"foo\"", "\"\"\"foo\"\"\""),
        new("\"fOo\"", "\"\"\"fOo\"\"\""),
        new("\"f.f\"", "\"\"\"f.f\"\"\""),
        new("foo\"bar\"", "\"foo\"\"bar\"\"\""),
        new("foo\"bar", "\"foo\"\"bar\""),
    ];

    private static string[] MalformedIdentifiers() =>
    [
        " ", "foo-1", "f.f", "f f", "f\"f", "f\"\"f", "\"foo", "\"fo\"o\"", "1o0", "@#$", "😅", "f😅", "$foo", "foo$"
    ];

    private static TestCaseData[] ValidUnquotedIdentifiers() =>
    [
        new("foo", "FOO"),
        new("fOo", "FOO"),
        new("FOO", "FOO"),
        new("fo_o", "FO_O"),
        new("_foo", "_FOO"),
    ];

    private static TestCaseData[] ValidQuotedIdentifiers() =>
    [
        new("\"FOO\"", "FOO"),
        new("\"foo\"", "foo"),
        new("\"fOo\"", "fOo"),
        new("\"$fOo\"", "$fOo"),
        new("\"f.f\"", "f.f"),
        new("\"f\"\"f\"", "f\"f"),
        new("\" \"", " "),
        new("\"   \"", "   "),
        new("\",\"", ","),
        new("\"😅\"", "😅"),
        new("\"f😅\"", "f😅"),
    ];
}
