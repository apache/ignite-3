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
using System.Diagnostics.CodeAnalysis;
using Ignite.Table;
using NUnit.Framework;

/// <summary>
/// Tests for <see cref="QualifiedName"/>.
/// </summary>
[SuppressMessage("ReSharper", "ObjectCreationAsStatement", Justification = "Tests")]
public class QualifiedNameTests
{
    [Test]
    public void TestInvalidNullNames()
    {
        Assert.Throws<ArgumentNullException>(() => QualifiedName.Parse(null!));
        Assert.Throws<ArgumentNullException>(() => QualifiedName.Of("s1", null!));
        Assert.Throws<ArgumentNullException>(() => QualifiedName.Of(null, null!));
    }

    [Test]
    public void TestDefaultSchemaName()
    {
        Assert.AreEqual(QualifiedName.DefaultSchemaName, QualifiedName.Parse("foo").SchemaName);
        Assert.AreEqual(QualifiedName.DefaultSchemaName, QualifiedName.Of(null, "foo").SchemaName);
    }

    [Test]
    public void TestCanonicalForm()
    {
        Assert.AreEqual("FOO.BAR", QualifiedName.Parse("foo.bar").CanonicalName);
        Assert.AreEqual("\"foo\".\"bar\"", QualifiedName.Parse("\"foo\".\"bar\"").CanonicalName);
    }

    [Test]
    [TestCaseSource(nameof(ValidSimpleNames))]
    public void TestValidSimpleNames(string actual, string expectedIdentifier)
    {
        var simple = QualifiedName.Of(null, actual);
        var parsed = QualifiedName.Parse(actual);

        Assert.AreEqual(expectedIdentifier, simple.ObjectName);
        Assert.AreEqual(expectedIdentifier, parsed.ObjectName);

        Assert.AreEqual(parsed, simple);
    }

    [Test]
    [TestCaseSource(nameof(ValidCanonicalNames))]
    public void TestValidCanonicalNames(string source, string schemaIdentifier, string objectIdentifier)
    {
        var parsed = QualifiedName.Parse(source);

        Assert.AreEqual(schemaIdentifier, parsed.SchemaName);
        Assert.AreEqual(objectIdentifier, parsed.ObjectName);

        Assert.AreEqual(parsed, QualifiedName.Parse(parsed.CanonicalName));
    }

    [Test]
    [TestCaseSource(nameof(MalformedSimpleNames))]
    public void TestMalformedSimpleNames(string source)
    {
        Assert.Throws<ArgumentException>(() => QualifiedName.Of(null, source));
        Assert.Throws<ArgumentException>(() => QualifiedName.Parse(source));
        Assert.Throws<ArgumentException>(() => QualifiedName.Of(source, "bar"));
    }

    [Test]
    public void TestUnexpectedCanonicalName()
    {
        string canonicalName = "f.f";

        Assert.Throws<ArgumentException>(() => QualifiedName.Of(canonicalName, "bar"));
        Assert.Throws<ArgumentException>(() => QualifiedName.Of(null, canonicalName));
    }

    [Test]
    [TestCaseSource(nameof(MalformedCanonicalNames))]
    public void TestMalformedCanonicalNames(string source)
    {
        Assert.Throws<ArgumentException>(() => QualifiedName.Parse(source));
    }

    [Test]
    [TestCase("x.", "Canonical name can't have empty parts: 'x.'")]
    [TestCase(".x", "Invalid identifier start '.' at position 0: '.x'. Unquoted identifiers must begin with a letter or an underscore.")]
    [TestCase("\"x", "Missing closing quote: '\"x")]
    [TestCase("y.\"x", "Missing closing quote: 'y.\"x")]
    [TestCase("\"xx\"yy\"", "Unexpected character '\"' after quote at position 3: '\"xx\"yy\"'")]
    [TestCase("123", "Invalid identifier start '1' at position 0: '123'. Unquoted identifiers must begin with a letter or an underscore.")]
    [TestCase("x.y.z", "Canonical name should have at most two parts: 'x.y.z'")]
    public void TestParsingErrors(string name, string expectedError)
    {
        var ex = Assert.Throws<ArgumentException>(() => QualifiedName.Parse(name));
        Assert.AreEqual(expectedError, ex.Message);
    }

    private static TestCaseData[] ValidSimpleNames() =>
    [
        new("foo", "FOO"),
        new("fOo", "FOO"),
        new("FOO", "FOO"),
        new("f23", "F23"),
        new("\"23f\"", "23f"),
        new("foo_", "FOO_"),
        new("foo_1", "FOO_1"),
        new("_foo", "_FOO"),
        new("__foo", "__FOO"),
        new("\"FOO\"", "FOO"),
        new("\"foo\"", "foo"),
        new("\"fOo\"", "fOo"),
        new("\"_foo\"", "_foo"),
        new("\"$foo\"", "$foo"),
        new("\"%foo\"", "%foo"),
        new("\"foo_\"", "foo_"),
        new("\"foo$\"", "foo$"),
        new("\"foo%\"", "foo%"),
        new("\"@#$\"", "@#$"),
        new("\"f.f\"", "f.f"),
        new("\"   \"", "   "),
        new("\"😅\"", "😅"),
        new("\"f\"\"f\"", "f\"f"),
        new("\"f\"\"\"\"f\"", "f\"\"f"),
        new("\"\"\"bar\"\"\"", "\"bar\""),
        new("\"\"\"\"\"bar\"\"\"", "\"\"bar\"")
    ];

    private static TestCaseData[] MalformedSimpleNames() =>
    [
        new(string.Empty),
        new(" "),
        new(".f"),
        new("f."),
        new("."),
        new("f f"),
        new("1o0"),
        new("@#$"),
        new("foo$"),
        new("foo%"),
        new("foo&"),
        new("f😅"),
        new("😅f"),
        new("f\"f"),
        new("f\"\"f"),
        new("\"foo"),
        new("\"fo\"o\""),
    ];

    private static TestCaseData[] MalformedCanonicalNames() =>
    [
        new("foo."),
        new(".bar"),
        new("."),
        new("foo..bar"),
        new("foo.bar."),
        new("foo.."),
        new("@#$.bar"),
        new("foo.@#$"),
        new("@#$"),
        new("1oo.bar"),
        new("foo.1ar"),
        new("1oo")
    ];

    private static TestCaseData[] ValidCanonicalNames() =>
    [
        new("\"foo.bar\".baz", "foo.bar", "BAZ"),
        new("foo.\"bar.baz\"", "FOO", "bar.baz"),
        new("\"foo.\"\"bar\"\"\".baz", "foo.\"bar\"", "BAZ"),
        new("foo.\"bar.\"\"baz\"", "FOO", "bar.\"baz"),
        new("_foo.bar", "_FOO", "BAR"),
        new("foo._bar", "FOO", "_BAR")
    ];
}
