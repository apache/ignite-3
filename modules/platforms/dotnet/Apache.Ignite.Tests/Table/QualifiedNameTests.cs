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
using System.Collections.Generic;
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
    public void TestParse()
    {
        // TODO: More test cases.
        var qn = QualifiedName.Parse("schema.table");
        Assert.AreEqual("SCHEMA", qn.SchemaName);
        Assert.AreEqual("TABLE", qn.ObjectName);
    }

    [Test]
    public void InvalidNullNames()
    {
        Assert.Throws<ArgumentNullException>(() => QualifiedName.Parse(null!));
        Assert.Throws<ArgumentNullException>(() => QualifiedName.Of("s1", null!));
        Assert.Throws<ArgumentNullException>(() => QualifiedName.Of(null, null!));
    }

    [Test]
    public void DefaultSchemaName()
    {
        Assert.AreEqual(QualifiedName.DefaultSchemaName, QualifiedName.Parse("foo").SchemaName);
        Assert.AreEqual(QualifiedName.DefaultSchemaName, QualifiedName.Of(null, "foo").SchemaName);
    }

    [Test]
    public void CanonicalForm()
    {
        Assert.AreEqual("FOO.BAR", QualifiedName.Parse("foo.bar").CanonicalName);
        Assert.AreEqual("\"foo\".\"bar\"", QualifiedName.Parse("\"foo\".\"bar\"").CanonicalName);
    }

    [Test]
    [TestCaseSource(nameof(ValidSimpleNamesArgs))]
    public void ValidSimpleNames(string actual, string expectedIdentifier)
    {
        var simple = QualifiedName.Of(null, actual);
        var parsed = QualifiedName.Parse(actual);

        Assert.AreEqual(expectedIdentifier, simple.ObjectName);
        Assert.AreEqual(expectedIdentifier, parsed.ObjectName);

        Assert.AreEqual(parsed, simple);
    }

    [Test]
    [TestCaseSource(nameof(ValidCanonicalNamesArgs))]
    public void ValidCanonicalNames(string source, string schemaIdentifier, string objectIdentifier)
    {
        var parsed = QualifiedName.Parse(source);

        Assert.AreEqual(schemaIdentifier, parsed.SchemaName);
        Assert.AreEqual(objectIdentifier, parsed.ObjectName);

        Assert.AreEqual(parsed, QualifiedName.Parse(parsed.CanonicalName));
    }

    [Test]
    [TestCaseSource(nameof(MalformedSimpleNamesArgs))]
    public void MalformedSimpleNames(string source)
    {
        Assert.Throws<ArgumentException>(() => QualifiedName.Of(null, source));
        Assert.Throws<ArgumentException>(() => QualifiedName.Parse(source));
        Assert.Throws<ArgumentException>(() => QualifiedName.Of(source, "bar"));
    }

    [Test]
    public void UnexpectedCanonicalName()
    {
        string canonicalName = "f.f";

        Assert.Throws<ArgumentException>(() => QualifiedName.Of(canonicalName, "bar"));
        Assert.Throws<ArgumentException>(() => QualifiedName.Of(null, canonicalName));
    }

    [Test]
    [TestCaseSource(nameof(MalformedCanonicalNamesArgs))]
    public void MalformedCanonicalNames(string source)
    {
        Assert.Throws<ArgumentException>(() => QualifiedName.Parse(source));
    }

    private static IEnumerable<TestCaseData> ValidSimpleNamesArgs() =>
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
        new("\"\"\"\"\"bar\"\"\"", "\"\"bar\""),
    ];

    private static TestCaseData[] MalformedSimpleNamesArgs() =>
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

    private static TestCaseData[] MalformedCanonicalNamesArgs() =>
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

    private static TestCaseData[] ValidCanonicalNamesArgs() =>
    [
        new("foo.bar", "FOO", "BAR"),
        new("foo\"bar.baz", "foo\"bar", "BAZ"),
        new("\"foo\".bar", "FOO", "BAR"),
        new("\"foo\".\"bar\"", "FOO", "BAR"),
        new("\"foo\".\"bar.baz\"", "FOO", "bar.baz"),
        new("\"foo\".\"bar\".baz", "FOO", "BAR")
    ];
}
