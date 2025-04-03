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

    private static IEnumerable<TestCaseData> ValidSimpleNamesArgs()
    {
        yield return new("foo", "FOO");
        yield return new("fOo", "FOO");
        yield return new("FOO", "FOO");
        yield return new("f23", "F23");
        yield return new("\"23f\"", "23f");
        yield return new("foo_", "FOO_");
        yield return new("foo_1", "FOO_1");
        yield return new("_foo", "_FOO");
        yield return new("__foo", "__FOO");
        yield return new("\"FOO\"", "FOO");
        yield return new("\"foo\"", "foo");
        yield return new("\"fOo\"", "fOo");
        yield return new("\"_foo\"", "_foo");
        yield return new("\"$foo\"", "$foo");
        yield return new("\"%foo\"", "%foo");
        yield return new("\"foo_\"", "foo_");
        yield return new("\"foo$\"", "foo$");
        yield return new("\"foo%\"", "foo%");
        yield return new("\"@#$\"", "@#$");
        yield return new("\"f.f\"", "f.f");
        yield return new("\"   \"", "   ");
        yield return new("\"😅\"", "😅");
        yield return new("\"f\"\"f\"", "f\"f");
        yield return new("\"f\"\"\"\"f\"", "f\"\"f");
        yield return new("\"\"\"bar\"\"\"", "\"bar\"");
        yield return new("\"\"\"\"\"bar\"\"\"", "\"\"bar\"");
    }

    private static IEnumerable<TestCaseData> MalformedSimpleNamesArgs()
    {
        yield return new(string.Empty);
        yield return new(" ");
        yield return new(".f");
        yield return new("f.");
        yield return new(".");
        yield return new("f f");
        yield return new("1o0");
        yield return new("@#$");
        yield return new("foo$");
        yield return new("foo%");
        yield return new("foo&");
        yield return new("f😅");
        yield return new("😅f");
        yield return new("f\"f");
        yield return new("f\"\"f");
        yield return new("\"foo");
        yield return new("\"fo\"o\"");
    }

    private static IEnumerable<TestCaseData> MalformedCanonicalNamesArgs()
    {
        yield return new("foo.");
        yield return new(".bar");
        yield return new(".");
        yield return new("foo..bar");
        yield return new("foo.bar.");
        yield return new("foo..");
        yield return new("@#$.bar");
        yield return new("foo.@#$");
        yield return new("@#$");
        yield return new("1oo.bar");
        yield return new("foo.1ar");
        yield return new("1oo");
    }

    private static IEnumerable<TestCaseData> ValidCanonicalNamesArgs()
    {
        yield return new("\"foo.bar\".baz", "foo.bar", "BAZ");
        yield return new("foo.\"bar.baz\"", "FOO", "bar.baz");
        yield return new("\"foo.\"\"bar\"\"\".baz", "foo.\"bar\"", "BAZ");
        yield return new("foo.\"bar.\"\"baz\"", "FOO", "bar.\"baz");
        yield return new("_foo.bar", "_FOO", "BAR");
        yield return new("foo._bar", "FOO", "_BAR");
    }
}
