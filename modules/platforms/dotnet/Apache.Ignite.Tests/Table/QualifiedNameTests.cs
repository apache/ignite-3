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
    public void TestCanonicalName()
    {
        Assert.Fail("TODO");
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
        yield return new TestCaseData("foo", "FOO");
        yield return new TestCaseData("fOo", "FOO");
        yield return new TestCaseData("FOO", "FOO");
        yield return new TestCaseData("f23", "F23");
        yield return new TestCaseData("\"23f\"", "23f");
        yield return new TestCaseData("foo_", "FOO_");
        yield return new TestCaseData("foo_1", "FOO_1");
        yield return new TestCaseData("_foo", "_FOO");
        yield return new TestCaseData("__foo", "__FOO");
        yield return new TestCaseData("\"FOO\"", "FOO");
        yield return new TestCaseData("\"foo\"", "foo");
        yield return new TestCaseData("\"fOo\"", "fOo");
        yield return new TestCaseData("\"_foo\"", "_foo");
        yield return new TestCaseData("\"$foo\"", "$foo");
        yield return new TestCaseData("\"%foo\"", "%foo");
        yield return new TestCaseData("\"foo_\"", "foo_");
        yield return new TestCaseData("\"foo$\"", "foo$");
        yield return new TestCaseData("\"foo%\"", "foo%");
        yield return new TestCaseData("\"@#$\"", "@#$");
        yield return new TestCaseData("\"f.f\"", "f.f");
        yield return new TestCaseData("\"   \"", "   ");
        yield return new TestCaseData("\"😅\"", "😅");
        yield return new TestCaseData("\"f\"\"f\"", "f\"f");
        yield return new TestCaseData("\"f\"\"\"\"f\"", "f\"\"f");
        yield return new TestCaseData("\"\"\"bar\"\"\"", "\"bar\"");
        yield return new TestCaseData("\"\"\"\"\"bar\"\"\"", "\"\"bar\"");
    }

    private static IEnumerable<TestCaseData> MalformedSimpleNamesArgs()
    {
        yield return new TestCaseData(string.Empty);
        yield return new TestCaseData(" ");
        yield return new TestCaseData(".f");
        yield return new TestCaseData("f.");
        yield return new TestCaseData(".");
        yield return new TestCaseData("f f");
        yield return new TestCaseData("1o0");
        yield return new TestCaseData("@#$");
        yield return new TestCaseData("foo$");
        yield return new TestCaseData("foo%");
        yield return new TestCaseData("foo&");
        yield return new TestCaseData("f😅");
        yield return new TestCaseData("😅f");
        yield return new TestCaseData("f\"f");
        yield return new TestCaseData("f\"\"f");
        yield return new TestCaseData("\"foo");
        yield return new TestCaseData("\"fo\"o\"");
    }

    private static IEnumerable<TestCaseData> MalformedCanonicalNamesArgs()
    {
        yield return new TestCaseData("foo.");
        yield return new TestCaseData(".bar");
        yield return new TestCaseData(".");
        yield return new TestCaseData("foo..bar");
        yield return new TestCaseData("foo.bar.");
        yield return new TestCaseData("foo..");
        yield return new TestCaseData("@#$.bar");
        yield return new TestCaseData("foo.@#$");
        yield return new TestCaseData("@#$");
        yield return new TestCaseData("1oo.bar");
        yield return new TestCaseData("foo.1ar");
        yield return new TestCaseData("1oo");
    }

    private static IEnumerable<TestCaseData> ValidCanonicalNamesArgs()
    {
        yield return new TestCaseData("\"foo.bar\".baz", "foo.bar", "BAZ");
        yield return new TestCaseData("foo.\"bar.baz\"", "FOO", "bar.baz");
        yield return new TestCaseData("\"foo.\"\"bar\"\"\".baz", "foo.\"bar\"", "BAZ");
        yield return new TestCaseData("foo.\"bar.\"\"baz\"", "FOO", "bar.\"baz\"");
        yield return new TestCaseData("_foo.bar", "_FOO", "BAR");
        yield return new TestCaseData("foo._bar", "FOO", "_BAR");
    }
}
