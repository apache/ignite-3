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

using Internal.Common;
using NUnit.Framework;

/// <summary>
/// Tests <see cref="IgniteToStringBuilder"/>.
/// </summary>
public class IgniteToStringBuilderTests
{
    [Test]
    public void TestEmpty()
    {
        var builder = new IgniteToStringBuilder(typeof(IgniteToStringBuilderTests));

        Assert.AreEqual("IgniteToStringBuilderTests { }", builder.Build());
    }

    [Test]
    public void TestAppendOne()
    {
        var builder = new IgniteToStringBuilder(typeof(IgniteClient));
        builder.Append("a", 1);

        Assert.AreEqual("IgniteClient { a = 1 }", builder.Build());
    }

    [Test]
    public void TestAppendMultiple()
    {
        var res = new IgniteToStringBuilder(typeof(IgniteToStringBuilderTests))
            .Append("a", 1)
            .Append("b", 2)
            .Append("c", 3)
            .Build();

        Assert.AreEqual("IgniteToStringBuilderTests { a = 1, b = 2, c = 3 }", res);
    }

    [Test]
    public void TestAppendNestedRecord()
    {
        var res = new IgniteToStringBuilder(typeof(IgniteToStringBuilderTests)).Append("a", 1)
            .Append("b", new Foo(123))
            .Append("c", 3)
            .Build();

        Assert.AreEqual("IgniteToStringBuilderTests { a = 1, b = Foo { X = 123 }, c = 3 }", res);
    }

    [Test]
    public void TestGetNested()
    {
        var res = new IgniteToStringBuilder("Foo").Append("a", 1)
            .GetNested("Bar")
            .Append("b", 2)
            .Append("c", 3)
            .CloseNested()
            .Append("d", 4)
            .Build();

        Assert.AreEqual("IgniteToStringBuilderTests { a = 1, b = Foo { X = 123 }, c = 3 }", res);
    }

    // ReSharper disable once NotAccessedPositionalProperty.Local
    private record Foo(int X);
}
