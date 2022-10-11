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

namespace Apache.Ignite.Tests;

using NUnit.Framework;

/// <summary>
/// Tests for <see cref="Option{T}"/>.
/// </summary>
public sealed class OptionTests
{
    [Test]
    public void TestDefaultValueEqualsNone()
    {
        Assert.AreEqual(default(Option<int>), Option.None<int>());
        Assert.IsTrue(Option.None<int>() == default);

        Assert.AreEqual(default(Option<string>), Option.None<string>());
        Assert.IsTrue(Option.None<string>() == default);
    }

    [Test]
    public void TestEquality()
    {
        Assert.Fail("TODO");
    }

    [Test]
    public void TestReferenceType()
    {
        Assert.Fail("TODO");
    }

    [Test]
    public void TestValueType()
    {
        Assert.Fail("TODO");
    }

    [Test]
    public void TestReferenceTypeDeconstruct()
    {
        Assert.Fail("TODO");
    }

    [Test]
    public void TestValueTypeDeconstruct()
    {
        Assert.Fail("TODO");
    }

    [Test]
    public void TestToStringNone()
    {
        Assert.AreEqual("Option { Value = , HasValue = False }", Option.None<int>().ToString());
        Assert.AreEqual("Option { Value = , HasValue = False }", Option.None<string>().ToString());
    }

    [Test]
    public void TestToStringSome()
    {
        Assert.AreEqual("Option { Value = 123, HasValue = True }", Option.Some(123).ToString());
        Assert.AreEqual("Option { Value = Foo, HasValue = True }", Option.Some("Foo").ToString());
    }
}