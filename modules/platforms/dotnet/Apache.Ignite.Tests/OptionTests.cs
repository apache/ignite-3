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

using System;
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
    public void TestNoneValueThrows()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
        {
            _ = Option.None<int>().Value;
        });

        Assert.AreEqual("Value is not present. Check HasValue property before accessing Value.", ex!.Message);
    }

    [Test]
    public void TestEquality()
    {
        Assert.AreEqual(Option.Some(123), Option.Some(123));
        Assert.AreNotEqual(Option.Some(123), Option.Some(124));
    }

    [Test]
    public void TestSomeReferenceTypeDeconstruct()
    {
        var (val, hasVal) = Option.Some("abc");

        Assert.IsTrue(hasVal);
        Assert.AreEqual("abc", val);
    }

    [Test]
    public void TestNoneReferenceTypeDeconstruct()
    {
        var (val, hasVal) = Option.None<string>();

        Assert.IsFalse(hasVal);
        Assert.IsNull(val);
    }

    [Test]
    public void TestSomeValueTypeDeconstruct()
    {
        var (val, hasVal) = Option.Some(123L);

        Assert.IsTrue(hasVal);
        Assert.AreEqual(123L, val);
    }

    [Test]
    public void TestNoneValueTypeDeconstruct()
    {
        var (val, hasVal) = Option.None<long>();

        Assert.IsFalse(hasVal);
        Assert.AreEqual(0L, val);
    }

    [Test]
    public void TestNoneToString()
    {
        Assert.AreEqual("Option { HasValue = False }", Option.None<int>().ToString());
        Assert.AreEqual("Option { HasValue = False }", Option.None<string>().ToString());
    }

    [Test]
    public void TestSomeToString()
    {
        Assert.AreEqual("Option { HasValue = True, Value = 123 }", Option.Some(123).ToString());
        Assert.AreEqual("Option { HasValue = True, Value = Foo }", Option.Some("Foo").ToString());
    }
}
