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

namespace Apache.Ignite.Tests.Linq;

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Internal.Linq;
using NUnit.Framework;

/// <summary>
/// Tests for <see cref="MemberInitCacheTarget"/>.
/// </summary>
public class MemberInitCacheTargetTest
{
    [Test]
    public void TestTargetsWithSameCtorAreEqual()
    {
        var target1 = new MemberInitCacheTarget(GetCtor(), ImmutableList<MemberBinding>.Empty);
        var target2 = new MemberInitCacheTarget(GetCtor(), ImmutableList<MemberBinding>.Empty);

        Assert.AreEqual(target1, target2);
        Assert.AreEqual(target1.GetHashCode(), target2.GetHashCode());
        Assert.IsTrue(target1 == target2);
    }

    [Test]
    public void TestTargetsWithTheSameMembersAreEqual()
    {
        var target1 = new MemberInitCacheTarget(GetCtor(), GetBindings());
        var target2 = new MemberInitCacheTarget(GetCtor(), GetBindings());

        Assert.AreEqual(target1, target2);
        Assert.AreEqual(target1.GetHashCode(), target2.GetHashCode());
        Assert.IsTrue(target1 == target2);
    }

    [Test]
    public void TestTargetsWithDifferentCtorAreNotEqual()
    {
        var target1 = new MemberInitCacheTarget(GetCtor(), ImmutableList<MemberBinding>.Empty);
        var target2 = new MemberInitCacheTarget(GetCtor(1), ImmutableList<MemberBinding>.Empty);

        Assert.AreNotEqual(target1, target2);
        Assert.AreNotEqual(target1.GetHashCode(), target2.GetHashCode());
        Assert.IsFalse(target1 == target2);
    }

    [Test]
    public void TestTargetsWithDifferentMembersAreNotEqual()
    {
        var target1 = new MemberInitCacheTarget(GetCtor(), GetBindings().Take(1).ToArray());
        var target2 = new MemberInitCacheTarget(GetCtor(), GetBindings().TakeLast(1).ToArray());

        Assert.AreNotEqual(target1, target2);
        Assert.AreNotEqual(target1.GetHashCode(), target2.GetHashCode());
        Assert.IsFalse(target1 == target2);
    }

    [Test]
    public void TestTargetsWithSameMembersDifferentOrderAreNotEqual()
    {
        var target1 = new MemberInitCacheTarget(GetCtor(), GetBindings());
        var target2 = new MemberInitCacheTarget(GetCtor(), GetBindings().Reverse().ToArray());

        Assert.AreNotEqual(target1, target2);
        Assert.AreNotEqual(target1.GetHashCode(), target2.GetHashCode());
        Assert.IsFalse(target1 == target2);
    }

    private static IReadOnlyList<MemberBinding> GetBindings()
    {
        Expression<Func<TestClass, object>> x = tc => new TestClass
        {
            I1 = tc.I1,
            I2 = tc.I2
        };

        return ((MemberInitExpression)x.Body).Bindings;
    }

    private static ConstructorInfo GetCtor(int i = 0) => typeof(TestClass).GetConstructors().Skip(i).First();

    private class TestClass
    {
        public TestClass()
        {
            // No-op.
        }

        // ReSharper disable once UnusedMember.Local
        // ReSharper disable once UnusedParameter.Local
        public TestClass(int i)
        {
            // No-op.
        }

        public int I1 { get; set; }

        public int I2 { get; set; }
    }
}
