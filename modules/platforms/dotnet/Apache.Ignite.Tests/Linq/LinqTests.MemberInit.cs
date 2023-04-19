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
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using NUnit.Framework;

/// <summary>
/// Linq MemberInitTests.
/// </summary>
public partial class LinqTests
{
    [Test]
    public void TestSelectMemberInitInitOnly()
    {
        var res = PocoView.AsQueryable()
            .Where(x => x.Key == 2)
            .Select(x => new CustomProjectionCtorAndInit { ValueProp = x.Key, RefProp = x.Val })
            .ToArray();

        Assert.AreEqual(1, res.Length);
        Assert.AreEqual(2, res[0].ValueProp);
        Assert.AreEqual("v-2", res[0].RefProp);
    }

    [Test]
    public void TestSelectMemberInitCtorOnly()
    {
        var res = PocoView.AsQueryable()
            .Where(x => x.Key == 2)
            .Select(x => new CustomProjectionCtorAndInit(x.Key + 42, x.Val))
            .ToArray();

        Assert.AreEqual(1, res.Length);
        Assert.AreEqual(44, res[0].Id);
        Assert.AreEqual("v-2", res[0].RefProp);
    }

    [Test]
    public void TestSelectMemberInitSupportsInitOnlyProps()
    {
        var res = PocoView.AsQueryable()
            .Where(x => x.Key == 2)
            .Select(x => new CustomProjectionCtorAndInit
            {
                RefPropInitOnly = x.Val,
                ValuePropInitOnly = x.Key + 2,
            })
            .ToArray();

        Assert.AreEqual(1, res.Length);
        var resRow = res[0];
        Assert.AreEqual("v-2", resRow.RefPropInitOnly);
        Assert.AreEqual(4, resRow.ValuePropInitOnly);
    }

    [Test]
    public void TestSelectMemberInitSupportsFields()
    {
        var res = PocoView.AsQueryable()
            .Where(x => x.Key == 2)
            .Select(x => new CustomProjectionCtorAndInit
            {
                RefField = x.Val,
                ValueField = x.Key + 3
            })
            .ToArray();

        Assert.AreEqual(1, res.Length);
        var resRow = res[0];
        Assert.AreEqual("v-2", resRow.RefField);
        Assert.AreEqual(5, resRow.ValueField);
    }

    [Test]
    public void TestSelectMemberInitCtorAndInit()
    {
        var res = PocoView.AsQueryable()
            .Where(x => x.Key == 2)
            .Select(x => new CustomProjectionCtorAndInit(x.Key)
            {
                RefProp = x.Val,
                ValueProp = x.Key + 1,
                RefPropInitOnly = x.Val,
                ValuePropInitOnly = x.Key + 2,
                RefField = x.Val,
                ValueField = x.Key + 3
            })
            .ToArray();

        Assert.AreEqual(1, res.Length);
        var resRow = res[0];
        Assert.AreEqual(2, resRow.Id);
        Assert.AreEqual("v-2", resRow.RefProp);
        Assert.AreEqual(3, resRow.ValueProp);
        Assert.AreEqual("v-2", resRow.RefPropInitOnly);
        Assert.AreEqual(4, resRow.ValuePropInitOnly);
        Assert.AreEqual("v-2", resRow.RefField);
        Assert.AreEqual(5, resRow.ValueField);
    }

    [Test]
    [SuppressMessage("ReSharper", "ReturnValueOfPureMethodIsNotUsed", Justification = "Reviewed")]
    public void TestSelectMemberInitSingleWithMultipleRowsThrows()
    {
        var ex = Assert.Throws<InvalidOperationException>(
            () => PocoView.AsQueryable()
                .Select(x => new CustomProjectionCtorAndInit(x.Key) { RefField = x.Val })
                .Single());

        const string expected = "ResultSet is expected to have one row, but has more: " +
                                "select _T0.KEY, _T0.VAL as REFFIELD from PUBLIC.TBL1 as _T0 limit 2";

        Assert.AreEqual(expected, ex!.Message);

        _ = Assert.Throws<InvalidOperationException>(
            () => PocoView.AsQueryable()
                .Select(x => new CustomProjectionCtorAndInit(x.Key)
                {
                    RefProp = x.Val,
                    ValueProp = x.Key + 1,
                    RefPropInitOnly = x.Val,
                    ValuePropInitOnly = x.Key + 2,
                    RefField = x.Val,
                    ValueField = x.Key + 3
                })
                .Single());

        _ = Assert.Throws<InvalidOperationException>(
            () => PocoView.AsQueryable()
                .Select(x => new CustomProjectionCtorAndInit(x.Key))
                .Single());

        _ = Assert.Throws<InvalidOperationException>(
            () => PocoView.AsQueryable()
                .Select(x => new CustomProjectionCtorAndInit { RefField = x.Val })
                .Single());
    }

    [Test]
    public void TestSelectMemberInitFirstOrDefaultReturnsNullWithEmptyResponse()
    {
        var query = PocoView.AsQueryable()
            .Where(poco => poco.Val == Guid.NewGuid().ToString());

        Assert.IsNull(query
            .Select(x => new CustomProjectionCtorAndInit(x.Key) { RefField = x.Val })
            .FirstOrDefault());

        Assert.IsNull(query
            .Select(x => new CustomProjectionCtorAndInit(x.Key))
            .FirstOrDefault());

        Assert.IsNull(query
            .Select(x => new CustomProjectionCtorAndInit(x.Key)
            {
                RefProp = x.Val,
                ValueProp = x.Key + 1,
                RefPropInitOnly = x.Val,
                ValuePropInitOnly = x.Key + 2,
                RefField = x.Val,
                ValueField = x.Key + 3
            })
            .FirstOrDefault());
    }

    private class CustomProjectionCtorAndInit
    {
        public CustomProjectionCtorAndInit()
        {
            // No-op.
        }

        public CustomProjectionCtorAndInit(long id)
            : this(id, null)
        {
            // No-op.
        }

        public CustomProjectionCtorAndInit(long id, string? value)
        {
            Id = id;
            RefProp = value;
        }

        public long Id { get; }

        public string? RefProp { get; set; }

        public long ValueProp { get; set; }

        public string? RefPropInitOnly { get; init; }

        public long ValuePropInitOnly { get; init; }

#pragma warning disable CS0649
#pragma warning disable SA1401
        public string? RefField;

        public long ValueField;
#pragma warning restore SA1401
#pragma warning restore CS0649
    }
}
