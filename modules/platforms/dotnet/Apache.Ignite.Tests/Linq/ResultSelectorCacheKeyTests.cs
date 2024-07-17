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

using System.Linq;
using Ignite.Sql;
using Internal.Linq;
using Internal.Sql;
using NUnit.Framework;

/// <summary>
/// Tests for <see cref="ResultSelectorCacheKey{T}"/>.
/// </summary>
public class ResultSelectorCacheKeyTests
{
    [Test]
    public void TestKeysWithSameColumnsAreEqual()
    {
        var target = new object();
        var columns = GetColumns();

        var key1 = new ResultSelectorCacheKey<object>(target, columns);
        var key2 = new ResultSelectorCacheKey<object>(target, columns);

        Assert.AreEqual(key1, key2);
        Assert.AreEqual(key1.GetHashCode(), key2.GetHashCode());
        Assert.IsTrue(key1 == key2);
    }

    [Test]
    public void TestKeysWithSameColumnTypeAndScaleAreEqual()
    {
        var target = new object();
        var columns = GetColumns();
        var columns2 = GetColumns();

        // Change everything except type and scale.
        columns2[0] = columns2[0] with { Name = "foo", Origin = new ColumnOrigin("a", "b", "c"), Precision = 123 };

        var key1 = new ResultSelectorCacheKey<object>(target, columns);
        var key2 = new ResultSelectorCacheKey<object>(target, columns2);

        Assert.AreEqual(key1, key2);
        Assert.AreEqual(key1.GetHashCode(), key2.GetHashCode());
        Assert.IsTrue(key1 == key2);
    }

    [Test]
    public void TestKeysWithDifferentColumnTypesAreNotEqual()
    {
        var target = new object();
        var columns = GetColumns();
        var columns2 = GetColumns();

        columns2[0] = columns2[0] with { Type = ColumnType.String };

        var key1 = new ResultSelectorCacheKey<object>(target, columns);
        var key2 = new ResultSelectorCacheKey<object>(target, columns2);

        Assert.AreNotEqual(key1, key2);
        Assert.AreNotEqual(key1.GetHashCode(), key2.GetHashCode());
        Assert.IsFalse(key1 == key2);
    }

    [Test]
    public void TestKeysWithDifferentColumnScalesAreNotEqual()
    {
        var target = new object();
        var columns = GetColumns();
        var columns2 = GetColumns();

        columns2[0] = columns2[0] with { Scale = -1 };

        var key1 = new ResultSelectorCacheKey<object>(target, columns);
        var key2 = new ResultSelectorCacheKey<object>(target, columns2);

        Assert.AreNotEqual(key1, key2);
        Assert.AreNotEqual(key1.GetHashCode(), key2.GetHashCode());
        Assert.IsFalse(key1 == key2);
    }

    [Test]
    public void TestKeysWithSameColumnsDifferentOrderAreNotEqual()
    {
        var target = new object();
        var columns = GetColumns();

        var key1 = new ResultSelectorCacheKey<object>(target, columns);
        var key2 = new ResultSelectorCacheKey<object>(target, columns.Reverse().ToList());

        Assert.AreNotEqual(key1, key2);
        Assert.AreNotEqual(key1.GetHashCode(), key2.GetHashCode());
        Assert.IsFalse(key1 == key2);
    }

    private static ColumnMetadata[] GetColumns()
    {
        return new[]
        {
            new ColumnMetadata(
                Name: "c1",
                Type: ColumnType.Date,
                Precision: 1,
                Scale: 2,
                Nullable: true,
                Origin: null),
            new ColumnMetadata(
                Name: "c2",
                Type: ColumnType.Float,
                Precision: 4,
                Scale: 6,
                Nullable: false,
                Origin: null)
        };
    }
}
