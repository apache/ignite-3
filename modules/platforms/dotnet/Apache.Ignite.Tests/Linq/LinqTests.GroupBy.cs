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

using System.Collections.Generic;
using System.Linq;
using Internal.Linq;
using NUnit.Framework;

/// <summary>
/// Linq GROUP BY tests.
/// </summary>
public partial class LinqTests
{
    [Test]
    public void TestGroupBySimple()
    {
        var query = PocoByteView.AsQueryable()
            .GroupBy(x => x.Val)
            .Select(x => x.Key)
            .OrderBy(x => x);

        List<sbyte> res = query.ToList();

        Assert.AreEqual(new[] { 0, 1, 2, 3 }, res);

        StringAssert.Contains(
            "select _T0.VAL " +
            "from PUBLIC.TBL_INT8 as _T0 " +
            "group by (_T0.VAL) " +
            "order by (_T0.VAL) asc",
            query.ToString());
    }

    [Test]
    public void TestGroupByMultiple()
    {
        var query = PocoByteView.AsQueryable()
            .GroupBy(x => new {x.Val, x.Key})
            .Select(x => x.Key.Key)
            .OrderBy(x => x)
            .Take(3);

        List<sbyte> res = query.ToList();

        Assert.AreEqual(new[] { 0, 1, 2 }, res);

        StringAssert.Contains(
            "select _T0.KEY " +
            "from PUBLIC.TBL_INT8 as _T0 " +
            "group by (_T0.VAL, _T0.KEY) " +
            "order by (_T0.KEY) asc",
            query.ToString());
    }

    [Test]
    public void TestGroupByWithAggregates()
    {
        var query = PocoByteView.AsQueryable()
            .GroupBy(x => x.Val)
            .Select(x => new { x.Key, Count = x.Count(), Sum = x.Sum(e => e.Key), Avg = x.Average(e => e.Key) })
            .OrderBy(x => x.Key);

        var res = query.ToList();

        Assert.AreEqual(1, res[1].Key);
        Assert.AreEqual(3, res[1].Count);
        Assert.AreEqual(4.0d, res[1].Avg);

        StringAssert.Contains(
            "select _T0.VAL, count(*), sum(cast(_T0.KEY as int)), avg(cast(_T0.KEY as int)) " +
            "from PUBLIC.TBL_INT8 as _T0 " +
            "group by (_T0.VAL) " +
            "order by (_T0.VAL) asc",
            query.ToString());
    }

    [Test]
    [Ignore("IGNITE-18215 Group by calculated value")]
    public void TestGroupBySubQuery()
    {
        var query = PocoByteView.AsQueryable()
            .Select(x => new {Id = x.Key + 1, Price = x.Val * 10})
            .GroupBy(x => x.Price)
            .Select(x => new { x.Key, Count = x.Count() })
            .OrderBy(x => x.Key);

        var res = query.ToList();

        Assert.AreEqual(1, res[1].Key);
        Assert.AreEqual(3, res[1].Count);

        StringAssert.Contains(
            "select (_T0.VAL * ?) as _G0, count (*)  " +
            "from PUBLIC.TBL_INT8 as _T0 " +
            "group by _G0 " +
            "order by _G0 asc",
            query.ToString());
    }

    [Test]
    public void TestGroupByWithJoinAndProjection()
    {
        var query1 = PocoView.AsQueryable();
        var query2 = PocoIntView.AsQueryable();

        var query = query1.Join(
                inner: query2,
                outerKeySelector: a => a.Key,
                innerKeySelector: b => b.Key,
                resultSelector: (a, b) => new
                {
                    Id = a.Key,
                    Category = b.Val,
                    Price = a.Val
                })
            .GroupBy(x => x.Category)
            .Select(g => new {Cat = g.Key, Count = g.Count()})
            .OrderBy(x => x.Cat);

        var res = query.ToList();

        Assert.AreEqual(0, res[0].Cat);
        Assert.AreEqual(1, res[0].Count);
        Assert.AreEqual(10, res.Count);

        StringAssert.Contains(
            "select _T0.VAL, count(*) " +
            "from PUBLIC.TBL1 as _T1 " +
            "inner join PUBLIC.TBL_INT32 as _T0 on (cast(_T0.KEY as bigint) = _T1.KEY) " +
            "group by (_T0.VAL) " +
            "order by (_T0.VAL) asc",
            query.ToString());
    }

    /// <summary>
    /// Tests grouping combined with join in a reverse order followed by a projection to an anonymous type with
    /// custom projected column names.
    /// <para />
    /// Covers <see cref="ExpressionWalker.GetProjectedMember"/>.
    /// </summary>
    [Test]
    public void TestGroupByWithReverseJoinAndAnonymousProjectionWithRename()
    {
        var query1 = PocoView.AsQueryable();
        var query2 = PocoIntView.AsQueryable();

        var query = query1.Join(
                query2,
                o => o.Key,
                p => p.Key,
                (org, person) => new
                {
                    Cat = org.Val,
                    Price = person.Val
                })
            .GroupBy(x => x.Cat)
            .Select(g => new {Category = g.Key, MaxPrice = g.Max(x => x.Price)})
            .OrderByDescending(x => x.MaxPrice);

        var res = query.ToList();

        Assert.AreEqual("v-9", res[0].Category);
        Assert.AreEqual(900, res[0].MaxPrice);

        StringAssert.Contains(
            "select _T0.VAL, max(_T1.VAL) " +
            "from PUBLIC.TBL1 as _T0 " +
            "inner join PUBLIC.TBL_INT32 as _T1 on (cast(_T1.KEY as bigint) = _T0.KEY) " +
            "group by (_T0.VAL) " +
            "order by (max(_T1.VAL)) desc",
            query.ToString());
    }
}
