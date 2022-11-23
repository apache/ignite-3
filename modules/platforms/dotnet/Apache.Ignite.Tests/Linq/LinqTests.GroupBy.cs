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
        // TODO IGNITE-18196 Remove cast to long for Sum and Count
        var query = PocoByteView.AsQueryable()
            .GroupBy(x => x.Val)
            .Select(x => new { x.Key, Count = (long)x.Count(), Sum = (long)x.Sum(e => e.Key), Avg = x.Average(e => e.Key) })
            .OrderBy(x => x.Key);

        var res = query.ToList();

        Assert.AreEqual(1, res[1].Key);
        Assert.AreEqual(3, res[1].Count);
        Assert.AreEqual(4.0d, res[1].Avg);

        StringAssert.Contains(
            "select _T0.VAL, count (*) , sum (_T0.KEY) , avg (_T0.KEY)  " +
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
        // TODO IGNITE-18196 Remove cast to long for Sum and Count
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
            .Select(g => new {Cat = g.Key, Count = (long)g.Count()})
            .OrderBy(x => x.Cat);

        var res = query.ToList();

        Assert.AreEqual(0, res[0].Cat);
        Assert.AreEqual(1, res[0].Count);
        Assert.AreEqual(10, res.Count);

        StringAssert.Contains(
            "select _T0.VAL, count (*)  " +
            "from PUBLIC.TBL1 as _T1 " +
            "inner join PUBLIC.TBL_INT32 as _T0 on (_T0.KEY = _T1.KEY) " +
            "group by (_T0.VAL) " +
            "order by (_T0.VAL) asc",
            query.ToString());
    }
}
