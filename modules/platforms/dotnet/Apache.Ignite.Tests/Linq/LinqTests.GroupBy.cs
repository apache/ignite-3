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
    public void TestGroupBySubQuery()
    {
        Assert.Fail("TODO");
    }

    [Test]
    public void TestGroupByWithJoin()
    {
        Assert.Fail("TODO");
    }

    [Test]
    public void TestGroupByWithReverseJoin()
    {
        Assert.Fail("TODO");
    }

    [Test]
    public void TestGroupByWithReverseJoinAndProjection()
    {
        Assert.Fail("TODO");
    }
}
