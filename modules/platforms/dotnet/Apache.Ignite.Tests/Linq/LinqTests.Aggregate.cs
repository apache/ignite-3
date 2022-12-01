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
using NUnit.Framework;

/// <summary>
/// Linq aggregate tests.
/// </summary>
public partial class LinqTests
{
    [Test]
    public void TestFilteredSum()
    {
        long res = PocoView.AsQueryable()
            .Where(x => x.Key < 3)
            .Select(x => x.Key)
            .Sum();

        Assert.AreEqual(3, res);
    }

    [Test]
    public void TestSum()
    {
        Assert.AreEqual(12, PocoByteView.AsQueryable().Sum(x => x.Val));
        Assert.AreEqual(90, PocoShortView.AsQueryable().Sum(x => x.Val));
        Assert.AreEqual(4500, PocoIntView.AsQueryable().Sum(x => x.Val));
        Assert.AreEqual(90, PocoLongView.AsQueryable().Sum(x => x.Val));
        Assert.AreEqual(45, PocoFloatView.AsQueryable().Sum(x => x.Val));
        Assert.AreEqual(45, PocoDecimalView.AsQueryable().Sum(x => x.Val));
        Assert.AreEqual(45, PocoDoubleView.AsQueryable().Sum(x => x.Val));
    }

    [Test]
    public void TestMin()
    {
        Assert.AreEqual(0, PocoByteView.AsQueryable().Min(x => x.Val));
        Assert.AreEqual(0, PocoShortView.AsQueryable().Min(x => x.Val));
        Assert.AreEqual(0, PocoIntView.AsQueryable().Min(x => x.Val));
        Assert.AreEqual(0, PocoLongView.AsQueryable().Min(x => x.Val));
        Assert.AreEqual(0, PocoFloatView.AsQueryable().Min(x => x.Val));
        Assert.AreEqual(0, PocoDecimalView.AsQueryable().Min(x => x.Val));
        Assert.AreEqual(0, PocoDoubleView.AsQueryable().Min(x => x.Val));
    }

    [Test]
    public void TestMax()
    {
        Assert.AreEqual(3, PocoByteView.AsQueryable().Max(x => x.Val));
        Assert.AreEqual(18, PocoShortView.AsQueryable().Max(x => x.Val));
        Assert.AreEqual(900, PocoIntView.AsQueryable().Max(x => x.Val));
        Assert.AreEqual(18, PocoLongView.AsQueryable().Max(x => x.Val));
        Assert.AreEqual(9.0f, PocoFloatView.AsQueryable().Max(x => x.Val));
        Assert.AreEqual(9m, PocoDecimalView.AsQueryable().Max(x => x.Val));
        Assert.AreEqual(9.0d, PocoDoubleView.AsQueryable().Max(x => x.Val));
    }

    [Test]
    public void TestAverage()
    {
        Assert.AreEqual(1.0d, PocoByteView.AsQueryable().Average(x => x.Val));
        Assert.AreEqual(9.0d, PocoShortView.AsQueryable().Average(x => x.Val));
        Assert.AreEqual(450d, PocoIntView.AsQueryable().Average(x => x.Val));
        Assert.AreEqual(9.0d, PocoLongView.AsQueryable().Average(x => x.Val));
        Assert.AreEqual(4.5f, PocoFloatView.AsQueryable().Average(x => x.Val));
        Assert.AreEqual(4.5m, PocoDecimalView.AsQueryable().Average(x => x.Val));
        Assert.AreEqual(4.5d, PocoDoubleView.AsQueryable().Average(x => x.Val));
    }

    [Test]
    public void TestGroupByAllAggregates()
    {
        var query = PocoIntView.AsQueryable()
            .GroupBy(x => x.Key)
            .Select(x => new
            {
                x.Key,
                Count = x.Count(),
                Sum = x.Sum(a => a.Key),
                Avg = x.Average(a => a.Key),
                Min = x.Min(a => a.Key),
                Max = x.Max(a => a.Key)
            })
            .OrderBy(x => x.Key);

        var res = query.ToList();

        Assert.AreEqual(2, res[2].Key);
        Assert.AreEqual(1, res[2].Count);
        Assert.AreEqual(2, res[2].Sum);
        Assert.AreEqual(2, res[2].Avg);
        Assert.AreEqual(2, res[2].Min);
        Assert.AreEqual(2, res[2].Max);

        StringAssert.Contains(
            "select _T0.KEY, count(*) as COUNT, sum(_T0.KEY) as SUM, avg(_T0.KEY) as AVG, min(_T0.KEY) as MIN, max(_T0.KEY) as MAX " +
            "from PUBLIC.TBL_INT32 as _T0 " +
            "group by (_T0.KEY) " +
            "order by (_T0.KEY) asc",
            query.ToString());
    }

    [Test]
    public void TestCount()
    {
        int res = PocoView
            .AsQueryable()
            .Count(x => x.Key < 3);

        Assert.AreEqual(3, res);
    }

    [Test]
    public void TestLongCount()
    {
        long res = PocoView
            .AsQueryable()
            .LongCount(x => x.Key < 3);

        Assert.AreEqual(3, res);
    }

    [Test]
    public void TestAll()
    {
        Assert.IsFalse(PocoView.AsQueryable().All(x => x.Key > 5));
        Assert.IsTrue(PocoView.AsQueryable().All(x => x.Key < 500));

        // Additional Where.
        Assert.IsTrue(PocoView.AsQueryable().Where(x => x.Key > 8).All(x => x.Key > 5));
    }

    [Test]
    public void TestAny()
    {
        Assert.IsFalse(PocoView.AsQueryable().Any(x => x.Key > 500));
        Assert.IsTrue(PocoView.AsQueryable().Any(x => x.Key < 5));

        // Additional Where.
        Assert.IsFalse(PocoView.AsQueryable().Where(x => x.Key > 7).Any(x => x.Key < 5));
    }
}
