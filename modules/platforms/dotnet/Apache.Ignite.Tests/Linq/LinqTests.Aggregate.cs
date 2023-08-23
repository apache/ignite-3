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
using Table;

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
    public void TestSumNullable()
    {
        Assert.AreEqual(0, PocoAllColumnsSqlNullableView.AsQueryable().Where(x => x.Int64 == null).Sum(x => x.Int64));
        Assert.AreEqual(0, PocoAllColumnsSqlNullableView.AsQueryable().Where(x => x.Int64 == null).Select(x => x.Int64).Sum());
        Assert.AreEqual(1, PocoAllColumnsSqlNullableView.AsQueryable().Count(x => x.Int64 == null));
    }

    [Test]
    public void TestSumWithEmptySubqueryReturnsZero()
    {
        Assert.AreEqual(0, PocoDoubleView.AsQueryable().Where(x => x.Key < -100).Sum(x => x.Val));
        Assert.AreEqual(0, PocoDoubleView.AsQueryable().Where(x => x.Key < -100).Select(x => x.Val).Sum());
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
    public void TestMinWithEmptySubqueryThrowsNoElements()
    {
        // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
        var ex = Assert.Throws<InvalidOperationException>(() => PocoIntView.AsQueryable().Where(x => x.Key > 1000).Min(x => x.Val));
        Assert.AreEqual("Sequence contains no elements", ex!.Message);
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
    public void TestMaxWithEmptySubqueryThrowsNoElements()
    {
        // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
        var ex = Assert.Throws<InvalidOperationException>(() => PocoIntView.AsQueryable().Where(x => x.Key > 1000).Max(x => x.Val));
        Assert.AreEqual("Sequence contains no elements", ex!.Message);
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
    public void TestAverageWithEmptySubqueryThrowsNoElements()
    {
        // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
        var ex = Assert.Throws<InvalidOperationException>(() => PocoIntView.AsQueryable().Where(x => x.Key > 1000).Average(x => x.Val));
        Assert.AreEqual("Sequence contains no elements", ex!.Message);
    }

    [Test]
    public void TestGroupByAllAggregates()
    {
        var query = PocoIntView.AsQueryable()
            .GroupBy(x => x.Key)
            .Select(x => new
            {
                x.Key,
                Cnt = x.Count(),
                SumC = x.Sum(a => a.Key),
                AvgC = x.Average(a => a.Key),
                MinC = x.Min(a => a.Key),
                MaxC = x.Max(a => a.Key)
            })
            .OrderBy(x => x.Key);

        var res = query.ToList();

        Assert.AreEqual(2, res[2].Key);
        Assert.AreEqual(1, res[2].Cnt);
        Assert.AreEqual(2, res[2].SumC);
        Assert.AreEqual(2, res[2].AvgC);
        Assert.AreEqual(2, res[2].MinC);
        Assert.AreEqual(2, res[2].MaxC);

        StringAssert.Contains(
            "select _T0.KEY as _G0, count(*) as CNT, sum(_T0.KEY) as SUMC, avg(_T0.KEY) as AVGC, min(_T0.KEY) as MINC, max(_T0.KEY) as MAXC " +
            "from PUBLIC.TBL_INT32 as _T0 " +
            "group by _G0 " +
            "order by _G0 asc",
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

    [Test]
    public void TestAggregateNullableDouble()
    {
        var query = PocoAllColumnsSqlNullableView.AsQueryable();

        double? sumNotNull = query.Sum(x => x.Double);
        double? sumNull = query.Where(x => x.Double == null).Sum(x => x.Double);

        Assert.AreEqual(110d, sumNotNull);
        Assert.AreEqual(0, sumNull);
    }

    [Test]
    [SuppressMessage("Assertion", "NUnit2021:Incompatible types for EqualTo constraint", Justification = "Reviewed.")]
    public void TestAggregateNullableAllTypes()
    {
        Test(q => q.Sum(x => x.Int8));
        Test(q => q.Sum(x => x.Int16));
        Test(q => q.Sum(x => x.Int32));
        Test(q => q.Sum(x => x.Int64));
        Test(q => q.Sum(x => x.Float));
        Test(q => q.Sum(x => x.Double));
        Test(q => q.Sum(x => x.Decimal));

        void Test<T>(Func<IQueryable<PocoAllColumnsSqlNullable>, T> sumFunc)
        {
            var query = PocoAllColumnsSqlNullableView.AsQueryable();

            var sumNotNull = sumFunc(query);
            var sumNull = sumFunc(query.Where(x => x.Double == null));

            Assert.IsNotNull(sumNotNull);
            Assert.AreEqual(0, sumNull);
        }
    }
}
