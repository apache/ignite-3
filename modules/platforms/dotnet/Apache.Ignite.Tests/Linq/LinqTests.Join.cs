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
using System.Linq;
using NUnit.Framework;

/// <summary>
/// Linq JOINs tests.
/// </summary>
public partial class LinqTests
{
    [Test]
    public void TestSelfJoin()
    {
        var query1 = PocoView.AsQueryable().Where(x => x.Key > 4);
        var query2 = PocoView.AsQueryable().Where(x => x.Key < 6);

        var joinQuery = query1.Join(query2, a => a.Key, b => b.Key, (a, b) => new
        {
            Key1 = a.Key,
            Val1 = a.Val,
            Key2 = b.Key,
            Val2 = b.Val
        });

        var res = joinQuery.Single();

        Assert.AreEqual(5, res.Key1);
        Assert.AreEqual(5, res.Key2);
        Assert.AreEqual("v-5", res.Val1);
        Assert.AreEqual("v-5", res.Val2);

        StringAssert.Contains(
            "select _T0.KEY, _T0.VAL, _T1.KEY, _T1.VAL " +
            "from PUBLIC.TBL1 as _T0 " +
            "inner join (select * from PUBLIC.TBL1 as _T2 where (_T2.KEY < ?) ) as _T1 on (_T1.KEY = _T0.KEY)",
            joinQuery.ToString());
    }

    [Test]
    public void TestTwoTableJoin()
    {
        var query1 = PocoView.AsQueryable();
        var query2 = PocoIntView.AsQueryable();

        var joinQuery = query1.Join(
                inner: query2,
                outerKeySelector: a => a.Key,
                innerKeySelector: b => b.Key,
                resultSelector: (a, b) => new
                {
                    Id = a.Key,
                    Price = b.Val
                })
            .Where(x => x.Id > 3)
            .OrderBy(x => x.Id)
            .Take(2);

        var res = joinQuery.ToList();

        Assert.AreEqual(2, res.Count);

        Assert.AreEqual(4, res[0].Id);
        Assert.AreEqual(400, res[0].Price);

        StringAssert.Contains(
            "select _T0.KEY, _T1.VAL " +
            "from PUBLIC.TBL1 as _T0 " +
            "inner join PUBLIC.TBL_INT32 as _T1 on (cast(_T1.KEY as bigint) = _T0.KEY) " +
            "where (_T0.KEY > ?) " +
            "order by _T0.KEY asc " +
            "limit ?",
            joinQuery.ToString());
    }

    [Test]
    public void TestThreeTableJoin()
    {
        var query1 = PocoView.AsQueryable();
        var query2 = PocoIntView.AsQueryable();
        var query3 = PocoLongView.AsQueryable();

        var joinQuery = query1.Join(
                inner: query2,
                outerKeySelector: a => a.Key,
                innerKeySelector: b => b.Key,
                resultSelector: (a, b) => new
                {
                    Id = a.Key,
                    Price = b.Val
                })
            .Join(
                inner: query3,
                outerKeySelector: a => a.Id,
                innerKeySelector: b => b.Key,
                resultSelector: (a, b) => new
                {
                    a.Id,
                    a.Price,
                    Price2 = b.Val
                })
            .Where(x => x.Id > 3)
            .OrderBy(x => x.Id);

        var res = joinQuery.First();

        Assert.AreEqual(4, res.Id);
        Assert.AreEqual(400, res.Price);
        Assert.AreEqual(8, res.Price2);

        StringAssert.Contains(
            "select _T0.KEY, _T1.VAL, _T2.VAL " +
            "from PUBLIC.TBL1 as _T0 " +
            "inner join PUBLIC.TBL_INT32 as _T1 on (cast(_T1.KEY as bigint) = _T0.KEY) " +
            "inner join PUBLIC.TBL_INT64 as _T2 on (_T2.KEY = _T0.KEY)",
            joinQuery.ToString());
    }

    [Test]
    public void TestTwoTableJoinQuerySyntax()
    {
        var query1 = PocoView.AsQueryable();
        var query2 = PocoIntView.AsQueryable();

        var joinQuery =
            from a in query1
            join b in query2
            on a.Key equals b.Key
            where b.Key > 3
            orderby b.Key
            select new { Id = a.Key, Price = b.Val };

        var res = joinQuery.Take(1).ToList();

        Assert.AreEqual(1, res.Count);

        Assert.AreEqual(4, res[0].Id);
        Assert.AreEqual(400, res[0].Price);

        StringAssert.Contains(
            "select _T0.KEY, _T1.VAL " +
            "from PUBLIC.TBL1 as _T0 " +
            "inner join PUBLIC.TBL_INT32 as _T1 on (cast(_T1.KEY as bigint) = _T0.KEY) " +
            "where (_T1.KEY > ?) " +
            "order by _T1.KEY asc",
            joinQuery.ToString());
    }

    [Test]
    public void TestJoinAsMultipleFrom()
    {
        var query1 = PocoView.AsQueryable();
        var query2 = PocoLongView.AsQueryable();

        var joinQuery =
            from a in query1
            from b in query2
            where a.Key == b.Key && a.Key > 3
            orderby b.Key
            select new { Id = a.Key, Price = b.Val };

        var res = joinQuery.Take(1).ToList();

        Assert.AreEqual(1, res.Count);

        Assert.AreEqual(4, res[0].Id);
        Assert.AreEqual(8, res[0].Price);

        StringAssert.Contains(
            "select _T0.KEY, _T1.VAL from PUBLIC.TBL1 as _T0 , PUBLIC.TBL_INT64 as _T1 " +
            "where ((_T0.KEY IS NOT DISTINCT FROM _T1.KEY) and (_T0.KEY > ?))",
            joinQuery.ToString());
    }

    [Test]
    public void TestMultiKeyJoin()
    {
        var query1 = PocoIntView.AsQueryable();
        var query2 = PocoIntView.AsQueryable();

        var joinQuery = query1.Join(
                inner: query2,
                outerKeySelector: a => new { a.Key, a.Val },
                innerKeySelector: b => new { b.Key, b.Val },
                resultSelector: (a, b) => new
                {
                    Id = b.Key,
                    Price = a.Val
                })
            .Where(x => x.Id > 1 && x.Price > 0)
            .OrderBy(x => x.Id);

        var res = joinQuery.First();

        Assert.AreEqual(2, res.Id);
        Assert.AreEqual(200, res.Price);

        StringAssert.Contains(
            "select _T0.KEY, _T1.VAL " +
            "from PUBLIC.TBL_INT32 as _T1 " +
            "inner join PUBLIC.TBL_INT32 as _T0 on (_T0.KEY = _T1.KEY and _T0.VAL = _T1.VAL)",
            joinQuery.ToString());
    }

    [Test]
    public void TestOuterJoinValueTypeKey()
    {
        var query1 = PocoIntView.AsQueryable(); // Sequential keys.
        var query2 = PocoShortView.AsQueryable(); // Sequential keys times 2.

        var joinQuery = query1.Join(
                inner: query2.DefaultIfEmpty(),
                outerKeySelector: a => a.Key,
                innerKeySelector: b => b.Key,
                resultSelector: (a, b) => new
                {
                    Id = a.Key,
                    Price = b.Val!.Value
                })
            .OrderBy(x => x.Id);

        var res = joinQuery.ToList();

        Assert.AreEqual(Count, res.Count);

        Assert.AreEqual(1, res[1].Id);
        Assert.AreEqual(0, res[1].Price);

        Assert.AreEqual(2, res[2].Id);
        Assert.AreEqual(2, res[2].Price);

        Assert.AreEqual(3, res[3].Id);
        Assert.AreEqual(0, res[3].Price);

        StringAssert.Contains(
            "select _T0.KEY, cast(_T1.VAL as smallint) as PRICE " +
            "from PUBLIC.TBL_INT32 as _T0 " +
            "left outer join (select * from PUBLIC.TBL_INT16 as _T2 ) as _T1 " +
            "on (cast(_T1.KEY as int) = _T0.KEY)",
            joinQuery.ToString());
    }

    [Test]
    public void TestOuterJoinReferenceTypeKey()
    {
        var query1 = PocoView.AsQueryable(); // Sequential values.
        var query2 = PocoStringView.AsQueryable(); // Sequential values times 2.

        var joinQuery = query1.Join(
                inner: query2.DefaultIfEmpty(),
                outerKeySelector: a => a.Val,
                innerKeySelector: b => b.Val,
                resultSelector: (a, b) => new
                {
                    Id = a.Key,
                    Id2 = b.Key,
                    Name = b.Val
                })
            .OrderBy(x => x.Id);

        var res = joinQuery.ToList();

        Assert.AreEqual(Count, res.Count);

        Assert.AreEqual(1, res[1].Id);
        Assert.AreEqual(null, res[1].Name);

        Assert.AreEqual(2, res[2].Id);
        Assert.AreEqual("v-2", res[2].Name);

        Assert.AreEqual(3, res[3].Id);
        Assert.AreEqual(null, res[3].Name);

        StringAssert.Contains(
            "select _T0.KEY, _T1.KEY, _T1.VAL " +
            "from PUBLIC.TBL1 as _T0 " +
            "left outer join (select * from PUBLIC.TBL_STRING as _T2 ) as _T1 on (_T1.VAL = _T0.VAL) " +
            "order by _T0.KEY asc",
            joinQuery.ToString());
    }

    [Test]
    public void TestLocalCollectionJoinThrowsNotSupportedException()
    {
        var query1 = PocoIntView.AsQueryable(); // Sequential keys.
        var query2 = new[] { 2, 4, 6 };

        var joinQuery = query1.Join(
                inner: query2,
                outerKeySelector: a => a.Key,
                innerKeySelector: b => b,
                resultSelector: (a, b) => new
                {
                    Id = a.Key,
                    Price = a.Val
                })
            .OrderBy(x => x.Id);

        // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
        var ex = Assert.Throws<NotSupportedException>(() => joinQuery.ToList());
        StringAssert.Contains("Local collection joins are not supported, try `.Contains()` instead:", ex!.Message);
    }
}
