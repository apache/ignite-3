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
/// Linq UNION/INTERSECT/EXCEPT tests.
/// </summary>
public partial class LinqTests
{
    [Test]
    public void TestUnion()
    {
        var subQuery = PocoView.AsQueryable()
            .Where(x => x.Key < 2)
            .Select(x => new { Id = x.Key });

        var query = PocoLongView.AsQueryable()
            .Where(x => x.Key > 8)
            .Select(x => new { Id = x.Key })
            .Union(subQuery);

        var res = query.ToList();

        CollectionAssert.AreEquivalent(new[] { 0, 1, 9 }, res.Select(x => x.Id));

        StringAssert.Contains(
            "select _T0.KEY from PUBLIC.TBL_INT64 as _T0 where (_T0.KEY > ?) " +
            "union (select _T1.KEY from PUBLIC.TBL1 as _T1 where (_T1.KEY < ?)",
            query.ToString());
    }

    [Test]
    public void TestUnionWithOrderBy()
    {
        var subQuery = PocoView.AsQueryable()
            .Where(x => x.Key < 2)
            .Select(x => new { Id = x.Key });

        var query = PocoLongView.AsQueryable()
            .Where(x => x.Key > 8)
            .Select(x => new { Id = x.Key })
            .Union(subQuery)
            .OrderBy(x => x.Id);

        var res = query.ToList();

        Assert.AreEqual(new[] { 0, 1, 9 }, res.Select(x => x.Id));

        StringAssert.Contains(
            "select * from " +
            "(select _T0.KEY from PUBLIC.TBL_INT64 as _T0 where (_T0.KEY > ?) " +
            "union (select _T1.KEY from PUBLIC.TBL1 as _T1 where (_T1.KEY < ?))) as _T2 " +
            "order by _T2.KEY asc",
            query.ToString());
    }

    [Test]
    public void TestUnionWithCast()
    {
        var subQuery = PocoIntView.AsQueryable()
            .Select(x => new { x.Key })
            .Where(x => x.Key > 3 && x.Key < 5);

        var query = PocoByteView.AsQueryable()
            .Where(x => x.Key > 8)
            .Select(x => new { Key = (int)x.Key })
            .Union(subQuery);

        var res = query.ToList();

        CollectionAssert.AreEquivalent(new[] { 4, 9 }, res.Select(x => x.Key));

        StringAssert.Contains(
            "select cast(_T0.KEY as int) as KEY " +
            "from PUBLIC.TBL_INT8 as _T0 " +
            "where (cast(_T0.KEY as int) > ?) " +
            "union (select _T1.KEY from PUBLIC.TBL_INT32 as _T1 where ((_T1.KEY > ?) and (_T1.KEY < ?)))",
            query.ToString());
    }

    [Test]
    public void TestIntersect()
    {
        var subQuery = PocoView.AsQueryable()
            .Where(x => x.Key < 5)
            .Select(x => new { Id = x.Key });

        var query = PocoLongView.AsQueryable()
            .Where(x => x.Key > 2)
            .Select(x => new { Id = x.Key })
            .Intersect(subQuery);

        var res = query.ToList();

        CollectionAssert.AreEquivalent(new[] { 3, 4 }, res.Select(x => x.Id));

        StringAssert.Contains(
            "select _T0.KEY from PUBLIC.TBL_INT64 as _T0 where (_T0.KEY > ?) " +
            "intersect (select _T1.KEY from PUBLIC.TBL1 as _T1 where (_T1.KEY < ?)",
            query.ToString());
    }

    [Test]
    public void TestIntersectEmpty()
    {
        var subQuery = PocoView.AsQueryable()
            .Where(x => x.Key < 2)
            .Select(x => new { Id = x.Key });

        var query = PocoLongView.AsQueryable()
            .Where(x => x.Key > 8)
            .Select(x => new { Id = x.Key })
            .Intersect(subQuery);

        var res = query.ToList();

        CollectionAssert.IsEmpty(res);
    }

    [Test]
    public void TestExcept()
    {
        var subQuery = PocoView.AsQueryable()
            .Where(x => x.Key < 7)
            .Select(x => new { Id = x.Key });

        var query = PocoLongView.AsQueryable()
            .Where(x => x.Key > 5)
            .Select(x => new { Id = x.Key })
            .Except(subQuery);

        var res = query.ToList();

        CollectionAssert.AreEquivalent(new[] { 7, 8, 9 }, res.Select(x => x.Id));

        StringAssert.Contains(
            "select _T0.KEY from PUBLIC.TBL_INT64 as _T0 where (_T0.KEY > ?) " +
            "except (select _T1.KEY from PUBLIC.TBL1 as _T1 where (_T1.KEY < ?))",
            query.ToString());
    }
}
