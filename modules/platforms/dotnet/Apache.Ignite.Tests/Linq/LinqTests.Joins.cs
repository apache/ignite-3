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
/// Linq JOINs tests.
/// TODO:
/// TestOuterJoin
/// TestSubqueryJoin
/// TestInvalidJoin
/// TestTwoFromSubquery
/// TestMultipleFromSubquery.
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
    }

    [Test]
    public void TestJoinAsMultipleFrom()
    {
        var query1 = PocoView.AsQueryable();
        var query2 = PocoIntView.AsQueryable();

        var joinQuery =
            from a in query1
            from b in query2
            where a.Key == b.Key && a.Key > 3
            orderby b.Key
            select new { Id = a.Key, Price = b.Val };

        var res = joinQuery.Take(1).ToList();

        Assert.AreEqual(1, res.Count);

        Assert.AreEqual(4, res[0].Id);
        Assert.AreEqual(400, res[0].Price);
    }

    [Test]
    public void TestMultiKeyJoin()
    {
        Assert.Fail("TODO");
    }

    private record PocoInt(int Key, int Val);
}
