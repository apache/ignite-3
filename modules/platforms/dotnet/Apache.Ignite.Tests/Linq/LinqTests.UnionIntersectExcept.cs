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
        var subQuery = PocoView.AsQueryable().Select(x => new { Id = x.Key });

        var query = PocoLongView.AsQueryable()
            .Select(x => new { Id = x.Key })
            .Union(subQuery);

        var res = query.ToList();

        Assert.AreEqual(new[] { 0, 1, 2, 3 }, res);

        StringAssert.Contains(
            "select _T0.KEY from PUBLIC.TBL_INT8 as _T0 " +
            "union (select _T1.KEY from PUBLIC.TBL_INT32 as _T1)",
            query.ToString());
    }

    [Test]
    public void TestUnionWithCast()
    {
        var subQuery = PocoIntView.AsQueryable().Select(x => new { x.Key });

        var query = PocoByteView.AsQueryable()
            .Select(x => new { Key = (int)x.Key })
            .Union(subQuery);

        var res = query.ToList();

        Assert.AreEqual(new[] { 0, 1, 2, 3 }, res);

        StringAssert.Contains(
            "select _T0.KEY from PUBLIC.TBL_INT8 as _T0 " +
            "union (select _T1.KEY from PUBLIC.TBL_INT32 as _T1)",
            query.ToString());
    }
}
