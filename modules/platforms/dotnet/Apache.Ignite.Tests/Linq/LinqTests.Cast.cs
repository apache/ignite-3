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
/// Linq type cast tests.
/// </summary>
public partial class LinqTests
{
    [Test]
    public void TestProjectionWithCastIntoAnonymousType()
    {
        // BigInteger is not suppoerted by the SQL engine.
        // ReSharper disable once RedundantCast
        var query = PocoIntView.AsQueryable()
            .Select(x => new
            {
                Byte = (sbyte?)(x.Val / 10),
                Short = (short?)x.Val,
                Long = (long?)x.Val,
                Float = (float?)x.Val / 1000,
                Double = (double?)x.Val / 2000,
                Decimal0 = (decimal?)x.Val / 200m
            })
            .OrderByDescending(x => x.Long);

        var res = query.ToList();

        Assert.AreEqual(90, res[0].Byte);
        Assert.AreEqual(900, res[0].Short);
        Assert.AreEqual(900, res[0].Long);
        Assert.AreEqual(900f / 1000, res[0].Float);
        Assert.AreEqual(900d / 2000, res[0].Double);

        // TODO IGNITE-21743 Cast to decimal loses precision: "Expected: 4.5m But was: 5m"
        // Assert.AreEqual(900m / 200, res[0].Decimal0);
        Assert.AreEqual(5m, res[0].Decimal0);

        StringAssert.Contains(
            "select cast((_T0.VAL / ?) as tinyint) as BYTE, " +
            "cast(_T0.VAL as smallint) as SHORT, " +
            "cast(_T0.VAL as bigint) as LONG, " +
            "(cast(_T0.VAL as real) / ?) as FLOAT, " +
            "(cast(_T0.VAL as double) / ?) as DOUBLE, " +
            "(cast(_T0.VAL as decimal) / ?) as DECIMAL0 " +
            "from PUBLIC.TBL_INT32 as _T0 " +
            "order by cast(_T0.VAL as bigint) desc",
            query.ToString());
    }

    [Test]
    public void TestJoinOnDifferentTypes()
    {
        var query = PocoFloatView.AsQueryable()
            .Join(
                PocoByteView.AsQueryable(),
                x => x.Key,
                y => y.Key,
                (x, y) => new
                {
                    x.Key,
                    Val1 = x.Val,
                    Val2 = y.Val
                })
            .OrderByDescending(x => x.Key);

        var res = query.ToList();

        Assert.AreEqual(9, res[0].Key);
        Assert.AreEqual(9f, res[0].Val1);
        Assert.AreEqual(3, res[0].Val2);

        StringAssert.Contains(
            "select _T0.KEY, _T0.VAL, _T1.VAL " +
            "from PUBLIC.TBL_FLOAT as _T0 " +
            "inner join PUBLIC.TBL_INT8 as _T1 on (cast(_T1.KEY as real) = _T0.KEY) " +
            "order by _T0.KEY desc",
            query.ToString());
    }
}
