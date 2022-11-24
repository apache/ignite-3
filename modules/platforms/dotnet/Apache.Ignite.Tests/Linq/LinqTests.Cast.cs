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
    public void TestCastProjectWithCastIntoAnonymousType()
    {
        var query = PocoIntView.AsQueryable()
            .Select(x => new
            {
                // TODO: Decimal = (decimal)x.Val / 3000
                Byte = (sbyte)x.Val,
                Short = (short)x.Val,
                Long = (long)x.Val,
                Float = (float)x.Val / 1000,
                Double = (double)x.Val / 2000
            })
            .OrderByDescending(x => x.Long);

        var res = query.ToList();

        Assert.AreEqual(-124, res[0].Byte);
        Assert.AreEqual(900, res[0].Short);
        Assert.AreEqual(900, res[0].Long);
        Assert.AreEqual(900f / 1000, res[0].Float);
        Assert.AreEqual(900d / 2000, res[0].Double);

        StringAssert.Contains(
            "select cast(_T0.VAL as tinyint), cast(_T0.VAL as smallint), cast(_T0.VAL as bigint), " +
            "(cast(_T0.VAL as real) / ?), (cast(_T0.VAL as double) / ?) " +
            "from PUBLIC.TBL_INT32 as _T0 " +
            "order by (cast(_T0.VAL as bigint)) desc",
            query.ToString());
    }
}
