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
using Ignite.Table;
using NUnit.Framework;

/// <summary>
/// Linq KvView tests.
/// </summary>
public partial class LinqTests
{
    private IKeyValueView<KeyPoco, ValPoco> KvView { get; set; } = null!;

    [Test]
    public void TestSelectPairKv()
    {
        var query = KvView.AsQueryable()
            .Where(x => x.Key.Key > 3 && x.Value.Val != null)
            .OrderBy(x => x.Key.Key);

        List<KeyValuePair<KeyPoco, ValPoco>> res = query.ToList();

        Assert.AreEqual(4, res[0].Key.Key);
        Assert.AreEqual("v-4", res[0].Value.Val);

        StringAssert.Contains(
            "select _T0.KEY, _T0.VAL " +
            "from PUBLIC.TBL1 as _T0 " +
            "where ((_T0.KEY > ?) and (_T0.VAL IS DISTINCT FROM ?)) " +
            "order by (_T0.KEY) asc",
            query.ToString());
    }

    [Test]
    public void TestSelectKeyKv()
    {
        var query = KvView.AsQueryable()
            .Select(x => x.Key)
            .Where(x => x.Key > 5)
            .OrderBy(x => x.Key);

        List<KeyPoco> res = query.ToList();

        Assert.AreEqual(6, res[0].Key);

        StringAssert.Contains(
            "select _T0.KEY from PUBLIC.TBL1 as _T0 " +
            "where (_T0.KEY > ?) " +
            "order by (_T0.KEY) asc",
            query.ToString());
    }

    [Test]
    public void TestSelectValKv()
    {
        var query = KvView.AsQueryable()
            .Select(x => x.Value)
            .Where(x => x.Val != "foo")
            .OrderBy(x => x.Val);

        List<ValPoco> res = query.ToList();

        Assert.AreEqual("v-0", res[0].Val);

        StringAssert.Contains(
            "select _T0.VAL from PUBLIC.TBL1 as _T0 " +
            "where (_T0.VAL IS DISTINCT FROM ?) " +
            "order by (_T0.VAL) asc",
            query.ToString());
    }

    [Test]
    public void TestSelectOneColumnKv()
    {
        var query = KvView.AsQueryable()
            .Select(x => x.Value.Val)
            .Where(x => x != "foo")
            .OrderBy(x => x);

        List<string?> res = query.ToList();

        Assert.AreEqual("v-0", res[0]);

        StringAssert.Contains(
            "select _T0.VAL from PUBLIC.TBL1 as _T0 " +
            "where (_T0.VAL IS DISTINCT FROM ?) " +
            "order by (_T0.VAL) asc",
            query.ToString());
    }

    [Test]
    public void TestJoinRecordWithKv()
    {
        var query1 = KvView.AsQueryable()
            .Where(x => x.Key.Key > 1);

        var query2 = PocoView.AsQueryable()
            .Where(x => x.Key > 7);

        var query = query1.Join(
                query2,
                a => a.Key.Key,
                b => b.Key,
                (a, b) => new
                {
                    Key1 = a.Key.Key,
                    Val1 = a.Value.Val,
                    Key2 = b.Key,
                    Val2 = b.Val
                })
            .OrderBy(x => x.Key1);

        var res = query.ToList();

        Assert.AreEqual(8, res[0].Key1);
        Assert.AreEqual(8, res[0].Key2);

        Assert.AreEqual("v-8", res[0].Val1);
        Assert.AreEqual("v-8", res[0].Val2);

        StringAssert.Contains(
            "select _T0.KEY, _T0.VAL, _T1.KEY, _T1.VAL from PUBLIC.TBL1 as _T0 " +
            "inner join (select * from PUBLIC.TBL1 as _T2 where (_T2.KEY > ?) ) as _T1 on (_T1.KEY = _T0.KEY) " +
            "where (_T0.KEY > ?) " +
            "order by (_T0.KEY) asc",
            query.ToString());
    }

    [OneTimeSetUp]
    protected void InitKvView()
    {
        KvView = Table.GetKeyValueView<KeyPoco, ValPoco>();
    }

    // ReSharper disable ClassNeverInstantiated.Local
    private record KeyPoco(long Key);

    private record ValPoco(string? Val);
}
