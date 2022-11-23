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
using Ignite.Table;
using NUnit.Framework;

/// <summary>
/// Linq KvView tests.
/// </summary>
public partial class LinqTests
{
    private IKeyValueView<KeyPoco, ValPoco> KvView { get; set; } = null!;

    [Test]
    public void TestSelectKeyValuePair()
    {
        var query = KvView.AsQueryable()
            .Where(x => x.Key.Key > 3 && x.Value.Val != null)
            .OrderBy(x => x.Key.Key);

        var res = query.ToList();

        Assert.AreEqual(4, res[0].Key.Key);
        Assert.AreEqual("v-4", res[0].Value.Val);

        StringAssert.Contains(
            "select _T0.KEY, _T0.VAL " +
            "from PUBLIC.TBL1 as _T0 " +
            "where ((_T0.KEY > ?) and (_T0.VAL IS DISTINCT FROM ?)) " +
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
