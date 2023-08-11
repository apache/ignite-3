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

namespace Apache.Ignite.Tests.Table;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Ignite.Table;
using NUnit.Framework;

/// <summary>
/// Tests for key-value POCO view.
/// </summary>
public class KeyValueViewPocoTests : IgniteTestsBase
{
    private IKeyValueView<Poco, ValPoco> KvView => Table.GetKeyValueView<Poco, ValPoco>();

    [TearDown]
    public async Task CleanTable()
    {
        await TupleView.DeleteAllAsync(null, Enumerable.Range(-1, 12).Select(x => GetTuple(x)));
    }

    [Test]
    public async Task TestPutGet()
    {
        await KvView.PutAsync(null, GetPoco(1L), GetPoco("val"));

        (ValPoco res, bool hasRes) = await KvView.GetAsync(null, GetPoco(1L));

        Assert.IsTrue(hasRes);
        Assert.AreEqual("val", res.Val);
    }

    [Test]
    public async Task TestGetNonExistentKeyReturnsEmptyOption()
    {
        (ValPoco res, bool hasRes) = await KvView.GetAsync(null, GetPoco(-111L));

        Assert.IsFalse(hasRes);
        Assert.IsNull(res);
    }

    [Test]
    public async Task TestGetAll()
    {
        await KvView.PutAsync(null, GetPoco(7L), GetPoco("val1"));
        await KvView.PutAsync(null, GetPoco(8L), GetPoco("val2"));

        IDictionary<Poco, ValPoco> res = await KvView.GetAllAsync(null, Enumerable.Range(-1, 100).Select(x => GetPoco(x)).ToList());
        IDictionary<Poco, ValPoco> resEmpty = await KvView.GetAllAsync(null, Array.Empty<Poco>());

        Assert.AreEqual(2, res.Count);
        Assert.AreEqual("val1", res.Single(x => x.Key.Key == 7).Value.Val);
        Assert.AreEqual("val2", res.Single(x => x.Key.Key == 8).Value.Val);

        Assert.AreEqual(0, resEmpty.Count);
    }

    [Test]
    public void TestGetAllWithNullKeyThrowsArgumentException()
    {
        var ex = Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await KvView.GetAllAsync(null, new[] { GetPoco(1L), null! }));

        Assert.AreEqual("Value cannot be null. (Parameter 'key')", ex!.Message);
    }

    [Test]
    public void TestPutNullThrowsArgumentException()
    {
        var keyEx = Assert.ThrowsAsync<ArgumentNullException>(async () => await KvView.PutAsync(null, null!, null!));
        Assert.AreEqual("Value cannot be null. (Parameter 'key')", keyEx!.Message);

        var valEx = Assert.ThrowsAsync<ArgumentNullException>(async () => await KvView.PutAsync(null, GetPoco(1L), null!));
        Assert.AreEqual("Value cannot be null. (Parameter 'val')", valEx!.Message);
    }

    [Test]
    public async Task TestContains()
    {
        await KvView.PutAsync(null, GetPoco(7L), GetPoco("val1"));

        bool res1 = await KvView.ContainsAsync(null, GetPoco(7L));
        bool res2 = await KvView.ContainsAsync(null, GetPoco(8L));

        Assert.IsTrue(res1);
        Assert.IsFalse(res2);
    }

    [Test]
    public async Task TestPutAll()
    {
        await KvView.PutAllAsync(null, new Dictionary<Poco, ValPoco>());
        await KvView.PutAllAsync(
            null,
            Enumerable.Range(-1, 7).Select(x => new KeyValuePair<Poco, ValPoco>(GetPoco(x), GetPoco("v" + x))));

        IDictionary<Poco, ValPoco> res = await KvView.GetAllAsync(null, Enumerable.Range(-10, 20).Select(x => GetPoco(x)));

        Assert.AreEqual(7, res.Count);

        for (int i = -1; i < 6; i++)
        {
            ValPoco val = res.Single(x => x.Key.Key == i).Value;
            Assert.AreEqual("v" + i, val.Val);
        }
    }

    [Test]
    public async Task TestGetAndPut()
    {
        Option<ValPoco> res1 = await KvView.GetAndPutAsync(null, GetPoco(1), GetPoco("1"));
        Option<ValPoco> res2 = await KvView.GetAndPutAsync(null, GetPoco(1), GetPoco("2"));
        Option<ValPoco> res3 = await KvView.GetAsync(null, GetPoco(1));

        Assert.IsFalse(res1.HasValue);
        Assert.IsTrue(res2.HasValue);
        Assert.IsTrue(res3.HasValue);

        Assert.AreEqual("1", res2.Value.Val);
        Assert.AreEqual("2", res3.Value.Val);
    }

    [Test]
    public async Task TestPutIfAbsent()
    {
        await KvView.PutAsync(null, GetPoco(1), GetPoco("1"));

        bool res1 = await KvView.PutIfAbsentAsync(null, GetPoco(1), GetPoco("11"));
        Option<ValPoco> res2 = await KvView.GetAsync(null, GetPoco(1));

        bool res3 = await KvView.PutIfAbsentAsync(null, GetPoco(2), GetPoco("2"));
        Option<ValPoco> res4 = await KvView.GetAsync(null, GetPoco(2));

        Assert.IsFalse(res1);
        Assert.AreEqual("1", res2.Value.Val);

        Assert.IsTrue(res3);
        Assert.AreEqual("2", res4.Value.Val);
    }

    [Test]
    public async Task TestRemove()
    {
        await KvView.PutAsync(null, GetPoco(1), GetPoco("1"));

        bool res1 = await KvView.RemoveAsync(null, GetPoco(1));
        bool res2 = await KvView.RemoveAsync(null, GetPoco(2));
        bool res3 = await KvView.ContainsAsync(null, GetPoco(1));

        Assert.IsTrue(res1);
        Assert.IsFalse(res2);
        Assert.IsFalse(res3);
    }

    [Test]
    public async Task TestRemoveExact()
    {
        await KvView.PutAsync(null, GetPoco(1), GetPoco("1"));

        bool res1 = await KvView.RemoveAsync(null, GetPoco(1), GetPoco("111"));
        bool res2 = await KvView.RemoveAsync(null, GetPoco(1), GetPoco("1"));
        bool res3 = await KvView.ContainsAsync(null, GetPoco(1));

        Assert.IsFalse(res1);
        Assert.IsTrue(res2);
        Assert.IsFalse(res3);
    }

    [Test]
    public async Task TestRemoveAll()
    {
        await KvView.PutAsync(null, GetPoco(1), GetPoco("1"));

        IList<Poco> res1 = await KvView.RemoveAllAsync(null, Enumerable.Range(-1, 8).Select(x => GetPoco(x, "foo")));
        bool res2 = await KvView.ContainsAsync(null, GetPoco(1));

        Assert.AreEqual(new[] { -1, 0, 2, 3, 4, 5, 6 }, res1.Select(x => x.Key).OrderBy(x => x));
        Assert.IsFalse(res2);
    }

    [Test]
    public async Task TestRemoveAllExact()
    {
        await KvView.PutAsync(null, GetPoco(1), GetPoco("1"));

        IList<Poco> res1 = await KvView.RemoveAllAsync(
            null,
            Enumerable.Range(-1, 8).Select(x => new KeyValuePair<Poco, ValPoco>(GetPoco(x), GetPoco(x.ToString()))));

        bool res2 = await KvView.ContainsAsync(null, GetPoco(1));

        Assert.AreEqual(new[] { -1, 0, 2, 3, 4, 5, 6 }, res1.Select(x => x.Key).OrderBy(x => x));
        Assert.IsFalse(res2);
    }

    [Test]
    public async Task TestGetAndRemove()
    {
        await KvView.PutAsync(null, GetPoco(1), GetPoco("1"));

        (ValPoco val1, bool hasVal1) = await KvView.GetAndRemoveAsync(null, GetPoco(1));
        (ValPoco val2, bool hasVal2) = await KvView.GetAndRemoveAsync(null, GetPoco(1));

        Assert.IsTrue(hasVal1);
        Assert.AreEqual("1", val1.Val);

        Assert.IsFalse(hasVal2);
        Assert.IsNull(val2);
    }

    [Test]
    public async Task TestReplace()
    {
        await KvView.PutAsync(null, GetPoco(1), GetPoco("1"));

        bool res1 = await KvView.ReplaceAsync(null, GetPoco(0), GetPoco("00"));
        Option<ValPoco> res2 = await KvView.GetAsync(null, GetPoco(0));

        bool res3 = await KvView.ReplaceAsync(null, GetPoco(1), GetPoco("11"));
        Option<ValPoco> res4 = await KvView.GetAsync(null, GetPoco(1));

        Assert.IsFalse(res1);
        Assert.IsFalse(res2.HasValue);

        Assert.IsTrue(res3);
        Assert.IsTrue(res4.HasValue);
        Assert.AreEqual("11", res4.Value.Val);
    }

    [Test]
    public async Task TestReplaceExact()
    {
        await KvView.PutAsync(null, GetPoco(1), GetPoco("1"));

        bool res1 = await KvView.ReplaceAsync(transaction: null, key: GetPoco(0), oldVal: GetPoco("0"), newVal: GetPoco("00"));
        Option<ValPoco> res2 = await KvView.GetAsync(null, GetPoco(0));

        bool res3 = await KvView.ReplaceAsync(transaction: null, key: GetPoco(1), oldVal: GetPoco("1"), newVal: GetPoco("11"));
        Option<ValPoco> res4 = await KvView.GetAsync(null, GetPoco(1));

        bool res5 = await KvView.ReplaceAsync(transaction: null, key: GetPoco(2), oldVal: GetPoco("1"), newVal: GetPoco("22"));
        Option<ValPoco> res6 = await KvView.GetAsync(null, GetPoco(1));

        Assert.IsFalse(res1);
        Assert.IsFalse(res2.HasValue);

        Assert.IsTrue(res3);
        Assert.IsTrue(res4.HasValue);
        Assert.AreEqual("11", res4.Value.Val);

        Assert.IsFalse(res5);
        Assert.AreEqual("11", res6.Value.Val);
    }

    [Test]
    public async Task TestGetAndReplace()
    {
        await KvView.PutAsync(null, GetPoco(1), GetPoco("1"));

        Option<ValPoco> res1 = await KvView.GetAndReplaceAsync(null, GetPoco(0), GetPoco("00"));
        Option<ValPoco> res2 = await KvView.GetAsync(null, GetPoco(0));

        Option<ValPoco> res3 = await KvView.GetAndReplaceAsync(null, GetPoco(1), GetPoco("11"));
        Option<ValPoco> res4 = await KvView.GetAsync(null, GetPoco(1));

        Assert.IsFalse(res1.HasValue);
        Assert.IsFalse(res2.HasValue);

        Assert.IsTrue(res3.HasValue);
        Assert.AreEqual("1", res3.Value.Val);

        Assert.IsTrue(res4.HasValue);
        Assert.AreEqual("11", res4.Value.Val);
    }

    [Test]
    public void TestToString()
    {
        StringAssert.StartsWith("KeyValueView`2[Poco, Poco] { Table = Table { Name = TBL1, Id =", KvView.ToString());
    }
}
