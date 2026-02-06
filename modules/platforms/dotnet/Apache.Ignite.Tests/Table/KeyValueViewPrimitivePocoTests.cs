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
using Common.Table;
using Ignite.Table;
using NUnit.Framework;

/// <summary>
/// Tests for key-value view where key is a primitive (long), and value is a user object (poco).
/// </summary>
public class KeyValueViewPrimitivePocoTests : IgniteTestsBase
{
    private IKeyValueView<long, ValPoco> KvView => Table.GetKeyValueView<long, ValPoco>();

    [TearDown]
    public async Task CleanTable()
    {
        await TupleView.DeleteAllAsync(null, Enumerable.Range(-1, 12).Select(x => GetTuple(x)));
    }

    [Test]
    public async Task TestPutGet()
    {
        await KvView.PutAsync(null, 1L, GetValPoco("val"));

        (ValPoco res, bool hasRes) = await KvView.GetAsync(null, 1L);

        Assert.IsTrue(hasRes);
        Assert.AreEqual("val", res.Val);
    }

    [Test]
    public async Task TestGetNonExistentKeyReturnsEmptyOption()
    {
        (ValPoco res, bool hasRes) = await KvView.GetAsync(null, -111L);

        Assert.IsFalse(hasRes);
        Assert.IsNull(res);
    }

    [Test]
    public async Task TestGetAll()
    {
        await KvView.PutAsync(null, 7L, GetValPoco("val1"));
        await KvView.PutAsync(null, 8L, GetValPoco("val2"));

        IDictionary<long, ValPoco> res = await KvView.GetAllAsync(null, Enumerable.Range(-1, 100).Select(x => (long)x).ToList());
        IDictionary<long, ValPoco> resEmpty = await KvView.GetAllAsync(null, Array.Empty<long>());

        Assert.AreEqual(2, res.Count);
        Assert.AreEqual("val1", res[7].Val);
        Assert.AreEqual("val2", res[8].Val);

        Assert.AreEqual(0, resEmpty.Count);
    }

    [Test]
    public void TestPutNullThrowsArgumentException()
    {
        var valEx = Assert.ThrowsAsync<ArgumentNullException>(async () => await KvView.PutAsync(null, 1L, null!));
        Assert.AreEqual("Value cannot be null. (Parameter 'val')", valEx!.Message);
    }

    [Test]
    public async Task TestContains()
    {
        await KvView.PutAsync(null, 7L, GetValPoco("val1"));

        bool res1 = await KvView.ContainsAsync(null, 7L);
        bool res2 = await KvView.ContainsAsync(null, 8L);

        Assert.IsTrue(res1);
        Assert.IsFalse(res2);
    }

    [Test]
    public async Task TestPutAll()
    {
        await KvView.PutAllAsync(null, new Dictionary<long, ValPoco>());
        await KvView.PutAllAsync(
            null,
            Enumerable.Range(-1, 7).Select(x => new KeyValuePair<long, ValPoco>(x, GetValPoco("v" + x))));

        IDictionary<long, ValPoco> res = await KvView.GetAllAsync(null, Enumerable.Range(-10, 20).Select(x => (long)x));

        Assert.AreEqual(7, res.Count);

        for (int i = -1; i < 6; i++)
        {
            ValPoco val = res[i];
            Assert.AreEqual("v" + i, val.Val);
        }
    }

    [Test]
    public async Task TestGetAndPut()
    {
        Option<ValPoco> res1 = await KvView.GetAndPutAsync(null, 1, GetValPoco("1"));
        Option<ValPoco> res2 = await KvView.GetAndPutAsync(null, 1, GetValPoco("2"));
        Option<ValPoco> res3 = await KvView.GetAsync(null, 1);

        Assert.IsFalse(res1.HasValue);
        Assert.IsTrue(res2.HasValue);
        Assert.IsTrue(res3.HasValue);

        Assert.AreEqual("1", res2.Value.Val);
        Assert.AreEqual("2", res3.Value.Val);
    }

    [Test]
    public async Task TestPutIfAbsent()
    {
        await KvView.PutAsync(null, 1, GetValPoco("1"));

        bool res1 = await KvView.PutIfAbsentAsync(null, 1, GetValPoco("11"));
        Option<ValPoco> res2 = await KvView.GetAsync(null, 1);

        bool res3 = await KvView.PutIfAbsentAsync(null, 2, GetValPoco("2"));
        Option<ValPoco> res4 = await KvView.GetAsync(null, 2);

        Assert.IsFalse(res1);
        Assert.AreEqual("1", res2.Value.Val);

        Assert.IsTrue(res3);
        Assert.AreEqual("2", res4.Value.Val);
    }

    [Test]
    public async Task TestRemove()
    {
        await KvView.PutAsync(null, 1, GetValPoco("1"));

        bool res1 = await KvView.RemoveAsync(null, 1);
        bool res2 = await KvView.RemoveAsync(null, 2);
        bool res3 = await KvView.ContainsAsync(null, 1);

        Assert.IsTrue(res1);
        Assert.IsFalse(res2);
        Assert.IsFalse(res3);
    }

    [Test]
    public async Task TestRemoveExact()
    {
        await KvView.PutAsync(null, 1, GetValPoco("1"));

        bool res1 = await KvView.RemoveAsync(null, 1, GetValPoco("111"));
        bool res2 = await KvView.RemoveAsync(null, 1, GetValPoco("1"));
        bool res3 = await KvView.ContainsAsync(null, 1);

        Assert.IsFalse(res1);
        Assert.IsTrue(res2);
        Assert.IsFalse(res3);
    }

    [Test]
    public async Task TestRemoveAll()
    {
        await KvView.PutAsync(null, 1, GetValPoco("1"));

        IList<long> res1 = await KvView.RemoveAllAsync(null, Enumerable.Range(-1, 8).Select(x => (long)x));
        bool res2 = await KvView.ContainsAsync(null, 1);

        Assert.AreEqual(new[] { -1, 0, 2, 3, 4, 5, 6 }, res1.OrderBy(x => x));
        Assert.IsFalse(res2);
    }

    [Test]
    public async Task TestRemoveAllExact()
    {
        await KvView.PutAsync(null, 1, GetValPoco("1"));

        IList<long> res1 = await KvView.RemoveAllAsync(
            null,
            Enumerable.Range(-1, 8).Select(x => new KeyValuePair<long, ValPoco>(x, GetValPoco(x.ToString()))));

        bool res2 = await KvView.ContainsAsync(null, 1);

        Assert.AreEqual(new[] { -1, 0, 2, 3, 4, 5, 6 }, res1.OrderBy(x => x));
        Assert.IsFalse(res2);
    }

    [Test]
    public async Task TestGetAndRemove()
    {
        await KvView.PutAsync(null, 1, GetValPoco("1"));

        (ValPoco val1, bool hasVal1) = await KvView.GetAndRemoveAsync(null, 1);
        (ValPoco val2, bool hasVal2) = await KvView.GetAndRemoveAsync(null, 1);

        Assert.IsTrue(hasVal1);
        Assert.AreEqual("1", val1.Val);

        Assert.IsFalse(hasVal2);
        Assert.IsNull(val2);
    }

    [Test]
    public async Task TestReplace()
    {
        await KvView.PutAsync(null, 1, GetValPoco("1"));

        bool res1 = await KvView.ReplaceAsync(null, 0, GetValPoco("00"));
        Option<ValPoco> res2 = await KvView.GetAsync(null, 0);

        bool res3 = await KvView.ReplaceAsync(null, 1, GetValPoco("11"));
        Option<ValPoco> res4 = await KvView.GetAsync(null, 1);

        Assert.IsFalse(res1);
        Assert.IsFalse(res2.HasValue);

        Assert.IsTrue(res3);
        Assert.IsTrue(res4.HasValue);
        Assert.AreEqual("11", res4.Value.Val);
    }

    [Test]
    public async Task TestReplaceExact()
    {
        await KvView.PutAsync(null, 1, GetValPoco("1"));

        bool res1 = await KvView.ReplaceAsync(transaction: null, key: 0, oldVal: GetValPoco("0"), newVal: GetValPoco("00"));
        Option<ValPoco> res2 = await KvView.GetAsync(null, 0);

        bool res3 = await KvView.ReplaceAsync(transaction: null, key: 1, oldVal: GetValPoco("1"), newVal: GetValPoco("11"));
        Option<ValPoco> res4 = await KvView.GetAsync(null, 1);

        bool res5 = await KvView.ReplaceAsync(transaction: null, key: 2, oldVal: GetValPoco("1"), newVal: GetValPoco("22"));
        Option<ValPoco> res6 = await KvView.GetAsync(null, 1);

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
        await KvView.PutAsync(null, 1, GetValPoco("1"));

        Option<ValPoco> res1 = await KvView.GetAndReplaceAsync(null, 0, GetValPoco("00"));
        Option<ValPoco> res2 = await KvView.GetAsync(null, 0);

        Option<ValPoco> res3 = await KvView.GetAndReplaceAsync(null, 1, GetValPoco("11"));
        Option<ValPoco> res4 = await KvView.GetAsync(null, 1);

        Assert.IsFalse(res1.HasValue);
        Assert.IsFalse(res2.HasValue);

        Assert.IsTrue(res3.HasValue);
        Assert.AreEqual("1", res3.Value.Val);

        Assert.IsTrue(res4.HasValue);
        Assert.AreEqual("11", res4.Value.Val);
    }
}
