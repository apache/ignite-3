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
/// Tests for key-value tuple view.
/// </summary>
public class KeyValueViewBinaryTests : IgniteTestsBase
{
    private IKeyValueView<IIgniteTuple, IIgniteTuple> KvView => Table.KeyValueBinaryView;

    [TearDown]
    public async Task CleanTable()
    {
        await TupleView.DeleteAllAsync(null, Enumerable.Range(-1, 12).Select(x => GetTuple(x)));
    }

    [Test]
    public async Task TestPutGet()
    {
        await KvView.PutAsync(null, GetTuple(1L), GetTuple("val"));

        (IIgniteTuple res, _) = await KvView.GetAsync(null, GetTuple(1L));

        Assert.AreEqual("val", res[0]);
        Assert.AreEqual("val", res[ValCol]);
    }

    [Test]
    public async Task TestGetNonExistentKeyReturnsEmptyOption()
    {
        (IIgniteTuple res, bool hasRes) = await KvView.GetAsync(null, GetTuple(-111L));

        Assert.IsFalse(hasRes);
        Assert.IsNull(res);
    }

    [Test]
    public async Task TestGetAll()
    {
        await KvView.PutAsync(null, GetTuple(7L), GetTuple("val1"));
        await KvView.PutAsync(null, GetTuple(8L), GetTuple("val2"));

        IDictionary<IIgniteTuple, IIgniteTuple> res = await KvView.GetAllAsync(null, Enumerable.Range(-1, 100).Select(x => GetTuple(x)).ToList());
        IDictionary<IIgniteTuple, IIgniteTuple> resEmpty = await KvView.GetAllAsync(null, Array.Empty<IIgniteTuple>());

        Assert.AreEqual(2, res.Count);
        Assert.AreEqual("val1", res[GetTuple(7L)][0]);
        Assert.AreEqual("val2", res[GetTuple(8L)][0]);

        Assert.AreEqual(0, resEmpty.Count);
    }

    [Test]
    public void TestGetAllWithNullKeyThrowsArgumentException()
    {
        var ex = Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await KvView.GetAllAsync(null, new[] { GetTuple(1L), null! }));

        Assert.AreEqual("Value cannot be null. (Parameter 'key')", ex!.Message);
    }

    [Test]
    public void TestPutNullThrowsArgumentException()
    {
        var keyEx = Assert.ThrowsAsync<ArgumentNullException>(async () => await KvView.PutAsync(null, null!, null!));
        Assert.AreEqual("Value cannot be null. (Parameter 'key')", keyEx!.Message);

        var valEx = Assert.ThrowsAsync<ArgumentNullException>(async () => await KvView.PutAsync(null, GetTuple(1L), null!));
        Assert.AreEqual("Value cannot be null. (Parameter 'val')", valEx!.Message);
    }

    [Test]
    public async Task TestContains()
    {
        await KvView.PutAsync(null, GetTuple(7L), GetTuple("val1"));

        bool res1 = await KvView.ContainsAsync(null, GetTuple(7L));
        bool res2 = await KvView.ContainsAsync(null, GetTuple(8L));

        Assert.IsTrue(res1);
        Assert.IsFalse(res2);
    }

    [Test]
    public async Task TestPutAll()
    {
        await KvView.PutAllAsync(null, new Dictionary<IIgniteTuple, IIgniteTuple>());
        await KvView.PutAllAsync(
            null,
            Enumerable.Range(-1, 7).Select(x => new KeyValuePair<IIgniteTuple, IIgniteTuple>(GetTuple(x), GetTuple("v" + x))));

        IDictionary<IIgniteTuple, IIgniteTuple> res = await KvView.GetAllAsync(null, Enumerable.Range(-10, 20).Select(x => GetTuple(x)));

        Assert.AreEqual(7, res.Count);

        for (int i = -1; i < 6; i++)
        {
            IIgniteTuple val = res[GetTuple(i)];
            Assert.AreEqual("v" + i, val[ValCol]);
        }
    }

    [Test]
    public async Task TestGetAndPut()
    {
        Option<IIgniteTuple> res1 = await KvView.GetAndPutAsync(null, GetTuple(1), GetTuple("1"));
        Option<IIgniteTuple> res2 = await KvView.GetAndPutAsync(null, GetTuple(1), GetTuple("2"));
        Option<IIgniteTuple> res3 = await KvView.GetAsync(null, GetTuple(1));

        Assert.IsFalse(res1.HasValue);
        Assert.IsTrue(res2.HasValue);
        Assert.IsTrue(res3.HasValue);

        Assert.AreEqual("1", res2.Value[0]);
        Assert.AreEqual("2", res3.Value[0]);
    }

    [Test]
    public async Task TestPutIfAbsent()
    {
        await KvView.PutAsync(null, GetTuple(1), GetTuple("1"));

        bool res1 = await KvView.PutIfAbsentAsync(null, GetTuple(1), GetTuple("11"));
        Option<IIgniteTuple> res2 = await KvView.GetAsync(null, GetTuple(1));

        bool res3 = await KvView.PutIfAbsentAsync(null, GetTuple(2), GetTuple("2"));
        Option<IIgniteTuple> res4 = await KvView.GetAsync(null, GetTuple(2));

        Assert.IsFalse(res1);
        Assert.AreEqual("1", res2.Value[0]);

        Assert.IsTrue(res3);
        Assert.AreEqual("2", res4.Value[0]);
    }

    [Test]
    public async Task TestRemove()
    {
        await KvView.PutAsync(null, GetTuple(1), GetTuple("1"));

        bool res1 = await KvView.RemoveAsync(null, GetTuple(1));
        bool res2 = await KvView.RemoveAsync(null, GetTuple(2));
        bool res3 = await KvView.ContainsAsync(null, GetTuple(1));

        Assert.IsTrue(res1);
        Assert.IsFalse(res2);
        Assert.IsFalse(res3);
    }

    [Test]
    public async Task TestRemoveExact()
    {
        await KvView.PutAsync(null, GetTuple(1), GetTuple("1"));

        bool res1 = await KvView.RemoveAsync(null, GetTuple(1), GetTuple("111"));
        bool res2 = await KvView.RemoveAsync(null, GetTuple(1), GetTuple("1"));
        bool res3 = await KvView.ContainsAsync(null, GetTuple(1));

        Assert.IsFalse(res1);
        Assert.IsTrue(res2);
        Assert.IsFalse(res3);
    }

    [Test]
    public async Task TestRemoveAll()
    {
        await KvView.PutAsync(null, GetTuple(1), GetTuple("1"));

        IList<IIgniteTuple> res1 = await KvView.RemoveAllAsync(null, Enumerable.Range(-1, 8).Select(x => GetTuple(x)));
        bool res2 = await KvView.ContainsAsync(null, GetTuple(1));

        Assert.AreEqual(new[] { -1, 0, 2, 3, 4, 5, 6 }, res1.Select(x => x[0]).OrderBy(x => x));
        Assert.IsFalse(res2);
    }

    [Test]
    public async Task TestRemoveAllExact()
    {
        await KvView.PutAsync(null, GetTuple(1), GetTuple("1"));

        IList<IIgniteTuple> res1 = await KvView.RemoveAllAsync(
            null,
            Enumerable.Range(-1, 8).Select(x => new KeyValuePair<IIgniteTuple, IIgniteTuple>(GetTuple(x), GetTuple(x.ToString()))));

        bool res2 = await KvView.ContainsAsync(null, GetTuple(1));

        Assert.AreEqual(new[] { -1, 0, 2, 3, 4, 5, 6 }, res1.Select(x => x[0]).OrderBy(x => x));
        Assert.IsFalse(res2);
    }

    [Test]
    public async Task TestGetAndRemove()
    {
        await KvView.PutAsync(null, GetTuple(1), GetTuple("1"));

        (IIgniteTuple val1, bool hasVal1) = await KvView.GetAndRemoveAsync(null, GetTuple(1));
        (IIgniteTuple val2, bool hasVal2) = await KvView.GetAndRemoveAsync(null, GetTuple(1));

        Assert.IsTrue(hasVal1);
        Assert.AreEqual("1", val1[0]);

        Assert.IsFalse(hasVal2);
        Assert.IsNull(val2);
    }

    [Test]
    public async Task TestReplace()
    {
        await KvView.PutAsync(null, GetTuple(1), GetTuple("1"));

        bool res1 = await KvView.ReplaceAsync(null, GetTuple(0), GetTuple("00"));
        Option<IIgniteTuple> res2 = await KvView.GetAsync(null, GetTuple(0));

        bool res3 = await KvView.ReplaceAsync(null, GetTuple(1), GetTuple("11"));
        Option<IIgniteTuple> res4 = await KvView.GetAsync(null, GetTuple(1));

        Assert.IsFalse(res1);
        Assert.IsFalse(res2.HasValue);

        Assert.IsTrue(res3);
        Assert.IsTrue(res4.HasValue);
        Assert.AreEqual("11", res4.Value[0]);
    }

    [Test]
    public async Task TestReplaceExact()
    {
        await KvView.PutAsync(null, GetTuple(1), GetTuple("1"));

        bool res1 = await KvView.ReplaceAsync(transaction: null, key: GetTuple(0), oldVal: GetTuple("0"), newVal: GetTuple("00"));
        Option<IIgniteTuple> res2 = await KvView.GetAsync(null, GetTuple(0));

        bool res3 = await KvView.ReplaceAsync(transaction: null, key: GetTuple(1), oldVal: GetTuple("1"), newVal: GetTuple("11"));
        Option<IIgniteTuple> res4 = await KvView.GetAsync(null, GetTuple(1));

        bool res5 = await KvView.ReplaceAsync(transaction: null, key: GetTuple(2), oldVal: GetTuple("1"), newVal: GetTuple("22"));
        Option<IIgniteTuple> res6 = await KvView.GetAsync(null, GetTuple(1));

        Assert.IsFalse(res1);
        Assert.IsFalse(res2.HasValue);

        Assert.IsTrue(res3);
        Assert.IsTrue(res4.HasValue);
        Assert.AreEqual("11", res4.Value[0]);

        Assert.IsFalse(res5);
        Assert.AreEqual("11", res6.Value[0]);
    }

    [Test]
    public async Task TestGetAndReplace()
    {
        await KvView.PutAsync(null, GetTuple(1), GetTuple("1"));

        Option<IIgniteTuple> res1 = await KvView.GetAndReplaceAsync(null, GetTuple(0), GetTuple("00"));
        Option<IIgniteTuple> res2 = await KvView.GetAsync(null, GetTuple(0));

        Option<IIgniteTuple> res3 = await KvView.GetAndReplaceAsync(null, GetTuple(1), GetTuple("11"));
        Option<IIgniteTuple> res4 = await KvView.GetAsync(null, GetTuple(1));

        Assert.IsFalse(res1.HasValue);
        Assert.IsFalse(res2.HasValue);

        Assert.IsTrue(res3.HasValue);
        Assert.AreEqual("1", res3.Value[0]);

        Assert.IsTrue(res4.HasValue);
        Assert.AreEqual("11", res4.Value[0]);
    }

    [Test]
    public void TestToString()
    {
        StringAssert.StartsWith("KeyValueView`2[IIgniteTuple, IIgniteTuple] { Table = Table { Name = TBL1, Id =", KvView.ToString());
    }
}
