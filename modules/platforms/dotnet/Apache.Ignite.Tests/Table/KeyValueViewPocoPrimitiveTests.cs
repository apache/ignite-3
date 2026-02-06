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
/// Tests for key-value view where key is a user object (poco), and value is a primitive (string).
/// </summary>
public class KeyValueViewPocoPrimitiveTests : IgniteTestsBase
{
    private IKeyValueView<KeyPoco, string> KvView => Table.GetKeyValueView<KeyPoco, string>();

    [TearDown]
    public async Task CleanTable()
    {
        await TupleView.DeleteAllAsync(null, Enumerable.Range(-1, 12).Select(x => GetTuple(x)));
    }

    [Test]
    public async Task TestPutGet()
    {
        await KvView.PutAsync(null, GetKeyPoco(1L), "val");

        (string res, bool hasRes) = await KvView.GetAsync(null, GetKeyPoco(1L));

        Assert.IsTrue(hasRes);
        Assert.AreEqual("val", res);
    }

    [Test]
    public async Task TestGetNonExistentKeyReturnsEmptyOption()
    {
        (string res, bool hasRes) = await KvView.GetAsync(null, GetKeyPoco(-111L));

        Assert.IsFalse(hasRes);
        Assert.IsNull(res);
    }

    [Test]
    public async Task TestGetAll()
    {
        await KvView.PutAsync(null, GetKeyPoco(7L), "val1");
        await KvView.PutAsync(null, GetKeyPoco(8L), "val2");

        IDictionary<KeyPoco, string> res = await KvView.GetAllAsync(null, Enumerable.Range(-1, 100).Select(x => GetKeyPoco(x)).ToList());
        IDictionary<KeyPoco, string> resEmpty = await KvView.GetAllAsync(null, Array.Empty<KeyPoco>());

        Assert.AreEqual(2, res.Count);
        Assert.AreEqual("val1", res.Single(x => x.Key.Key == 7).Value);
        Assert.AreEqual("val2", res.Single(x => x.Key.Key == 8).Value);

        Assert.AreEqual(0, resEmpty.Count);
    }

    [Test]
    public void TestGetAllWithNullKeyThrowsArgumentException()
    {
        var ex = Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await KvView.GetAllAsync(null, new[] { GetKeyPoco(1L), null! }));

        Assert.AreEqual("Value cannot be null. (Parameter 'key')", ex!.Message);
    }

    [Test]
    public void TestPutNullKeyThrowsArgumentException()
    {
        var keyEx = Assert.ThrowsAsync<ArgumentNullException>(async () => await KvView.PutAsync(null, null!, null!));
        Assert.AreEqual("Value cannot be null. (Parameter 'key')", keyEx!.Message);
    }

    [Test]
    public async Task TestContains()
    {
        await KvView.PutAsync(null, GetKeyPoco(7L), "val1");

        bool res1 = await KvView.ContainsAsync(null, GetKeyPoco(7L));
        bool res2 = await KvView.ContainsAsync(null, GetKeyPoco(8L));

        Assert.IsTrue(res1);
        Assert.IsFalse(res2);
    }

    [Test]
    public async Task TestPutAll()
    {
        await KvView.PutAllAsync(null, new Dictionary<KeyPoco, string>());
        await KvView.PutAllAsync(
            null,
            Enumerable.Range(-1, 7).Select(x => new KeyValuePair<KeyPoco, string>(GetKeyPoco(x), "v" + x)));

        IDictionary<KeyPoco, string> res = await KvView.GetAllAsync(null, Enumerable.Range(-10, 20).Select(x => GetKeyPoco(x)));

        Assert.AreEqual(7, res.Count);

        for (int i = -1; i < 6; i++)
        {
            string val = res.Single(x => x.Key.Key == i).Value;
            Assert.AreEqual("v" + i, val);
        }
    }

    [Test]
    public async Task TestGetAndPut()
    {
        Option<string> res1 = await KvView.GetAndPutAsync(null, GetKeyPoco(1), "1");
        Option<string> res2 = await KvView.GetAndPutAsync(null, GetKeyPoco(1), "2");
        Option<string> res3 = await KvView.GetAsync(null, GetKeyPoco(1));

        Assert.IsFalse(res1.HasValue);
        Assert.IsTrue(res2.HasValue);
        Assert.IsTrue(res3.HasValue);

        Assert.AreEqual("1", res2.Value);
        Assert.AreEqual("2", res3.Value);
    }

    [Test]
    public async Task TestPutIfAbsent()
    {
        await KvView.PutAsync(null, GetKeyPoco(1), "1");

        bool res1 = await KvView.PutIfAbsentAsync(null, GetKeyPoco(1), "11");
        Option<string> res2 = await KvView.GetAsync(null, GetKeyPoco(1));

        bool res3 = await KvView.PutIfAbsentAsync(null, GetKeyPoco(2), "2");
        Option<string> res4 = await KvView.GetAsync(null, GetKeyPoco(2));

        Assert.IsFalse(res1);
        Assert.AreEqual("1", res2.Value);

        Assert.IsTrue(res3);
        Assert.AreEqual("2", res4.Value);
    }

    [Test]
    public async Task TestRemove()
    {
        await KvView.PutAsync(null, GetKeyPoco(1), "1");

        bool res1 = await KvView.RemoveAsync(null, GetKeyPoco(1));
        bool res2 = await KvView.RemoveAsync(null, GetKeyPoco(2));
        bool res3 = await KvView.ContainsAsync(null, GetKeyPoco(1));

        Assert.IsTrue(res1);
        Assert.IsFalse(res2);
        Assert.IsFalse(res3);
    }

    [Test]
    public async Task TestRemoveExact()
    {
        await KvView.PutAsync(null, GetKeyPoco(1), "1");

        bool res1 = await KvView.RemoveAsync(null, GetKeyPoco(1), "111");
        bool res2 = await KvView.RemoveAsync(null, GetKeyPoco(1), "1");
        bool res3 = await KvView.ContainsAsync(null, GetKeyPoco(1));

        Assert.IsFalse(res1);
        Assert.IsTrue(res2);
        Assert.IsFalse(res3);
    }

    [Test]
    public async Task TestRemoveAll()
    {
        await KvView.PutAsync(null, GetKeyPoco(1), "1");

        IList<KeyPoco> res1 = await KvView.RemoveAllAsync(null, Enumerable.Range(-1, 8).Select(x => GetKeyPoco(x)));
        bool res2 = await KvView.ContainsAsync(null, GetKeyPoco(1));

        Assert.AreEqual(new[] { -1, 0, 2, 3, 4, 5, 6 }, res1.Select(x => x.Key).OrderBy(x => x));
        Assert.IsFalse(res2);
    }

    [Test]
    public async Task TestRemoveAllExact()
    {
        await KvView.PutAsync(null, GetKeyPoco(1), "1");

        IList<KeyPoco> res1 = await KvView.RemoveAllAsync(
            null,
            Enumerable.Range(-1, 8).Select(x => new KeyValuePair<KeyPoco, string>(GetKeyPoco(x), x.ToString())));

        bool res2 = await KvView.ContainsAsync(null, GetKeyPoco(1));

        Assert.AreEqual(new[] { -1, 0, 2, 3, 4, 5, 6 }, res1.Select(x => x.Key).OrderBy(x => x));
        Assert.IsFalse(res2);
    }

    [Test]
    public async Task TestGetAndRemove()
    {
        await KvView.PutAsync(null, GetKeyPoco(1), "1");

        (string val1, bool hasVal1) = await KvView.GetAndRemoveAsync(null, GetKeyPoco(1));
        (string val2, bool hasVal2) = await KvView.GetAndRemoveAsync(null, GetKeyPoco(1));

        Assert.IsTrue(hasVal1);
        Assert.AreEqual("1", val1);

        Assert.IsFalse(hasVal2);
        Assert.IsNull(val2);
    }

    [Test]
    public async Task TestReplace()
    {
        await KvView.PutAsync(null, GetKeyPoco(1), "1");

        bool res1 = await KvView.ReplaceAsync(null, GetKeyPoco(0), "00");
        Option<string> res2 = await KvView.GetAsync(null, GetKeyPoco(0));

        bool res3 = await KvView.ReplaceAsync(null, GetKeyPoco(1), "11");
        Option<string> res4 = await KvView.GetAsync(null, GetKeyPoco(1));

        Assert.IsFalse(res1);
        Assert.IsFalse(res2.HasValue);

        Assert.IsTrue(res3);
        Assert.IsTrue(res4.HasValue);
        Assert.AreEqual("11", res4.Value);
    }

    [Test]
    public async Task TestReplaceExact()
    {
        await KvView.PutAsync(null, GetKeyPoco(1), "1");

        bool res1 = await KvView.ReplaceAsync(transaction: null, key: GetKeyPoco(0), oldVal: "0", newVal: "00");
        Option<string> res2 = await KvView.GetAsync(null, GetKeyPoco(0));

        bool res3 = await KvView.ReplaceAsync(transaction: null, key: GetKeyPoco(1), oldVal: "1", newVal: "11");
        Option<string> res4 = await KvView.GetAsync(null, GetKeyPoco(1));

        bool res5 = await KvView.ReplaceAsync(transaction: null, key: GetKeyPoco(2), oldVal: "1", newVal: "22");
        Option<string> res6 = await KvView.GetAsync(null, GetKeyPoco(1));

        Assert.IsFalse(res1);
        Assert.IsFalse(res2.HasValue);

        Assert.IsTrue(res3);
        Assert.IsTrue(res4.HasValue);
        Assert.AreEqual("11", res4.Value);

        Assert.IsFalse(res5);
        Assert.AreEqual("11", res6.Value);
    }

    [Test]
    public async Task TestGetAndReplace()
    {
        await KvView.PutAsync(null, GetKeyPoco(1), "1");

        Option<string> res1 = await KvView.GetAndReplaceAsync(null, GetKeyPoco(0), "00");
        Option<string> res2 = await KvView.GetAsync(null, GetKeyPoco(0));

        Option<string> res3 = await KvView.GetAndReplaceAsync(null, GetKeyPoco(1), "11");
        Option<string> res4 = await KvView.GetAsync(null, GetKeyPoco(1));

        Assert.IsFalse(res1.HasValue);
        Assert.IsFalse(res2.HasValue);

        Assert.IsTrue(res3.HasValue);
        Assert.AreEqual("1", res3.Value);

        Assert.IsTrue(res4.HasValue);
        Assert.AreEqual("11", res4.Value);
    }
}
