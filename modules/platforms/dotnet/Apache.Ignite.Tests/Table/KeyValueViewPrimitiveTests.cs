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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Threading.Tasks;
using Ignite.Table;
using NodaTime;
using NUnit.Framework;

/// <summary>
/// Tests for key-value POCO view.
/// </summary>
public class KeyValueViewPrimitiveTests : IgniteTestsBase
{
    private IKeyValueView<long, string> KvView => Table.GetKeyValueView<long, string>();

    [TearDown]
    public async Task CleanTable()
    {
        await TupleView.DeleteAllAsync(null, Enumerable.Range(-1, 12).Select(x => GetTuple(x)));
    }

    [Test]
    public async Task TestPutGet()
    {
        await KvView.PutAsync(null, 1L, "val");

        (string res, _) = await KvView.GetAsync(null, 1L);

        Assert.AreEqual("val", res);
    }

    [Test]
    public async Task TestPutGetNullable()
    {
        // TODO: Refactor this test somehow.
        await Client.Sql.ExecuteAsync(null, "CREATE TABLE IF NOT EXISTS TestPutGetNullable (ID BIGINT PRIMARY KEY, VAL BIGINT)");

        var table = await Client.Tables.GetTableAsync("TestPutGetNullable");

        var recView = table!.RecordBinaryView;
        await recView.UpsertAsync(null, new IgniteTuple { ["ID"] = 1L, ["VAL"] = 1L });
        await recView.UpsertAsync(null, new IgniteTuple { ["ID"] = 1L, ["VAL"] = null });

        var view = table.GetKeyValueView<long, long?>();
        var res1 = await view.GetAsync(null, 1);
        var res2 = await view.GetAsync(null, 2);
        var res3 = await view.GetAsync(null, 3);

        Assert.IsTrue(res1.HasValue);
        Assert.AreEqual(1, res1.Value);

        Assert.IsTrue(res2.HasValue);
        Assert.AreEqual(null, res2.Value);

        Assert.IsFalse(res3.HasValue);
        Assert.AreEqual(null, res3.Value);
    }

    [Test]
    public async Task TestGetNonExistentKeyReturnsEmptyOption()
    {
        (string res, bool hasRes) = await KvView.GetAsync(null, -111L);

        Assert.IsFalse(hasRes);
        Assert.IsNull(res);
    }

    [Test]
    public async Task TestGetAll()
    {
        await KvView.PutAsync(null, 7L, "val1");
        await KvView.PutAsync(null, 8L, "val2");

        IDictionary<long, string> res = await KvView.GetAllAsync(null, Enumerable.Range(-1, 100).Select(x => (long)x).ToList());
        IDictionary<long, string> resEmpty = await KvView.GetAllAsync(null, Array.Empty<long>());

        Assert.AreEqual(2, res.Count);
        Assert.AreEqual("val1", res[7L]);
        Assert.AreEqual("val2", res[8L]);

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
        await KvView.PutAsync(null, 7L, "val1");

        bool res1 = await KvView.ContainsAsync(null, 7L);
        bool res2 = await KvView.ContainsAsync(null, 8L);

        Assert.IsTrue(res1);
        Assert.IsFalse(res2);
    }

    [Test]
    public async Task TestPutAll()
    {
        await KvView.PutAllAsync(null, new Dictionary<long, string>());
        await KvView.PutAllAsync(
            null,
            Enumerable.Range(-1, 7).Select(x => new KeyValuePair<long, string>(x, "v" + x)));

        IDictionary<long, string> res = await KvView.GetAllAsync(null, Enumerable.Range(-10, 20).Select(x => (long)x));

        Assert.AreEqual(7, res.Count);

        for (int i = -1; i < 6; i++)
        {
            string val = res[i];
            Assert.AreEqual("v" + i, val);
        }
    }

    [Test]
    public async Task TestGetAndPut()
    {
        Option<string> res1 = await KvView.GetAndPutAsync(null, 1, "1");
        Option<string> res2 = await KvView.GetAndPutAsync(null, 1, "2");
        Option<string> res3 = await KvView.GetAsync(null, 1);

        Assert.IsFalse(res1.HasValue);
        Assert.IsTrue(res2.HasValue);
        Assert.IsTrue(res3.HasValue);

        Assert.AreEqual("1", res2.Value);
        Assert.AreEqual("2", res3.Value);
    }

    [Test]
    public async Task TestPutIfAbsent()
    {
        await KvView.PutAsync(null, 1, "1");

        bool res1 = await KvView.PutIfAbsentAsync(null, 1, "11");
        Option<string> res2 = await KvView.GetAsync(null, 1);

        bool res3 = await KvView.PutIfAbsentAsync(null, 2, "2");
        Option<string> res4 = await KvView.GetAsync(null, 2);

        Assert.IsFalse(res1);
        Assert.AreEqual("1", res2.Value);

        Assert.IsTrue(res3);
        Assert.AreEqual("2", res4.Value);
    }

    [Test]
    public async Task TestRemove()
    {
        await KvView.PutAsync(null, 1, "1");

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
        await KvView.PutAsync(null, 1, "1");

        bool res1 = await KvView.RemoveAsync(null, 1, "111");
        bool res2 = await KvView.RemoveAsync(null, 1, "1");
        bool res3 = await KvView.ContainsAsync(null, 1);

        Assert.IsFalse(res1);
        Assert.IsTrue(res2);
        Assert.IsFalse(res3);
    }

    [Test]
    public async Task TestRemoveAll()
    {
        await KvView.PutAsync(null, 1, "1");

        IList<long> res1 = await KvView.RemoveAllAsync(null, Enumerable.Range(-1, 8).Select(x => (long)x));
        bool res2 = await KvView.ContainsAsync(null, 1);

        Assert.AreEqual(new[] { -1, 0, 2, 3, 4, 5, 6 }, res1.OrderBy(x => x));
        Assert.IsFalse(res2);
    }

    [Test]
    public async Task TestRemoveAllExact()
    {
        await KvView.PutAsync(null, 1, "1");

        IList<long> res1 = await KvView.RemoveAllAsync(
            null,
            Enumerable.Range(-1, 8).Select(x => new KeyValuePair<long, string>(x, x.ToString())));

        bool res2 = await KvView.ContainsAsync(null, 1);

        Assert.AreEqual(new[] { -1, 0, 2, 3, 4, 5, 6 }, res1.OrderBy(x => x));
        Assert.IsFalse(res2);
    }

    [Test]
    public async Task TestGetAndRemove()
    {
        await KvView.PutAsync(null, 1, "1");

        (string val1, bool hasVal1) = await KvView.GetAndRemoveAsync(null, 1);
        (string val2, bool hasVal2) = await KvView.GetAndRemoveAsync(null, 1);

        Assert.IsTrue(hasVal1);
        Assert.AreEqual("1", val1);

        Assert.IsFalse(hasVal2);
        Assert.IsNull(val2);
    }

    [Test]
    public async Task TestReplace()
    {
        await KvView.PutAsync(null, 1, "1");

        bool res1 = await KvView.ReplaceAsync(null, 0, "00");
        Option<string> res2 = await KvView.GetAsync(null, 0);

        bool res3 = await KvView.ReplaceAsync(null, 1, "11");
        Option<string> res4 = await KvView.GetAsync(null, 1);

        Assert.IsFalse(res1);
        Assert.IsFalse(res2.HasValue);

        Assert.IsTrue(res3);
        Assert.IsTrue(res4.HasValue);
        Assert.AreEqual("11", res4.Value);
    }

    [Test]
    public async Task TestReplaceExact()
    {
        await KvView.PutAsync(null, 1, "1");

        bool res1 = await KvView.ReplaceAsync(transaction: null, key: 0, oldVal: "0", newVal: "00");
        Option<string> res2 = await KvView.GetAsync(null, 0);

        bool res3 = await KvView.ReplaceAsync(transaction: null, key: 1, oldVal: "1", newVal: "11");
        Option<string> res4 = await KvView.GetAsync(null, 1);

        bool res5 = await KvView.ReplaceAsync(transaction: null, key: 2, oldVal: "1", newVal: "22");
        Option<string> res6 = await KvView.GetAsync(null, 1);

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
        await KvView.PutAsync(null, 1, "1");

        Option<string> res1 = await KvView.GetAndReplaceAsync(null, 0, "00");
        Option<string> res2 = await KvView.GetAsync(null, 0);

        Option<string> res3 = await KvView.GetAndReplaceAsync(null, 1, "11");
        Option<string> res4 = await KvView.GetAsync(null, 1);

        Assert.IsFalse(res1.HasValue);
        Assert.IsFalse(res2.HasValue);

        Assert.IsTrue(res3.HasValue);
        Assert.AreEqual("1", res3.Value);

        Assert.IsTrue(res4.HasValue);
        Assert.AreEqual("11", res4.Value);
    }

    [Test]
    public async Task TestAllTypes()
    {
        await TestKey((sbyte)1, TableInt8Name);
        await TestKey((short)1, TableInt16Name);
        await TestKey(1, TableInt32Name);
        await TestKey(1L, TableInt64Name);
        await TestKey(1.1f, TableFloatName);
        await TestKey(1.1d, TableDoubleName);
        await TestKey(1.234m, TableDecimalName);
        await TestKey("foo", TableStringName);
        await TestKey(new LocalDateTime(2022, 10, 13, 8, 4, 42), TableDateTimeName);
        await TestKey(new LocalTime(3, 4, 5), TableTimeName);
        await TestKey(Instant.FromUnixTimeMilliseconds(123456789101112), TableTimestampName);
        await TestKey(new BigInteger(123456789101112), TableNumberName);
        await TestKey(new byte[] { 1, 2, 3 }, TableBytesName);
        await TestKey(new BitArray(new[] { byte.MaxValue }), TableBitmaskName);
    }

    [Test]
    public void TestToString()
    {
        StringAssert.StartsWith("KeyValueView`2[Int64, String] { Table = Table { Name = TBL1, Id =", KvView.ToString());
    }

    private static async Task TestKey<T>(T val, IKeyValueView<T, T> kvView)
        where T : notnull
    {
        // Tests EmitKvWriter.
        await kvView.PutAsync(null, val, val);

        // Tests EmitKvValuePartReader.
        var (getRes, _) = await kvView.GetAsync(null, val);

        // Tests EmitKvReader.
        var getAllRes = await kvView.GetAllAsync(null, new[] { val });

        Assert.AreEqual(val, getRes);
        Assert.AreEqual(val, getAllRes.Single().Value);
    }

    private async Task TestKey<T>(T val, string tableName)
        where T : notnull
    {
        var table = await Client.Tables.GetTableAsync(tableName);

        Assert.IsNotNull(table, tableName);

        await TestKey(val, table!.GetKeyValueView<T, T>());
    }
}
