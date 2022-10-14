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
using System.Linq;
using System.Threading.Tasks;
using Ignite.Table;
using NUnit.Framework;

/// <summary>
/// Tests for key-value tuple view.
/// </summary>
public class KeyValueViewBinaryTests : IgniteTestsBase
{
    [TearDown]
    public async Task CleanTable()
    {
        await TupleView.DeleteAllAsync(null, Enumerable.Range(-1, 12).Select(x => GetTuple(x)));
    }

    [Test]
    public async Task TestPutGet()
    {
        var kvView = Table.KeyValueBinaryView;
        await kvView.PutAsync(null, GetTuple(1L), GetTuple("val"));

        var (res, _) = await kvView.GetAsync(null, GetTuple(1L));

        Assert.AreEqual("val", res[0]);
        Assert.AreEqual("val", res[ValCol]);
    }

    [Test]
    public async Task TestGetNonExistentKeyReturnsEmptyOption()
    {
        var (res, hasRes) = await Table.KeyValueBinaryView.GetAsync(null, GetTuple(-111L));

        Assert.IsFalse(hasRes);
        Assert.IsNull(res);
    }

    [Test]
    public async Task TestGetAll()
    {
        var kvView = Table.KeyValueBinaryView;
        await kvView.PutAsync(null, GetTuple(7L), GetTuple("val1"));
        await kvView.PutAsync(null, GetTuple(8L), GetTuple("val2"));

        var res = await kvView.GetAllAsync(null, Enumerable.Range(-1, 100).Select(x => GetTuple(x)).ToList());
        var resEmpty = await kvView.GetAllAsync(null, Array.Empty<IIgniteTuple>());

        Assert.AreEqual(2, res.Count);
        Assert.AreEqual("val1", res[GetTuple(7L)][0]);
        Assert.AreEqual("val2", res[GetTuple(8L)][0]);

        Assert.AreEqual(0, resEmpty.Count);
    }

    [Test]
    public async Task TestContains()
    {
        var kvView = Table.KeyValueBinaryView;
        await kvView.PutAsync(null, GetTuple(7L), GetTuple("val1"));

        var res1 = await kvView.ContainsAsync(null, GetTuple(7L));
        var res2 = await kvView.ContainsAsync(null, GetTuple(8L));

        Assert.IsTrue(res1);
        Assert.IsFalse(res2);
    }

    [Test]
    public void TestGetAllWithNullKeyThrowsArgumentException()
    {
        var ex = Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await Table.KeyValueBinaryView.GetAllAsync(null, new[] { GetTuple(1L), null! }));

        Assert.AreEqual("Value cannot be null. (Parameter 'key')", ex!.Message);
    }

    [Test]
    public void TestPutNullThrowsArgumentException()
    {
        var keyEx = Assert.ThrowsAsync<ArgumentNullException>(async () => await Table.KeyValueBinaryView.PutAsync(null, null!, null!));
        Assert.AreEqual("Value cannot be null. (Parameter 'key')", keyEx!.Message);

        var valEx = Assert.ThrowsAsync<ArgumentNullException>(async () => await Table.KeyValueBinaryView.PutAsync(null, GetTuple(1L), null!));
        Assert.AreEqual("Value cannot be null. (Parameter 'val')", valEx!.Message);
    }
}
