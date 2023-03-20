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

using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Ignite.Sql;
using Internal.Linq;
using NUnit.Framework;

/// <summary>
/// Linq MemberInitTests.
/// </summary>
public partial class LinqTests
{
    [Test]
    public void Do()
    {
        var res = PocoView.AsQueryable()
            .Where(x => x.Key == 2)
            .Select(x => new CustomProjection { Key = x.Key, Value = x.Val })
            .ToArray();

        Assert.AreEqual(1, res.Length);
        Assert.AreEqual(2, res[0].Key);
        Assert.AreEqual("v-2", res[0].Value);
    }

    [Test]
    public void Do1()
    {
        var res1 = PocoView.AsQueryable()
            .Where(x => x.Key == 2)
            .Select(x => new { Id = x.Key, Value = x.Val })
            .ToArray();
        var res = PocoView.AsQueryable()
            .Where(x => x.Key == 2)
            .Select(x => new CustomProjectionCtorAndInit(x.Key) { Value = x.Val })
            .ToArray();

        Assert.AreEqual(1, res.Length);
        Assert.AreEqual(2, res[0].Id);
        Assert.AreEqual("v-2", res[0].Value);
    }

    [Test]
    public void Do2()
    {
        // var res1 = PocoView.AsQueryable()
        //     .Where(x => x.Key == 2)
        //     .Select(x => new { Id = x.Key, Value = x.Val })
        //     .ToArray();
        var query = PocoView.AsQueryable()
            .Where(x => x.Key == 2)
            .Select(x => new CustomProjectionCtor(x.Key + 42, x.Val));
        var queryString = query.ToQueryString();
        var res = query
            .ToArray();

        Assert.AreEqual(1, res.Length);
        Assert.AreEqual(44, res[0].Id);
        Assert.AreEqual("v-2", res[0].Data);
    }

    [Test]
    [Ignore("Why not. Complains about not supported stuff")]
    public async Task TestSelectTwoColumns1()
    {
        var tableAllColumns = await Client.Tables.GetTableAsync(TableAllColumnsName);

        var view = tableAllColumns!.GetRecordView<Poco1>();
        var res = view.AsQueryable()
            .Where(x => x.Key == 2)
            .Select(x => new { Id = x.Key + 1, Id1 = (long)x.Int32 })
            .ToArray();

        var res1 = view.AsQueryable()
            .Where(x => x.Key == 2)
            .Select(x => new { Id1 = x.Key + 1, Id = (long)x.Int32 })
            .ToArray();

        Assert.AreEqual(1, res.Length);
    }

    private class Poco1
    {
        public long Key { get; set; }

        public string? Val { get; set; }

        public int Int32 { get; set; }

        [NotMapped]
        public Guid UnmappedId { get; set; }

        [NotMapped]
        public string? UnmappedStr { get; set; }
    }

    private class CustomProjection
    {
        public long Key { get; set; }

        public string? Value { get; set; }
    }

    private class CustomProjectionCtorAndInit
    {
        public CustomProjectionCtorAndInit(long id)
        {
            Id = id;
        }

        public long Id { get; }

        public string? Value { get; set; }
    }

    private class CustomProjectionCtor
    {
        public CustomProjectionCtor(long id, string? data)
        {
            Id = id;
            Data = data;
        }

        public long Id { get; }

        public string? Data { get; }
    }
}
