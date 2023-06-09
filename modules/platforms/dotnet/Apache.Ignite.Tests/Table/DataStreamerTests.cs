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
/// Tests for <see cref="IDataStreamerTarget{T}.StreamDataAsync"/>.
/// </summary>
public class DataStreamerTests : IgniteTestsBase
{
    [Test]
    public async Task TestBasicStreaming()
    {
        var data = Enumerable.Range(1, 10).Select(x => GetTuple(x, "t" + x)).ToList();
        await Table.RecordBinaryView.StreamDataAsync(data.ToAsyncEnumerable());

        foreach (var tuple in data)
        {
            var (val, hasVal) = await Table.RecordBinaryView.GetAsync(null, tuple);
            Assert.IsTrue(hasVal, tuple.ToString());
            Assert.AreEqual(val, tuple);
        }
    }

    [Test]
    public async Task TestAutoFlushFrequency()
    {
        var streamTask = Table.RecordBinaryView.StreamDataAsync(
            GetTuplesWithDelay(),
            new() { AutoFlushFrequency = TimeSpan.FromMilliseconds(5000) });

        await Task.Delay(100);

        var (_, hasVal1) = await Table.RecordBinaryView.GetAsync(null, GetTuple(0));
        Assert.IsTrue(hasVal1);

        await streamTask;

        async IAsyncEnumerable<IIgniteTuple> GetTuplesWithDelay()
        {
            for (var i = 0; i < 3; i++)
            {
                yield return GetTuple(i, "t" + i);
                await Task.Delay(500);
            }
        }
    }

    [Test]
    public async Task TestOptionsValidation()
    {
        await Task.Delay(1);
        Assert.Fail("TODO");
    }
}
