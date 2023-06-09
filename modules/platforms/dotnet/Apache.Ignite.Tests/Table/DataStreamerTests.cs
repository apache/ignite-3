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
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Ignite.Table;
using NUnit.Framework;

/// <summary>
/// Tests for <see cref="IDataStreamerTarget{T}.StreamDataAsync"/>.
/// </summary>
public class DataStreamerTests : IgniteTestsBase
{
    private const int Count = 100;

    [SetUp]
    public async Task SetUp() =>
        await TupleView.DeleteAllAsync(null, Enumerable.Range(0, Count).Select(x => GetTuple(x)));

    [Test]
    public async Task TestBasicStreaming()
    {
        var options = DataStreamerOptions.Default with { BatchSize = 10 };
        var data = Enumerable.Range(1, Count).Select(x => GetTuple(x, "t" + x)).ToList();
        await TupleView.StreamDataAsync(data.ToAsyncEnumerable(), options);
        var res = await TupleView.GetAllAsync(null, data);

        foreach (var ((_, hasVal), tuple) in res.Zip(data))
        {
            Assert.IsTrue(hasVal, tuple.ToString());

            // TODO: GetAll does not behave as documented
            // Assert.AreEqual(val, tuple);
        }
    }

    [Test]
    public async Task TestAutoFlushFrequency()
    {
        using var cts = new CancellationTokenSource();

        _ = TupleView.StreamDataAsync(
            GetTuplesWithDelay(cts.Token),
            new() { AutoFlushFrequency = TimeSpan.FromMilliseconds(50) });

        await Task.Delay(100);

        Assert.IsTrue(await TupleView.ContainsKeyAsync(null, GetTuple(0)));
        Assert.IsFalse(await TupleView.ContainsKeyAsync(null, GetTuple(1)));

        cts.Cancel();

        async IAsyncEnumerable<IIgniteTuple> GetTuplesWithDelay([EnumeratorCancellation] CancellationToken ct = default)
        {
            for (var i = 0; i < 3; i++)
            {
                yield return GetTuple(i, "t" + i);
                await Task.Delay(15000, ct);
            }
        }
    }

    [Test]
    public async Task TestOptionsValidation()
    {
        await Task.Delay(1);
        Assert.Fail("TODO");
    }

    [Test]
    public async Task TestPartitionAssignmentUpdate()
    {
        await Task.Delay(1);
        Assert.Fail("TODO");
    }
}
