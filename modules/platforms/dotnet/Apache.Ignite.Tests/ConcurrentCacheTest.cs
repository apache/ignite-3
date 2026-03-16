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

namespace Apache.Ignite.Tests;

using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Common;
using Internal.Common;
using NUnit.Framework;

/// <summary>
/// Tests for <see cref="ConcurrentCache{TKey,TValue}"/>.
/// </summary>
public class ConcurrentCacheTest
{
    [Test]
    public void TestTryAdd()
    {
        var cache = new ConcurrentCache<int, string>(10);

        Assert.IsTrue(cache.TryAdd(1, "one"));
        Assert.AreEqual("one", cache.GetValueOrDefault(1));
    }

    [Test]
    public void TestTryAddExistingKey()
    {
        var cache = new ConcurrentCache<int, string>(10);

        Assert.IsTrue(cache.TryAdd(1, "one"));
        Assert.IsFalse(cache.TryAdd(1, "uno"));
        Assert.AreEqual("one", cache.GetValueOrDefault(1));
    }

    [Test]
    public void TestGetValueOrDefaultMissingKey()
    {
        var cache = new ConcurrentCache<int, string>(10);

        Assert.IsNull(cache.GetValueOrDefault(1));
    }

    [Test]
    public void TestEviction()
    {
        var cache = new ConcurrentCache<int, string>(2);

        cache.TryAdd(1, "one");
        cache.TryAdd(2, "two");
        cache.GetValueOrDefault(1); // Mark entry 1 as visited
        cache.TryAdd(3, "four");

        Assert.IsNull(cache.GetValueOrDefault(2), "Unused entry 2 should be evicted");
        Assert.AreEqual("one", cache.GetValueOrDefault(1));
        Assert.AreEqual("four", cache.GetValueOrDefault(3));
    }

    [Test]
    public void TestMultipleVisitedEntriesArePreserved()
    {
        var cache = new ConcurrentCache<int, string>(3);

        cache.TryAdd(1, "one");
        cache.TryAdd(2, "two");
        cache.TryAdd(3, "three");

        // Mark entries 1 and 2 as visited
        cache.GetValueOrDefault(1);
        cache.GetValueOrDefault(2);

        cache.TryAdd(4, "four");

        Assert.IsNull(cache.GetValueOrDefault(3), "Unused entry 3 should be evicted");
        Assert.AreEqual("one", cache.GetValueOrDefault(1));
        Assert.AreEqual("two", cache.GetValueOrDefault(2));
        Assert.AreEqual("four", cache.GetValueOrDefault(4));
    }

    [Test]
    public void TestEvictUnderLoad()
    {
        var cts = new CancellationTokenSource();
        var keys = Enumerable.Range(1, 10).ToArray();
        var cache = new ConcurrentCache<int, string>(keys.Length);
        var gate = new ManualResetEventSlim();

        foreach (var key in keys)
        {
            cache.TryAdd(key, string.Empty);
        }

        var visiterTask = Task.Run(() =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                foreach (var key in keys)
                {
                    cache.GetValueOrDefault(key);
                }

                gate.Set();
            }
        });

        using var cleaner = new DisposeAction(() =>
        {
            cts.Cancel();
            TestUtils.WaitForCondition(() => visiterTask.IsCompleted);
        });

        gate.Wait();

        var added = cache.TryAdd(-1, "new");

        Assert.IsTrue(added, "Should be able to add new entry");
    }
}
