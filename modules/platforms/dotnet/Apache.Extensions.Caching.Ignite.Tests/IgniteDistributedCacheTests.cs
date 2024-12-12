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

namespace Apache.Extensions.Cache.Ignite.Tests;

using Apache.Ignite;
using Apache.Ignite.Tests;
using Caching.Ignite;
using Microsoft.Extensions.Caching.Distributed;

/// <summary>
/// Tests for <see cref="IgniteDistributedCache"/>.
/// </summary>
public class IgniteDistributedCacheTests : IgniteTestsBase
{
    private IgniteClientGroup _clientGroup = null!;

    [OneTimeSetUp]
    public void InitClientGroup() =>
        _clientGroup = new IgniteClientGroup(new IgniteClientGroupConfiguration { ClientConfiguration = GetConfig() });

    [OneTimeTearDown]
    public void StopClientGroup() =>
        _clientGroup.Dispose();

    [Test]
    public void TestBasicCaching()
    {
        const string key = "TestBasicCaching";
        byte[] value = [1, 2, 3];

        var cache = new IgniteDistributedCache(new(), _clientGroup);

        cache.Set(key, value, new());
        var resValue = cache.Get(key);

        CollectionAssert.AreEqual(value, resValue);
    }
}
