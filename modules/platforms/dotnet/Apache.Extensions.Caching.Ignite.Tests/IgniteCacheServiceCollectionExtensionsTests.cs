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
using Microsoft.Extensions.DependencyInjection;

public class IgniteCacheServiceCollectionExtensionsTests
{
    [Test]
    public void TestAddIgniteDistributedCacheAddsServices()
    {
        var services = new ServiceCollection();

        services
            .AddIgniteClientGroup(new IgniteClientGroupConfiguration
            {
                ClientConfiguration = new IgniteClientConfiguration("localhost")
            })
            .AddIgniteDistributedCache(options => options.CacheKeyPrefix = "prefix")
            .AddOptions();

        ServiceProvider serviceProvider = services.BuildServiceProvider();

        IDistributedCache distributedCache = serviceProvider.GetRequiredService<IDistributedCache>();
        Assert.IsInstanceOf<IgniteDistributedCache>(distributedCache);

        IgniteDistributedCacheOptions opts = distributedCache.GetFieldValue<IgniteDistributedCacheOptions>("_options");
        Assert.AreEqual("prefix", opts.CacheKeyPrefix);
    }

    [Test]
    public void TestKeyedIgniteClientGroup()
    {
        var services = new ServiceCollection();

        var serviceKey = "ignite-key";
        services.AddIgniteClientGroupKeyed(serviceKey, new IgniteClientGroupConfiguration
        {
            ClientConfiguration = new IgniteClientConfiguration("localhost")
        });

        var serviceKey2 = "ignite-key-2";
        services.AddIgniteClientGroupKeyed(serviceKey2, new IgniteClientGroupConfiguration
        {
            ClientConfiguration = new IgniteClientConfiguration("localhost-2")
        });

        services.AddIgniteDistributedCache(opts => opts.IgniteClientGroupServiceKey = serviceKey);

        ServiceProvider serviceProvider = services.BuildServiceProvider();

        IDistributedCache distributedCache = serviceProvider.GetRequiredService<IDistributedCache>();
        Assert.IsInstanceOf<IgniteDistributedCache>(distributedCache);

        IgniteClientGroup igniteClientGroup = distributedCache.GetFieldValue<IgniteClientGroup>("_igniteClientGroup");
        Assert.AreEqual("localhost", igniteClientGroup.Configuration.ClientConfiguration.Endpoints.Single());
    }
}
