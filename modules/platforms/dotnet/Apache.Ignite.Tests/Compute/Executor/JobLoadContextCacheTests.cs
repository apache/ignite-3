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

namespace Apache.Ignite.Tests.Compute.Executor;

using System;
using System.Threading.Tasks;
using Internal.Compute.Executor;
using NUnit.Framework;

/// <summary>
/// Tests for <see cref="JobLoadContextCache"/>.
/// </summary>
public class JobLoadContextCacheTests
{
    [Test]
    public async Task TestGetOrAddJobLoadContextReturnsSameInstanceForSamePaths()
    {
        using var cache = new JobLoadContextCache();
        var paths = new DeploymentUnitPaths(["foo", "bar"]);

        var ctx1 = await cache.GetOrAddJobLoadContext(paths);
        var ctx2 = await cache.GetOrAddJobLoadContext(paths);

        Assert.AreSame(ctx1.AssemblyLoadContext, ctx2.AssemblyLoadContext);
    }

    [Test]
    public async Task TestGetOrAddJobLoadContextReturnsDifferentInstanceForDifferentPaths()
    {
        using var cache = new JobLoadContextCache();

        var paths1 = new DeploymentUnitPaths(["foo", "bar"]);
        var paths2 = new DeploymentUnitPaths(["foo", "bar", "baz"]);

        var ctx1 = await cache.GetOrAddJobLoadContext(paths1);
        var ctx2 = await cache.GetOrAddJobLoadContext(paths2);

        Assert.AreNotSame(ctx1.AssemblyLoadContext, ctx2.AssemblyLoadContext);
    }

    [Test]
    public async Task TestExpiredContextsAreCleanedUp()
    {
        using var cache = new JobLoadContextCache(ttlMs: 100, cacheCleanupIntervalMs: 50);
        var paths = new DeploymentUnitPaths(["x", "y", "z"]);

        var ctx0 = await cache.GetOrAddJobLoadContext(paths);
        var ctx1 = await cache.GetOrAddJobLoadContext(paths);

        await Task.Delay(200);
        var ctx2 = await cache.GetOrAddJobLoadContext(paths);

        Assert.AreSame(ctx0.AssemblyLoadContext, ctx1.AssemblyLoadContext);
        Assert.AreNotSame(ctx1.AssemblyLoadContext, ctx2.AssemblyLoadContext);
    }

    [Test]
    public async Task TestFrequentlyUsedContextsAreNotCleanedUp()
    {
        using var cache = new JobLoadContextCache(ttlMs: 300, cacheCleanupIntervalMs: 10);
        var paths1 = new DeploymentUnitPaths(["a", "b", "c"]);
        var paths2 = new DeploymentUnitPaths(["c", "b", "a"]);

        var ctx1 = await cache.GetOrAddJobLoadContext(paths1);
        var ctx2 = await cache.GetOrAddJobLoadContext(paths2);

        for (int i = 0; i < 33; i++)
        {
            await Task.Delay(millisecondsDelay: 10);

            var ctx10 = await cache.GetOrAddJobLoadContext(paths1);
            Assert.AreSame(ctx1.AssemblyLoadContext, ctx10.AssemblyLoadContext, "Iteration {0}", i);
        }

        // ctx2 was not used and should be cleaned up.
        var ctx20 = await cache.GetOrAddJobLoadContext(paths2);
        Assert.AreNotSame(ctx2.AssemblyLoadContext, ctx20.AssemblyLoadContext);
    }

    [Test]
    public async Task TestUseAfterDisposeThrows()
    {
        var cache = new JobLoadContextCache();
        var paths = new DeploymentUnitPaths(["a", "b"]);

        await cache.GetOrAddJobLoadContext(paths);
        cache.Dispose();

        Assert.ThrowsAsync<ObjectDisposedException>(async () => await cache.GetOrAddJobLoadContext(paths));
    }

    [Test]
    public async Task TestUndeployUnitsRemovesAllRelatedContexts()
    {
        using var cache = new JobLoadContextCache();

        var paths1 = new DeploymentUnitPaths(["unit1", "unit2"]);
        var paths2 = new DeploymentUnitPaths(["unit2", "unit3"]);
        var paths3 = new DeploymentUnitPaths(["unit3", "unit4"]);

        var ctx1 = await cache.GetOrAddJobLoadContext(paths1);
        var ctx2 = await cache.GetOrAddJobLoadContext(paths2);
        var ctx3 = await cache.GetOrAddJobLoadContext(paths3);

        await cache.UndeployUnits(["unit1", "unit2"]);

        var ctx10 = await cache.GetOrAddJobLoadContext(paths1);
        var ctx20 = await cache.GetOrAddJobLoadContext(paths2);
        var ctx30 = await cache.GetOrAddJobLoadContext(paths3);

        // ctx1 and ctx2 should be disposed, ctx3 should remain intact.
        Assert.AreNotSame(ctx1.AssemblyLoadContext, ctx10.AssemblyLoadContext);
        Assert.AreNotSame(ctx2.AssemblyLoadContext, ctx20.AssemblyLoadContext);
        Assert.AreSame(ctx3.AssemblyLoadContext, ctx30.AssemblyLoadContext);
    }
}
