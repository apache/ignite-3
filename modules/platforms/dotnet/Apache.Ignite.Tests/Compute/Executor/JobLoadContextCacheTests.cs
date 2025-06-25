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
    public async Task TestUseAfterDisposeThrows()
    {
        var cache = new JobLoadContextCache();
        var paths = new DeploymentUnitPaths(["a", "b"]);

        await cache.GetOrAddJobLoadContext(paths);
        cache.Dispose();

        Assert.ThrowsAsync<ObjectDisposedException>(async () => await cache.GetOrAddJobLoadContext(paths));
    }
}
