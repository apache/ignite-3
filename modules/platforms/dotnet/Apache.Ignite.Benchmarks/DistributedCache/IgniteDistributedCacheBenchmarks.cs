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

namespace Apache.Ignite.Benchmarks.DistributedCache;

using System;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Extensions.Caching.Ignite;
using DistributedCacheEntryOptions = Microsoft.Extensions.Caching.Distributed.DistributedCacheEntryOptions;

/// <summary>
/// Benchmarks for <see cref="IgniteDistributedCache"/>.
/// <para />
/// Results on i9-12900H, .NET SDK 8.0.15, Ubuntu 22.04:
/// | Method  | Mean        | Error     | StdDev    | Allocated |
/// |-------- |------------:|----------:|----------:|----------:|
/// | Get     |    48.35 us |  0.960 us |  1.179 us |   2.95 KB |
/// | Set     |   108.16 us |  2.133 us |  3.563 us |   2.73 KB |
/// | Refresh | 1,388.33 us | 27.221 us | 48.385 us |   2.38 KB |.
/// </summary>
[MemoryDiagnoser]
public class IgniteDistributedCacheBenchmarks : ServerBenchmarkBase
{
    private const string Key = "key1";

    private const string KeySliding = "keySliding";

    private static readonly DistributedCacheEntryOptions DefaultOpts = new();

    private static readonly byte[] Val = [1, 2, 3];

    private IgniteClientGroup _clientGroup = null!;

    private IgniteDistributedCache _cache = null!;

    public override async Task GlobalSetup()
    {
        await base.GlobalSetup();

        var groupCfg = new IgniteClientGroupConfiguration { ClientConfiguration = Client.Configuration };
        _clientGroup = new IgniteClientGroup(groupCfg);

        var cacheOptions = new IgniteDistributedCacheOptions
        {
            ExpiredItemsCleanupInterval = Timeout.InfiniteTimeSpan
        };

        _cache = new IgniteDistributedCache(cacheOptions, _clientGroup);

        await _cache.SetAsync(Key, Val, DefaultOpts, CancellationToken.None);

        var slidingOpts = new DistributedCacheEntryOptions
        {
            SlidingExpiration = TimeSpan.FromHours(1)
        };

        await _cache.SetAsync(KeySliding, Val, slidingOpts, CancellationToken.None);
    }

    public override async Task GlobalCleanup()
    {
        _cache.Dispose();
        _clientGroup.Dispose();

        await base.GlobalCleanup();
    }

    [Benchmark]
    public async Task Get() =>
        await _cache.GetAsync(Key, CancellationToken.None);

    [Benchmark]
    public async Task Set() =>
        await _cache.SetAsync(Key, Val, DefaultOpts, CancellationToken.None);

    [Benchmark]
    public async Task Refresh() =>
        await _cache.RefreshAsync(KeySliding, CancellationToken.None);
}
