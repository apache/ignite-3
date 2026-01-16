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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Extensions.Caching.Ignite;
using DistributedCacheEntryOptions = Microsoft.Extensions.Caching.Distributed.DistributedCacheEntryOptions;

/// <summary>
/// Benchmarks for <see cref="IgniteDistributedCache"/>.
/// <para />
/// Results on i9-12900H, .NET SDK 8.0.15, Ubuntu 22.04:
/// | Method  | Mean        | Error     | StdDev     | Median      | Allocated |
/// |-------- |------------:|----------:|-----------:|------------:|----------:|
/// | Get     |    47.55 us |  1.251 us |   3.548 us |    47.04 us |   3.91 KB |
/// | Set     |   102.55 us |  2.605 us |   7.391 us |   100.93 us |    2.8 KB |
/// | Refresh | 1,316.49 us | 42.580 us | 121.484 us | 1,254.74 us |   2.38 KB |.
/// </summary>
[MemoryDiagnoser]
public class IgniteDistributedCacheBenchmarks : ServerBenchmarkBase
{
    private const string Key = "key1";

    private const string KeySliding = "keySliding";

    private static readonly byte[] Val = Enumerable.Range(1, 1000).Select(x => (byte)x).ToArray();

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

        await _cache.SetAsync(Key, Val, new DistributedCacheEntryOptions(), CancellationToken.None);

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
        await _cache.SetAsync(Key, Val, new DistributedCacheEntryOptions(), CancellationToken.None);

    [Benchmark]
    public async Task Refresh() =>
        await _cache.RefreshAsync(KeySliding, CancellationToken.None);
}
