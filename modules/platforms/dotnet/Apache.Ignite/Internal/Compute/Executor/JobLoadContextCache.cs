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

namespace Apache.Ignite.Internal.Compute.Executor;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;

internal sealed class JobLoadContextCache : IDisposable
{
    private const int CacheTtlMs = 10_000;

    private readonly Dictionary<DeploymentUnitPaths, (JobLoadContext Ctx, long Ts)> _jobLoadContextCache = new();

    private readonly Dictionary<string, List<DeploymentUnitPaths>> _deploymentUnitSets = new();

    private readonly SemaphoreSlim _cacheLock = new(1);

    private readonly CancellationTokenSource _cts = new();

    internal JobLoadContextCache(int cacheCleanupIntervalMs = 5_000)
    {
        _ = StartCacheCleanupAsync(cacheCleanupIntervalMs);
    }

    public void Dispose()
    {
        _cts.Cancel();

        _cacheLock.Wait();

        try
        {
            foreach (var cachedJobCtx in _jobLoadContextCache)
            {
                cachedJobCtx.Value.Ctx.Dispose();
            }
        }
        finally
        {
            _cacheLock.Release();
            _cacheLock.Dispose();
            _cts.Dispose();
        }
    }

    private static long Now() => Stopwatch.GetTimestamp();

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Thread root.")]
    private async Task StartCacheCleanupAsync(int cacheCleanupIntervalMs)
    {
        var ct = _cts.Token;

        while (!ct.IsCancellationRequested)
        {
            await Task.Delay(cacheCleanupIntervalMs, ct).ConfigureAwait(false);

            await _cacheLock.WaitAsync(ct).ConfigureAwait(false);

            try
            {
                CleanUpExpiredJobContexts();
            }
            catch (Exception)
            {
                // Ignore.
            }
            finally
            {
                _cacheLock.Release();
            }
        }
    }

    private void CleanUpExpiredJobContexts()
    {
        List<KeyValuePair<DeploymentUnitPaths, (JobLoadContext Ctx, long Ts)>> toRemove = new();

        foreach (var cachedJobCtx in _jobLoadContextCache)
        {
            if (cachedJobCtx.Value.Ts + CacheTtlMs < Now())
            {
                toRemove.Add(cachedJobCtx);
            }
        }

        foreach (var cachedJobCtx in toRemove)
        {
            var paths = cachedJobCtx.Key;
            _jobLoadContextCache.Remove(paths);
            cachedJobCtx.Value.Ctx.Dispose();

            foreach (var unitPath in paths.Paths)
            {
                if (_deploymentUnitSets.TryGetValue(unitPath, out var unitSet))
                {
                    unitSet.Remove(paths);

                    if (unitSet.Count == 0)
                    {
                        _deploymentUnitSets.Remove(unitPath);
                    }
                }
            }
        }
    }
}
