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
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Cache for job load contexts.
/// <para />
/// Every job load context is associated with a set of deployment unit paths. For example, [unit1, unit2] and [unit1, unit3] sets
/// represent a different set of assemblies, so they will have different job load contexts. Unit order matters too.
/// <para />
/// Loading assemblies and resolving types can take from ~100us to several seconds, depending on the number of assemblies,
/// type initializers (static constructors), and other factors.
/// <para />
/// If a deployment unit U is undeployed, all job load contexts with U in their set will be removed from the cache immediately.
/// </summary>
internal sealed class JobLoadContextCache : IDisposable
{
    private static readonly long TicksPerMs = Stopwatch.Frequency / 1000L;

    private readonly long _ttlTicks;

    /** Main cache of job load contexts. */
    private readonly Dictionary<DeploymentUnitPaths, (JobLoadContext Ctx, long Ts)> _jobLoadContextCache = new();

    /** Additional map to quickly find all job load contexts that use a given deployment unit path. */
    private readonly Dictionary<string, List<DeploymentUnitPaths>> _deploymentUnitSets = new();

    private readonly SemaphoreSlim _cacheLock = new(1);

    private readonly CancellationTokenSource _cts = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="JobLoadContextCache"/> class.
    /// </summary>
    /// <param name="ttlMs">Cache entry time-to-live in milliseconds.</param>
    /// <param name="cacheCleanupIntervalMs">Cache cleanup interval.</param>
    internal JobLoadContextCache(int ttlMs = 30_000, int cacheCleanupIntervalMs = 5_000)
    {
        _ttlTicks = ttlMs * TicksPerMs;

        _ = StartCacheCleanupAsync(cacheCleanupIntervalMs);
    }

    /// <summary>
    /// Gets or adds a job load context for the specified deployment unit paths.
    /// </summary>
    /// <param name="paths">Deployment unit paths.</param>
    /// <returns>Job load context.</returns>
    public async ValueTask<JobLoadContext> GetOrAddJobLoadContext(DeploymentUnitPaths paths)
    {
        await _cacheLock.WaitAsync().ConfigureAwait(false);

        try
        {
            return GetOrAddImpl();
        }
        finally
        {
            _cacheLock.Release();
        }

        JobLoadContext GetOrAddImpl()
        {
            ref (JobLoadContext Ctx, long Ts) valRef = ref CollectionsMarshal.GetValueRefOrAddDefault(_jobLoadContextCache, paths, out var exists);

            if (!exists)
            {
                valRef.Ctx = DeploymentUnitLoader.GetJobLoadContext(paths);

                foreach (var path in paths.Paths)
                {
                    ref var listRef = ref CollectionsMarshal.GetValueRefOrAddDefault(_deploymentUnitSets, path, out _);
                    listRef ??= new List<DeploymentUnitPaths>();

                    listRef.Add(paths);
                }
            }

            valRef.Ts = NowTicks();

            return valRef.Ctx;
        }
    }

    /// <summary>
    /// Un-deploys the specified deployment unit paths and cleans up associated job load contexts.
    /// </summary>
    /// <param name="deploymentUnitPaths">Deployment unit paths to undeploy.</param>
    /// <returns><see cref="Task"/> representing the asynchronous operation.</returns>
    public async ValueTask<bool> UndeployUnits(ICollection<string> deploymentUnitPaths)
    {
        await _cacheLock.WaitAsync().ConfigureAwait(false);

        try
        {
            bool res = false;

            foreach (var deploymentUnitPath in deploymentUnitPaths)
            {
                if (!_deploymentUnitSets.TryGetValue(deploymentUnitPath, out var unitSet))
                {
                    continue;
                }

                foreach (DeploymentUnitPaths paths in unitSet)
                {
                    if (_jobLoadContextCache.TryGetValue(paths, out var cachedJobCtx))
                    {
                        cachedJobCtx.Ctx.Dispose();
                        _jobLoadContextCache.Remove(paths);
                    }
                }

                _deploymentUnitSets.Remove(deploymentUnitPath);
                res = true;
            }

            return res;
        }
        finally
        {
            _cacheLock.Release();
        }
    }

    /// <inheritdoc/>
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

            _jobLoadContextCache.Clear();
            _deploymentUnitSets.Clear();
        }
        finally
        {
            _cacheLock.Release();
            _cacheLock.Dispose();
            _cts.Dispose();
        }
    }

    private static long NowTicks() => Stopwatch.GetTimestamp();

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
        List<KeyValuePair<DeploymentUnitPaths, (JobLoadContext Ctx, long Ts)>>? toRemove = null;
        long nowTicks = NowTicks();

        foreach (KeyValuePair<DeploymentUnitPaths, (JobLoadContext Ctx, long Ts)> cachedJobCtx in _jobLoadContextCache)
        {
            if (cachedJobCtx.Value.Ts + _ttlTicks < nowTicks)
            {
                toRemove ??= new();
                toRemove.Add(cachedJobCtx);
            }
        }

        if (toRemove is null)
        {
            return;
        }

        foreach (KeyValuePair<DeploymentUnitPaths, (JobLoadContext Ctx, long Ts)> cachedJobCtx in toRemove)
        {
            DeploymentUnitPaths paths = cachedJobCtx.Key;
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
