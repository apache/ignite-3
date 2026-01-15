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

namespace Apache.Extensions.Caching.Ignite;

using System.Diagnostics.CodeAnalysis;
using Apache.Ignite;
using Apache.Ignite.Table;
using Internal;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.ObjectPool;
using Microsoft.Extensions.Options;

/// <summary>
/// Ignite-based distributed cache.
/// </summary>
public sealed class IgniteDistributedCache : IDistributedCache, IDisposable
{
    [SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "Not owned, injected.")]
    private readonly IgniteClientGroup _igniteClientGroup;

    private readonly IgniteDistributedCacheOptions _options;

    private readonly ObjectPool<IgniteTuple> _tuplePool = new DefaultObjectPool<IgniteTuple>(
        new IgniteTuplePooledObjectPolicy(),
        maximumRetained: Environment.ProcessorCount * 10);

    private readonly SemaphoreSlim _initLock = new(1);

    private readonly CancellationTokenSource _cleanupCts = new();

    private readonly Task? _cleanupTask;

    private volatile ITable? _table;

    /// <summary>
    /// Initializes a new instance of the <see cref="IgniteDistributedCache"/> class.
    /// </summary>
    /// <param name="optionsAccessor">Options.</param>
    /// <param name="serviceProvider">Service provider.</param>
    public IgniteDistributedCache(
        IOptions<IgniteDistributedCacheOptions> optionsAccessor,
        IServiceProvider serviceProvider)
        : this(
            options: optionsAccessor.Value,
            igniteClientGroup: serviceProvider.GetRequiredKeyedService<IgniteClientGroup>(optionsAccessor.Value.IgniteClientGroupServiceKey))
    {
        // No-op.
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="IgniteDistributedCache"/> class.
    /// </summary>
    /// <param name="options">Options.</param>
    /// <param name="igniteClientGroup">Ignite client group.</param>
    public IgniteDistributedCache(
        IgniteDistributedCacheOptions options,
        IgniteClientGroup igniteClientGroup)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(igniteClientGroup);

        _options = options;
        _igniteClientGroup = igniteClientGroup;
        if (_options.ExpiredItemsCleanupInterval != Timeout.InfiniteTimeSpan)
        {
            _cleanupTask = Task.Run(() => CleanupLoopAsync(_cleanupCts.Token));
        }
    }

    /// <inheritdoc/>
    public byte[]? Get(string key) =>
        GetAsync(key, CancellationToken.None).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public async Task<byte[]?> GetAsync(string key, CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(key);

        var view = await GetViewAsync().ConfigureAwait(false);
        var tuple = GetKey(key);

        try
        {
            var (val, hasVal) = await view.GetAsync(null, tuple).ConfigureAwait(false);
            if (!hasVal)
            {
                return null;
            }

            // Check expiration
            var expirationCol = _options.ExpirationColumnName;
            var colIdx = val.GetOrdinal(expirationCol);
            if (colIdx != -1 && val[expirationCol] is long exp && exp > 0)
            {
                var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                if (exp <= now)
                {
                    // Optionally remove expired entry
                    await view.DeleteAsync(null, tuple).ConfigureAwait(false);
                    return null;
                }
            }

            return (byte[]?)val[_options.ValueColumnName];
        }
        finally
        {
            _tuplePool.Return(tuple);
        }
    }

    /// <inheritdoc/>
    public void Set(string key, byte[] value, DistributedCacheEntryOptions options) =>
        SetAsync(key, value, options, CancellationToken.None).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public async Task SetAsync(string key, byte[] value, DistributedCacheEntryOptions options, CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        ArgumentNullException.ThrowIfNull(options);

        IRecordView<IIgniteTuple> view = await GetViewAsync().ConfigureAwait(false);
        IgniteTuple tuple = GetKeyVal(key, value);

        try
        {
            await view.UpsertAsync(null, tuple).ConfigureAwait(false);
        }
        finally
        {
            _tuplePool.Return(tuple);
        }
    }

    /// <inheritdoc/>
    public void Refresh(string key)
    {
        ArgumentNullException.ThrowIfNull(key);

        // TODO: IGNITE-23973 Add expiration support
    }

    /// <inheritdoc/>
    public Task RefreshAsync(string key, CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(key);

        // TODO: IGNITE-23973 Add expiration support
        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public void Remove(string key) =>
        RemoveAsync(key, CancellationToken.None).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public async Task RemoveAsync(string key, CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(key);

        var view = await GetViewAsync().ConfigureAwait(false);
        var tuple = GetKey(key);

        try
        {
            await view.DeleteAsync(null, tuple).ConfigureAwait(false);
        }
        finally
        {
            _tuplePool.Return(tuple);
        }
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        _initLock.Dispose();
        _cleanupCts.Cancel();
        if (_cleanupTask != null)
        {
            try
            {
                _cleanupTask.Wait();
            }
            catch (AggregateException ex) when (ex.InnerExceptions.Count == 1 && ex.InnerException is OperationCanceledException)
            {
                // ignore expected cancellation
            }
        }

        _cleanupCts.Dispose();
    }

    private IgniteTuple GetKey(string key)
    {
        IgniteTuple tuple = _tuplePool.Get();
        tuple[_options.KeyColumnName] = _options.CacheKeyPrefix + key;

        return tuple;
    }

    private IgniteTuple GetKeyVal(string key, byte[] val)
    {
        IgniteTuple tuple = GetKey(key);
        tuple[_options.ValueColumnName] = val;

        return tuple;
    }

    private async Task<IRecordView<IIgniteTuple>> GetViewAsync()
    {
        var table = _table;
        if (table != null)
        {
            return table.RecordBinaryView;
        }

        await _initLock.WaitAsync().ConfigureAwait(false);

        try
        {
            table = _table;
            if (table != null)
            {
                return table.RecordBinaryView;
            }

            IIgnite ignite = await _igniteClientGroup.GetIgniteAsync().ConfigureAwait(false);

            var tableName = _options.TableName;

            // NOTE: We assume that table name and column names are safe to concatenate into SQL.
            var sql = $"CREATE TABLE IF NOT EXISTS {tableName} (" +
                      $"{_options.KeyColumnName} VARCHAR PRIMARY KEY, " +
                      $"{_options.ValueColumnName} VARBINARY, " +
                      $"{_options.ExpirationColumnName} BIGINT, " +
                      $"{_options.SlidingExpirationColumnName} BIGINT" +
                      ")";

            await ignite.Sql.ExecuteAsync(transaction: null, sql).ConfigureAwait(false);

            table = await ignite.Tables.GetTableAsync(tableName).ConfigureAwait(false);

            _table = table ?? throw new InvalidOperationException("Table not found: " + tableName);

            return table.RecordBinaryView;
        }
        finally
        {
            _initLock.Release();
        }
    }

    private async Task CleanupLoopAsync(CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_options.ExpiredItemsCleanupInterval, token).ConfigureAwait(false);
                await CleanupExpiredEntriesAsync().ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            // Ignore all other errors in cleanup (do not catch general Exception).
            // TODO: ???
        }
    }

    private async Task CleanupExpiredEntriesAsync()
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var expirationCol = _options.ExpirationColumnName;
        var sql = $"DELETE FROM {_options.TableName} WHERE {expirationCol} IS NOT NULL AND {expirationCol} <= ?";

        IIgnite ignite = await _igniteClientGroup.GetIgniteAsync().ConfigureAwait(false);
        await ignite.Sql.ExecuteAsync(null, sql, now).ConfigureAwait(false);
    }
}
