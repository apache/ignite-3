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
using Apache.Ignite.Sql;
using Apache.Ignite.Table;
using Internal;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

/// <summary>
/// Ignite-based distributed cache.
/// </summary>
public sealed class IgniteDistributedCache : IDistributedCache, IDisposable
{
    [SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "Not owned, injected.")]
    private readonly IgniteClientGroup _igniteClientGroup;

    private readonly IgniteDistributedCacheOptions _options;

    private readonly CacheEntryMapper _cacheEntryMapper;

    private readonly SemaphoreSlim _initLock = new(1);

    private readonly CancellationTokenSource _cleanupCts = new();

    private readonly SqlStatement _refreshSql;

    private readonly SqlStatement _cleanupSql;

    private volatile IKeyValueView<string, CacheEntry>? _view;

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

        Validate(options);

        _options = options;
        _cacheEntryMapper = new CacheEntryMapper(options);
        _igniteClientGroup = igniteClientGroup;

        _refreshSql = $"UPDATE {_options.TableName} " +
                      $"SET {_options.ExpirationColumnName} = {_options.SlidingExpirationColumnName} + ? " +
                      $"WHERE {_options.KeyColumnName} = ? AND {_options.SlidingExpirationColumnName} IS NOT NULL";

        var expireAtCol = _options.ExpirationColumnName;
        _cleanupSql = $"DELETE FROM {_options.TableName} WHERE {expireAtCol} IS NOT NULL AND {expireAtCol} <= ?";

        if (_options.ExpiredItemsCleanupInterval != Timeout.InfiniteTimeSpan)
        {
            _ = CleanupLoopAsync();
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

        var (val, hasVal) = await view.GetAsync(null, key).ConfigureAwait(false);
        if (!hasVal)
        {
            return null;
        }

        var now = UtcNowMillis();
        if (val.ExpiresAt is { } exp)
        {
            var diff = exp - now;

            if (diff <= 0)
            {
                return null;
            }

            if (val.SlidingExpiration is { } sliding &&
                diff < sliding * _options.SlidingExpirationRefreshThreshold)
            {
                await RefreshAsync(key, token).ConfigureAwait(false);
            }
        }

        return val.Value;
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

        (long? expiresAt, long? sliding) = GetExpiration(options);
        var entry = new CacheEntry(value, expiresAt, sliding);

        IKeyValueView<string, CacheEntry> view = await GetViewAsync().ConfigureAwait(false);
        await view.PutAsync(transaction: null, key, entry).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public void Refresh(string key) =>
        RefreshAsync(key, CancellationToken.None).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public async Task RefreshAsync(string key, CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(key);

        IIgnite ignite = await _igniteClientGroup.GetIgniteAsync().ConfigureAwait(false);

        var actualKey = _options.CacheKeyPrefix + key;

        await ignite.Sql.ExecuteAsync(
            transaction: null,
            _refreshSql,
            token,
            args: [UtcNowMillis(), actualKey]).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public void Remove(string key) =>
        RemoveAsync(key, CancellationToken.None).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public async Task RemoveAsync(string key, CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(key);

        IKeyValueView<string, CacheEntry> view = await GetViewAsync().ConfigureAwait(false);
        await view.RemoveAsync(null, key).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (_cleanupCts.IsCancellationRequested)
        {
            return;
        }

        _cleanupCts.Cancel();
        _initLock.Dispose();
        _cleanupCts.Dispose();
    }

    private static void Validate(IgniteDistributedCacheOptions options)
    {
        if (options.ExpiredItemsCleanupInterval != Timeout.InfiniteTimeSpan && options.ExpiredItemsCleanupInterval <= TimeSpan.Zero)
        {
            throw new ArgumentException("ExpiredItemsCleanupInterval must be positive or Timeout.InfiniteTimeSpan.", nameof(options));
        }

        if (options.SlidingExpirationRefreshThreshold is < 0.0 or > 1.0)
        {
            throw new ArgumentException("SlidingExpirationRefreshThreshold must be between 0.0 and 1.0.", nameof(options));
        }

        if (string.IsNullOrWhiteSpace(options.TableName))
        {
            throw new ArgumentException("TableName cannot be null or whitespace.", nameof(options));
        }

        if (string.IsNullOrWhiteSpace(options.KeyColumnName))
        {
            throw new ArgumentException("KeyColumnName cannot be null or whitespace.", nameof(options));
        }

        if (string.IsNullOrWhiteSpace(options.ValueColumnName))
        {
            throw new ArgumentException("ValueColumnName cannot be null or whitespace.", nameof(options));
        }

        if (string.IsNullOrWhiteSpace(options.ExpirationColumnName))
        {
            throw new ArgumentException("ExpirationColumnName cannot be null or whitespace.", nameof(options));
        }

        if (string.IsNullOrWhiteSpace(options.SlidingExpirationColumnName))
        {
            throw new ArgumentException("SlidingExpirationColumnName cannot be null or whitespace.", nameof(options));
        }
    }

    private static long UtcNowMillis() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    private static (long? Absolute, long? Sliding) GetExpiration(DistributedCacheEntryOptions options)
    {
        long absExpAt = options.AbsoluteExpiration is { } absExp
            ? absExp.ToUnixTimeMilliseconds()
            : long.MaxValue;

        long absExpAtRel = options.AbsoluteExpirationRelativeToNow is { } absExpRel
            ? UtcNowMillis() + (long)absExpRel.TotalMilliseconds
            : long.MaxValue;

        long? sliding = options.SlidingExpiration is { } slidingExp
            ? (long)slidingExp.TotalMilliseconds
            : null;

        long absExpAtSliding = sliding is { } sliding0
            ? UtcNowMillis() + sliding0
            : long.MaxValue;

        long? expiresAt = Math.Min(Math.Min(absExpAt, absExpAtRel), absExpAtSliding) switch
        {
            var min when min != long.MaxValue => min,
            _ => null
        };

        return (expiresAt, sliding);
    }

    private async Task<IKeyValueView<string, CacheEntry>> GetViewAsync()
    {
        var view = _view;
        if (view != null)
        {
            return view;
        }

        await _initLock.WaitAsync().ConfigureAwait(false);

        try
        {
            view = _view;
            if (view != null)
            {
                return view;
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

            var table = await ignite.Tables.GetTableAsync(tableName).ConfigureAwait(false)
                        ?? throw new InvalidOperationException("Table not found: " + tableName);

            view = table.GetKeyValueView(_cacheEntryMapper);
            _view = view;

            return view;
        }
        finally
        {
            _initLock.Release();
        }
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Background loop.")]
    private async Task CleanupLoopAsync()
    {
        while (!_cleanupCts.Token.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_options.ExpiredItemsCleanupInterval, _cleanupCts.Token).ConfigureAwait(false);
                await CleanupExpiredEntriesAsync(_cleanupCts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception)
            {
                // Swallow exceptions - might be intermittent connection errors.
                // Client will log the error and retry on the next iteration.
            }
        }
    }

    private async Task CleanupExpiredEntriesAsync(CancellationToken token)
    {
        IIgnite ignite = await _igniteClientGroup.GetIgniteAsync().ConfigureAwait(false);

        await ignite.Sql.ExecuteAsync(
            transaction: null,
            _cleanupSql,
            token,
            args: UtcNowMillis()).ConfigureAwait(false);
    }
}
