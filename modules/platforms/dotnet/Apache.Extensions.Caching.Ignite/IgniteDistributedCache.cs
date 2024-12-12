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
public sealed class IgniteDistributedCache : IDistributedCache
{
    private readonly IgniteClientGroup _igniteClientGroup;

    private readonly IgniteDistributedCacheOptions _options;

    private readonly ObjectPool<IgniteTuple> _tuplePool = new DefaultObjectPool<IgniteTuple>(
        new IgniteTuplePooledObjectPolicy(),
        maximumRetained: Environment.ProcessorCount * 10);

    /// <summary>
    /// Initializes a new instance of the <see cref="IgniteDistributedCache"/> class.
    /// </summary>
    /// <param name="optionsAccessor">Options.</param>
    /// <param name="serviceProvider">Service provider.</param>
    public IgniteDistributedCache(
        IOptions<IgniteDistributedCacheOptions> optionsAccessor,
        IServiceProvider serviceProvider)
    {
        ArgumentNullException.ThrowIfNull(optionsAccessor);
        ArgumentNullException.ThrowIfNull(serviceProvider);

        _options = optionsAccessor.Value;
        _igniteClientGroup = serviceProvider.GetRequiredKeyedService<IgniteClientGroup>(_options.IgniteClientGroupServiceKey);
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
    }

    /// <inheritdoc/>
    public byte[]? Get(string key) =>
        GetAsync(key, CancellationToken.None).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public async Task<byte[]?> GetAsync(string key, CancellationToken token)
    {
        var view = await GetViewAsync().ConfigureAwait(false);
        var tuple = GetKey(key);

        try
        {
            var (val, hasVal) = await view.GetAsync(null, tuple).ConfigureAwait(false);

            return hasVal ? (byte[]?)val[_options.ValueColumnName] : null;
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
        // TODO: Expiration is not supported in Ignite - throw when specified.
        var view = await GetViewAsync().ConfigureAwait(false);

        var tuple = GetKeyVal(key, value);

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
        // No-op.
        // Expiration is not supported in Ignite.
    }

    /// <inheritdoc/>
    public Task RefreshAsync(string key, CancellationToken token)
    {
        // No-op.
        // Expiration is not supported in Ignite.
        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public void Remove(string key) =>
        RemoveAsync(key, CancellationToken.None).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public async Task RemoveAsync(string key, CancellationToken token)
    {
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

    private IgniteTuple GetKey(string key)
    {
        var tuple = _tuplePool.Get();
        tuple[_options.KeyColumnName] = key;

        return tuple;
    }

    private IgniteTuple GetKeyVal(string key, byte[] val)
    {
        var tuple = GetKey(key);
        tuple[_options.ValueColumnName] = val;

        return tuple;
    }

    private async Task<IRecordView<IIgniteTuple>> GetViewAsync()
    {
        // TODO: Cache created table.
        IIgnite ignite = await _igniteClientGroup.GetIgniteAsync().ConfigureAwait(false);

        var tableName = _options.TableName;

        await ignite.Sql
            .ExecuteAsync(
                transaction: null,
                "CREATE TABLE IF NOT EXISTS ? (? VARCHAR PRIMARY KEY, ? BLOB)",
                tableName,
                _options.KeyColumnName,
                _options.ValueColumnName)
            .ConfigureAwait(false);

        ITable? table = await ignite.Tables.GetTableAsync(tableName).ConfigureAwait(false);

        if (table == null)
        {
            throw new InvalidOperationException("Table not found: " + tableName);
        }

        return table.RecordBinaryView;
    }
}
