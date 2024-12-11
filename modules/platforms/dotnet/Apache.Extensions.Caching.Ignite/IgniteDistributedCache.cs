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

namespace Apache.Extensions.Cache.Ignite;

using System.Buffers;
using Apache.Ignite;
using Apache.Ignite.Internal.Table;
using Apache.Ignite.Table;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

/// <summary>
/// Ignite-based distributed cache.
/// </summary>
public sealed class IgniteDistributedCache : IBufferDistributedCache
{
    private readonly IgniteClientGroup _igniteClientGroup;

    private readonly IgniteDistributedCacheOptions _options;

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
    public byte[]? Get(string key)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public async Task<byte[]?> GetAsync(string key, CancellationToken token)
    {
        RecordView<IIgniteTuple> view = await GetViewAsync().ConfigureAwait(false);

        await view.GetInternalAsync(null, GetKey(key)).ConfigureAwait(false);

        return val;
    }

    /// <inheritdoc/>
    public void Set(string key, byte[] value, DistributedCacheEntryOptions options)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public async Task SetAsync(string key, byte[] value, DistributedCacheEntryOptions options, CancellationToken token)
    {
        // TODO: Expiration is not supported in Ignite - throw when specified.
        RecordView<IIgniteTuple> kvView = await GetViewAsync().ConfigureAwait(false);

        await kvView.PutAsync(null, key, value).ConfigureAwait(false);
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
    public void Remove(string key)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public async Task RemoveAsync(string key, CancellationToken token)
    {
        RecordView<IIgniteTuple> kvView = await GetViewAsync().ConfigureAwait(false);

        await kvView.RemoveAsync(null, key).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public bool TryGet(string key, IBufferWriter<byte> destination)
    {
        // TODO: An internal API to copy bytes from BinaryTuple? Or a public one?
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public ValueTask<bool> TryGetAsync(string key, IBufferWriter<byte> destination, CancellationToken token)
    {
        // TODO: An internal API to copy bytes from BinaryTuple? Or a public one?
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public void Set(string key, ReadOnlySequence<byte> value, DistributedCacheEntryOptions options)
    {
        // TODO: An internal API to copy bytes efficiently? Or a public one?
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public ValueTask SetAsync(
        string key,
        ReadOnlySequence<byte> value,
        DistributedCacheEntryOptions options,
        CancellationToken token)
    {
        // TODO: An internal API to copy bytes efficiently? Or a public one?
        throw new NotImplementedException();
    }

    private static IgniteTuple GetKey(string key)
    {
        // TODO: Object pooling to avoid allocations? Or custom implementation? Or thread local?
        return new IgniteTuple(1) { ["KEY"] = key };
    }

    private async Task<RecordView<IIgniteTuple>> GetViewAsync()
    {
        // TODO: Cache created table and record view, synchronize.
        IIgnite ignite = await _igniteClientGroup.GetIgniteAsync().ConfigureAwait(false);

        var tableName = _options.TableName;

        await ignite.Sql
            .ExecuteAsync(null, "CREATE TABLE IF NOT EXISTS ? (KEY VARCHAR PRIMARY KEY, VAL BLOB)", tableName)
            .ConfigureAwait(false);

        ITable? table = await ignite.Tables.GetTableAsync(tableName).ConfigureAwait(false);

        if (table == null)
        {
            throw new InvalidOperationException("Table not found: " + tableName);
        }

        return (RecordView<IIgniteTuple>)table.RecordBinaryView;
    }
}
