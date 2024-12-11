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
using Apache.Ignite.Table;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Logging;
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
    /// <param name="igniteClientGroup">Ignite client group.</param>
    public IgniteDistributedCache(
        IOptions<IgniteDistributedCacheOptions> optionsAccessor,
        IgniteClientGroup igniteClientGroup)
    {
        ArgumentNullException.ThrowIfNull(igniteClientGroup);

        _options = optionsAccessor.Value;
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
        var kvView = await GetKvViewAsync().ConfigureAwait(false);

        (byte[]? val, bool _) = await kvView.GetAsync(null, key).ConfigureAwait(false);

        return val;
    }

    /// <inheritdoc/>
    public void Set(string key, byte[] value, DistributedCacheEntryOptions options)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public Task SetAsync(string key, byte[] value, DistributedCacheEntryOptions options, CancellationToken token)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public void Refresh(string key)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public Task RefreshAsync(string key, CancellationToken token)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public void Remove(string key)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public Task RemoveAsync(string key, CancellationToken token)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public bool TryGet(string key, IBufferWriter<byte> destination)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public ValueTask<bool> TryGetAsync(string key, IBufferWriter<byte> destination, CancellationToken token)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public void Set(string key, ReadOnlySequence<byte> value, DistributedCacheEntryOptions options)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public ValueTask SetAsync(
        string key,
        ReadOnlySequence<byte> value,
        DistributedCacheEntryOptions options,
        CancellationToken token)
    {
        throw new NotImplementedException();
    }

    private async Task<IKeyValueView<string, byte[]?>> GetKvViewAsync()
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

        return table.GetKeyValueView<string, byte[]?>();
    }
}
