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
using Microsoft.Extensions.Caching.Distributed;

/// <summary>
/// Ignite-based distributed cache.
/// </summary>
public sealed class IgniteDistributedCache : IBufferDistributedCache
{
    private readonly IgniteClientGroup _igniteClientGroup;

    /// <summary>
    /// Initializes a new instance of the <see cref="IgniteDistributedCache"/> class.
    /// </summary>
    /// <param name="igniteClientGroup">Ignite client group.</param>
    public IgniteDistributedCache(IgniteClientGroup igniteClientGroup)
    {
        ArgumentNullException.ThrowIfNull(igniteClientGroup);

        _igniteClientGroup = igniteClientGroup;
    }

    /// <inheritdoc/>
    public byte[]? Get(string key)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public Task<byte[]?> GetAsync(string key, CancellationToken token)
    {
        throw new NotImplementedException();
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
}
