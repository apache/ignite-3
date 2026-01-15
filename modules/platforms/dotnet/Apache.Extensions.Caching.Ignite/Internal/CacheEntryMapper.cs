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

namespace Apache.Extensions.Caching.Ignite.Internal;

using Apache.Ignite.Table.Mapper;

/// <summary>
/// Cache entry mapper.
/// </summary>
internal sealed class CacheEntryMapper : IMapper<KeyValuePair<string, CacheEntry>>
{
    private readonly IgniteDistributedCacheOptions _options;

    /// <summary>
    /// Initializes a new instance of the <see cref="CacheEntryMapper"/> class.
    /// </summary>
    /// <param name="options">Options.</param>
    public CacheEntryMapper(IgniteDistributedCacheOptions options) => _options = options;

    /// <inheritdoc/>
    public void Write(KeyValuePair<string, CacheEntry> obj, ref RowWriter rowWriter, IMapperSchema schema)
    {
        foreach (var column in schema.Columns)
        {
            if (column.Name == _options.KeyColumnName)
            {
                rowWriter.WriteString(obj.Key);
            }
            else if (column.Name == _options.ValueColumnName)
            {
                rowWriter.WriteBytes(obj.Value.Value);
            }
            else if (column.Name == _options.ExpirationColumnName)
            {
                rowWriter.WriteLong(obj.Value.ExpiresAt);
            }
            else if (column.Name == _options.SlidingExpirationColumnName)
            {
                rowWriter.WriteLong(obj.Value.SlidingExpiration);
            }
            else
            {
                rowWriter.Skip();
            }
        }
    }

    /// <inheritdoc/>
    public KeyValuePair<string, CacheEntry> Read(ref RowReader rowReader, IMapperSchema schema)
    {
        string? key = null;
        byte[]? value = null;
        long? expiresAt = null;
        long? slidingExpiration = null;

        foreach (var column in schema.Columns)
        {
            if (column.Name == _options.KeyColumnName)
            {
                key = rowReader.ReadString();
            }
            else if (column.Name == _options.ValueColumnName)
            {
                value = rowReader.ReadBytes();
            }
            else if (column.Name == _options.ExpirationColumnName)
            {
                expiresAt = rowReader.ReadLong();
            }
            else if (column.Name == _options.SlidingExpirationColumnName)
            {
                slidingExpiration = rowReader.ReadLong();
            }
            else
            {
                rowReader.Skip();
            }
        }

        return new KeyValuePair<string, CacheEntry>(
            key ?? throw new InvalidOperationException("Key column is missing."),
            new CacheEntry(
                value ?? throw new InvalidOperationException("Value column is missing."),
                expiresAt,
                slidingExpiration));
    }
}
