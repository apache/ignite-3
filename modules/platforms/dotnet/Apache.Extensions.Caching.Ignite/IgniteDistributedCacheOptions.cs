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
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

/// <summary>
/// Configuration options for <see cref="IgniteDistributedCache"/>.
/// </summary>
public sealed record IgniteDistributedCacheOptions : IOptions<IgniteDistributedCacheOptions>
{
    /// <summary>
    /// Gets or sets the table name to use for the cache.
    /// <para />
    /// The table will be created automatically. When using an existing table, make sure it has VARCHAR key and VARBINARY value columns,
    /// named as specified in <see cref="KeyColumnName"/> and <see cref="ValueColumnName"/> properties.
    /// </summary>
    public string TableName { get; set; } = "IGNITE_DOTNET_DISTRIBUTED_CACHE";

    /// <summary>
    /// Gets or sets the name of the key column. Column type should be VARCHAR.
    /// </summary>
    public string KeyColumnName { get; set; } = "KEY";

    /// <summary>
    /// Gets or sets the name of the value column. Column type should be VARBINARY.
    /// </summary>
    public string ValueColumnName { get; set; } = "VAL";

    /// <summary>
    /// Gets or sets the name of the expiration column. Column type should be BIGINT.
    /// </summary>
    public string ExpirationColumnName { get; set; } = "EXPIRATION";

    /// <summary>
    /// Gets or sets the name of the sliding expiration column. Column type should be BIGINT.
    /// </summary>
    public string SlidingExpirationColumnName { get; set; } = "SLIDING_EXPIRATION";

    /// <summary>
    /// Gets or sets optional cache key prefix. Allows to use the same table for multiple caches.
    /// </summary>
    public string? CacheKeyPrefix { get; set; }

    /// <summary>
    /// Gets or sets the service key (<see cref="ServiceDescriptor.ServiceKey"/>) to retrieve <see cref="IgniteClientGroup"/>
    /// from the service provider.
    /// </summary>
    public object? IgniteClientGroupServiceKey { get; set; }

    /// <summary>
    /// Gets or sets the interval for expired items cleanup.
    /// <para />
    /// Set to <see cref="Timeout.InfiniteTimeSpan"/> to disable automatic cleanup.
    /// <para />
    /// Default is <see cref="Timeout.InfiniteTimeSpan"/> (disabled).
    /// </summary>
    public TimeSpan ExpiredItemsCleanupInterval { get; set; } = Timeout.InfiniteTimeSpan;

    /// <inheritdoc/>
    IgniteDistributedCacheOptions IOptions<IgniteDistributedCacheOptions>.Value => this;
}
