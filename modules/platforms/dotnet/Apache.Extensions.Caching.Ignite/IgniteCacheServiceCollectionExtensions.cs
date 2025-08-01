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
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Extension methods for setting up Redis distributed cache related services in an <see cref="IServiceCollection" />.
/// </summary>
public static class IgniteCacheServiceCollectionExtensions
{
    /// <summary>
    /// Adds Ignite distributed caching services to the specified <see cref="IServiceCollection" />.
    /// <para />
    /// Depends on <see cref="IgniteClientGroup"/>, make sure to add it as well with
    /// <see cref="IgniteServiceCollectionExtensions.AddIgniteClientGroup(IServiceCollection,IgniteClientGroupConfiguration,ServiceLifetime)"/>.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection" /> to add services to.</param>
    /// <param name="setupAction">An <see cref="Action{IgniteDistributedCacheOptions}"/> to configure the provided
    /// <see cref="IgniteDistributedCacheOptions"/>.</param>
    /// <returns>The <see cref="IServiceCollection"/> so that additional calls can be chained.</returns>
    public static IServiceCollection AddIgniteDistributedCache(
        this IServiceCollection services,
        Action<IgniteDistributedCacheOptions> setupAction)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(setupAction);

        services.AddOptions();

        services.Configure(setupAction);
        services.Add(ServiceDescriptor.Singleton<IDistributedCache, IgniteDistributedCache>());

        return services;
    }
}
