// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

namespace Apache.Ignite;

using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

/// <summary>
/// Extension methods for setting up Apache Ignite services
/// in an <see cref="Microsoft.Extensions.DependencyInjection.IServiceCollection" />.
/// </summary>
public static class IgniteServiceCollectionExtensions
{
    /// <summary>
    /// Registers an <see cref="IgniteClientGroup" />.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection" /> to add services to.</param>
    /// <param name="configuration">
    /// <see cref="IgniteClientGroupConfiguration" /> instance.
    /// </param>
    /// <param name="clientGroupLifetime">
    /// The lifetime with which to register the <see cref="IgniteClientGroup" /> service in the container.
    /// Defaults to <see cref="ServiceLifetime.Singleton" />.
    /// </param>
    /// <returns>Original service collection to chain multiple calls.</returns>
    public static IServiceCollection AddIgniteClientGroup(
        this IServiceCollection services,
        IgniteClientGroupConfiguration configuration,
        ServiceLifetime clientGroupLifetime = ServiceLifetime.Singleton) =>
        AddIgniteClientGroupCore(services, (_, _) => configuration, clientGroupLifetime);

    /// <summary>
    /// Registers an <see cref="IgniteClientGroup" />.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection" /> to add services to.</param>
    /// <param name="configure">
    /// Function to build the <see cref="IgniteClientGroupConfiguration" />.
    /// </param>
    /// <param name="clientGroupLifetime">
    /// The lifetime with which to register the <see cref="IgniteClientGroup" /> service in the container.
    /// Defaults to <see cref="ServiceLifetime.Singleton" />.
    /// </param>
    /// <returns>Original service collection to chain multiple calls.</returns>
    public static IServiceCollection AddIgniteClientGroup(
        this IServiceCollection services,
        Func<IServiceProvider, IgniteClientGroupConfiguration> configure,
        ServiceLifetime clientGroupLifetime = ServiceLifetime.Singleton) =>
        AddIgniteClientGroupCore(services, (sp, _) => configure(sp), clientGroupLifetime);

    /// <summary>
    /// Registers an <see cref="IgniteClientGroup" />.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection" /> to add services to.</param>
    /// <param name="serviceKey">
    /// The <see cref="ServiceDescriptor.ServiceKey"/> of the client group.
    /// </param>
    /// <param name="configuration">
    /// <see cref="IgniteClientGroupConfiguration" /> instance.
    /// </param>
    /// <param name="clientGroupLifetime">
    /// The lifetime with which to register the <see cref="IgniteClientGroup" /> service in the container.
    /// Defaults to <see cref="ServiceLifetime.Singleton" />.
    /// </param>
    /// <returns>Original service collection to chain multiple calls.</returns>
    public static IServiceCollection AddIgniteClientGroupKeyed(
        this IServiceCollection services,
        object? serviceKey,
        IgniteClientGroupConfiguration configuration,
        ServiceLifetime clientGroupLifetime = ServiceLifetime.Singleton) =>
        AddIgniteClientGroupCore(services, (_, _) => configuration, clientGroupLifetime, serviceKey);

    /// <summary>
    /// Registers an <see cref="IgniteClientGroup" />.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection" /> to add services to.</param>
    /// <param name="serviceKey">
    /// The <see cref="ServiceDescriptor.ServiceKey"/> of the client group.
    /// </param>
    /// <param name="configure">
    /// Function to build the <see cref="IgniteClientGroupConfiguration" />.
    /// </param>
    /// <param name="clientGroupLifetime">
    /// The lifetime with which to register the <see cref="IgniteClientGroup" /> service in the container.
    /// Defaults to <see cref="ServiceLifetime.Singleton" />.
    /// </param>
    /// <returns>Original service collection to chain multiple calls.</returns>
    public static IServiceCollection AddIgniteClientGroupKeyed(
        this IServiceCollection services,
        object? serviceKey,
        Func<IServiceProvider, IgniteClientGroupConfiguration> configure,
        ServiceLifetime clientGroupLifetime = ServiceLifetime.Singleton) =>
        AddIgniteClientGroupCore(
            services,
            (sp, _) => configure(sp),
            clientGroupLifetime,
            serviceKey);

    /// <summary>
    /// Registers an <see cref="IgniteClientGroup" />.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection" /> to add services to.</param>
    /// <param name="serviceKey">
    /// The <see cref="ServiceDescriptor.ServiceKey"/> of the client group.
    /// </param>
    /// <param name="configure">
    /// Function to build the <see cref="IgniteClientGroupConfiguration" />.
    /// </param>
    /// <param name="clientGroupLifetime">
    /// The lifetime with which to register the <see cref="IgniteClientGroup" /> service in the container.
    /// Defaults to <see cref="ServiceLifetime.Singleton" />.
    /// </param>
    /// <returns>Original service collection to chain multiple calls.</returns>
    public static IServiceCollection AddIgniteClientGroupKeyed(
        this IServiceCollection services,
        object? serviceKey,
        Func<IServiceProvider, object?, IgniteClientGroupConfiguration> configure,
        ServiceLifetime clientGroupLifetime = ServiceLifetime.Singleton) =>
        AddIgniteClientGroupCore(services, configure, clientGroupLifetime, serviceKey);

    private static IServiceCollection AddIgniteClientGroupCore(
        IServiceCollection services,
        Func<IServiceProvider, object?, IgniteClientGroupConfiguration> configure,
        ServiceLifetime clientGroupLifetime = ServiceLifetime.Singleton,
        object? key = null)
    {
        services.TryAdd(new ServiceDescriptor(
            typeof(IgniteClientGroup),
            key,
            (serviceProvider, innerKey) =>
            {
                IgniteClientGroupConfiguration cfg = configure(serviceProvider, innerKey);

                if (cfg.ClientConfiguration.LoggerFactory == NullLoggerFactory.Instance)
                {
                    // Use DI logger factory if none was provided.
                    cfg.ClientConfiguration.LoggerFactory =
                        serviceProvider.GetService<ILoggerFactory>() ?? NullLoggerFactory.Instance;
                }

                return new IgniteClientGroup(cfg);
            },
            clientGroupLifetime));

        return services;
    }
}
