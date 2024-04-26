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

namespace Apache.Ignite.EntityFrameworkCore.Extensions;

using System;
using System.Data.Common;
using Apache.Ignite.EntityFrameworkCore.Infrastructure;
using Apache.Ignite.EntityFrameworkCore.Infrastructure.Internal;
using Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

public static class IgniteDbContextOptionsBuilderExtensions
{
    public static DbContextOptionsBuilder UseIgnite(
        this DbContextOptionsBuilder optionsBuilder,
        Action<IgniteDbContextOptionsBuilder>? igniteOptionsAction = null)
    {
        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(GetOrCreateExtension(optionsBuilder));

        ConfigureWarnings(optionsBuilder);

        igniteOptionsAction?.Invoke(new IgniteDbContextOptionsBuilder(optionsBuilder));

        return optionsBuilder;
    }

    public static DbContextOptionsBuilder UseIgnite(
        this DbContextOptionsBuilder optionsBuilder,
        string? connectionString,
        Action<IgniteDbContextOptionsBuilder>? igniteOptionsAction = null)
    {
        var extension = (IgniteOptionsExtension)GetOrCreateExtension(optionsBuilder).WithConnectionString(connectionString);
        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);

        ConfigureWarnings(optionsBuilder);

        igniteOptionsAction?.Invoke(new IgniteDbContextOptionsBuilder(optionsBuilder));

        return optionsBuilder;
    }

    public static DbContextOptionsBuilder UseIgnite(
        this DbContextOptionsBuilder optionsBuilder,
        DbConnection connection,
        Action<IgniteDbContextOptionsBuilder>? igniteOptionsAction = null)
        => UseIgnite(optionsBuilder, connection, false, igniteOptionsAction);

    public static DbContextOptionsBuilder UseIgnite(
        this DbContextOptionsBuilder optionsBuilder,
        DbConnection connection,
        bool contextOwnsConnection,
        Action<IgniteDbContextOptionsBuilder>? igniteOptionsAction = null)
    {
        Check.NotNull(connection, nameof(connection));

        var extension = (IgniteOptionsExtension)GetOrCreateExtension(optionsBuilder).WithConnection(connection, contextOwnsConnection);
        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);

        ConfigureWarnings(optionsBuilder);

        igniteOptionsAction?.Invoke(new IgniteDbContextOptionsBuilder(optionsBuilder));

        return optionsBuilder;
    }

    public static DbContextOptionsBuilder<TContext> UseIgnite<TContext>(
        this DbContextOptionsBuilder<TContext> optionsBuilder,
        Action<IgniteDbContextOptionsBuilder>? igniteOptionsAction = null)
        where TContext : DbContext
        => (DbContextOptionsBuilder<TContext>)UseIgnite(
            (DbContextOptionsBuilder)optionsBuilder, igniteOptionsAction);

    public static DbContextOptionsBuilder<TContext> UseIgnite<TContext>(
        this DbContextOptionsBuilder<TContext> optionsBuilder,
        string? connectionString,
        Action<IgniteDbContextOptionsBuilder>? igniteOptionsAction = null)
        where TContext : DbContext
        => (DbContextOptionsBuilder<TContext>)UseIgnite(
            (DbContextOptionsBuilder)optionsBuilder, connectionString, igniteOptionsAction);

    public static DbContextOptionsBuilder<TContext> UseIgnite<TContext>(
        this DbContextOptionsBuilder<TContext> optionsBuilder,
        DbConnection connection,
        Action<IgniteDbContextOptionsBuilder>? igniteOptionsAction = null)
        where TContext : DbContext
        => (DbContextOptionsBuilder<TContext>)UseIgnite(
            (DbContextOptionsBuilder)optionsBuilder, connection, igniteOptionsAction);

    public static DbContextOptionsBuilder<TContext> UseIgnite<TContext>(
        this DbContextOptionsBuilder<TContext> optionsBuilder,
        DbConnection connection,
        bool contextOwnsConnection,
        Action<IgniteDbContextOptionsBuilder>? igniteOptionsAction = null)
        where TContext : DbContext
        => (DbContextOptionsBuilder<TContext>)UseIgnite(
            (DbContextOptionsBuilder)optionsBuilder, connection, contextOwnsConnection, igniteOptionsAction);

    private static IgniteOptionsExtension GetOrCreateExtension(DbContextOptionsBuilder options)
        => options.Options.FindExtension<IgniteOptionsExtension>()
            ?? new IgniteOptionsExtension();

    private static void ConfigureWarnings(DbContextOptionsBuilder optionsBuilder)
    {
        var coreOptionsExtension
            = optionsBuilder.Options.FindExtension<CoreOptionsExtension>()
            ?? new CoreOptionsExtension();

        coreOptionsExtension = RelationalOptionsExtension.WithDefaultWarningConfiguration(coreOptionsExtension);

        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(coreOptionsExtension);
    }
}
