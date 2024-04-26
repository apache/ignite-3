namespace Apache.Ignite.EntityFrameworkCore.Extensions;

using System;
using Apache.Ignite.EntityFrameworkCore.Diagnostics.Internal;
using Apache.Ignite.EntityFrameworkCore.Infrastructure;
using Apache.Ignite.EntityFrameworkCore.Infrastructure.Internal;
using Apache.Ignite.EntityFrameworkCore.Migrations;
using Apache.Ignite.EntityFrameworkCore.Migrations.Internal;
using Apache.Ignite.EntityFrameworkCore.Query.Internal;
using Apache.Ignite.EntityFrameworkCore.Storage.Internal;
using Apache.Ignite.EntityFrameworkCore.Update.Internal;
using Metadata.Conventions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;
using Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Ignite-specific extension methods for <see cref="IServiceCollection" />.
/// </summary>
public static class IgniteServiceCollectionExtensions
{
    public static IServiceCollection AddIgnite<TContext>(
        this IServiceCollection serviceCollection,
        string? connectionString,
        Action<IgniteDbContextOptionsBuilder>? igniteOptionsAction = null,
        Action<DbContextOptionsBuilder>? optionsAction = null)
        where TContext : DbContext
        => serviceCollection.AddDbContext<TContext>(
            (_, options) =>
            {
                optionsAction?.Invoke(options);
                options.UseIgnite(connectionString, igniteOptionsAction);
            });

    public static IServiceCollection AddEntityFrameworkIgnite(this IServiceCollection serviceCollection)
    {
        var builder = new EntityFrameworkRelationalServicesBuilder(serviceCollection)
            .TryAdd<IParameterNameGeneratorFactory, IgniteParameterNameGeneratorFactory>()
            .TryAdd<IRelationalCommandBuilderFactory, IgniteRelationalCommandBuilderFactory>()
            .TryAdd<LoggingDefinitions, IgniteLoggingDefinitions>()
            .TryAdd<IDatabaseProvider, DatabaseProvider<IgniteOptionsExtension>>()
            .TryAdd<IRelationalTypeMappingSource, IgniteTypeMappingSource>()
            .TryAdd<ISqlGenerationHelper, IgniteSqlGenerationHelper>()
            .TryAdd<IModelValidator, IgniteModelValidator>()
            .TryAdd<IProviderConventionSetBuilder, IgniteConventionSetBuilder>()
            .TryAdd<IModificationCommandBatchFactory, IgniteModificationCommandBatchFactory>()
            .TryAdd<IRelationalConnection>(p => p.GetRequiredService<IIgniteRelationalConnection>())
            .TryAdd<IMigrationsSqlGenerator, IgniteMigrationsSqlGenerator>()
            .TryAdd<IRelationalDatabaseCreator, IgniteDatabaseCreator>()
            .TryAdd<IHistoryRepository, IgniteHistoryRepository>()
            .TryAdd<IRelationalQueryStringFactory, IgniteQueryStringFactory>()
            .TryAdd<IMethodCallTranslatorProvider, IgniteMethodCallTranslatorProvider>()
            .TryAdd<IMemberTranslatorProvider, IgniteMemberTranslatorProvider>()
            .TryAdd<IQuerySqlGeneratorFactory, IgniteQuerySqlGeneratorFactory>()
            .TryAdd<IRelationalSqlTranslatingExpressionVisitorFactory, IgniteSqlTranslatingExpressionVisitorFactory>()
            .TryAdd<IQueryTranslationPostprocessorFactory, IgniteQueryTranslationPostprocessorFactory>()
            .TryAdd<IUpdateSqlGenerator, IgniteUpdateSqlGenerator>()
            .TryAdd<ISqlExpressionFactory, IgniteSqlExpressionFactory>()
            .TryAddProviderSpecificServices(
                b => b.TryAddScoped<IIgniteRelationalConnection, IgniteRelationalConnection>());

        builder.TryAddCoreServices();

        return serviceCollection;
    }
}
