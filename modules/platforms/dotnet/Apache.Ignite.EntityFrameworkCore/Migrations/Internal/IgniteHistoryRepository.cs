
namespace Apache.Ignite.EntityFrameworkCore.Migrations.Internal;

using System;
using Common;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Storage;

public class IgniteHistoryRepository : HistoryRepository
{
    public IgniteHistoryRepository(HistoryRepositoryDependencies dependencies)
        : base(dependencies)
    {
    }

    protected override string ExistsSql
    {
        get
        {
            var stringTypeMapping = Dependencies.TypeMappingSource.GetMapping(typeof(string));

            return
                $"""
                 SELECT COUNT(*) FROM "IGNITE_MASTER_TODO" 
                                 WHERE "name" = {stringTypeMapping.GenerateSqlLiteral(TableName)} 
                                   AND "type" = 'table';
                 """;
        }
    }

    protected override bool InterpretExistsResult(object? value)
        => (long)value! != 0L;

    public override string GetCreateIfNotExistsScript()
    {
        var script = GetCreateScript();

        const string createTable = "CREATE TABLE";
        return script.Insert(script.IndexOf(createTable, StringComparison.Ordinal) + createTable.Length, " IF NOT EXISTS");
    }

    public override string GetBeginIfNotExistsScript(string migrationId)
        => throw new NotSupportedException(IgniteStrings.MigrationScriptGenerationNotSupported);

    public override string GetBeginIfExistsScript(string migrationId)
        => throw new NotSupportedException(IgniteStrings.MigrationScriptGenerationNotSupported);

    public override string GetEndIfScript()
        => throw new NotSupportedException(IgniteStrings.MigrationScriptGenerationNotSupported);
}
