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

namespace Apache.Ignite.EntityFrameworkCore.Migrations;

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Storage;

public class IgniteMigrationsSqlGenerator : MigrationsSqlGenerator
{
    public IgniteMigrationsSqlGenerator(MigrationsSqlGeneratorDependencies dependencies)
        : base(dependencies)
    {
    }

    public override IReadOnlyList<MigrationCommand> Generate(
        IReadOnlyList<MigrationOperation> operations,
        IModel? model = null,
        MigrationsSqlGenerationOptions options = MigrationsSqlGenerationOptions.Default)
        => base.Generate(RewriteOperations(operations, model), model, options);

    private IReadOnlyList<MigrationOperation> RewriteOperations(
        IReadOnlyList<MigrationOperation> migrationOperations,
        IModel? model)
    {
        var operations = new List<MigrationOperation>();
        var rebuilds = new Dictionary<(string Table, string? Schema), RebuildContext>();
        foreach (var operation in migrationOperations)
        {
            switch (operation)
            {
                case AddPrimaryKeyOperation:
                case AddUniqueConstraintOperation:
                case AddCheckConstraintOperation:
                case AlterTableOperation:
                case DropCheckConstraintOperation:
                case DropForeignKeyOperation:
                case DropPrimaryKeyOperation:
                case DropUniqueConstraintOperation:
                {
                    var tableOperation = (ITableMigrationOperation)operation;
                    var rebuild = rebuilds.GetOrAddNew((tableOperation.Table, tableOperation.Schema));
                    rebuild.OperationsToReplace.Add(operation);

                    operations.Add(operation);

                    break;
                }

                case DropColumnOperation dropColumnOperation:
                {
                    var rebuild = rebuilds.GetOrAddNew((dropColumnOperation.Table, dropColumnOperation.Schema));
                    rebuild.OperationsToReplace.Add(dropColumnOperation);
                    rebuild.DropColumnsDeferred.Add(dropColumnOperation.Name);

                    operations.Add(dropColumnOperation);

                    break;
                }

                case AddForeignKeyOperation foreignKeyOperation:
                {
                    var table = operations
                        .OfType<CreateTableOperation>()
                        .FirstOrDefault(o => o.Name == foreignKeyOperation.Table);

                    if (table != null)
                    {
                        table.ForeignKeys.Add(foreignKeyOperation);
                    }
                    else
                    {
                        var rebuild = rebuilds.GetOrAddNew((foreignKeyOperation.Table, foreignKeyOperation.Schema));
                        rebuild.OperationsToReplace.Add(foreignKeyOperation);

                        operations.Add(foreignKeyOperation);
                    }

                    break;
                }

                case AlterColumnOperation alterColumnOperation:
                {
                    var rebuild = rebuilds.GetOrAddNew((alterColumnOperation.Table, alterColumnOperation.Schema));
                    rebuild.OperationsToReplace.Add(alterColumnOperation);
                    rebuild.AlterColumnsDeferred.Add(alterColumnOperation.Name, alterColumnOperation);

                    operations.Add(alterColumnOperation);

                    break;
                }

                case CreateIndexOperation createIndexOperation:
                {
                    // TODO: CREATE INDEX hangs in Ignite, skip for now.
                    // if (rebuilds.TryGetValue((createIndexOperation.Table, createIndexOperation.Schema), out var rebuild)
                    //     && (rebuild.AddColumnsDeferred.Keys.Intersect(createIndexOperation.Columns).Any()
                    //         || rebuild.RenameColumnsDeferred.Keys.Intersect(createIndexOperation.Columns).Any()))
                    // {
                    //     rebuild.OperationsToReplace.Add(createIndexOperation);
                    //     rebuild.CreateIndexesDeferred.Add(createIndexOperation.Name);
                    // }
                    //
                    // operations.Add(createIndexOperation);
                    break;
                }

                case RenameIndexOperation renameIndexOperation:
                {
                    var index = renameIndexOperation.Table != null
                        ? model?.GetRelationalModel().FindTable(renameIndexOperation.Table, renameIndexOperation.Schema)
                            ?.Indexes.FirstOrDefault(i => i.Name == renameIndexOperation.NewName)
                        : null;
                    if (index != null)
                    {
                        operations.Add(
                            new DropIndexOperation
                            {
                                Table = renameIndexOperation.Table,
                                Schema = renameIndexOperation.Schema,
                                Name = renameIndexOperation.Name
                            });

                        operations.Add(CreateIndexOperation.CreateFrom(index));
                    }
                    else
                    {
                        operations.Add(renameIndexOperation);
                    }

                    break;
                }

                case AddColumnOperation addColumnOperation:
                {
                    if (rebuilds.TryGetValue((addColumnOperation.Table, addColumnOperation.Schema), out var rebuild)
                        && rebuild.DropColumnsDeferred.Contains(addColumnOperation.Name))
                    {
                        rebuild.OperationsToReplace.Add(addColumnOperation);
                        rebuild.AddColumnsDeferred.Add(addColumnOperation.Name, addColumnOperation);
                    }
                    else if (addColumnOperation.Comment != null)
                    {
                        rebuilds.GetOrAddNew((addColumnOperation.Table, addColumnOperation.Schema));
                    }

                    operations.Add(addColumnOperation);

                    break;
                }

                case RenameColumnOperation renameColumnOperation:
                {
                    if (rebuilds.TryGetValue((renameColumnOperation.Table, renameColumnOperation.Schema), out var rebuild))
                    {
                        if (rebuild.DropColumnsDeferred.Contains(renameColumnOperation.NewName))
                        {
                            rebuild.OperationsToReplace.Add(renameColumnOperation);
                            rebuild.DropColumnsDeferred.Add(renameColumnOperation.Name);
                            rebuild.RenameColumnsDeferred.Add(renameColumnOperation.NewName, renameColumnOperation);
                        }
                    }

                    operations.Add(renameColumnOperation);

                    break;
                }

                case RenameTableOperation renameTableOperation:
                {
                    if (rebuilds.Remove((renameTableOperation.Name, renameTableOperation.Schema), out var rebuild))
                    {
                        rebuilds.Add(
                            (renameTableOperation.NewName ?? renameTableOperation.Name, renameTableOperation.NewSchema), rebuild);
                    }

                    operations.Add(renameTableOperation);

                    break;
                }

                case CreateTableOperation:
                case AlterSequenceOperation:
                case CreateSequenceOperation:
                case DropIndexOperation:
                case DropSchemaOperation:
                case DropSequenceOperation:
                case DropTableOperation:
                case EnsureSchemaOperation:
                case RenameSequenceOperation:
                case RestartSequenceOperation:
                {
                    operations.Add(operation);

                    break;
                }

                case DeleteDataOperation:
                case InsertDataOperation:
                case UpdateDataOperation:
                {
                    var tableOperation = (ITableMigrationOperation)operation;
                    if (rebuilds.TryGetValue((tableOperation.Table, tableOperation.Schema), out var rebuild))
                    {
                        rebuild.OperationsToWarnFor.Add(operation);
                    }

                    operations.Add(operation);

                    break;
                }

                default:
                {
                    foreach (var rebuild in rebuilds.Values)
                    {
                        rebuild.OperationsToWarnFor.Add(operation);
                    }

                    operations.Add(operation);

                    break;
                }
            }
        }

        var skippedRebuilds = new List<(string Table, string? Schema)>();
        var indexesToRebuild = new List<ITableIndex>();
        foreach (var (key, rebuildContext) in rebuilds)
        {
            var table = model?.GetRelationalModel().FindTable(key.Table, key.Schema);
            if (table == null)
            {
                skippedRebuilds.Add(key);

                continue;
            }

            foreach (var operationToWarnFor in rebuildContext.OperationsToWarnFor)
            {
                // TODO: Log warning
            }

            foreach (var operationToReplace in rebuildContext.OperationsToReplace)
            {
                operations.Remove(operationToReplace);
            }

            var createTableOperation = new CreateTableOperation
            {
                Name = "ef_temp_" + table.Name,
                Schema = table.Schema,
                Comment = table.Comment
            };

            var primaryKey = table.PrimaryKey;
            if (primaryKey != null)
            {
                createTableOperation.PrimaryKey = AddPrimaryKeyOperation.CreateFrom(primaryKey);
            }

            foreach (var column in table.Columns.Where(c => c.Order.HasValue).OrderBy(c => c.Order!.Value)
                         .Concat(table.Columns.Where(c => !c.Order.HasValue)))
            {
                if (!column.TryGetDefaultValue(out var defaultValue))
                {
                    defaultValue = null;
                }

                var addColumnOperation = new AddColumnOperation
                {
                    Name = column.Name,
                    ColumnType = column.StoreType,
                    IsNullable = column.IsNullable,
                    DefaultValue = rebuildContext.AddColumnsDeferred.TryGetValue(column.Name, out var originalOperation)
                        && !originalOperation.IsNullable
                            ? originalOperation.DefaultValue
                            : defaultValue,
                    DefaultValueSql = column.DefaultValueSql,
                    ComputedColumnSql = column.ComputedColumnSql,
                    IsStored = column.IsStored,
                    Comment = column.Comment,
                    Collation = column.Collation,
                    Table = createTableOperation.Name
                };
                addColumnOperation.AddAnnotations(column.GetAnnotations());
                createTableOperation.Columns.Add(addColumnOperation);
            }

            foreach (var foreignKey in table.ForeignKeyConstraints)
            {
                createTableOperation.ForeignKeys.Add(AddForeignKeyOperation.CreateFrom(foreignKey));
            }

            foreach (var uniqueConstraint in table.UniqueConstraints.Where(c => !c.GetIsPrimaryKey()))
            {
                createTableOperation.UniqueConstraints.Add(AddUniqueConstraintOperation.CreateFrom(uniqueConstraint));
            }

            foreach (var checkConstraint in table.CheckConstraints)
            {
                createTableOperation.CheckConstraints.Add(AddCheckConstraintOperation.CreateFrom(checkConstraint));
            }

            createTableOperation.AddAnnotations(table.GetAnnotations());
            operations.Add(createTableOperation);

            foreach (var index in table.Indexes)
            {
                if (index.IsUnique && rebuildContext.CreateIndexesDeferred.Contains(index.Name))
                {
                    var createIndexOperation = CreateIndexOperation.CreateFrom(index);
                    createIndexOperation.Table = createTableOperation.Name;
                    operations.Add(createIndexOperation);
                }
                else
                {
                    indexesToRebuild.Add(index);
                }
            }

            var intoBuilder = new StringBuilder();
            var selectBuilder = new StringBuilder();
            var first = true;
            foreach (var column in table.Columns)
            {
                if (column.ComputedColumnSql != null
                    || rebuildContext.AddColumnsDeferred.ContainsKey(column.Name))
                {
                    continue;
                }

                if (first)
                {
                    first = false;
                }
                else
                {
                    intoBuilder.Append(", ");
                    selectBuilder.Append(", ");
                }

                intoBuilder.Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(column.Name));

                var defaultValue = rebuildContext.AlterColumnsDeferred.TryGetValue(column.Name, out var alterColumnOperation)
                    && alterColumnOperation is { IsNullable: false, OldColumn.IsNullable: true }
                        ? alterColumnOperation.DefaultValue
                        : null;
                if (defaultValue != null)
                {
                    selectBuilder.Append("IFNULL(");
                }

                selectBuilder.Append(
                    Dependencies.SqlGenerationHelper.DelimitIdentifier(
                        rebuildContext.RenameColumnsDeferred.TryGetValue(column.Name, out var renameColumnOperation)
                            ? renameColumnOperation.Name
                            : column.Name));

                if (defaultValue != null)
                {
                    var defaultValueTypeMapping = (column.StoreType == null
                            ? null
                            : Dependencies.TypeMappingSource.FindMapping(defaultValue.GetType(), column.StoreType))
                        ?? Dependencies.TypeMappingSource.GetMappingForValue(defaultValue);

                    selectBuilder
                        .Append(", ")
                        .Append(defaultValueTypeMapping.GenerateSqlLiteral(defaultValue))
                        .Append(')');
                }
            }

            operations.Add(
                new SqlOperation
                {
                    Sql = new StringBuilder()
                        .Append("INSERT INTO ")
                        .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(createTableOperation.Name))
                        .Append(" (")
                        .Append(intoBuilder)
                        .AppendLine(")")
                        .Append("SELECT ")
                        .Append(selectBuilder)
                        .AppendLine()
                        .Append("FROM ")
                        .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(table.Name))
                        .Append(Dependencies.SqlGenerationHelper.StatementTerminator)
                        .ToString()
                });
        }

        foreach (var skippedRebuild in skippedRebuilds)
        {
            rebuilds.Remove(skippedRebuild);
        }

        if (rebuilds.Any())
        {
            operations.Add(
                new SqlOperation { Sql = "PRAGMA foreign_keys = 0;", SuppressTransaction = true });
        }

        foreach (var ((table, schema), _) in rebuilds)
        {
            operations.Add(
                new DropTableOperation { Name = table, Schema = schema });
            operations.Add(
                new RenameTableOperation
                {
                    Name = "ef_temp_" + table,
                    Schema = schema,
                    NewName = table,
                    NewSchema = schema
                });
        }

        if (rebuilds.Any())
        {
            operations.Add(
                new SqlOperation { Sql = "PRAGMA foreign_keys = 1;", SuppressTransaction = true });
        }

        foreach (var index in indexesToRebuild)
        {
            operations.Add(CreateIndexOperation.CreateFrom(index));
        }

        return operations;
    }

    protected override void Generate(AlterDatabaseOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        builder
            .Append("SELECT InitSpatialMetaData()")
            .AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
        EndStatement(builder);
    }

    protected override void Generate(
        DropIndexOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate)
    {
        builder
            .Append("DROP INDEX ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name));

        if (terminate)
        {
            builder
                .AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator)
                .EndCommand();
        }
    }

    protected override void Generate(RenameIndexOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw new NotSupportedException(
            IgniteStrings.InvalidMigrationOperation(operation.GetType().ShortDisplayName()));

    protected override void Generate(RenameTableOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        if (operation.NewName != null
            && operation.NewName != operation.Name)
        {
            builder
                .Append("ALTER TABLE ")
                .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name))
                .Append(" RENAME TO ")
                .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.NewName))
                .AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator)
                .EndCommand();
        }
    }

    protected override void Generate(RenameColumnOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => builder
            .Append("ALTER TABLE ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Table))
            .Append(" RENAME COLUMN ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name))
            .Append(" TO ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.NewName))
            .AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator)
            .EndCommand();

    protected override void Generate(
        CreateTableOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        builder
            .Append("CREATE TABLE ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name, operation.Schema))
            .AppendLine(" (");

        using (builder.Indent())
        {
            if (!string.IsNullOrEmpty(operation.Comment))
            {
                builder
                    .AppendLines(Dependencies.SqlGenerationHelper.GenerateComment(operation.Comment))
                    .AppendLine();
            }

            CreateTableColumns(operation, model, builder);
            CreateTableConstraints(operation, model, builder);
            builder.AppendLine();
        }

        builder.Append(")");

        if (terminate)
        {
            builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
            EndStatement(builder);
        }
    }

    protected override void CreateTableColumns(
        CreateTableOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        if (operation.Columns.All(c => string.IsNullOrEmpty(c.Comment)))
        {
            base.CreateTableColumns(operation, model, builder);
        }
        else
        {
            CreateTableColumnsWithComments(operation, model, builder);
        }
    }

    private void CreateTableColumnsWithComments(
        CreateTableOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        for (var i = 0; i < operation.Columns.Count; i++)
        {
            var column = operation.Columns[i];

            if (i > 0)
            {
                builder.AppendLine();
            }

            if (!string.IsNullOrEmpty(column.Comment))
            {
                builder.AppendLines(Dependencies.SqlGenerationHelper.GenerateComment(column.Comment));
            }

            ColumnDefinition(column, model, builder);

            if (i != operation.Columns.Count - 1)
            {
                builder.AppendLine(",");
            }
        }
    }

    protected override void Generate(
        AddForeignKeyOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
        => throw new NotSupportedException(
            IgniteStrings.InvalidMigrationOperation(operation.GetType().ShortDisplayName()));

    protected override void Generate(
        AddPrimaryKeyOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
        => throw new NotSupportedException(
            IgniteStrings.InvalidMigrationOperation(operation.GetType().ShortDisplayName()));

    protected override void Generate(AddUniqueConstraintOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw new NotSupportedException(
            IgniteStrings.InvalidMigrationOperation(operation.GetType().ShortDisplayName()));

    protected override void Generate(AddCheckConstraintOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw new NotSupportedException(
            IgniteStrings.InvalidMigrationOperation(operation.GetType().ShortDisplayName()));

    protected override void Generate(
        DropColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
        => throw new NotSupportedException(
            IgniteStrings.InvalidMigrationOperation(operation.GetType().ShortDisplayName()));

    protected override void Generate(
        DropForeignKeyOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
        => throw new NotSupportedException(
            IgniteStrings.InvalidMigrationOperation(operation.GetType().ShortDisplayName()));

    protected override void Generate(
        DropPrimaryKeyOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
        => throw new NotSupportedException(
            IgniteStrings.InvalidMigrationOperation(operation.GetType().ShortDisplayName()));

    protected override void Generate(DropUniqueConstraintOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw new NotSupportedException(
            IgniteStrings.InvalidMigrationOperation(operation.GetType().ShortDisplayName()));

    protected override void Generate(DropCheckConstraintOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw new NotSupportedException(
            IgniteStrings.InvalidMigrationOperation(operation.GetType().ShortDisplayName()));

    protected override void Generate(AlterColumnOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw new NotSupportedException(
            IgniteStrings.InvalidMigrationOperation(operation.GetType().ShortDisplayName()));

    protected override void ComputedColumnDefinition(
        string? schema,
        string table,
        string name,
        ColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        builder.Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(name));

        builder
            .Append(" AS (")
            .Append(operation.ComputedColumnSql!)
            .Append(")");

        if (operation.IsStored == true)
        {
            builder.Append(" STORED");
        }

        if (operation.Collation != null)
        {
            builder
                .Append(" COLLATE ")
                .Append(operation.Collation);
        }
    }

    protected override void Generate(EnsureSchemaOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
    }

    protected override void Generate(DropSchemaOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
    }

    protected override void Generate(RestartSequenceOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw new NotSupportedException(IgniteStrings.SequencesNotSupported);

    protected override void Generate(CreateSequenceOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw new NotSupportedException(IgniteStrings.SequencesNotSupported);

    protected override void Generate(RenameSequenceOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw new NotSupportedException(IgniteStrings.SequencesNotSupported);

    protected override void Generate(AlterSequenceOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw new NotSupportedException(IgniteStrings.SequencesNotSupported);

    protected override void Generate(DropSequenceOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw new NotSupportedException(IgniteStrings.SequencesNotSupported);

    protected override void PrimaryKeyConstraint(AddPrimaryKeyOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        // Ignite-specific: no constraints.
        // if (operation.Name != null)
        // {
        //     builder
        //         .Append("CONSTRAINT ")
        //         .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name))
        //         .Append(" ");
        // }
        builder
            .Append("PRIMARY KEY ");

        IndexTraits(operation, model, builder);

        builder.Append("(")
            .Append(ColumnList(operation.Columns))
            .Append(")");
    }

    protected override void ForeignKeyConstraint(AddForeignKeyOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        // Ignite-specific: no constraints.
        // No-op.
    }

    protected override void CreateTableForeignKeys(CreateTableOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        // Ignite-specific: no constraints.
        // No-op.
    }

    [SuppressMessage("StyleCop.CSharp.MaintainabilityRules", "SA1401:Fields should be private", Justification = "Private class.")]
    private sealed class RebuildContext
    {
        public readonly IDictionary<string, AlterColumnOperation> AlterColumnsDeferred = new Dictionary<string, AlterColumnOperation>();

        public readonly IDictionary<string, RenameColumnOperation> RenameColumnsDeferred = new Dictionary<string, RenameColumnOperation>();

        public ICollection<MigrationOperation> OperationsToReplace { get; } = new List<MigrationOperation>();

        public IDictionary<string, AddColumnOperation> AddColumnsDeferred { get; } = new Dictionary<string, AddColumnOperation>();

        public ICollection<string> DropColumnsDeferred { get; } = new HashSet<string>();

        public ICollection<string> CreateIndexesDeferred { get; } = new HashSet<string>();

        public ICollection<MigrationOperation> OperationsToWarnFor { get; } = new List<MigrationOperation>();
    }
}
