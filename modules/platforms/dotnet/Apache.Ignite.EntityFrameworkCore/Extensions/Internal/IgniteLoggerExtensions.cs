namespace Apache.Ignite.EntityFrameworkCore.Extensions.Internal;

using System;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;

public static class IgniteLoggerExtensions
{
    public static void ForeignKeyReferencesMissingTableWarning(
        this IDiagnosticsLogger<DbLoggerCategory.Scaffolding> diagnostics,
        string? id,
        string? tableName,
        string? principalTableName)
    {
    }

    public static void ForeignKeyPrincipalColumnMissingWarning(
        this IDiagnosticsLogger<DbLoggerCategory.Scaffolding> diagnostics,
        string? foreignKeyName,
        string? tableName,
        string? principalColumnName,
        string? principalTableName)
    {
    }

    public static void IndexFound(
        this IDiagnosticsLogger<DbLoggerCategory.Scaffolding> diagnostics,
        string? indexName,
        string? tableName,
        bool? unique)
    {
    }

    public static void ForeignKeyFound(
        this IDiagnosticsLogger<DbLoggerCategory.Scaffolding> diagnostics,
        string? tableName,
        long id,
        string? principalTableName,
        string? deleteAction)
    {
    }

    public static void PrimaryKeyFound(
        this IDiagnosticsLogger<DbLoggerCategory.Scaffolding> diagnostics,
        string? primaryKeyName,
        string? tableName)
    {
    }

    public static void UniqueConstraintFound(
        this IDiagnosticsLogger<DbLoggerCategory.Scaffolding> diagnostics,
        string? uniqueConstraintName,
        string? tableName)
    {
    }

    public static void UnexpectedConnectionTypeWarning(
        this IDiagnosticsLogger<DbLoggerCategory.Infrastructure> diagnostics,
        Type connectionType)
    {
    }

    public static void OutOfRangeWarning(
        this IDiagnosticsLogger<DbLoggerCategory.Scaffolding> diagnostics,
        string? columnName,
        string? tableName,
        string? type)
    {
    }

    public static void FormatWarning(
        this IDiagnosticsLogger<DbLoggerCategory.Scaffolding> diagnostics,
        string? columnName,
        string? tableName,
        string? type)
    {
    }
}
