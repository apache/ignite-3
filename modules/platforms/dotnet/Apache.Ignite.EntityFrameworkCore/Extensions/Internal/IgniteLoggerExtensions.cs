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
