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
