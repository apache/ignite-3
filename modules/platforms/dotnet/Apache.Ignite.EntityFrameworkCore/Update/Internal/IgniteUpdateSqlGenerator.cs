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

namespace Apache.Ignite.EntityFrameworkCore.Update.Internal;

using System;
using System.Text;
using Common;
using Microsoft.EntityFrameworkCore.Update;

public class IgniteUpdateSqlGenerator : UpdateAndSelectSqlGenerator
{
    public IgniteUpdateSqlGenerator(UpdateSqlGeneratorDependencies dependencies)
        : base(dependencies)
    {
    }

    protected override void AppendIdentityWhereCondition(StringBuilder commandStringBuilder, IColumnModification columnModification)
    {
        Check.NotNull(commandStringBuilder, nameof(commandStringBuilder));
        Check.NotNull(columnModification, nameof(columnModification));

        // SqlGenerationHelper.DelimitIdentifier(commandStringBuilder, "rowid");
        // commandStringBuilder.Append(" = ").Append("last_insert_rowid()");
        throw new NotSupportedException("Ignite does not support identity columns.");
    }

    protected override ResultSetMapping AppendSelectAffectedCountCommand(
        StringBuilder commandStringBuilder,
        string name,
        string? schema,
        int commandPosition)
    {
        // Ignite-specific: no-op, affected rows in ResultSet.
        return ResultSetMapping.NoResults;
    }

    protected override void AppendRowsAffectedWhereCondition(StringBuilder commandStringBuilder, int expectedRowsAffected)
    {
        Check.NotNull(commandStringBuilder, nameof(commandStringBuilder));

        throw new NotSupportedException("Ignite does not support affected rows check.");
    }

    public override string GenerateNextSequenceValueOperation(string name, string? schema)
        => throw new NotSupportedException(IgniteStrings.SequencesNotSupported);
}
