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

namespace Apache.Ignite.EntityFrameworkCore.Storage.Internal;

using System.Text;
using Microsoft.EntityFrameworkCore.Storage;

public class IgniteSqlGenerationHelper : RelationalSqlGenerationHelper
{
    public IgniteSqlGenerationHelper(RelationalSqlGenerationHelperDependencies dependencies)
        : base(dependencies)
    {
    }

    public override string StartTransactionStatement
        => "BEGIN TRANSACTION" + StatementTerminator;

    public override string DelimitIdentifier(string name, string? schema)
        => base.DelimitIdentifier(name);

    public override void DelimitIdentifier(StringBuilder builder, string name, string? schema)
        => base.DelimitIdentifier(builder, name);

    public override string GenerateParameterName(string name) => "?";

    public override void GenerateParameterName(StringBuilder builder, string name) => builder.Append('?');

    public override string GenerateParameterNamePlaceholder(string name)
    {
        // TODO: ??
        return base.GenerateParameterNamePlaceholder(name);
    }

    public override void GenerateParameterNamePlaceholder(StringBuilder builder, string name)
    {
        // TODO: ??
        base.GenerateParameterNamePlaceholder(builder, name);
    }
}
