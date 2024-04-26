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

namespace Apache.Ignite.EntityFrameworkCore.Query.Internal;

using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

public class IgniteCharMethodTranslator : IMethodCallTranslator
{
    private static readonly Dictionary<MethodInfo, string> SupportedMethods = new()
    {
        { typeof(char).GetRuntimeMethod(nameof(char.ToLower), new[] { typeof(char) })!, "lower" },
        { typeof(char).GetRuntimeMethod(nameof(char.ToUpper), new[] { typeof(char) })!, "upper" }
    };

    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    public IgniteCharMethodTranslator(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public virtual SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (SupportedMethods.TryGetValue(method, out var sqlFunctionName))
        {
            return _sqlExpressionFactory.Function(
                sqlFunctionName,
                arguments,
                nullable: true,
                argumentsPropagateNullability: arguments.Select(_ => true).ToList(),
                method.ReturnType,
                arguments[0].TypeMapping);
        }

        return null;
    }
}
