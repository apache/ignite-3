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

using System;
using System.Collections.Generic;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

public class IgniteDateOnlyMemberTranslator : IMemberTranslator
{
    private static readonly Dictionary<string, string> DatePartMapping
        = new()
        {
            { nameof(DateOnly.Year), "%Y" },
            { nameof(DateOnly.Month), "%m" },
            { nameof(DateOnly.DayOfYear), "%j" },
            { nameof(DateOnly.Day), "%d" },
            { nameof(DateOnly.DayOfWeek), "%w" }
        };

    private readonly IgniteSqlExpressionFactory _sqlExpressionFactory;

    public IgniteDateOnlyMemberTranslator(IgniteSqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public virtual SqlExpression? Translate(
        SqlExpression? instance,
        MemberInfo member,
        Type returnType,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
        => member.DeclaringType == typeof(DateOnly) && DatePartMapping.TryGetValue(member.Name, out var datePart)
            ? _sqlExpressionFactory.Convert(
                _sqlExpressionFactory.Strftime(
                    typeof(string),
                    datePart,
                    instance!),
                returnType)
            : null;
}
