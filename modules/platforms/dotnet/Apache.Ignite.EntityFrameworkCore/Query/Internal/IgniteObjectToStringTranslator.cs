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

public class IgniteObjectToStringTranslator : IMethodCallTranslator
{
    private static readonly HashSet<Type> TypeMapping = new()
    {
        typeof(bool),
        typeof(byte),
        typeof(byte[]),
        typeof(char),
        typeof(DateOnly),
        typeof(DateTime),
        typeof(DateTimeOffset),
        typeof(decimal),
        typeof(double),
        typeof(float),
        typeof(Guid),
        typeof(int),
        typeof(long),
        typeof(sbyte),
        typeof(short),
        typeof(TimeOnly),
        typeof(TimeSpan),
        typeof(uint),
        typeof(ushort)
    };

    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    public IgniteObjectToStringTranslator(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public virtual SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (instance == null || method.Name != nameof(ToString) || arguments.Count != 0)
        {
            return null;
        }

        if (instance.TypeMapping?.ClrType == typeof(string))
        {
            return instance;
        }

        if (instance.Type == typeof(bool))
        {
            if (instance is ColumnExpression { IsNullable: true })
            {
                return _sqlExpressionFactory.Case(
                    new[]
                    {
                        new CaseWhenClause(
                            _sqlExpressionFactory.Equal(instance, _sqlExpressionFactory.Constant(false)),
                            _sqlExpressionFactory.Constant(false.ToString())),
                        new CaseWhenClause(
                            _sqlExpressionFactory.Equal(instance, _sqlExpressionFactory.Constant(true)),
                            _sqlExpressionFactory.Constant(true.ToString()))
                    },
                    _sqlExpressionFactory.Constant(null));
            }

            return _sqlExpressionFactory.Case(
                new[]
                {
                    new CaseWhenClause(
                        _sqlExpressionFactory.Equal(instance, _sqlExpressionFactory.Constant(false)),
                        _sqlExpressionFactory.Constant(false.ToString()))
                },
                _sqlExpressionFactory.Constant(true.ToString()));
        }

        return TypeMapping.Contains(instance.Type)
            ? _sqlExpressionFactory.Convert(instance, typeof(string))
            : null;
    }
}
