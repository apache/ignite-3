
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
