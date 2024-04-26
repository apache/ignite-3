
namespace Apache.Ignite.EntityFrameworkCore.Query.Internal;

using System;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

public class IgniteStringLengthTranslator : IMemberTranslator
{
    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    public IgniteStringLengthTranslator(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public virtual SqlExpression? Translate(
        SqlExpression? instance,
        MemberInfo member,
        Type returnType,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
        => instance?.Type == typeof(string)
            && member.Name == nameof(string.Length)
                ? _sqlExpressionFactory.Function(
                    "length",
                    new[] { instance },
                    nullable: true,
                    argumentsPropagateNullability: new[] { true },
                    returnType)
                : null;
}
