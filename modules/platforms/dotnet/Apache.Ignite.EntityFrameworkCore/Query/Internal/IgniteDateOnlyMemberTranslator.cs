
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
