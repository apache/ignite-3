
namespace Apache.Ignite.EntityFrameworkCore.Query.Internal;

using System;
using System.Collections.Generic;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

public class IgniteDateTimeMethodTranslator : IMethodCallTranslator
{
    private static readonly MethodInfo AddMilliseconds
        = typeof(DateTime).GetRuntimeMethod(nameof(DateTime.AddMilliseconds), new[] { typeof(double) })!;

    private static readonly MethodInfo AddTicks
        = typeof(DateTime).GetRuntimeMethod(nameof(DateTime.AddTicks), new[] { typeof(long) })!;

    private readonly Dictionary<MethodInfo, string> _methodInfoToUnitSuffix = new()
    {
        { typeof(DateTime).GetRuntimeMethod(nameof(DateTime.AddYears), new[] { typeof(int) })!, " years" },
        { typeof(DateTime).GetRuntimeMethod(nameof(DateTime.AddMonths), new[] { typeof(int) })!, " months" },
        { typeof(DateTime).GetRuntimeMethod(nameof(DateTime.AddDays), new[] { typeof(double) })!, " days" },
        { typeof(DateTime).GetRuntimeMethod(nameof(DateTime.AddHours), new[] { typeof(double) })!, " hours" },
        { typeof(DateTime).GetRuntimeMethod(nameof(DateTime.AddMinutes), new[] { typeof(double) })!, " minutes" },
        { typeof(DateTime).GetRuntimeMethod(nameof(DateTime.AddSeconds), new[] { typeof(double) })!, " seconds" },
        { typeof(DateOnly).GetRuntimeMethod(nameof(DateOnly.AddYears), new[] { typeof(int) })!, " years" },
        { typeof(DateOnly).GetRuntimeMethod(nameof(DateOnly.AddMonths), new[] { typeof(int) })!, " months" },
        { typeof(DateOnly).GetRuntimeMethod(nameof(DateOnly.AddDays), new[] { typeof(int) })!, " days" }
    };

    private readonly IgniteSqlExpressionFactory _sqlExpressionFactory;

    public IgniteDateTimeMethodTranslator(IgniteSqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public virtual SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
        => method.DeclaringType == typeof(DateTime)
            ? TranslateDateTime(instance, method, arguments)
            : method.DeclaringType == typeof(DateOnly)
                ? TranslateDateOnly(instance, method, arguments)
                : null;

    private SqlExpression? TranslateDateTime(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments)
    {
        SqlExpression? modifier = null;
        if (AddMilliseconds.Equals(method))
        {
            modifier = _sqlExpressionFactory.Add(
                _sqlExpressionFactory.Convert(
                    _sqlExpressionFactory.Divide(
                        arguments[0],
                        _sqlExpressionFactory.Constant(1000.0)),
                    typeof(string)),
                _sqlExpressionFactory.Constant(" seconds"));
        }
        else if (AddTicks.Equals(method))
        {
            modifier = _sqlExpressionFactory.Add(
                _sqlExpressionFactory.Convert(
                    _sqlExpressionFactory.Divide(
                        arguments[0],
                        _sqlExpressionFactory.Constant((double)TimeSpan.TicksPerSecond)),
                    typeof(string)),
                _sqlExpressionFactory.Constant(" seconds"));
        }
        else if (_methodInfoToUnitSuffix.TryGetValue(method, out var unitSuffix))
        {
            modifier = _sqlExpressionFactory.Add(
                _sqlExpressionFactory.Convert(arguments[0], typeof(string)),
                _sqlExpressionFactory.Constant(unitSuffix));
        }

        if (modifier != null)
        {
            return _sqlExpressionFactory.Function(
                "rtrim",
                new SqlExpression[]
                {
                    _sqlExpressionFactory.Function(
                        "rtrim",
                        new SqlExpression[]
                        {
                            _sqlExpressionFactory.Strftime(
                                method.ReturnType,
                                "%Y-%m-%d %H:%M:%f",
                                instance!,
                                new[] { modifier }),
                            _sqlExpressionFactory.Constant("0")
                        },
                        nullable: true,
                        argumentsPropagateNullability: new[] { true, false },
                        method.ReturnType),
                    _sqlExpressionFactory.Constant(".")
                },
                nullable: true,
                argumentsPropagateNullability: new[] { true, false },
                method.ReturnType);
        }

        return null;
    }

    private SqlExpression? TranslateDateOnly(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments)
    {
        if (instance is not null && _methodInfoToUnitSuffix.TryGetValue(method, out var unitSuffix))
        {
            return _sqlExpressionFactory.Date(
                method.ReturnType,
                instance,
                new[]
                {
                    _sqlExpressionFactory.Add(
                        _sqlExpressionFactory.Convert(arguments[0], typeof(string)),
                        _sqlExpressionFactory.Constant(unitSuffix))
                });
        }

        return null;
    }
}
