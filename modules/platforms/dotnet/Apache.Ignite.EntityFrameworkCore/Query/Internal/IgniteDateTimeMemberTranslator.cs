
namespace Apache.Ignite.EntityFrameworkCore.Query.Internal;

using System;
using System.Collections.Generic;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

public class IgniteDateTimeMemberTranslator : IMemberTranslator
{
    private static readonly Dictionary<string, string> DatePartMapping
        = new()
        {
            { nameof(DateTime.Year), "%Y" },
            { nameof(DateTime.Month), "%m" },
            { nameof(DateTime.DayOfYear), "%j" },
            { nameof(DateTime.Day), "%d" },
            { nameof(DateTime.Hour), "%H" },
            { nameof(DateTime.Minute), "%M" },
            { nameof(DateTime.Second), "%S" },
            { nameof(DateTime.DayOfWeek), "%w" }
        };

    private readonly IgniteSqlExpressionFactory _sqlExpressionFactory;

    public IgniteDateTimeMemberTranslator(IgniteSqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public virtual SqlExpression? Translate(
        SqlExpression? instance,
        MemberInfo member,
        Type returnType,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (member.DeclaringType == typeof(DateTime))
        {
            var memberName = member.Name;

            if (DatePartMapping.TryGetValue(memberName, out var datePart))
            {
                return _sqlExpressionFactory.Convert(
                    _sqlExpressionFactory.Strftime(
                        typeof(string),
                        datePart,
                        instance!),
                    returnType);
            }

            if (memberName == nameof(DateTime.Ticks))
            {
                return _sqlExpressionFactory.Convert(
                    _sqlExpressionFactory.Multiply(
                        _sqlExpressionFactory.Subtract(
                            _sqlExpressionFactory.Function(
                                "julianday",
                                new[] { instance! },
                                nullable: true,
                                argumentsPropagateNullability: new[] { true },
                                typeof(double)),
                            _sqlExpressionFactory.Constant(1721425.5)), // NB: Result of julianday('0001-01-01 00:00:00')
                        _sqlExpressionFactory.Constant(TimeSpan.TicksPerDay)),
                    typeof(long));
            }

            if (memberName == nameof(DateTime.Millisecond))
            {
                return _sqlExpressionFactory.Modulo(
                    _sqlExpressionFactory.Multiply(
                        _sqlExpressionFactory.Convert(
                            _sqlExpressionFactory.Strftime(
                                typeof(string),
                                "%f",
                                instance!),
                            typeof(double)),
                        _sqlExpressionFactory.Constant(1000)),
                    _sqlExpressionFactory.Constant(1000));
            }

            var format = "%Y-%m-%d %H:%M:%f";
            SqlExpression timestring;
            var modifiers = new List<SqlExpression>();

            switch (memberName)
            {
                case nameof(DateTime.Now):
                    timestring = _sqlExpressionFactory.Constant("now");
                    modifiers.Add(_sqlExpressionFactory.Constant("localtime"));
                    break;

                case nameof(DateTime.UtcNow):
                    timestring = _sqlExpressionFactory.Constant("now");
                    break;

                case nameof(DateTime.Date):
                    timestring = instance!;
                    modifiers.Add(_sqlExpressionFactory.Constant("start of day"));
                    break;

                case nameof(DateTime.Today):
                    timestring = _sqlExpressionFactory.Constant("now");
                    modifiers.Add(_sqlExpressionFactory.Constant("localtime"));
                    modifiers.Add(_sqlExpressionFactory.Constant("start of day"));
                    break;

                case nameof(DateTime.TimeOfDay):
                    format = "%H:%M:%f";
                    timestring = instance!;
                    break;

                default:
                    return null;
            }

            return _sqlExpressionFactory.Function(
                "rtrim",
                new SqlExpression[]
                {
                    _sqlExpressionFactory.Function(
                        "rtrim",
                        new SqlExpression[]
                        {
                            _sqlExpressionFactory.Strftime(
                                returnType,
                                format,
                                timestring,
                                modifiers),
                            _sqlExpressionFactory.Constant("0")
                        },
                        nullable: true,
                        argumentsPropagateNullability: new[] { true, false },
                        returnType),
                    _sqlExpressionFactory.Constant(".")
                },
                nullable: true,
                argumentsPropagateNullability: new[] { true, false },
                returnType);
        }

        return null;
    }
}
