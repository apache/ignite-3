
namespace Apache.Ignite.EntityFrameworkCore.Query.Internal;

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

public class IgniteSqlExpressionFactory : SqlExpressionFactory
{
    public IgniteSqlExpressionFactory(SqlExpressionFactoryDependencies dependencies)
        : base(dependencies)
    {
    }

    public virtual SqlFunctionExpression Strftime(
        Type returnType,
        string format,
        SqlExpression timestring,
        IEnumerable<SqlExpression>? modifiers = null,
        RelationalTypeMapping? typeMapping = null)
    {
        modifiers ??= Enumerable.Empty<SqlExpression>();

        // If the inner call is another strftime then shortcut a double call
        if (timestring is SqlFunctionExpression { Name: "rtrim" } rtrimFunction
            && rtrimFunction.Arguments!.Count == 2
            && rtrimFunction.Arguments[0] is SqlFunctionExpression { Name: "rtrim" } rtrimFunction2
            && rtrimFunction2.Arguments!.Count == 2
            && rtrimFunction2.Arguments[0] is SqlFunctionExpression { Name: "strftime" } strftimeFunction
            && strftimeFunction.Arguments!.Count > 1)
        {
            // Use its timestring parameter directly in place of ours
            timestring = strftimeFunction.Arguments[1];

            // Prepend its modifier arguments (if any) to the current call
            modifiers = strftimeFunction.Arguments.Skip(2).Concat(modifiers);
        }

        if (timestring is SqlFunctionExpression { Name: "date" } dateFunction)
        {
            timestring = dateFunction.Arguments![0];
            modifiers = dateFunction.Arguments.Skip(1).Concat(modifiers);
        }

        var finalArguments = new[] { Constant(format), timestring }.Concat(modifiers);

        return Function(
            "strftime",
            finalArguments,
            nullable: true,
            argumentsPropagateNullability: finalArguments.Select(_ => true),
            returnType,
            typeMapping);
    }

    public virtual SqlFunctionExpression Date(
        Type returnType,
        SqlExpression timestring,
        IEnumerable<SqlExpression>? modifiers = null,
        RelationalTypeMapping? typeMapping = null)
    {
        modifiers ??= Enumerable.Empty<SqlExpression>();

        if (timestring is SqlFunctionExpression { Name: "date" } dateFunction)
        {
            timestring = dateFunction.Arguments![0];
            modifiers = dateFunction.Arguments.Skip(1).Concat(modifiers);
        }

        var finalArguments = new[] { timestring }.Concat(modifiers);

        return Function(
            "date",
            finalArguments,
            nullable: true,
            argumentsPropagateNullability: finalArguments.Select(_ => true),
            returnType,
            typeMapping);
    }
}
